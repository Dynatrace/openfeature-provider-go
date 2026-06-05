package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	// DefaultMetricsFlushInterval is the periodic flush cadence.
	DefaultMetricsFlushInterval = 30 * time.Second
	// metricsHTTPTimeout bounds a single metrics delivery.
	metricsHTTPTimeout = 10 * time.Second
	// failedFlushCooldown suppresses capacity-triggered flushes after a delivery failure.
	failedFlushCooldown = 30 * time.Second
	// maxSealedBatches bounds the queue of version-sealed batches awaiting delivery.
	maxSealedBatches = 10
	// sdkTypeServer is the sdkType reported in the payload.
	sdkTypeServer = "server"
)

// MetricsFlusher periodically delivers aggregated evaluation metrics to the Dynatrace ingest
// endpoint. All HTTP runs on a single dedicated goroutine, mirroring PollingManager; the
// evaluation path never blocks on delivery.
//
// Four triggers cause a flush: the periodic ticker, a capacity callback (OnRecord) once the
// aggregator exceeds the soft cap, a config rotation (EnqueueSealedBatch + TriggerFlush), and a
// final synchronous flush on Stop.
type MetricsFlusher struct {
	aggregator    *MetricsAggregator
	httpClient    *http.Client
	metricsURL    string
	authHeader    string
	flushInterval time.Duration
	softCap       int
	sdkVersion    string
	providerName  string

	trigger chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu            sync.Mutex
	sealedBatches []DrainedBatch
	cooldownUntil time.Time
	started       bool
	stopped       bool

	// clock is injectable for deterministic cooldown tests (defaults to time.Now).
	clock func() time.Time
}

// NewMetricsFlusher constructs a flusher. A nil httpClient gets a default client with a 10s
// timeout; an injected client is reused as-is (and never owned). A non-positive flushInterval
// falls back to DefaultMetricsFlushInterval.
func NewMetricsFlusher(
	aggregator *MetricsAggregator,
	httpClient *http.Client,
	sdkKey, metricsURL string,
	flushInterval time.Duration,
	softCap int,
	sdkVersion, providerName string,
) *MetricsFlusher {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: metricsHTTPTimeout}
	}
	if flushInterval <= 0 {
		flushInterval = DefaultMetricsFlushInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &MetricsFlusher{
		aggregator:    aggregator,
		httpClient:    httpClient,
		metricsURL:    metricsURL,
		authHeader:    "Bearer " + sdkKey,
		flushInterval: flushInterval,
		softCap:       softCap,
		sdkVersion:    sdkVersion,
		providerName:  providerName,
		trigger:       make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		clock:         time.Now,
	}
}

// Start launches the flush goroutine. Idempotent; a no-op once stopped.
func (f *MetricsFlusher) Start() {
	f.mu.Lock()
	if f.started || f.stopped {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.mu.Unlock()

	f.wg.Add(1)
	go f.run()
}

// Stop halts periodic flushing, waits for the goroutine to exit, then performs one final
// synchronous flush. Idempotent.
func (f *MetricsFlusher) Stop() {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return
	}
	f.stopped = true
	started := f.started
	f.mu.Unlock()

	f.cancel()
	if started {
		f.wg.Wait()
	}
	f.flush()
}

func (f *MetricsFlusher) run() {
	defer f.wg.Done()
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.flush()
		case <-f.trigger:
			f.flush()
		case <-f.ctx.Done():
			return
		}
	}
}

// OnRecord is the capacity callback invoked by the hook after each record. It schedules a
// coalesced out-of-band flush when the aggregator exceeds the soft cap and no cooldown is active.
func (f *MetricsFlusher) OnRecord() {
	f.mu.Lock()
	active := f.started && !f.stopped
	cooling := f.clock().Before(f.cooldownUntil)
	f.mu.Unlock()
	if !active || cooling {
		return
	}
	if f.aggregator.Size() < f.softCap {
		return
	}
	f.triggerFlush()
}

// EnqueueSealedBatch appends a version-sealed batch (from a config rotation) to the delivery
// queue. Empty batches are ignored; the queue is bounded at maxSealedBatches (oldest preserved,
// the incoming batch dropped when full).
func (f *MetricsFlusher) EnqueueSealedBatch(batch DrainedBatch) {
	if batch.IsEmpty() {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sealedBatches) >= maxSealedBatches {
		slog.Warn("metrics: sealed batch queue full; dropping batch", "version", batch.Version)
		return
	}
	f.sealedBatches = append(f.sealedBatches, batch)
}

// TriggerFlush schedules a coalesced out-of-band flush (used after a config rotation).
func (f *MetricsFlusher) TriggerFlush() { f.triggerFlush() }

func (f *MetricsFlusher) triggerFlush() {
	select {
	case f.trigger <- struct{}{}:
	default:
	}
}

// flush delivers all sealed batches in FIFO order, then the live aggregator. On a sealed-batch
// failure the batch is left at the head and the cycle stops, preserving per-version ordering. A
// live-batch failure pushes the drained snapshot to the head of the sealed queue so counts are
// never lost. flush only ever runs on the single flush goroutine (or synchronously in Stop after
// the goroutine has exited), so it is never concurrent with itself.
func (f *MetricsFlusher) flush() {
	for {
		f.mu.Lock()
		if len(f.sealedBatches) == 0 {
			f.mu.Unlock()
			break
		}
		batch := f.sealedBatches[0]
		f.mu.Unlock()

		if !f.flushBatch(batch) {
			return
		}

		f.mu.Lock()
		if len(f.sealedBatches) > 0 {
			f.sealedBatches = f.sealedBatches[1:]
		}
		f.mu.Unlock()
	}

	live := f.aggregator.Drain()
	if live.IsEmpty() {
		return
	}
	if !f.flushBatch(live) {
		f.mu.Lock()
		f.sealedBatches = append([]DrainedBatch{live}, f.sealedBatches...)
		f.mu.Unlock()
	}
}

// flushBatch delivers a single batch. It returns true when the batch is done with (2xx success or
// a non-retryable 400 drop) and false when it should be requeued (other status or transport
// error), in which case a cooldown is set.
func (f *MetricsFlusher) flushBatch(batch DrainedBatch) bool {
	body, err := buildMetricsPayload(batch, f.sdkVersion, f.providerName)
	if err != nil {
		slog.Warn("metrics: failed to build payload; dropping batch", "error", err)
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), metricsHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.metricsURL, bytes.NewReader(body))
	if err != nil {
		slog.Warn("metrics: failed to build request; requeueing", "error", err)
		f.setCooldown()
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", f.authHeader)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		slog.Warn("metrics: delivery failed; requeueing", "error", err)
		f.setCooldown()
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		f.resetCooldown()
		return true
	case resp.StatusCode == http.StatusBadRequest:
		slog.Warn("metrics: 400 bad request; dropping batch")
		return true
	default:
		slog.Warn("metrics: unexpected status; requeueing", "status", resp.StatusCode)
		f.setCooldown()
		return false
	}
}

func (f *MetricsFlusher) setCooldown() {
	f.mu.Lock()
	f.cooldownUntil = f.clock().Add(failedFlushCooldown)
	f.mu.Unlock()
}

func (f *MetricsFlusher) resetCooldown() {
	f.mu.Lock()
	f.cooldownUntil = time.Time{}
	f.mu.Unlock()
}

type metricsPayload struct {
	Version      string             `json:"version"`
	SDKType      string             `json:"sdkType"`
	SDKVersion   string             `json:"sdkVersion"`
	ProviderName string             `json:"providerName"`
	Evaluations  []metricEvaluation `json:"evaluations"`
}

type metricEvaluation struct {
	FlagKey          string `json:"flagKey"`
	Variant          string `json:"variant"`
	EvaluationReason string `json:"evaluationReason"`
	ErrorCode        string `json:"errorCode,omitempty"`
	TimestampSec     int64  `json:"timestampSec"`
	Count            int64  `json:"count"`
}

// buildMetricsPayload serializes a drained batch into the ingest JSON shape. The field names and
// structure must match the Java and Python providers (all three post to the same endpoint); the
// evaluations array order is not significant. errorCode is omitted when empty.
func buildMetricsPayload(batch DrainedBatch, sdkVersion, providerName string) ([]byte, error) {
	evaluations := make([]metricEvaluation, 0, len(batch.Evaluations))
	for flagKey, buckets := range batch.Evaluations {
		for key, count := range buckets {
			evaluations = append(evaluations, metricEvaluation{
				FlagKey:          flagKey,
				Variant:          key.Variant,
				EvaluationReason: key.Reason,
				ErrorCode:        key.ErrorCode,
				TimestampSec:     key.TimestampSec,
				Count:            count,
			})
		}
	}
	return json.Marshal(metricsPayload{
		Version:      batch.Version,
		SDKType:      sdkTypeServer,
		SDKVersion:   sdkVersion,
		ProviderName: providerName,
		Evaluations:  evaluations,
	})
}
