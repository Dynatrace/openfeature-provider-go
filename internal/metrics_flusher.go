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
	// metricsChannelBufferSize buffers rotation hand-offs so a config rotation never blocks the
	// state loop on the flush goroutine.
	metricsChannelBufferSize = maxSealedBatches
)

// MetricsFlusher periodically delivers aggregated evaluation metrics to the Dynatrace ingest
// endpoint. All flush state (the sealed-batch queue, the cooldown) and all HTTP live on a single
// goroutine started by Run; the evaluation path never blocks on delivery and the flusher needs
// no locking.
//
// Four triggers cause a flush: the periodic ticker, a capacity signal from the hook (recordChan)
// once the aggregator exceeds the soft cap, a config rotation (EnqueueSealedBatch), and a final
// flush when the ctx passed to Run is cancelled.
type MetricsFlusher struct {
	aggregator    *MetricsAggregator
	httpClient    *http.Client
	metricsURL    string
	authHeader    string
	flushInterval time.Duration
	softCap       int
	sdkVersion    string
	providerName  string

	sealedChan chan DrainedBatch
	recordChan chan struct{}

	// sealed and cooldownUntil are owned by the Run goroutine and touched without locking.
	sealed        []DrainedBatch
	cooldownUntil time.Time

	// clock is injectable for deterministic cooldown tests (defaults to time.Now).
	clock func() time.Time

	// runOnce guards Run so the flush goroutine is launched at most once; done is captured on
	// that first call and returned to any later callers.
	runOnce sync.Once
	done    <-chan struct{}
}

// NewMetricsFlusher constructs a flusher. A nil httpClient gets a default client with a 10s
// timeout; an injected client is reused as-is. A non-positive flushInterval falls back to
// DefaultMetricsFlushInterval.
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
	return &MetricsFlusher{
		aggregator:    aggregator,
		httpClient:    httpClient,
		metricsURL:    metricsURL,
		authHeader:    "Bearer " + sdkKey,
		flushInterval: flushInterval,
		softCap:       softCap,
		sdkVersion:    sdkVersion,
		providerName:  providerName,
		sealedChan:    make(chan DrainedBatch, metricsChannelBufferSize),
		recordChan:    make(chan struct{}, 1),
		clock:         time.Now,
	}
}

// RecordChan returns the coalescing capacity-signal channel the hook sends on after each record.
func (f *MetricsFlusher) RecordChan() chan<- struct{} { return f.recordChan }

// Run launches the flush goroutine. It runs until ctx is cancelled, then performs a final flush
// before exiting. Returns a done channel that closes once the goroutine has exited; the caller
// owns the context and shuts down by cancelling it then receiving from done. Run launches the
// goroutine at most once; later calls are no-ops that return the same done channel from the first
// call (the ctx passed to subsequent calls is ignored).
func (f *MetricsFlusher) Run(ctx context.Context) <-chan struct{} {
	f.runOnce.Do(func() {
		done := make(chan struct{})
		f.done = done
		go func() {
			defer close(done)
			ticker := time.NewTicker(f.flushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					f.flush()
				case batch := <-f.sealedChan:
					f.enqueueSealed(batch)
					f.flush()
				case <-f.recordChan:
					if f.aggregator.Size() >= f.softCap && !f.cooling() {
						f.flush()
					}
				case <-ctx.Done():
					f.drainSealedChan()
					f.flush() // final flush
					return
				}
			}
		}()
	})
	return f.done
}

// EnqueueSealedBatch hands a version-sealed batch (from a config rotation) to the flush goroutine,
// which flushes on receipt. Empty batches are ignored; a full hand-off channel drops the batch
// (bounded, oldest preserved) rather than blocking the state loop.
func (f *MetricsFlusher) EnqueueSealedBatch(batch DrainedBatch) {
	if batch.IsEmpty() {
		return
	}
	select {
	case f.sealedChan <- batch:
	default:
		slog.Warn("metrics: sealed batch hand-off full; dropping batch", "version", batch.Version)
	}
}

// enqueueSealed appends a batch to the Run-goroutine-local queue, bounded at maxSealedBatches.
func (f *MetricsFlusher) enqueueSealed(batch DrainedBatch) {
	if batch.IsEmpty() {
		return
	}
	if len(f.sealed) >= maxSealedBatches {
		slog.Warn("metrics: sealed batch queue full; dropping batch", "version", batch.Version)
		return
	}
	f.sealed = append(f.sealed, batch)
}

// drainSealedChan moves any buffered hand-offs into the local queue ahead of the final flush.
func (f *MetricsFlusher) drainSealedChan() {
	for {
		select {
		case batch := <-f.sealedChan:
			f.enqueueSealed(batch)
		default:
			return
		}
	}
}

func (f *MetricsFlusher) cooling() bool { return f.clock().Before(f.cooldownUntil) }

// flush delivers all sealed batches in FIFO order, then the live aggregator. On a sealed-batch
// failure the batch is left at the head and the cycle stops, preserving per-version ordering. A
// live-batch failure pushes the drained snapshot to the head of the sealed queue so counts are
// never lost. flush runs only on the Run goroutine (or synchronously in tests), so it is never
// concurrent with itself and needs no locking.
func (f *MetricsFlusher) flush() {
	for len(f.sealed) > 0 {
		if !f.flushBatch(f.sealed[0]) {
			return
		}
		f.sealed = f.sealed[1:]
	}

	live := f.aggregator.Drain()
	if live.IsEmpty() {
		return
	}
	if !f.flushBatch(live) {
		f.sealed = append([]DrainedBatch{live}, f.sealed...)
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
	f.cooldownUntil = f.clock().Add(failedFlushCooldown)
}

func (f *MetricsFlusher) resetCooldown() {
	f.cooldownUntil = time.Time{}
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
