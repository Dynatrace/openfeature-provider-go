package internal

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeIngest struct {
	srv *httptest.Server

	mu       sync.Mutex
	bodies   [][]byte
	statuses []int // queue of statuses to return; default 200 when empty

	hits chan struct{}
}

func newFakeIngest() *fakeIngest {
	fi := &fakeIngest{hits: make(chan struct{}, 100)}
	fi.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fi.mu.Lock()
		fi.bodies = append(fi.bodies, body)
		status := http.StatusOK
		if len(fi.statuses) > 0 {
			status = fi.statuses[0]
			fi.statuses = fi.statuses[1:]
		}
		fi.mu.Unlock()
		w.WriteHeader(status)
		select {
		case fi.hits <- struct{}{}:
		default:
		}
	}))
	return fi
}

func (fi *fakeIngest) pushStatus(s int) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.statuses = append(fi.statuses, s)
}

func (fi *fakeIngest) count() int {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	return len(fi.bodies)
}

func (fi *fakeIngest) all() [][]byte {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	out := make([][]byte, len(fi.bodies))
	copy(out, fi.bodies)
	return out
}

func (fi *fakeIngest) waitForHit(t *testing.T) {
	t.Helper()
	select {
	case <-fi.hits:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for metrics delivery")
	}
}

func newTestFlusher(fi *fakeIngest, agg *MetricsAggregator, interval time.Duration, softCap int) *MetricsFlusher {
	return NewMetricsFlusher(agg, fi.srv.Client(), "sdk-key", fi.srv.URL, interval, softCap, "1.2.3", "dynatrace")
}

func makeBatch(version string) DrainedBatch {
	return DrainedBatch{
		Version: version,
		Evaluations: map[string]map[EvaluationMetricKey]int64{
			"flag": {{Variant: "on", Reason: "static", TimestampSec: 100}: 1},
		},
	}
}

func parseVersion(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Version string `json:"version"`
	}
	require.NoError(t, json.Unmarshal(body, &p))
	return p.Version
}

func TestMetricsFlusher_FinalFlushOnStop(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	agg.SetVersion("v1")
	agg.Record("flag", "on", "static", "", 100, "")

	f := newTestFlusher(fi, agg, time.Hour, DefaultMetricsSoftCap)
	f.Start()
	f.Stop() // triggers the final synchronous flush

	assert.Equal(t, 1, fi.count())
}

func TestMetricsFlusher_PeriodicFlush(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	agg.SetVersion("v1")
	agg.Record("flag", "on", "static", "", 100, "")

	f := newTestFlusher(fi, agg, 15*time.Millisecond, DefaultMetricsSoftCap)
	f.Start()
	defer f.Stop()

	fi.waitForHit(t)
}

func TestMetricsFlusher_RotationTrigger(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	f := newTestFlusher(fi, agg, time.Hour, DefaultMetricsSoftCap)
	f.Start()
	defer f.Stop()

	f.EnqueueSealedBatch(makeBatch("v1"))
	f.TriggerFlush()

	fi.waitForHit(t)
	assert.Equal(t, "v1", parseVersion(t, fi.all()[0]))
}

func TestMetricsFlusher_CapacityTrigger(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	agg.SetVersion("v1")
	agg.Record("flag", "on", "static", "", 100, "")

	f := newTestFlusher(fi, agg, time.Hour, 1) // soft cap of 1
	f.Start()
	defer f.Stop()

	f.OnRecord() // size (1) >= soft cap (1), no cooldown -> out-of-band flush
	fi.waitForHit(t)
}

func TestMetricsFlusher_SealedBeforeLive(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	agg.SetVersion("v-live")
	agg.Record("flag", "on", "static", "", 100, "")

	f := newTestFlusher(fi, agg, time.Hour, DefaultMetricsSoftCap)
	f.EnqueueSealedBatch(makeBatch("v-sealed"))

	f.flush() // synchronous; delivers sealed first, then live

	bodies := fi.all()
	require.Len(t, bodies, 2)
	assert.Equal(t, "v-sealed", parseVersion(t, bodies[0]))
	assert.Equal(t, "v-live", parseVersion(t, bodies[1]))
}

func TestMetricsFlusher_400Drops(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()
	fi.pushStatus(http.StatusBadRequest)

	f := newTestFlusher(fi, NewMetricsAggregator(DefaultMetricsHardCap), time.Hour, DefaultMetricsSoftCap)
	f.EnqueueSealedBatch(makeBatch("v1"))

	f.flush()
	assert.Equal(t, 1, fi.count())

	f.mu.Lock()
	remaining := len(f.sealedBatches)
	f.mu.Unlock()
	assert.Equal(t, 0, remaining, "400 is non-retryable; batch must be dropped")
}

func TestMetricsFlusher_OtherStatusRequeues(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()
	fi.pushStatus(http.StatusInternalServerError)

	f := newTestFlusher(fi, NewMetricsAggregator(DefaultMetricsHardCap), time.Hour, DefaultMetricsSoftCap)
	f.EnqueueSealedBatch(makeBatch("v1"))

	f.flush()
	assert.Equal(t, 1, fi.count())

	f.mu.Lock()
	remaining := len(f.sealedBatches)
	f.mu.Unlock()
	assert.Equal(t, 1, remaining, "5xx must requeue the batch at the head")
}

func TestMetricsFlusher_CooldownSetThenReset(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	f := newTestFlusher(fi, NewMetricsAggregator(DefaultMetricsHardCap), time.Hour, DefaultMetricsSoftCap)
	f.clock = func() time.Time { return time.Unix(1000, 0) }

	fi.pushStatus(http.StatusInternalServerError)
	f.EnqueueSealedBatch(makeBatch("v1"))
	f.flush() // fails -> cooldown set, batch requeued

	f.mu.Lock()
	cooldown := f.cooldownUntil
	f.mu.Unlock()
	assert.Equal(t, time.Unix(1000, 0).Add(failedFlushCooldown), cooldown)

	fi.pushStatus(http.StatusOK)
	f.flush() // succeeds -> cooldown reset

	f.mu.Lock()
	cooldown = f.cooldownUntil
	f.mu.Unlock()
	assert.True(t, cooldown.IsZero(), "2xx must reset the cooldown")
}

func TestMetricsFlusher_MaxSealedBatches(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	f := newTestFlusher(fi, NewMetricsAggregator(DefaultMetricsHardCap), time.Hour, DefaultMetricsSoftCap)
	for range maxSealedBatches + 5 {
		f.EnqueueSealedBatch(makeBatch("v1"))
	}

	f.mu.Lock()
	queued := len(f.sealedBatches)
	f.mu.Unlock()
	assert.Equal(t, maxSealedBatches, queued)
}

func TestMetricsFlusher_PayloadShape(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	agg := NewMetricsAggregator(DefaultMetricsHardCap)
	agg.SetVersion("v9")
	agg.Record("header", "control", "targeting_match", "", 1741780820, "")
	agg.Record("missing", "__default__", "error", "flag_not_found", 1741780820, "")

	f := newTestFlusher(fi, agg, time.Hour, DefaultMetricsSoftCap)
	f.flush()

	require.Equal(t, 1, fi.count())
	var p struct {
		Version      string           `json:"version"`
		SDKType      string           `json:"sdkType"`
		SDKVersion   string           `json:"sdkVersion"`
		ProviderName string           `json:"providerName"`
		Evaluations  []map[string]any `json:"evaluations"`
	}
	require.NoError(t, json.Unmarshal(fi.all()[0], &p))
	assert.Equal(t, "v9", p.Version)
	assert.Equal(t, "server", p.SDKType)
	assert.Equal(t, "1.2.3", p.SDKVersion)
	assert.Equal(t, "dynatrace", p.ProviderName)
	require.Len(t, p.Evaluations, 2)

	var success, errored map[string]any
	for _, e := range p.Evaluations {
		if e["flagKey"] == "header" {
			success = e
		} else {
			errored = e
		}
	}
	require.NotNil(t, success)
	require.NotNil(t, errored)

	assert.Equal(t, "control", success["variant"])
	assert.Equal(t, "targeting_match", success["evaluationReason"])
	assert.Equal(t, float64(1741780820), success["timestampSec"])
	assert.Equal(t, float64(1), success["count"])
	_, hasErrorCode := success["errorCode"]
	assert.False(t, hasErrorCode, "errorCode must be omitted on success")

	assert.Equal(t, "flag_not_found", errored["errorCode"])
	assert.Equal(t, "error", errored["evaluationReason"])
}

func TestMetricsFlusher_StopIsIdempotent(t *testing.T) {
	fi := newFakeIngest()
	defer fi.srv.Close()

	f := newTestFlusher(fi, NewMetricsAggregator(DefaultMetricsHardCap), time.Hour, DefaultMetricsSoftCap)
	f.Start()
	assert.NotPanics(t, func() {
		f.Stop()
		f.Stop()
	})
}
