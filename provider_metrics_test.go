package dynatrace

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	openfeature "github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const versionedFlagJSON = `{
  "metadata": {"version": "cfg-1"},
  "flags": {
    "my-bool": {
      "state": "ENABLED",
      "variants": {"on": true, "off": false},
      "defaultVariant": "on"
    }
  }
}`

const versionedFlagJSONv2 = `{
  "metadata": {"version": "cfg-2"},
  "flags": {
    "my-bool": {
      "state": "ENABLED",
      "variants": {"on": true, "off": false},
      "defaultVariant": "on"
    }
  }
}`

// metricsRecorder is a minimal stand-in for the Dynatrace metrics ingest endpoint.
type metricsRecorder struct {
	srv *httptest.Server

	mu     sync.Mutex
	bodies [][]byte
	hits   chan struct{}
}

func newMetricsRecorder() *metricsRecorder {
	m := &metricsRecorder{hits: make(chan struct{}, 100)}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.bodies = append(m.bodies, body)
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case m.hits <- struct{}{}:
		default:
		}
	}))
	return m
}

func (m *metricsRecorder) versions(t *testing.T) []string {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.bodies))
	for _, b := range m.bodies {
		var p struct {
			Version string `json:"version"`
		}
		require.NoError(t, json.Unmarshal(b, &p))
		out = append(out, p.Version)
	}
	return out
}

func (m *metricsRecorder) waitForHit(t *testing.T) {
	t.Helper()
	select {
	case <-m.hits:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for metrics delivery")
	}
}

func newMetricsProvider(t *testing.T, cdn *httptest.Server, metrics *metricsRecorder) *DynatraceProvider {
	t.Helper()
	p, err := NewProvider(validSDKKey,
		WithConfigOrigin(cdn.URL),
		WithHTTPClient(cdn.Client()),
		WithMetricsOrigin(metrics.srv.URL),
		withPollInterval(500*time.Millisecond),
	)
	require.NoError(t, err)
	return p
}

func TestProvider_HooksReturnsMetricsHook(t *testing.T) {
	p, err := NewProvider(validSDKKey)
	require.NoError(t, err)
	assert.Len(t, p.Hooks(), 1)
}

func TestProvider_MetricsURLDerivedFromGeo(t *testing.T) {
	p, err := NewProvider(validSDKKey)
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.us.fm.dynatrace.com/v1/metrics", p.config.metricsURL)
}

func TestProvider_MetricsOriginOverrideRoutesDelivery(t *testing.T) {
	cdn := newTestServer(versionedFlagJSON, 200)
	defer cdn.Close()
	override := newMetricsRecorder()
	defer override.srv.Close()

	// WithMetricsOrigin is applied in NewProvider, taking precedence over the derived geo default.
	// httptest.Server.URL is an origin; the provider appends /v1/metrics.
	p, err := NewProvider(validSDKKey,
		WithConfigOrigin(cdn.URL),
		WithHTTPClient(cdn.Client()),
		WithMetricsOrigin(override.srv.URL),
		withPollInterval(500*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, override.srv.URL+"/v1/metrics", p.config.metricsURL)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))

	p.metricsAggregator.Record("my-bool", "on", "static", "", 100, "")
	p.Shutdown() // final flush

	assert.Equal(t, []string{"cfg-1"}, override.versions(t),
		"delivery should use the WithMetricsOrigin override, not the derived geo URL")
}

func TestProvider_SetsVersionOnInit(t *testing.T) {
	cdn := newTestServer(versionedFlagJSON, 200)
	defer cdn.Close()
	metrics := newMetricsRecorder()
	defer metrics.srv.Close()

	p := newMetricsProvider(t, cdn, metrics)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	assert.Equal(t, "cfg-1", p.metricsAggregator.Version())
}

func TestProvider_ShutdownFlushesMetrics(t *testing.T) {
	cdn := newTestServer(versionedFlagJSON, 200)
	defer cdn.Close()
	metrics := newMetricsRecorder()
	defer metrics.srv.Close()

	p := newMetricsProvider(t, cdn, metrics)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))

	p.metricsAggregator.Record("my-bool", "on", "static", "", 100, "")
	p.Shutdown() // final flush

	assert.Equal(t, []string{"cfg-1"}, metrics.versions(t))
}

func TestProvider_RotationSealsUnderOldVersion(t *testing.T) {
	cdn := newTestServer(versionedFlagJSON, 200)
	defer cdn.Close()
	metrics := newMetricsRecorder()
	defer metrics.srv.Close()

	p := newMetricsProvider(t, cdn, metrics)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	// Record under cfg-1, then rotate to cfg-2 via the poller's store callback.
	p.metricsAggregator.Record("my-bool", "on", "static", "", 100, "")
	_, err := p.commitFlags(versionedFlagJSONv2)
	require.NoError(t, err)

	metrics.waitForHit(t) // rotation triggers an out-of-band flush
	assert.Contains(t, metrics.versions(t), "cfg-1", "pre-rotation counts flush under the old version")
	assert.Equal(t, "cfg-2", p.metricsAggregator.Version(), "live version bumped after rotation")
}

func TestProvider_EndToEndHookRecords(t *testing.T) {
	cdn := newTestServer(versionedFlagJSON, 200)
	defer cdn.Close()
	metrics := newMetricsRecorder()
	defer metrics.srv.Close()

	p := newMetricsProvider(t, cdn, metrics)
	require.NoError(t, openfeature.SetNamedProviderAndWait("metrics-e2e", p))
	defer p.Shutdown()

	client := openfeature.NewClient("metrics-e2e")
	val := client.Boolean(t.Context(), "my-bool", false, openfeature.EvaluationContext{})
	assert.True(t, val)

	assert.Equal(t, 1, p.metricsAggregator.Size(), "evaluation flows through the metrics hook")
}
