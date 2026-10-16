package dynatrace

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	openfeature "github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sseTestWait = 5 * time.Second
	sseTestStep = 10 * time.Millisecond
)

// sseEnv is a CDN that serves a flag config and an event stream that counts its connected clients. Both live
// on one server, so a structured endpoint can resolve against the CDN origin.
type sseEnv struct {
	*httptest.Server
	clients atomic.Int32
	// total counts every stream connection ever opened
	total atomic.Int32
	// configBody builds the CDN response from the server's own origin
	configBody func(origin string) string
	// failConfig makes the CDN answer 503 instead of the config, while true
	failConfig atomic.Bool
}

func newSSEEnv(t *testing.T, configBody func(origin string) string) *sseEnv {
	t.Helper()
	env := &sseEnv{configBody: configBody}
	env.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sse" {
			env.clients.Add(1)
			env.total.Add(1)
			defer env.clients.Add(-1)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if env.failConfig.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"test-etag"`)
		fmt.Fprint(w, env.configBody(env.URL))
	}))
	t.Cleanup(env.Close)
	return env
}

// configWithEventStreams serves a one-flag config; {origin} in eventStreams stands for the serving origin.
func configWithEventStreams(eventStreams string) func(string) string {
	return func(origin string) string {
		return fmt.Sprintf(`{
  "flags": {"my-bool": {"state": "ENABLED", "variants": {"on": true, "off": false}, "defaultVariant": "on"}},
  "eventStreams": %s
}`, strings.ReplaceAll(eventStreams, "{origin}", origin))
	}
}

func (e *sseEnv) provider(t *testing.T, opts ...ProviderOption) *DynatraceProvider {
	t.Helper()
	all := append([]ProviderOption{
		WithConfigOrigin(e.URL),
		WithHTTPClient(e.Client()),
		withPollInterval(500 * time.Millisecond),
	}, opts...)
	p, err := NewProvider(validSDKKey, all...)
	require.NoError(t, err)
	return p
}

func initProvider(t *testing.T, p *DynatraceProvider) {
	t.Helper()
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	t.Cleanup(p.Shutdown)
}

func TestProvider_SSE_OpensTheAdvertisedStreamOnce(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	// every poll gets the full config again, so a second stream would open here if starting were not idempotent
	initProvider(t, env.provider(t, withPollInterval(100*time.Millisecond)))

	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep)
	require.Never(t, func() bool { return env.total.Load() > 1 }, 400*time.Millisecond, sseTestStep,
		"repeated polls must not open a second stream")
}

func TestProvider_SSE_DisabledOpensNoStream(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	initProvider(t, env.provider(t, WithSSEDisabled()))

	require.Never(t, func() bool { return env.total.Load() > 0 }, 300*time.Millisecond, sseTestStep,
		"a disabled SSE must never connect")
}

// lockedBuffer is a log sink that background goroutines can write to while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestProvider_SSE_UnusableAdvertisedStreamWarnsOnceWithoutTheURL(t *testing.T) {
	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	// the env answers every poll with the full config, so the warning would repeat without the once-guard
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"ftp://stream.example.test/sse?token=dummy-marker"}]`))
	p := env.provider(t, withPollInterval(100*time.Millisecond))
	initProvider(t, p)

	const warning = "advertised sse stream is unusable"
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), warning) }, sseTestWait, sseTestStep)
	require.Never(t, func() bool { return strings.Count(logs.String(), warning) > 1 }, 600*time.Millisecond, sseTestStep,
		"one warning, not one per poll")
	out := logs.String()
	assert.NotContains(t, out, "dummy-marker")
	assert.NotContains(t, out, "stream.example.test")
	assert.Equal(t, int32(0), env.total.Load())
}

func TestProvider_SSE_UnreachableStreamFallsBackToPollingAndStillReachesReady(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"http://127.0.0.1:1/sse"}]`))
	p := env.provider(t)

	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)), "a stream that cannot open must not fail init")
	t.Cleanup(p.Shutdown)

	assert.Equal(t, openfeature.ReadyState, p.getState())
	assert.True(t, p.BooleanEvaluation(context.Background(), "my-bool", false, nil).Value)
}

func TestProvider_SSE_ShutdownClosesTheStream(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	p := env.provider(t)
	initProvider(t, p)
	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep)

	stopped := make(chan struct{})
	go func() {
		p.Shutdown()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(sseTestWait):
		t.Fatal("Shutdown did not return")
	}
	require.Eventually(t, func() bool { return env.clients.Load() == 0 }, sseTestWait, sseTestStep, "the stream must be closed")
}

// the config carrying the stream could not be read at init; the first poll that succeeds must open it
func TestProvider_SSE_OpensTheStreamAfterAFailedInitRecovers(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	env.failConfig.Store(true)
	p := env.provider(t)

	require.Error(t, p.Init(openfeature.NewEvaluationContext("", nil)), "init fails while the CDN is down")
	t.Cleanup(p.Shutdown)
	require.Never(t, func() bool { return env.total.Load() > 0 }, 200*time.Millisecond, sseTestStep,
		"no config yet, so no stream address")

	env.failConfig.Store(false)

	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep,
		"the first successful poll must open the stream")
}
