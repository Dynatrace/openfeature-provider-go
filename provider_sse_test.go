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
	// configRequests counts every request the CDN received for the config
	configRequests atomic.Int32
	// configBody builds the CDN response from the server's own origin
	configBody func(origin string) string
	// failConfig makes the CDN answer 503 instead of the config, while true
	failConfig atomic.Bool
	// rejectConfig makes the CDN answer 404, a failure the fetcher does not retry, while true
	rejectConfig atomic.Bool
	// invalidConfig makes the CDN answer 200 with a body that cannot be applied, while true
	invalidConfig atomic.Bool
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
		env.configRequests.Add(1)
		if env.failConfig.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if env.rejectConfig.Load() {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if env.invalidConfig.Load() {
			fmt.Fprint(w, `{"flags": "not a flag set"}`)
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

// configRequestsDuring is how many config requests the CDN receives over the next d.
func (e *sseEnv) configRequestsDuring(d time.Duration) int32 {
	before := e.configRequests.Load()
	time.Sleep(d)
	return e.configRequests.Load() - before
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

func TestProvider_SSE_ConnectTriggersOneCatchUpFetch(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	// polling never ticks, so every request beyond the first is the catch-up
	initProvider(t, env.provider(t, withPollInterval(time.Hour)))

	require.Eventually(t, func() bool { return env.configRequests.Load() == 2 }, sseTestWait, sseTestStep,
		"init fetch plus one catch-up fetch on connect")
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(2), env.configRequests.Load(), "the catch-up is a single fetch")
}

func TestProvider_SSE_NoStreamMeansNoCatchUpFetch(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[]`))
	initProvider(t, env.provider(t, withPollInterval(time.Hour)))

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(1), env.configRequests.Load(), "only the init fetch")
}

func TestProvider_SSE_PollingRelaxesWhileConnectedAndSuccessfulFetch(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	initProvider(t, env.provider(t,
		withPollInterval(50*time.Millisecond),
		WithSSEConnectedPollInterval(time.Hour),
	))
	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep)
	time.Sleep(300 * time.Millisecond) // lets the catch-up and any tick before the switch settle
	settled := env.configRequests.Load()

	time.Sleep(400 * time.Millisecond)

	assert.Equal(t, settled, env.configRequests.Load(), "50ms polling must give way to the hour-long connected interval")
}

func TestProvider_SSE_ConnectedIntervalSetsTheCadence(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	// the disconnected interval would never tick; only the connected one can produce these requests
	initProvider(t, env.provider(t,
		withPollInterval(time.Hour),
		WithSSEConnectedPollInterval(100*time.Millisecond),
	))

	require.Eventually(t, func() bool { return env.configRequests.Load() >= 5 }, sseTestWait, sseTestStep)
}

func TestProvider_SSE_UnreachableStreamKeepsTheDisconnectedCadence(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"http://127.0.0.1:1/sse"}]`))
	initProvider(t, env.provider(t,
		withPollInterval(50*time.Millisecond),
		WithSSEConnectedPollInterval(time.Hour),
	))

	require.Eventually(t, func() bool { return env.configRequests.Load() >= 5 }, sseTestWait, sseTestStep,
		"never connected, so polling stays at the aggressive cadence")
}

func TestNewProvider_SSEConnectedPollIntervalDefaultsToTenMinutes(t *testing.T) {
	p, err := NewProvider(validSDKKey)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, p.sseConnectedPollInterval)
	assert.Equal(t, 10*time.Second, p.pollInterval)
}

// cadenceFast and cadenceSlow are far enough apart that a window of cadenceWindow tells them apart even under -race
const (
	cadenceFast   = 20 * time.Millisecond
	cadenceSlow   = 300 * time.Millisecond
	cadenceWindow = 400 * time.Millisecond
)

// relaxedProvider starts a provider whose stream is connected and whose catch-up fetch succeeded, so polling
// runs at the slow, connected cadence.
func relaxedProvider(t *testing.T, env *sseEnv) {
	t.Helper()
	initProvider(t, env.provider(t, withPollInterval(cadenceFast), WithSSEConnectedPollInterval(cadenceSlow)))
	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep)
	require.Eventually(t, func() bool { return env.configRequestsDuring(cadenceWindow) <= 3 }, sseTestWait, sseTestStep,
		"polling must relax once the stream is connected and the catch-up fetch succeeded")
}

func TestProvider_SSE_FailedFetchWhileConnectedRestoresTheShortCadence(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	relaxedProvider(t, env)

	env.rejectConfig.Store(true)
	// the failure is only seen by the next (slow) poll; after it, polling must be fast again
	require.Eventually(t, func() bool { return env.configRequestsDuring(cadenceWindow) >= 5 }, sseTestWait, sseTestStep,
		"a failed fetch must restore the short cadence although the stream is still connected")
	assert.Equal(t, int32(1), env.clients.Load(), "the stream stays connected")

	env.rejectConfig.Store(false)
	require.Eventually(t, func() bool { return env.configRequestsDuring(cadenceWindow) <= 3 }, sseTestWait, sseTestStep,
		"the next successful fetch relaxes polling again")
}

func TestProvider_SSE_UnapplicableConfigWhileConnectedRestoresTheShortCadence(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	relaxedProvider(t, env)

	env.invalidConfig.Store(true)
	require.Eventually(t, func() bool { return env.configRequestsDuring(cadenceWindow) >= 5 }, sseTestWait, sseTestStep,
		"a config that cannot be applied must restore the short cadence")

	env.invalidConfig.Store(false)
	require.Eventually(t, func() bool { return env.configRequestsDuring(cadenceWindow) <= 3 }, sseTestWait, sseTestStep,
		"a valid config relaxes polling again")
}

func TestProvider_SSE_ConnectingAloneDoesNotRelaxPolling(t *testing.T) {
	env := newSSEEnv(t, configWithEventStreams(`[{"type":"sse","url":"{origin}/sse"}]`))
	p := env.provider(t, withPollInterval(cadenceFast), WithSSEConnectedPollInterval(time.Hour))
	initProvider(t, p)
	// every fetch after init fails, including the catch-up the connection triggers
	env.rejectConfig.Store(true)
	require.Eventually(t, func() bool { return env.clients.Load() == 1 }, sseTestWait, sseTestStep)

	assert.GreaterOrEqual(t, env.configRequestsDuring(cadenceWindow), int32(8),
		"a connected stream must not relax polling while fetches fail")
}
