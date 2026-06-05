package dynatrace

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	openfeature "github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFlagJSON = `{
  "flags": {
    "my-bool": {
      "state": "ENABLED",
      "variants": {"on": true, "off": false},
      "defaultVariant": "on"
    },
    "my-string": {
      "state": "ENABLED",
      "variants": {"hello": "world"},
      "defaultVariant": "hello"
    }
  }
}`

func newTestServer(body string, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(status)
		if body != "" {
			fmt.Fprint(w, body)
		}
	}))
}

func newTestProvider(t *testing.T, srv *httptest.Server) *DynatraceProvider {
	t.Helper()
	p, err := NewProvider(validSDKKey,
		WithConfigOrigin(srv.URL),
		WithHTTPClient(srv.Client()),
		withPollInterval(500*time.Millisecond),
	)
	require.NoError(t, err)
	return p
}

func TestProvider_Metadata(t *testing.T) {
	p, err := NewProvider(validSDKKey)
	require.NoError(t, err)
	assert.Equal(t, "dynatrace", p.Metadata().Name)
}

func TestProvider_Init_Success(t *testing.T) {
	srv := newTestServer(testFlagJSON, 200)
	defer srv.Close()

	p := newTestProvider(t, srv)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	// PROVIDER_READY is synthesized by the SDK after Init returns; the provider
	// does not emit it on the channel (matching Java provider behaviour).
	assert.True(t, p.isReady())
}

func TestProvider_Init_Failure_Non2xx(t *testing.T) {
	srv := newTestServer("", 503)
	defer srv.Close()
	require.Error(t, newTestProvider(t, srv).Init(openfeature.NewEvaluationContext("", nil)))
}

func TestProvider_Init_Failure_EmptyBody(t *testing.T) {
	srv := newTestServer("", 200)
	defer srv.Close()
	require.Error(t, newTestProvider(t, srv).Init(openfeature.NewEvaluationContext("", nil)))
}

func TestProvider_BooleanEvaluation_BeforeInit(t *testing.T) {
	p, err := NewProvider(validSDKKey)
	require.NoError(t, err)
	result := p.BooleanEvaluation(context.Background(), "my-bool", false, nil)
	assert.False(t, result.Value)
	assert.NotEmpty(t, result.ResolutionError.Error())
}

func TestProvider_BooleanEvaluation_AfterInit(t *testing.T) {
	srv := newTestServer(testFlagJSON, 200)
	defer srv.Close()

	p := newTestProvider(t, srv)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	assert.True(t, p.BooleanEvaluation(context.Background(), "my-bool", false, nil).Value)
}

func TestProvider_Shutdown_Idempotent(t *testing.T) {
	srv := newTestServer(testFlagJSON, 200)
	defer srv.Close()

	p := newTestProvider(t, srv)
	_ = p.Init(openfeature.NewEvaluationContext("", nil))
	go func() {
		for range p.EventChannel() {
		}
	}()
	p.Shutdown()
	p.Shutdown()
}

func TestProvider_StateTransition_StaleOnPollFailure(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(200)
			fmt.Fprint(w, testFlagJSON)
		} else {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()

	p := newTestProvider(t, srv)
	p.gracePeriod = 5 * time.Second
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	done := make(chan struct{})
	var events []openfeature.EventType
	go func() {
		defer close(done)
		for e := range p.EventChannel() {
			events = append(events, e.EventType)
			if e.EventType == openfeature.ProviderStale {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for PROVIDER_STALE")
	}
	assert.Contains(t, events, openfeature.ProviderStale)
}

func TestProvider_GraceExpiry_TransitionsToError(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(200)
			fmt.Fprint(w, testFlagJSON)
		} else {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()

	p := newTestProvider(t, srv)
	p.gracePeriod = 50 * time.Millisecond
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	var seenError bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range p.EventChannel() {
			if e.EventType == openfeature.ProviderError {
				seenError = true
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for PROVIDER_ERROR")
	}
	assert.True(t, seenError)
}

// TestProvider_ConfigChange_PopulatesFlagKeys verifies that PROVIDER_CONFIG_CHANGE
// events carry the actual changed flag keys (added, removed, or modified) rather
// than an empty slice. Workaround for flagd-core v0.15.4 not exposing
// setFlagsAndGetChangedKeys.
func TestProvider_ConfigChange_PopulatesFlagKeys(t *testing.T) {
	const initialJSON = `{
  "flags": {
    "my-bool":   {"state": "ENABLED", "variants": {"on": true, "off": false}, "defaultVariant": "on"},
    "my-string": {"state": "ENABLED", "variants": {"hello": "world"}, "defaultVariant": "hello"}
  }
}`
	// my-bool: defaultVariant flipped (modified)
	// my-string: removed
	// my-other: added
	const updatedJSON = `{
  "flags": {
    "my-bool":  {"state": "ENABLED", "variants": {"on": true, "off": false}, "defaultVariant": "off"},
    "my-other": {"state": "ENABLED", "variants": {"a": 1, "b": 2}, "defaultVariant": "a"}
  }
}`

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		if callCount == 1 {
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(200)
			fmt.Fprint(w, initialJSON)
			return
		}
		w.Header().Set("ETag", `"v2"`)
		w.WriteHeader(200)
		fmt.Fprint(w, updatedJSON)
	}))
	defer srv.Close()

	p := newTestProvider(t, srv)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	done := make(chan openfeature.Event, 1)
	go func() {
		for e := range p.EventChannel() {
			if e.EventType == openfeature.ProviderConfigChange {
				select {
				case done <- e:
				default:
				}
				return
			}
		}
	}()

	select {
	case e := <-done:
		assert.ElementsMatch(t,
			[]string{"my-bool", "my-string", "my-other"},
			e.ProviderEventDetails.FlagChanges,
			"PROVIDER_CONFIG_CHANGE should report added/removed/modified keys",
		)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for PROVIDER_CONFIG_CHANGE")
	}
}

// TestProvider_ConfigChange_NoOpPoll_NoEvent verifies that a poll returning the
// same config does NOT fire PROVIDER_CONFIG_CHANGE. PollingManager filters out
// empty changed-key slices, so identical-config polls are silent.
func TestProvider_ConfigChange_NoOpPoll_NoEvent(t *testing.T) {
	// Server always returns the same body but with a different ETag so the client
	// can't short-circuit via 304. This forces storeFlags to run, which then
	// produces an empty diff -> no event.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, callCount))
		w.WriteHeader(200)
		fmt.Fprint(w, testFlagJSON)
	}))
	defer srv.Close()

	p := newTestProvider(t, srv)
	require.NoError(t, p.Init(openfeature.NewEvaluationContext("", nil)))
	defer p.Shutdown()

	gotChange := make(chan openfeature.Event, 4)
	go func() {
		for e := range p.EventChannel() {
			if e.EventType == openfeature.ProviderConfigChange {
				gotChange <- e
			}
		}
	}()

	// Wait long enough for at least 2 polls (poll interval = 500ms).
	select {
	case e := <-gotChange:
		t.Fatalf("unexpected PROVIDER_CONFIG_CHANGE on no-op poll: keys=%v", e.ProviderEventDetails.FlagChanges)
	case <-time.After(1500 * time.Millisecond):
		// Expected: no event.
	}
}
