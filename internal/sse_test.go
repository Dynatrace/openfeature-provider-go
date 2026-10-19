package internal

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sseTestTimeout = 5 * time.Second
	// a credential that must never reach a log line
	sseSecret = "SECRET-STREAM-TOKEN"
)

// fastRetry keeps retry loops quick in tests.
var fastRetry = SSEOptions{InitialRetry: 5 * time.Millisecond, MaxRetry: 20 * time.Millisecond}

// syncBuffer is a bytes.Buffer safe for the concurrent writes slog makes from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs routes the default slog logger into a buffer for the test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	out := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return out
}

// streamServer answers every request with an open event stream, optionally failing the first failFirst
// requests with a 503.
type streamServer struct {
	*httptest.Server
	requests  atomic.Int32
	failFirst int32
}

func newStreamServer(t *testing.T, failFirst int32) *streamServer {
	t.Helper()
	s := &streamServer{failFirst: failFirst}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.requests.Add(1) <= s.failFirst {
			http.Error(w, "stream unavailable, body mentions "+sseSecret, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(s.Close)
	return s
}

// runSSE starts RunSSE and stops it on cleanup; connected receives one value per onConnected call.
func runSSE(t *testing.T, rawURL string, opts SSEOptions) (connected <-chan struct{}, stop func()) {
	t.Helper()
	ch := make(chan struct{}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := RunSSE(ctx, rawURL, opts, func() { ch <- struct{}{} })
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(sseTestTimeout):
				t.Error("RunSSE did not stop after its context was cancelled")
			}
		})
	}
	t.Cleanup(stop)
	return ch, stop
}

func TestRunSSE_ConnectsAndCallsOnConnectedOnce(t *testing.T) {
	srv := newStreamServer(t, 0)
	connected, _ := runSSE(t, srv.URL, SSEOptions{})

	select {
	case <-connected:
	case <-time.After(sseTestTimeout):
		t.Fatal("onConnected was not called")
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), srv.requests.Load(), "one connection, no reconnect churn")
	assert.Empty(t, connected, "onConnected is called once")
}

func TestRunSSE_RetriesAnUnavailableStreamUntilItConnects(t *testing.T) {
	srv := newStreamServer(t, 3)
	connected, _ := runSSE(t, srv.URL, fastRetry)

	select {
	case <-connected:
	case <-time.After(sseTestTimeout):
		t.Fatal("never connected after the stream became available")
	}
	assert.Equal(t, int32(4), srv.requests.Load(), "three refused attempts, then the connection")
}

func TestRunSSE_NeverConnectedStillStopsPromptlyOnCancel(t *testing.T) {
	srv := newStreamServer(t, 1<<30)
	// a retry delay far longer than the test: cancel must interrupt the wait, not outlive it
	_, stop := runSSE(t, srv.URL, SSEOptions{InitialRetry: time.Hour, MaxRetry: time.Hour})
	require.Eventually(t, func() bool { return srv.requests.Load() >= 1 }, sseTestTimeout, 5*time.Millisecond)

	stop()
}

// the library can deadlock when a stream is closed while it holds a decoded event; events that never stop
// arriving make that the common case, so every iteration must still shut down
func TestRunSSE_StopsWhileEventsKeepArriving(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		for i := 0; r.Context().Err() == nil; i++ {
			if _, err := fmt.Fprintf(w, "data: {\"type\":\"refetchConfig\",\"n\":%d}\n\n", i); err != nil {
				return
			}
			flusher.Flush()
		}
	}))
	t.Cleanup(srv.Close)

	for range 10 {
		connected, stop := runSSE(t, srv.URL, SSEOptions{})
		select {
		case <-connected:
		case <-time.After(sseTestTimeout):
			t.Fatal("not connected")
		}
		time.Sleep(5 * time.Millisecond) // let events flow before stopping
		stop()
	}
}

func TestRunSSE_NoCredentialsInLogs(t *testing.T) {
	logs := captureLogs(t)

	t.Run("http error status then connect", func(t *testing.T) {
		srv := newStreamServer(t, 2)
		connected, stop := runSSE(t, srv.URL+"/sse?key="+sseSecret, fastRetry)
		select {
		case <-connected:
		case <-time.After(sseTestTimeout):
			t.Fatal("not connected")
		}
		stop()
	})
	t.Run("connection refused", func(t *testing.T) {
		_, stop := runSSE(t, "http://user:"+sseSecret+"@127.0.0.1:1/sse?key="+sseSecret, fastRetry)
		require.Eventually(t, func() bool { return strings.Contains(logs.String(), "sse: connection failed") },
			sseTestTimeout, 5*time.Millisecond)
		stop()
	})

	out := logs.String()
	assert.NotContains(t, out, sseSecret, "no log line may carry a stream credential")
	assert.Contains(t, out, redactedQuery, "the masked query marker shows a query existed")
}
