package internal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeFetcher(server *httptest.Server) *CDNFetcher {
	f := NewCDNFetcher(server.URL+"/flags.json", server.Client())
	f.Retry = &RetryStrategy{
		MaxAttempts: 1, BaseDelayMS: 0, Multiplier: 1,
		MaxJitterFactor: 0, Sleep: noopSleep, Rng: nil,
	}
	return f
}

func TestCDNFetcher_200(t *testing.T) {
	body := `{"flags":{}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
		w.WriteHeader(200)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	result, err := makeFetcher(srv).Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsSuccess())
	assert.Equal(t, body, result.Body)
	assert.Equal(t, `"abc"`, result.ETag)
}

func TestCDNFetcher_304(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(304)
	}))
	defer srv.Close()

	result, err := makeFetcher(srv).Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsNotModified())
}

func TestCDNFetcher_429_RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
	}))
	defer srv.Close()

	now := time.Now()
	f := makeFetcher(srv)
	f.clock = func() time.Time { return now }

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsRateLimited())
	assert.True(t, f.IsRateLimited())
}

func TestCDNFetcher_RateLimit_SkipsFetch(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	f.mu.Lock()
	f.rateLimitedUntil = time.Now().Add(10 * time.Minute)
	f.mu.Unlock()

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsRateLimited())
	assert.Equal(t, 0, calls)
}

func TestCDNFetcher_StaleNodeDefense(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	storedTime, _ := ParseHTTPDate("Thu, 01 Jan 2026 00:00:00 GMT")
	f.mu.Lock()
	f.storedLastModifiedAt = storedTime
	f.headerLastModified = "Thu, 01 Jan 2026 00:00:00 GMT"
	f.mu.Unlock()

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsNotModified(), "stale-node defense should return 304, got %d", result.Status)
}

func TestCDNFetcher_ConditionalHeaders(t *testing.T) {
	var receivedIfNoneMatch, receivedIfModifiedSince string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedIfNoneMatch = r.Header.Get("If-None-Match")
		receivedIfModifiedSince = r.Header.Get("If-Modified-Since")
		w.WriteHeader(304)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	f.mu.Lock()
	f.headerETag = `"xyz"`
	f.headerLastModified = "Thu, 01 Jan 2026 00:00:00 GMT"
	f.mu.Unlock()

	_, _ = f.Fetch(context.Background())
	assert.Equal(t, `"xyz"`, receivedIfNoneMatch)
	assert.Equal(t, "Thu, 01 Jan 2026 00:00:00 GMT", receivedIfModifiedSince)
}

func TestCDNFetcher_ContextCancellation(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done() // hang until cancelled
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	f := makeFetcher(srv)

	done := make(chan error, 1)
	go func() {
		_, err := f.Fetch(ctx)
		done <- err
	}()

	<-started
	cancel()
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

func TestParseRetryAfterDuration_Seconds(t *testing.T) {
	assert.Equal(t, 30*time.Second, ParseRetryAfterDuration("30", time.Now()))
}

func TestParseRetryAfterDuration_Empty(t *testing.T) {
	assert.Equal(t, DefaultRetryAfterMS*time.Millisecond, ParseRetryAfterDuration("", time.Now()))
}

func TestFetchResult_Predicates(t *testing.T) {
	assert.True(t, (FetchResult{Status: 304}).IsNotModified())
	assert.True(t, (FetchResult{Status: 429}).IsRateLimited())
	assert.True(t, (FetchResult{Status: 200}).IsSuccess())
	assert.True(t, (FetchResult{Status: 204}).IsSuccess())
	assert.False(t, (FetchResult{Status: 500}).IsSuccess())
}
