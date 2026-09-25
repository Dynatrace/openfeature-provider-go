package internal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeFetcher(server *httptest.Server) *CDNFetcher {
	f := NewCDNFetcher(server.URL+"/flags.json", server.Client())
	f.Retry = &RetryStrategy{
		MaxAttempts: 1, BaseDelayMS: 0,
		MaxJitterFactor: 0, NewTimer: firedTimer,
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
	f.rateLimitedUntil = time.Now().Add(10 * time.Minute)

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	// No request was made, so the tick is a skip, not a server 429: the caller must be able to
	// tell "we chose not to ask" from "the CDN told us to back off".
	assert.True(t, result.IsSkipped())
	assert.False(t, result.IsRateLimited())
	assert.Equal(t, 0, calls)
}

// The validators are stored as soon as a 2xx lands, before the caller has judged the body. If a
// rejected body left them in place the next fetch would revalidate to 304 for a config that was
// never applied, and the caller would read that as "what you have is current".
func TestCDNFetcher_InvalidateCache_ForcesUnconditionalFetch(t *testing.T) {
	var conditional atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			conditional.Store(true)
		}
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	_, err := f.Fetch(context.Background())
	require.NoError(t, err)

	f.InvalidateCache()
	_, err = f.Fetch(context.Background())
	require.NoError(t, err)
	assert.False(t, conditional.Load(), "fetch after InvalidateCache must not be conditional")
}

// A body that never fully arrives must leave no validators behind. The client timeout covers
// reading the body, so this is the ordinary "slow response, small budget" case, not just a
// severed connection: caching first would make the very next request revalidate against an ETag
// whose config was never received, and a 304 reports that as "what you have is current".
func TestCDNFetcher_BodyReadFailure_LeavesNoValidators(t *testing.T) {
	var requests atomic.Int32
	var conditional atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			conditional.Store(true)
		}
		w.Header().Set("ETag", `"v2"`)
		w.Header().Set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
		if requests.Add(1) == 1 {
			// Promise more than we send, then drop the connection mid-body.
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(200)
			fmt.Fprint(w, `{"flags":`)
			// Flush so the client sees the headers (and caches their validators) before the
			// connection dies; without this the response never reaches it at all.
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	_, err := f.Fetch(context.Background())
	require.Error(t, err, "a truncated body is a failed fetch")

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.False(t, conditional.Load(), "the failed read must not have cached validators")
	assert.True(t, result.IsSuccess())
	assert.Equal(t, `{"flags":{}}`, result.Body)
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
	f.storedLastModifiedAt = storedTime
	f.headerLastModified = "Thu, 01 Jan 2026 00:00:00 GMT"

	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsNotModified(), "stale-node defense should return 304, got %d", result.Status)
}

// TestCDNFetcher_2xx_UnparseableLastModified_StillUpdatesETag verifies that a 200
// with a malformed Last-Modified header still updates the stored ETag, so the next
// poll sends If-None-Match. Matches Java CdnFetcher.java behavior on 2xx.
func TestCDNFetcher_2xx_UnparseableLastModified_StillUpdatesETag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"new-etag"`)
		w.Header().Set("Last-Modified", "not-a-real-date")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	result, err := f.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsSuccess())

	gotETag := f.headerETag
	gotLM := f.headerLastModified
	assert.Equal(t, `"new-etag"`, gotETag, "ETag must update even when Last-Modified is unparseable")
	assert.Empty(t, gotLM, "Last-Modified must NOT be stored when unparseable")
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
	f.headerETag = `"xyz"`
	f.headerLastModified = "Thu, 01 Jan 2026 00:00:00 GMT"

	_, _ = f.Fetch(context.Background())
	assert.Equal(t, `"xyz"`, receivedIfNoneMatch)
	assert.Equal(t, "Thu, 01 Jan 2026 00:00:00 GMT", receivedIfModifiedSince)
}

func TestCDNFetcher_ContextCancellation(t *testing.T) {
	startedChan := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(startedChan)
		<-r.Context().Done() // hang until cancelled
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	f := makeFetcher(srv)

	doneChan := make(chan error, 1)
	go func() {
		_, err := f.Fetch(ctx)
		doneChan <- err
	}()

	<-startedChan
	cancel()
	err := <-doneChan
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

// providers.md §2.1.1: at most one CDN fetch in flight. A contended caller is turned away
// with ErrFetchInFlight rather than issuing a second request.
func TestCDNFetcher_TryFetch_SkipsWhenFetchInFlight(t *testing.T) {
	releaseChan := make(chan struct{})
	startedChan := make(chan struct{})
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(startedChan)
			<-releaseChan
		}
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	firstDoneChan := make(chan struct{})
	go func() {
		defer close(firstDoneChan)
		_, err := f.Fetch(context.Background())
		assert.NoError(t, err)
	}()

	<-startedChan
	_, err := f.TryFetch(context.Background())
	assert.ErrorIs(t, err, ErrFetchInFlight)
	assert.Equal(t, int32(1), requests.Load(), "the skipped tick must not reach the CDN")

	close(releaseChan)
	<-firstDoneChan

	// The slot is free again, so the next tick proceeds normally.
	result, err := f.TryFetch(context.Background())
	require.NoError(t, err)
	assert.True(t, result.IsSuccess())
	assert.Equal(t, int32(2), requests.Load())
}
