package dynatrace

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func validKey() string {
	return "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
}

func validOptions(cdnURL string) Options {
	key := validKey()
	return Options{sdkKey: key, cdnURL: cdnURL}
}

func makeFetcher(server *httptest.Server) *CDNFetcher {
	opts := validOptions(server.URL + "/flags.json")
	f := newCDNFetcher(opts)
	f.client = server.Client()
	f.retry = &RetryStrategy{
		maxAttempts:     1,
		baseDelayMS:     0,
		multiplier:      1,
		maxJitterFactor: 0,
		sleep:           noopSleep,
		rng:             nil,
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

	f := makeFetcher(srv)
	result, err := f.Fetch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsSuccess() {
		t.Errorf("expected success, status=%d", result.Status)
	}
	if result.Body != body {
		t.Errorf("body = %q, want %q", result.Body, body)
	}
	if result.ETag != `"abc"` {
		t.Errorf("etag = %q, want %q", result.ETag, `"abc"`)
	}
}

func TestCDNFetcher_304(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(304)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	result, err := f.Fetch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsNotModified() {
		t.Errorf("expected not-modified, status=%d", result.Status)
	}
}

func TestCDNFetcher_429_RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
	}))
	defer srv.Close()

	now := time.Now()
	f := makeFetcher(srv)
	f.clock = func() time.Time { return now }

	result, err := f.Fetch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsRateLimited() {
		t.Errorf("expected rate-limited, status=%d", result.Status)
	}
	if !f.IsRateLimited() {
		t.Error("expected fetcher to be rate-limited after 429")
	}
}

func TestCDNFetcher_RateLimit_SkipsFetch(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	// Manually set rate limit window to the future
	f.mu.Lock()
	f.rateLimitedUntil = time.Now().Add(10 * time.Minute)
	f.mu.Unlock()

	result, err := f.Fetch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsRateLimited() {
		t.Errorf("expected rate-limited synthetic 429, got status=%d", result.Status)
	}
	if calls != 0 {
		t.Errorf("expected no HTTP calls, got %d", calls)
	}
}

func TestCDNFetcher_StaleNodeDefense(t *testing.T) {
	// Server returns 200 with an older Last-Modified than what the fetcher already has.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT") // older
		w.WriteHeader(200)
		fmt.Fprint(w, `{"flags":{}}`)
	}))
	defer srv.Close()

	f := makeFetcher(srv)
	// Pre-set a newer stored Last-Modified
	storedTime, _ := parseHTTPDate("Thu, 01 Jan 2026 00:00:00 GMT")
	f.mu.Lock()
	f.lastModifiedAt = storedTime
	f.hasLastModified = true
	f.lastModified = "Thu, 01 Jan 2026 00:00:00 GMT"
	f.mu.Unlock()

	result, err := f.Fetch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsNotModified() {
		t.Errorf("expected stale-node defense to return 304, got status=%d", result.Status)
	}
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
	f.etag = `"xyz"`
	f.lastModified = "Thu, 01 Jan 2026 00:00:00 GMT"
	f.mu.Unlock()

	_, _ = f.Fetch()
	if receivedIfNoneMatch != `"xyz"` {
		t.Errorf("If-None-Match = %q, want %q", receivedIfNoneMatch, `"xyz"`)
	}
	if receivedIfModifiedSince != "Thu, 01 Jan 2026 00:00:00 GMT" {
		t.Errorf("If-Modified-Since = %q", receivedIfModifiedSince)
	}
}

func TestParseRetryAfterDuration_Seconds(t *testing.T) {
	d := parseRetryAfterDuration("30")
	if d != 30*time.Second {
		t.Errorf("expected 30s, got %v", d)
	}
}

func TestParseRetryAfterDuration_Empty(t *testing.T) {
	d := parseRetryAfterDuration("")
	if d != defaultRetryAfterMS*time.Millisecond {
		t.Errorf("expected default, got %v", d)
	}
}

func TestFetchResult_Predicates(t *testing.T) {
	if !(FetchResult{Status: 304}).IsNotModified() {
		t.Error("304 should be not-modified")
	}
	if !(FetchResult{Status: 429}).IsRateLimited() {
		t.Error("429 should be rate-limited")
	}
	if !(FetchResult{Status: 200}).IsSuccess() {
		t.Error("200 should be success")
	}
	if !(FetchResult{Status: 204}).IsSuccess() {
		t.Error("204 should be success")
	}
	if (FetchResult{Status: 500}).IsSuccess() {
		t.Error("500 should not be success")
	}
}
