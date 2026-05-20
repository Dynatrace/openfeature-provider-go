package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RequestTimeout      = 5 * time.Second
	DefaultRetryAfterMS = 60_000
)

// FetchResult holds the outcome of a single CDN fetch attempt.
type FetchResult struct {
	Status       int
	Body         string
	ETag         string
	LastModified string
}

func (r FetchResult) IsNotModified() bool { return r.Status == 304 }
func (r FetchResult) IsRateLimited() bool { return r.Status == 429 }
func (r FetchResult) IsSuccess() bool     { return r.Status >= 200 && r.Status < 300 }

// CDNFetcher performs conditional HTTP GETs against the Dynatrace CDN.
// The fetchMu guards state mutation against concurrent callers (e.g. a future
// SSE-triggered re-fetch racing the polling path).
type CDNFetcher struct {
	cdnURL string
	Retry  *RetryStrategy
	client *http.Client
	clock  func() time.Time

	mu               sync.Mutex
	etag             string
	lastModified     string
	lastModifiedAt   time.Time
	rateLimitedUntil time.Time

	fetchMu sync.Mutex
}

func NewCDNFetcher(cdnURL string, client *http.Client) *CDNFetcher {
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	return &CDNFetcher{
		cdnURL: cdnURL,
		Retry:  NewRetryStrategy(),
		client: client,
		clock:  time.Now,
	}
}

func (f *CDNFetcher) IsRateLimited() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clock().Before(f.rateLimitedUntil)
}

// Fetch performs a conditional GET with retry. Respects ctx cancellation.
func (f *CDNFetcher) Fetch(ctx context.Context) (FetchResult, error) {
	if f.IsRateLimited() {
		slog.Debug("rate limited; skipping cdn fetch")
		return FetchResult{Status: 429}, nil
	}
	if !f.fetchMu.TryLock() {
		slog.Debug("fetch already in progress; returning 304")
		return FetchResult{Status: 304}, nil
	}
	defer f.fetchMu.Unlock()
	return f.Retry.Execute(ctx, f.doFetch)
}

func (f *CDNFetcher) doFetch(ctx context.Context) (FetchResult, error) {
	f.mu.Lock()
	etag := f.etag
	lastModified := f.lastModified
	lastModifiedAt := f.lastModifiedAt
	f.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.cdnURL, nil)
	if err != nil {
		return FetchResult{}, fmt.Errorf("building cdn request: %w", err)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return FetchResult{}, fmt.Errorf("cdn request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	status := resp.StatusCode

	if status == 304 {
		return FetchResult{Status: 304, ETag: etag, LastModified: lastModified}, nil
	}

	if status == 429 {
		delay := ParseRetryAfterDuration(resp.Header.Get("Retry-After"), f.clock())
		f.mu.Lock()
		f.rateLimitedUntil = f.clock().Add(delay)
		f.mu.Unlock()
		slog.Warn("cdn rate limited (429); pausing fetches", "delay_ms", delay.Milliseconds())
		return FetchResult{Status: 429}, nil
	}

	newETag := resp.Header.Get("ETag")
	newLastModifiedStr := resp.Header.Get("Last-Modified")

	if status >= 200 && status < 300 {
		// Stale-node defense: reject 200 if Last-Modified is not strictly newer (providers.md §5.7).
		if newLastModifiedStr != "" {
			newLM, ok := ParseHTTPDate(newLastModifiedStr)
			if ok && !newLM.After(lastModifiedAt) {
				slog.Warn("cdn 200 with non-newer Last-Modified; treating as 304 (suspected stale node)",
					"received", newLastModifiedStr)
				return FetchResult{Status: 304, ETag: etag, LastModified: lastModified}, nil
			}
			if ok {
				f.mu.Lock()
				f.etag = newETag
				f.lastModified = newLastModifiedStr
				f.lastModifiedAt = newLM
				f.mu.Unlock()
			}
		} else {
			f.mu.Lock()
			f.etag = newETag
			f.mu.Unlock()
		}

		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return FetchResult{}, fmt.Errorf("reading cdn response body: %w", readErr)
		}
		return FetchResult{Status: status, Body: string(body), ETag: newETag, LastModified: newLastModifiedStr}, nil
	}

	return FetchResult{Status: status}, nil
}

func ParseRetryAfterDuration(header string, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return DefaultRetryAfterMS * time.Millisecond
	}
	if secs, err := strconv.ParseInt(header, 10, 64); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, ok := ParseHTTPDate(header); ok {
		d := t.Sub(now)
		if d < 0 {
			return 0
		}
		return d
	}
	return DefaultRetryAfterMS * time.Millisecond
}

func ParseHTTPDate(s string) (time.Time, bool) {
	t, err := http.ParseTime(strings.TrimSpace(s))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
