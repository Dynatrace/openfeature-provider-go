package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

func (r FetchResult) IsNotModified() bool { return r.Status == http.StatusNotModified }
func (r FetchResult) IsRateLimited() bool { return r.Status == http.StatusTooManyRequests }
func (r FetchResult) IsSuccess() bool {
	return r.Status >= http.StatusOK && r.Status < http.StatusMultipleChoices
}

// CDNFetcher performs conditional HTTP GETs against the Dynatrace CDN.
type CDNFetcher struct {
	cdnURL string
	Retry  *RetryStrategy
	client *http.Client
	clock  func() time.Time

	// 1-buffered channel acting as a ctx-aware mutex (semaphore)
	sem chan struct{}

	headerETag           string
	headerLastModified   string
	storedLastModifiedAt time.Time
	rateLimitedUntil     time.Time
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
		sem:    make(chan struct{}, 1),
	}
}

// Fetch performs a conditional GET with retry.Respects ctx cancellation
func (f *CDNFetcher) Fetch(ctx context.Context) (FetchResult, error) {
	select {
	case f.sem <- struct{}{}:
	case <-ctx.Done():
		return FetchResult{}, ctx.Err()
	}
	defer func() { <-f.sem }()

	if f.clock().Before(f.rateLimitedUntil) {
		slog.Debug("rate limited; skipping cdn fetch")
		return FetchResult{Status: http.StatusTooManyRequests}, nil
	}
	return f.Retry.Execute(ctx, f.doFetch)
}

func (f *CDNFetcher) doFetch(ctx context.Context) (FetchResult, error) {
	etag := f.headerETag
	lastModified := f.headerLastModified
	lastModifiedAt := f.storedLastModifiedAt

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
		_ = resp.Body.Close()
	}()

	status := resp.StatusCode

	if status == http.StatusNotModified {
		return FetchResult{Status: http.StatusNotModified, ETag: etag, LastModified: lastModified}, nil
	}

	if status == http.StatusTooManyRequests {
		delay := ParseRetryAfterDuration(resp.Header.Get("Retry-After"), f.clock())
		f.rateLimitedUntil = f.clock().Add(delay)
		slog.Warn("cdn rate limited (429); pausing fetches", "delay_ms", delay.Milliseconds())
		return FetchResult{Status: http.StatusTooManyRequests}, nil
	}

	newETag := resp.Header.Get("ETag")
	newLastModifiedStr := resp.Header.Get("Last-Modified")

	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		// Stale-node defense: reject 200 if Last-Modified is not strictly newer (providers.md §5.7).
		var newLastModifiedAt time.Time
		var lastModifiedParsed bool
		if newLastModifiedStr != "" {
			newLastModifiedAt, lastModifiedParsed = ParseHTTPDate(newLastModifiedStr)
			if lastModifiedParsed && !newLastModifiedAt.After(lastModifiedAt) {
				slog.Warn("cdn 200 with non-newer Last-Modified; treating as 304 (suspected stale node)",
					"received", newLastModifiedStr)
				return FetchResult{Status: http.StatusNotModified, ETag: etag, LastModified: lastModified}, nil
			}
		}

		f.headerETag = newETag
		if lastModifiedParsed {
			f.headerLastModified = newLastModifiedStr
			f.storedLastModifiedAt = newLastModifiedAt
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
