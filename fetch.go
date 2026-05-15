package dynatrace

import (
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
	requestTimeout      = 5 * time.Second
	defaultRetryAfterMS = 60_000
)

// FetchResult holds the outcome of a single CDN fetch attempt.
type FetchResult struct {
	Status       int
	Body         string
	ETag         string
	LastModified string
}

// IsNotModified returns true when the CDN reports no change (304).
func (r FetchResult) IsNotModified() bool { return r.Status == 304 }

// IsRateLimited returns true for a 429 response.
func (r FetchResult) IsRateLimited() bool { return r.Status == 429 }

// IsSuccess returns true for 2xx responses.
func (r FetchResult) IsSuccess() bool { return r.Status >= 200 && r.Status < 300 }

// CDNFetcher performs conditional HTTP GETs against the Dynatrace CDN.
// It stores ETag and Last-Modified from successful responses and sends
// If-None-Match / If-Modified-Since on subsequent requests.
// Concurrent calls while a fetch is in progress return a synthetic 304.
type CDNFetcher struct {
	options Options
	retry   *RetryStrategy
	client  *http.Client
	clock   func() time.Time

	mu              sync.Mutex
	etag            string
	lastModified    string
	lastModifiedAt  time.Time
	hasLastModified bool
	rateLimitedUntil time.Time

	fetchMu sync.Mutex
}

func newCDNFetcher(options Options) *CDNFetcher {
	return &CDNFetcher{
		options: options,
		retry:   newRetryStrategy(),
		client:  &http.Client{Timeout: requestTimeout},
		clock:   time.Now,
	}
}

// IsRateLimited returns true if a 429 Retry-After window is still active.
func (f *CDNFetcher) IsRateLimited() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clock().Before(f.rateLimitedUntil)
}

// Fetch performs a conditional GET with retry. Returns a synthetic 429 when
// rate-limited and a synthetic 304 when another fetch is already in flight.
func (f *CDNFetcher) Fetch() (FetchResult, error) {
	if f.IsRateLimited() {
		slog.Debug("rate limited; skipping cdn fetch")
		return FetchResult{Status: 429}, nil
	}
	if !f.fetchMu.TryLock() {
		slog.Debug("fetch already in progress; returning 304")
		return FetchResult{Status: 304}, nil
	}
	defer f.fetchMu.Unlock()
	return f.retry.Execute(f.doFetch)
}

func (f *CDNFetcher) doFetch() (FetchResult, error) {
	f.mu.Lock()
	etag := f.etag
	lastModified := f.lastModified
	lastModifiedAt := f.lastModifiedAt
	hasLastModified := f.hasLastModified
	f.mu.Unlock()

	req, err := http.NewRequest(http.MethodGet, f.options.CDNURL(), nil)
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
		retryAfter := resp.Header.Get("Retry-After")
		delay := parseRetryAfterDuration(retryAfter)
		f.mu.Lock()
		f.rateLimitedUntil = f.clock().Add(delay)
		f.mu.Unlock()
		slog.Warn("cdn rate limited (429); pausing fetches", "delay_ms", delay.Milliseconds())
		return FetchResult{Status: 429}, nil
	}

	newETag := resp.Header.Get("ETag")
	newLastModifiedStr := resp.Header.Get("Last-Modified")

	if status >= 200 && status < 300 {
		// Stale-node defense: a CDN edge node can return 200 with an older body + older ETag
		// even after a newer version exists. Reject any 200 whose Last-Modified is not strictly
		// newer than what we already have (per providers.md §5.7).
		if newLastModifiedStr != "" {
			newLM, ok := parseHTTPDate(newLastModifiedStr)
			if ok && hasLastModified && !newLM.After(lastModifiedAt) {
				slog.Warn("cdn 200 with non-newer Last-Modified; treating as 304 (suspected stale node)",
					"received", newLastModifiedStr)
				return FetchResult{Status: 304, ETag: etag, LastModified: lastModified}, nil
			}
			if ok {
				f.mu.Lock()
				f.etag = newETag
				f.lastModified = newLastModifiedStr
				f.lastModifiedAt = newLM
				f.hasLastModified = true
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
		return FetchResult{
			Status:       status,
			Body:         string(body),
			ETag:         newETag,
			LastModified: newLastModifiedStr,
		}, nil
	}

	return FetchResult{Status: status}, nil
}

// parseRetryAfterDuration parses the Retry-After header as seconds or HTTP-date.
func parseRetryAfterDuration(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return defaultRetryAfterMS * time.Millisecond
	}
	if secs, err := strconv.ParseInt(header, 10, 64); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, ok := parseHTTPDate(header); ok {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		return d
	}
	return defaultRetryAfterMS * time.Millisecond
}

// parseHTTPDate parses an RFC 7231 / RFC 1123 HTTP-date.
func parseHTTPDate(s string) (time.Time, bool) {
	t, err := http.ParseTime(strings.TrimSpace(s))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
