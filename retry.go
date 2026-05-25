package dynatrace

import (
	"fmt"
	"math/rand"
	"time"
)

// FetchResult holds the outcome of a CDN fetch attempt.
type FetchResult struct {
	Status       int
	Body         string
	ETag         string
	LastModified string
}

func (r FetchResult) IsNotModified() bool { return r.Status == 304 }
func (r FetchResult) IsRateLimited() bool { return r.Status == 429 }
func (r FetchResult) IsSuccess() bool     { return r.Status >= 200 && r.Status < 300 }


const (
	retryMaxAttempts    = 5
	retryBaseDelayMS    = 100.0
	retryMultiplier     = 2.0
	retryMaxJitterFactor = 0.20
)

// RetryStrategy applies exponential backoff with jitter for transient HTTP failures.
// Retries on 5xx and network errors. Does not retry 4xx (429 is handled by CDNFetcher).
type RetryStrategy struct {
	maxAttempts     int
	baseDelayMS     float64
	multiplier      float64
	maxJitterFactor float64
	sleep           func(time.Duration)
	rng             *rand.Rand
}

func newRetryStrategy() *RetryStrategy {
	return &RetryStrategy{
		maxAttempts:     retryMaxAttempts,
		baseDelayMS:     retryBaseDelayMS,
		multiplier:      retryMultiplier,
		maxJitterFactor: retryMaxJitterFactor,
		sleep:           time.Sleep,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
	}
}

// ShouldRetry returns true for 5xx responses.
func ShouldRetry(status int) bool {
	return status >= 500 && status < 600
}

// Execute calls fn with retry on 5xx or network error.
func (r *RetryStrategy) Execute(fn func() (FetchResult, error)) (FetchResult, error) {
	var lastErr error
	var lastResult FetchResult
	delayMS := r.baseDelayMS

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		result, err := fn()
		if err != nil {
			lastErr = err
		} else {
			lastResult = result
			if !ShouldRetry(result.Status) {
				return result, nil
			}
			lastErr = fmt.Errorf("HTTP %d", result.Status)
		}

		if attempt < r.maxAttempts {
			jitter := delayMS * r.rng.Float64() * r.maxJitterFactor
			r.sleep(time.Duration(delayMS+jitter) * time.Millisecond)
			delayMS *= r.multiplier
		}
	}

	return lastResult, lastErr
}
