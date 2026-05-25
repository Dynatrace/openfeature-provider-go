package dynatrace

import (
	"fmt"
	"math/rand"
	"time"
)

// fetchResult holds the outcome of a CDN fetch attempt.
type fetchResult struct {
	Status       int
	Body         string
	ETag         string
	LastModified string
}

func (r fetchResult) IsNotModified() bool { return r.Status == 304 }
func (r fetchResult) IsRateLimited() bool { return r.Status == 429 }
func (r fetchResult) IsSuccess() bool     { return r.Status >= 200 && r.Status < 300 }


const (
	retryMaxAttempts    = 5
	retryBaseDelayMS    = 100.0
	retryMultiplier     = 2.0
	retryMaxJitterFactor = 0.20
)

// retryStrategy applies exponential backoff with jitter for transient HTTP failures.
// Retries on 5xx and network errors. Does not retry 4xx (429 is handled by CDNFetcher).
type retryStrategy struct {
	maxAttempts     int
	baseDelayMS     float64
	multiplier      float64
	maxJitterFactor float64
	sleep           func(time.Duration)
	rng             *rand.Rand
}

func newretryStrategy() *retryStrategy {
	return &retryStrategy{
		maxAttempts:     retryMaxAttempts,
		baseDelayMS:     retryBaseDelayMS,
		multiplier:      retryMultiplier,
		maxJitterFactor: retryMaxJitterFactor,
		sleep:           time.Sleep,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
	}
}

// shouldRetry returns true for 5xx responses.
func shouldRetry(status int) bool {
	return status >= 500 && status < 600
}

// Execute calls fn with retry on 5xx or network error.
func (r *retryStrategy) Execute(fn func() (fetchResult, error)) (fetchResult, error) {
	var lastErr error
	var lastResult fetchResult
	delayMS := r.baseDelayMS

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		result, err := fn()
		if err != nil {
			lastErr = err
		} else {
			lastResult = result
			if !shouldRetry(result.Status) {
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
