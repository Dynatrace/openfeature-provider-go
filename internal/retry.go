package internal

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

const (
	// RetryMaxAttempts is one initial attempt plus one retry. It is deliberately not a loop:
	// a fetch must not outlive the shortest poll interval (providers.md §2.1/§2.4), and
	// recovery beyond the single retry is the next poll tick's job. This applies to the
	// initial fetch too, which therefore gives up after MaxFetchDuration rather than
	// retrying for half a minute.
	RetryMaxAttempts     = 2
	RetryBaseDelayMS     = 100.0
	RetryMultiplier      = 2.0
	RetryMaxJitterFactor = 0.20
)

// MaxFetchDuration is the worst-case wall-clock duration of one fetch: every attempt runs to
// its timeout and every backoff lands at maximum jitter. providers.md §2.1 makes
// "MaxFetchDuration < shortest poll interval" an invariant, so two polls can never be in
// flight at once; any change to the timeout, attempt count or backoff must preserve it.
func MaxFetchDuration() time.Duration {
	total := time.Duration(RetryMaxAttempts) * RequestTimeout
	delayMS := RetryBaseDelayMS
	for attempt := 1; attempt < RetryMaxAttempts; attempt++ {
		total += time.Duration(delayMS * (1 + RetryMaxJitterFactor) * float64(time.Millisecond))
		delayMS *= RetryMultiplier
	}
	return total
}

// RetryStrategy applies backoff with jitter for transient HTTP failures. Retries on 5xx and
// network errors; does not retry 4xx, including 429 (a rate-limit window suppresses later poll
// ticks instead of holding this fetch open; see providers.md §2.3).
type RetryStrategy struct {
	MaxAttempts     int
	BaseDelayMS     float64
	Multiplier      float64
	MaxJitterFactor float64
	NewTimer        func(time.Duration) *time.Timer
}

func NewRetryStrategy() *RetryStrategy {
	return &RetryStrategy{
		MaxAttempts:     RetryMaxAttempts,
		BaseDelayMS:     RetryBaseDelayMS,
		Multiplier:      RetryMultiplier,
		MaxJitterFactor: RetryMaxJitterFactor,
		NewTimer:        time.NewTimer,
	}
}

func ShouldRetry(status int) bool {
	return status >= 500 && status < 600
}

// Execute calls fn with retry on 5xx or network error.
// Respects ctx cancellation between attempts.
func (r *RetryStrategy) Execute(ctx context.Context, fetch func(context.Context) (FetchResult, error)) (FetchResult, error) {
	var lastErr error
	var lastResult FetchResult
	delayMS := r.BaseDelayMS

	for attempt := 1; attempt <= r.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return FetchResult{}, ctx.Err()
		}

		result, err := fetch(ctx)
		if err != nil {
			// A cancelled attempt says nothing about the CDN; do not spend the retry on it.
			if ctx.Err() != nil {
				return FetchResult{}, ctx.Err()
			}
			lastErr = err
		} else {
			lastResult = result
			if !ShouldRetry(result.Status) {
				return result, nil
			}
			lastErr = fmt.Errorf("HTTP %d", result.Status)
		}

		if attempt < r.MaxAttempts {
			jitter := delayMS * rand.Float64() * r.MaxJitterFactor
			delay := time.Duration(delayMS+jitter) * time.Millisecond
			slog.Warn("cdn fetch attempt failed; retrying",
				"attempt", attempt, "max", r.MaxAttempts, "error", lastErr)
			t := r.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return FetchResult{}, ctx.Err()
			}
			delayMS *= r.Multiplier
		}
	}

	if lastErr != nil {
		return lastResult, lastErr
	}
	return lastResult, nil
}
