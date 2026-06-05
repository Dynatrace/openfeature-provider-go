package internal

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

const (
	RetryMaxAttempts     = 5
	RetryBaseDelayMS     = 100.0
	RetryMultiplier      = 2.0
	RetryMaxJitterFactor = 0.20
)

// RetryStrategy applies exponential backoff with jitter for transient HTTP failures.
// Retries on 5xx and network errors; does not retry 4xx.
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
