package internal

import (
	"context"
	"fmt"
	"math/rand"
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
	Sleep           func(time.Duration)
	Rng             *rand.Rand
}

func NewRetryStrategy() *RetryStrategy {
	return &RetryStrategy{
		MaxAttempts:     RetryMaxAttempts,
		BaseDelayMS:     RetryBaseDelayMS,
		Multiplier:      RetryMultiplier,
		MaxJitterFactor: RetryMaxJitterFactor,
		Sleep:           time.Sleep,
		Rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func ShouldRetry(status int) bool {
	return status >= 500 && status < 600
}

// Execute calls fn with retry on 5xx or network error.
// Respects ctx cancellation between attempts — a cancelled context stops retrying immediately.
func (r *RetryStrategy) Execute(ctx context.Context, fn func(context.Context) (FetchResult, error)) (FetchResult, error) {
	var lastErr error
	var lastResult FetchResult
	delayMS := r.BaseDelayMS

	for attempt := 1; attempt <= r.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return FetchResult{}, ctx.Err()
		}

		result, err := fn(ctx)
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
			jitter := delayMS * r.Rng.Float64() * r.MaxJitterFactor
			delay := time.Duration(delayMS+jitter) * time.Millisecond
			done := make(chan struct{})
			go func() { r.Sleep(delay); close(done) }()
			select {
			case <-done:
			case <-ctx.Done():
				return FetchResult{}, ctx.Err()
			}
			delayMS *= r.Multiplier
		}
	}

	if lastErr != nil {
		return FetchResult{}, lastErr
	}
	return lastResult, nil
}
