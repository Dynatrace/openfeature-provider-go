package internal

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	RetryMaxAttempts    = 5
	RetryBaseDelayMS    = 100.0
	RetryMultiplier     = 2.0
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

func (r *RetryStrategy) Execute(fn func() (FetchResult, error)) (FetchResult, error) {
	var lastErr error
	var lastResult FetchResult
	delayMS := r.BaseDelayMS

	for attempt := 1; attempt <= r.MaxAttempts; attempt++ {
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

		if attempt < r.MaxAttempts {
			jitter := delayMS * r.Rng.Float64() * r.MaxJitterFactor
			r.Sleep(time.Duration(delayMS+jitter) * time.Millisecond)
			delayMS *= r.Multiplier
		}
	}

	if lastErr != nil {
		return FetchResult{}, lastErr
	}
	return lastResult, nil
}
