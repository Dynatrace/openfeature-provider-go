package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// firedTimer returns a timer whose channel has already fired, so select cases
// pick it up immediately. Useful for skipping the backoff wait in tests.
func firedTimer(_ time.Duration) *time.Timer {
	t := time.NewTimer(0)
	return t
}

func seededRetry() *RetryStrategy {
	return &RetryStrategy{
		MaxAttempts:     RetryMaxAttempts,
		BaseDelayMS:     RetryBaseDelayMS,
		MaxJitterFactor: RetryMaxJitterFactor,
		NewTimer:        firedTimer,
	}
}

func TestRetry_SuccessFirstAttempt(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 200, Body: "ok"}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 200, result.Status)
}

func TestRetry_RetriesOn5xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		if calls < 2 {
			return FetchResult{Status: 503}, nil
		}
		return FetchResult{Status: 200, Body: "ok"}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	assert.Equal(t, 200, result.Status)
}

// providers.md §2.4: one retry, not a loop. Recovery past the single retry is the next tick's job.
func TestRetry_StopsAfterOneRetry(t *testing.T) {
	assert.Equal(t, 2, RetryMaxAttempts, "spec caps a fetch at an initial attempt plus one retry")

	r := seededRetry()
	calls := 0
	_, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 503}, nil
	})
	require.Error(t, err)
	assert.Equal(t, 2, calls)
}

// providers.md §2.3: a 429 ends the fetch; it is never retried within the same fetch.
func TestRetry_NoRetryOn429(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 429}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 429, result.Status)
}

// providers.md §2.1: max fetch duration must stay under the shortest poll interval (10s).
func TestMaxFetchDuration_UnderShortestPollInterval(t *testing.T) {
	got := MaxFetchDuration()
	assert.Equal(t, 8120*time.Millisecond, got)
	assert.Less(t, got, 10*time.Second, "a fetch must never outlive the shortest poll interval")
}

func TestRetry_NoRetryOn4xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 404}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 404, result.Status)
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
	r := seededRetry()
	calls := 0
	_, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{}, errors.New("network error")
	})
	require.Error(t, err)
	assert.Equal(t, RetryMaxAttempts, calls)
}

func TestRetry_ExhaustsOn5xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	_, err := r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 500}, nil
	})
	require.Error(t, err)
	assert.Equal(t, RetryMaxAttempts, calls)
}

func TestRetry_SingleBackoffWithinJitterBounds(t *testing.T) {
	r := seededRetry()
	var delays []time.Duration
	r.NewTimer = func(d time.Duration) *time.Timer {
		delays = append(delays, d)
		return time.NewTimer(0)
	}

	_, _ = r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		return FetchResult{Status: 500}, nil
	})

	require.Len(t, delays, RetryMaxAttempts-1, "one backoff, before the single retry")
	base := time.Duration(RetryBaseDelayMS) * time.Millisecond
	assert.GreaterOrEqual(t, delays[0], base)
	assert.LessOrEqual(t, delays[0], time.Duration(RetryBaseDelayMS*(1+RetryMaxJitterFactor))*time.Millisecond)
}

func TestRetry_ContextCancelledDuringSleep(t *testing.T) {
	r := seededRetry()
	r.NewTimer = time.NewTimer

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := r.Execute(ctx, func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 500}, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, calls, RetryMaxAttempts, "should stop retrying when context is cancelled")
}

func TestShouldRetry(t *testing.T) {
	cases := []struct {
		status int
		retry  bool
	}{
		{200, false}, {304, false}, {400, false}, {404, false}, {429, false}, {499, false},
		{500, true}, {503, true}, {599, true}, {600, false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.retry, ShouldRetry(tc.status), "ShouldRetry(%d)", tc.status)
	}
}
