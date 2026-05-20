package internal

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noopSleep(_ time.Duration) {}

func seededRetry() *RetryStrategy {
	return &RetryStrategy{
		MaxAttempts:     RetryMaxAttempts,
		BaseDelayMS:     RetryBaseDelayMS,
		Multiplier:      RetryMultiplier,
		MaxJitterFactor: RetryMaxJitterFactor,
		Sleep:           noopSleep,
		Rng:             rand.New(rand.NewSource(42)),
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
		if calls < 3 {
			return FetchResult{Status: 503}, nil
		}
		return FetchResult{Status: 200, Body: "ok"}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
	assert.Equal(t, 200, result.Status)
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

func TestRetry_DelayGrowsExponentially(t *testing.T) {
	r := seededRetry()
	var delays []time.Duration
	r.Sleep = func(d time.Duration) { delays = append(delays, d) }
	r.Rng = rand.New(rand.NewSource(0))

	_, _ = r.Execute(context.Background(), func(_ context.Context) (FetchResult, error) {
		return FetchResult{Status: 500}, nil
	})

	require.Len(t, delays, RetryMaxAttempts-1)
	for i := 1; i < len(delays); i++ {
		assert.Greater(t, delays[i], delays[i-1])
	}
}

func TestRetry_ContextCancelledDuringSleep(t *testing.T) {
	r := seededRetry()
	r.Sleep = time.Sleep // use real sleep so cancellation works
	r.Rng = rand.New(rand.NewSource(0)) //nolint:gosec

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := r.Execute(ctx, func(_ context.Context) (FetchResult, error) {
		calls++
		return FetchResult{Status: 500}, nil // always fail → triggers retry sleep
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
