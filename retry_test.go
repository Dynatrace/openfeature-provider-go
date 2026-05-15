package dynatrace

import (
	"errors"
	"math/rand"
	"testing"
	"time"
)

func noopSleep(_ time.Duration) {}

func seededRetry() *RetryStrategy {
	return &RetryStrategy{
		maxAttempts:     retryMaxAttempts,
		baseDelayMS:     retryBaseDelayMS,
		multiplier:      retryMultiplier,
		maxJitterFactor: retryMaxJitterFactor,
		sleep:           noopSleep,
		rng:             rand.New(rand.NewSource(42)),
	}
}

func TestRetry_SuccessFirstAttempt(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(func() (FetchResult, error) {
		calls++
		return FetchResult{Status: 200, Body: "ok"}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if result.Status != 200 {
		t.Errorf("status = %d, want 200", result.Status)
	}
}

func TestRetry_RetriesOn5xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(func() (FetchResult, error) {
		calls++
		if calls < 3 {
			return FetchResult{Status: 503}, nil
		}
		return FetchResult{Status: 200, Body: "ok"}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if result.Status != 200 {
		t.Errorf("status = %d, want 200", result.Status)
	}
}

func TestRetry_NoRetryOn4xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(func() (FetchResult, error) {
		calls++
		return FetchResult{Status: 404}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if result.Status != 404 {
		t.Errorf("status = %d, want 404", result.Status)
	}
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
	r := seededRetry()
	calls := 0
	_, err := r.Execute(func() (FetchResult, error) {
		calls++
		return FetchResult{}, errors.New("network error")
	})
	if err == nil {
		t.Fatal("expected error after exhausted attempts")
	}
	if calls != retryMaxAttempts {
		t.Errorf("calls = %d, want %d", calls, retryMaxAttempts)
	}
}

func TestRetry_ExhaustsOn5xx(t *testing.T) {
	r := seededRetry()
	calls := 0
	result, err := r.Execute(func() (FetchResult, error) {
		calls++
		return FetchResult{Status: 500}, nil
	})
	if err == nil {
		t.Fatal("expected error after exhausted 5xx attempts")
	}
	_ = result
	if calls != retryMaxAttempts {
		t.Errorf("calls = %d, want %d", calls, retryMaxAttempts)
	}
}

func TestRetry_DelayGrowsExponentially(t *testing.T) {
	r := seededRetry()
	var delays []time.Duration
	r.sleep = func(d time.Duration) { delays = append(delays, d) }
	r.rng = rand.New(rand.NewSource(0)) // zero jitter seed

	calls := 0
	_, _ = r.Execute(func() (FetchResult, error) {
		calls++
		return FetchResult{Status: 500}, nil
	})

	// With seed 0 and MAX_JITTER=0.20, jitter should be positive
	// Verify delays grow (not necessarily strictly 2x due to jitter, but increasing)
	if len(delays) != retryMaxAttempts-1 {
		t.Fatalf("expected %d delays, got %d", retryMaxAttempts-1, len(delays))
	}
	for i := 1; i < len(delays); i++ {
		if delays[i] <= delays[i-1] {
			t.Errorf("delays[%d]=%v not > delays[%d]=%v (should grow)", i, delays[i], i-1, delays[i-1])
		}
	}
}

func TestShouldRetry(t *testing.T) {
	cases := []struct {
		status int
		retry  bool
	}{
		{200, false},
		{304, false},
		{400, false},
		{404, false},
		{429, false},
		{499, false},
		{500, true},
		{503, true},
		{599, true},
		{600, false},
	}
	for _, tc := range cases {
		if got := ShouldRetry(tc.status); got != tc.retry {
			t.Errorf("ShouldRetry(%d) = %v, want %v", tc.status, got, tc.retry)
		}
	}
}
