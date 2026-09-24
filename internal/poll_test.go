package internal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoll_NotModified(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 304}, nil },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	cancel()
	<-doneChan

	assert.Greater(t, len(successChan), 0, "should have at least one success on 304")
	for body := range successChan {
		assert.Empty(t, body, "304 should produce empty body")
	}
	assert.Equal(t, 0, len(failureChan))
}

func TestPoll_Success_CarriesBody(t *testing.T) {
	const body = `{"flags":{}}`
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) {
			return FetchResult{Status: 200, Body: body}, nil
		},
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-doneChan

	assert.Greater(t, len(successChan), 0)
	for got := range successChan {
		assert.Equal(t, body, got)
	}
	assert.Equal(t, 0, len(failureChan))
}

func TestPoll_RateLimited_Silent(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 429}, nil },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-doneChan

	for range successChan {
		t.Fatal("expected no success messages")
	}
	for range failureChan {
		t.Fatal("expected no failure messages")
	}
}

// A skipped fetch made no request, so it is neither a success (which would reset the staleness
// grace timer) nor a failure (it learned nothing about the CDN).
func TestPoll_SkippedResult_EmitsNothing(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: StatusSkipped}, nil },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-doneChan

	for range successChan {
		t.Fatal("expected no success messages")
	}
	for range failureChan {
		t.Fatal("expected no failure messages")
	}
}

func TestPoll_NonSuccess_EmitsFailure(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 503}, nil },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-doneChan

	assert.Equal(t, 0, len(successChan))
	assert.Greater(t, len(failureChan), 0)
	for r := range failureChan {
		assert.Contains(t, r, "503")
	}
}

func TestPoll_CancelInterruptsInFlightFetch(t *testing.T) {
	startedChan := make(chan struct{})
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(ctx context.Context) (FetchResult, error) {
			close(startedChan)
			<-ctx.Done()
			return FetchResult{}, ctx.Err()
		},
		successChan, failureChan, 10*time.Millisecond)
	<-startedChan
	cancel()
	select {
	case <-doneChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Poll did not return within 500ms after ctx cancel")
	}
}

// providers.md §2.1.1: a tick that finds a fetch in flight is skipped and has no observable
// effect. Reporting it as a success would reset the caller's staleness grace timer on every
// tick, holding the provider READY for the whole of a CDN outage.
func TestPoll_SkippedTick_EmitsNothing(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{}, ErrFetchInFlight },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	cancel()
	<-doneChan

	for range successChan {
		t.Fatal("a skipped tick must not report success; that would reset the grace timer")
	}
	for range failureChan {
		t.Fatal("a skipped tick must not report failure; it learned nothing about the CDN")
	}
}
