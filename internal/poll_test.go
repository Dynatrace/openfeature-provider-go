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
	cancel, doneChan := Poll(context.Background(),
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
	cancel, doneChan := Poll(context.Background(),
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
	cancel, doneChan := Poll(context.Background(),
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

func TestPoll_NonSuccess_EmitsFailure(t *testing.T) {
	successChan := make(chan string, 8)
	failureChan := make(chan string, 8)
	cancel, doneChan := Poll(context.Background(),
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
	cancel, doneChan := Poll(context.Background(),
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
