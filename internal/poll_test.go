package internal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoll_NotModified(t *testing.T) {
	success := make(chan string, 8)
	failure := make(chan string, 8)
	cancel, done := Poll(context.Background(),
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 304}, nil },
		success, failure, 10*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	cancel()
	<-done

	assert.Greater(t, len(success), 0, "should have at least one success on 304")
	for body := range success {
		assert.Empty(t, body, "304 should produce empty body")
	}
	assert.Equal(t, 0, len(failure))
}

func TestPoll_Success_CarriesBody(t *testing.T) {
	const body = `{"flags":{}}`
	success := make(chan string, 8)
	failure := make(chan string, 8)
	cancel, done := Poll(context.Background(),
		func(_ context.Context) (FetchResult, error) {
			return FetchResult{Status: 200, Body: body}, nil
		},
		success, failure, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-done

	assert.Greater(t, len(success), 0)
	for got := range success {
		assert.Equal(t, body, got)
	}
	assert.Equal(t, 0, len(failure))
}

func TestPoll_RateLimited_Silent(t *testing.T) {
	success := make(chan string, 8)
	failure := make(chan string, 8)
	cancel, done := Poll(context.Background(),
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 429}, nil },
		success, failure, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-done

	for range success {
		t.Fatal("expected no success messages")
	}
	for range failure {
		t.Fatal("expected no failure messages")
	}
}

func TestPoll_NonSuccess_EmitsFailure(t *testing.T) {
	success := make(chan string, 8)
	failure := make(chan string, 8)
	cancel, done := Poll(context.Background(),
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 503}, nil },
		success, failure, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, 0, len(success))
	assert.Greater(t, len(failure), 0)
	for r := range failure {
		assert.Contains(t, r, "503")
	}
}

func TestPoll_CancelInterruptsInFlightFetch(t *testing.T) {
	started := make(chan struct{})
	success := make(chan string, 8)
	failure := make(chan string, 8)
	cancel, done := Poll(context.Background(),
		func(ctx context.Context) (FetchResult, error) {
			close(started)
			<-ctx.Done()
			return FetchResult{}, ctx.Err()
		},
		success, failure, 10*time.Millisecond)
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Poll did not return within 500ms after ctx cancel")
	}
}
