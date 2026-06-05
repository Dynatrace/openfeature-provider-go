package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func noFetch(_ context.Context) (FetchResult, error) { return FetchResult{Status: 304}, nil }

func TestPollingManager_NotModified(t *testing.T) {
	var successCalls int32
	pm := NewPollingManager(
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 304}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) { t.Error("emitChange should not be called on 304") },
		func() { atomic.AddInt32(&successCalls, 1) },
		func(string) { t.Error("emitFailure should not be called on 304") },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(35 * time.Millisecond)
	pm.Stop()
	assert.Greater(t, atomic.LoadInt32(&successCalls), int32(0))
}

func TestPollingManager_Success_EmitsSuccess_NoChange_WhenNoKeys(t *testing.T) {
	// storeFlags returns nil: success fires but change does not (no keys returned).
	var changeCalls, successCalls int32
	pm := NewPollingManager(
		func(_ context.Context) (FetchResult, error) {
			return FetchResult{Status: 200, Body: `{"flags":{}}`}, nil
		},
		func(string) ([]string, error) { return nil, nil },
		func([]string) { atomic.AddInt32(&changeCalls, 1) },
		func() { atomic.AddInt32(&successCalls, 1) },
		func(string) { t.Error("emitFailure should not be called on 200") },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()
	assert.Greater(t, atomic.LoadInt32(&successCalls), int32(0))
	assert.Equal(t, int32(0), atomic.LoadInt32(&changeCalls), "emitChange must not fire when storeFlags returns nil keys")
}

func TestPollingManager_Success_EmitsChange_WhenKeysReturned(t *testing.T) {
	// storeFlags returns keys: both success and change fire.
	var changeCalls, successCalls int32
	pm := NewPollingManager(
		func(_ context.Context) (FetchResult, error) {
			return FetchResult{Status: 200, Body: `{"flags":{}}`}, nil
		},
		func(string) ([]string, error) { return []string{"flag-a"}, nil },
		func([]string) { atomic.AddInt32(&changeCalls, 1) },
		func() { atomic.AddInt32(&successCalls, 1) },
		func(string) { t.Error("emitFailure should not be called on 200") },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()
	assert.Greater(t, atomic.LoadInt32(&successCalls), int32(0))
	assert.Greater(t, atomic.LoadInt32(&changeCalls), int32(0))
}

func TestPollingManager_RateLimited_Silent(t *testing.T) {
	var failureCalls int32
	pm := NewPollingManager(
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 429}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) {}, func() {},
		func(string) { atomic.AddInt32(&failureCalls, 1) },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()
	assert.Equal(t, int32(0), atomic.LoadInt32(&failureCalls))
}

func TestPollingManager_NonSuccess_EmitsFailure(t *testing.T) {
	var failureCalls int32
	pm := NewPollingManager(
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 503}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) {},
		func() { t.Error("emitSuccess should not be called on 503") },
		func(string) { atomic.AddInt32(&failureCalls, 1) },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()
	assert.Greater(t, atomic.LoadInt32(&failureCalls), int32(0))
}

func TestPollingManager_Stop_Idempotent(t *testing.T) {
	pm := NewPollingManager(noFetch, func(string) ([]string, error) { return nil, nil },
		func([]string) {}, func() {}, func(string) {}, 100*time.Millisecond)
	pm.Start()
	pm.Stop()
	pm.Stop()
}

func TestPollingManager_StopInterruptsInFlightPoll(t *testing.T) {
	started := make(chan struct{})
	done := make(chan struct{})
	pm := NewPollingManager(
		func(ctx context.Context) (FetchResult, error) {
			close(started)
			<-ctx.Done()
			return FetchResult{}, ctx.Err()
		},
		func(string) ([]string, error) { return nil, nil },
		func([]string) {}, func() {}, func(string) {},
		10*time.Millisecond,
	)
	pm.Start()
	<-started
	go func() { pm.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop() did not return within 500ms")
	}
}
