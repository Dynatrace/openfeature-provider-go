package dynatrace

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPollingManager_NotModified(t *testing.T) {
	var successCalls int32
	pm := newPollingManager(
		func() (FetchResult, error) { return FetchResult{Status: 304}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) { t.Error("emitChange should not be called on 304") },
		func() { atomic.AddInt32(&successCalls, 1) },
		func(string) { t.Error("emitFailure should not be called on 304") },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(35 * time.Millisecond)
	pm.Stop()

	if n := atomic.LoadInt32(&successCalls); n == 0 {
		t.Error("emitSuccess should be called on 304")
	}
}

func TestPollingManager_Success_EmitsChangeAndSuccess(t *testing.T) {
	var changeCalls, successCalls int32
	pm := newPollingManager(
		func() (FetchResult, error) {
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

	if atomic.LoadInt32(&successCalls) == 0 {
		t.Error("emitSuccess should be called on 200")
	}
	if atomic.LoadInt32(&changeCalls) == 0 {
		t.Error("emitChange should be called on 200")
	}
}

func TestPollingManager_RateLimited_Silent(t *testing.T) {
	var failureCalls int32
	pm := newPollingManager(
		func() (FetchResult, error) { return FetchResult{Status: 429}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) {},
		func() {},
		func(string) { atomic.AddInt32(&failureCalls, 1) },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()

	if n := atomic.LoadInt32(&failureCalls); n != 0 {
		t.Errorf("emitFailure should not be called on 429, got %d calls", n)
	}
}

func TestPollingManager_NonSuccess_EmitsFailure(t *testing.T) {
	var failureCalls int32
	pm := newPollingManager(
		func() (FetchResult, error) { return FetchResult{Status: 503}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) {},
		func() { t.Error("emitSuccess should not be called on 503") },
		func(string) { atomic.AddInt32(&failureCalls, 1) },
		10*time.Millisecond,
	)
	pm.Start()
	time.Sleep(25 * time.Millisecond)
	pm.Stop()

	if atomic.LoadInt32(&failureCalls) == 0 {
		t.Error("emitFailure should be called on 503")
	}
}

func TestPollingManager_Stop_Idempotent(t *testing.T) {
	pm := newPollingManager(
		func() (FetchResult, error) { return FetchResult{Status: 304}, nil },
		func(string) ([]string, error) { return nil, nil },
		func([]string) {},
		func() {},
		func(string) {},
		100*time.Millisecond,
	)
	pm.Start()
	pm.Stop()
	pm.Stop() // should not panic
}
