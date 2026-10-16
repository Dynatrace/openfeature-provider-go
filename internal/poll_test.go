package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoll_NotModified(t *testing.T) {
	successChan := make(chan FetchResult, 8)
	failureChan := make(chan string, 8)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := Poll(ctx,
		func(_ context.Context) (FetchResult, error) { return FetchResult{Status: 304}, nil },
		successChan, failureChan, 10*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	cancel()
	<-doneChan

	assert.Greater(t, len(successChan), 0, "should have at least one success on 304")
	for res := range successChan {
		assert.Empty(t, res.Body, "304 should produce empty body")
	}
	assert.Equal(t, 0, len(failureChan))
}

func TestPoll_Success_CarriesBody(t *testing.T) {
	const body = `{"flags":{}}`
	successChan := make(chan FetchResult, 8)
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
		assert.Equal(t, body, got.Body)
	}
	assert.Equal(t, 0, len(failureChan))
}

func TestPoll_RateLimited_Silent(t *testing.T) {
	successChan := make(chan FetchResult, 8)
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
	successChan := make(chan FetchResult, 8)
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
	successChan := make(chan FetchResult, 8)
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
	successChan := make(chan FetchResult, 8)
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

// providers.md 2.1.1: a tick that finds a fetch in flight is skipped and has no observable
// effect. Reporting it as a success would reset the caller's staleness grace timer on every
// tick, holding the provider READY for the whole of a CDN outage.
func TestPoll_SkippedTick_EmitsNothing(t *testing.T) {
	successChan := make(chan FetchResult, 8)
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

// pollerHarness runs a Poller over counting fetch functions and stops it on cleanup.
type pollerHarness struct {
	poller      *Poller
	tickFetches atomic.Int32
	nowFetches  atomic.Int32
	successChan chan FetchResult
	failureChan chan string
	done        <-chan struct{}
	cancel      context.CancelFunc
}

func startPoller(t *testing.T, interval time.Duration, nowFetch func(context.Context) (FetchResult, error)) *pollerHarness {
	t.Helper()
	h := &pollerHarness{successChan: make(chan FetchResult, 64), failureChan: make(chan string, 64)}
	tick := func(_ context.Context) (FetchResult, error) {
		h.tickFetches.Add(1)
		return FetchResult{Status: 304}, nil
	}
	if nowFetch == nil {
		nowFetch = func(_ context.Context) (FetchResult, error) {
			h.nowFetches.Add(1)
			return FetchResult{Status: 304}, nil
		}
	}
	h.poller = NewPoller(tick, nowFetch, h.successChan, h.failureChan, interval)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = h.poller.Start(ctx)
	t.Cleanup(h.stop)
	return h
}

func (h *pollerHarness) stop() {
	h.cancel()
	<-h.done
}

func TestPoller_SetInterval_SpeedsUpTicks(t *testing.T) {
	h := startPoller(t, time.Hour, nil)
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(0), h.tickFetches.Load(), "an hour-long interval must not tick yet")

	h.poller.SetInterval(10 * time.Millisecond)

	assert.Eventually(t, func() bool { return h.tickFetches.Load() >= 3 }, 2*time.Second, 5*time.Millisecond)
}

func TestPoller_SetInterval_SlowsDownTicks(t *testing.T) {
	h := startPoller(t, 10*time.Millisecond, nil)
	require.Eventually(t, func() bool { return h.tickFetches.Load() >= 2 }, 2*time.Second, 5*time.Millisecond)

	h.poller.SetInterval(time.Hour)
	time.Sleep(30 * time.Millisecond) // lets a tick already in flight settle
	settled := h.tickFetches.Load()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, settled, h.tickFetches.Load(), "no tick may follow once the interval is an hour")
}

func TestPoller_SetInterval_LatestWins(t *testing.T) {
	h := startPoller(t, time.Hour, nil)
	// the loop may not have consumed the first value yet; only the last one may take effect
	h.poller.SetInterval(time.Hour)
	h.poller.SetInterval(10 * time.Millisecond)

	assert.Eventually(t, func() bool { return h.tickFetches.Load() >= 2 }, 2*time.Second, 5*time.Millisecond)
}

func TestPoller_Trigger_FetchesOnceThroughFetchNow(t *testing.T) {
	h := startPoller(t, time.Hour, nil)

	h.poller.Trigger()

	require.Eventually(t, func() bool { return h.nowFetches.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, int32(0), h.tickFetches.Load(), "a trigger must not go through the tick fetch")
	select {
	case res := <-h.successChan:
		assert.True(t, res.IsNotModified(), "the triggered result is reported like a poll result")
	case <-time.After(2 * time.Second):
		t.Fatal("triggered fetch reported no result")
	}
}

func TestPoller_Trigger_CollapsesWhilePending(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var calls atomic.Int32
	h := startPoller(t, time.Hour, func(_ context.Context) (FetchResult, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return FetchResult{Status: 304}, nil
	})

	h.poller.Trigger()
	<-entered // first trigger is now running and holds the loop
	h.poller.Trigger()
	h.poller.Trigger()
	h.poller.Trigger() // all three collapse into one pending trigger
	close(release)

	require.Eventually(t, func() bool { return calls.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(2), calls.Load(), "one running + one collapsed pending trigger, no more")
}

func TestPoller_SteeringAfterStopDoesNotPanicOrBlock(t *testing.T) {
	h := startPoller(t, time.Hour, nil)
	h.stop()

	assert.NotPanics(t, func() {
		for range 5 {
			h.poller.SetInterval(time.Second)
			h.poller.Trigger()
		}
	})
}

func TestPoller_ClosesBothChannelsOnStop(t *testing.T) {
	h := startPoller(t, time.Hour, nil)
	h.stop()

	_, okSuccess := <-h.successChan
	_, okFailure := <-h.failureChan
	assert.False(t, okSuccess, "successChan must be closed after stop")
	assert.False(t, okFailure, "failureChan must be closed after stop")
}
