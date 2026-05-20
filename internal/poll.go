package internal

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

const minPollInterval = time.Millisecond

// PollingManager drives periodic flag-config polling.
//
// The ticker fires every pollInterval and runs each poll synchronously with its
// own child context. Stop() cancels any in-flight fetch immediately by cancelling
// the child context, then cancels the overall lifecycle context to exit the loop.
//
// The ticker cadence is anchored to request initiation: time.NewTicker sends ticks
// at regular wall-clock intervals regardless of how long poll() takes, so the
// effective cadence is fixed-rate as long as polls finish well within the interval
// (which holds in production: 10s interval, 5s per-request timeout, 5 retries ≈ 1.6s
// worst-case, see providers.md §2.1).
type PollingManager struct {
	fetchFlags  func(context.Context) (FetchResult, error)
	storeFlags  func(string) ([]string, error)
	emitChange  func([]string)
	emitSuccess func()
	emitFailure func(string)
	interval    time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	pollCancelMu sync.Mutex
	pollCancel   context.CancelFunc
}

func NewPollingManager(
	fetchFlags func(context.Context) (FetchResult, error),
	storeFlags func(string) ([]string, error),
	emitChange func([]string),
	emitSuccess func(),
	emitFailure func(string),
	interval time.Duration,
) *PollingManager {
	if interval < minPollInterval {
		interval = minPollInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PollingManager{
		fetchFlags:  fetchFlags,
		storeFlags:  storeFlags,
		emitChange:  emitChange,
		emitSuccess: emitSuccess,
		emitFailure: emitFailure,
		interval:    interval,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start launches the polling goroutine.
func (pm *PollingManager) Start() {
	go pm.run()
}

// Stop cancels any in-flight fetch immediately and exits the polling loop.
func (pm *PollingManager) Stop() {
	pm.cancelCurrentPoll()
	pm.cancel()
}

func (pm *PollingManager) run() {
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithCancel(pm.ctx)
			pm.pollCancelMu.Lock()
			pm.pollCancel = cancel
			pm.pollCancelMu.Unlock()

			pm.poll(ctx)

			cancel()
			pm.pollCancelMu.Lock()
			pm.pollCancel = nil
			pm.pollCancelMu.Unlock()

		case <-pm.ctx.Done():
			return
		}
	}
}

func (pm *PollingManager) cancelCurrentPoll() {
	pm.pollCancelMu.Lock()
	defer pm.pollCancelMu.Unlock()
	if pm.pollCancel != nil {
		pm.pollCancel()
		pm.pollCancel = nil
	}
}

func (pm *PollingManager) poll(ctx context.Context) {
	result, err := pm.fetchFlags(ctx)

	if pm.ctx.Err() != nil {
		slog.Debug("poll: manager stopped; discarding result")
		return
	}

	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			slog.Debug("poll: cancelled; discarding result")
			return
		}
		slog.Warn("poll: fetch failed", "error", err)
		pm.emitFailure(err.Error())
		return
	}

	if result.IsNotModified() {
		slog.Debug("poll: flags not modified")
		pm.emitSuccess()
		return
	}

	if result.IsRateLimited() {
		slog.Debug("poll: rate limited; serving cached values")
		return
	}

	if !result.IsSuccess() {
		slog.Warn("poll: non-success response", "status", result.Status)
		pm.emitFailure("HTTP " + strconv.Itoa(result.Status))
		return
	}

	if result.Body == "" {
		slog.Warn("poll: empty response body")
		pm.emitFailure("empty response body")
		return
	}

	changedKeys, storeErr := pm.storeFlags(result.Body)
	if storeErr != nil {
		slog.Warn("poll: failed to store flags", "error", storeErr)
		pm.emitFailure("failed to store flags: " + storeErr.Error())
		return
	}

	pm.emitSuccess()
	pm.emitChange(changedKeys)
}
