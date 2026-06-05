package internal

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

const minPollInterval = time.Millisecond

// PollingManager drives periodic flag-config polling.
type PollingManager struct {
	fetchFlags  func() (FetchResult, error)
	storeFlags  func(string) ([]string, error)
	emitChange  func([]string)
	emitSuccess func()
	emitFailure func(string)
	interval    time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPollingManager(
	fetchFlags func() (FetchResult, error),
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

func (pm *PollingManager) Start() {
	go pm.run()
}

func (pm *PollingManager) Stop() {
	pm.cancel()
}

func (pm *PollingManager) run() {
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pm.poll()
		case <-pm.ctx.Done():
			return
		}
	}
}

func (pm *PollingManager) poll() {
	result, err := pm.fetchFlags()
	if err != nil {
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
