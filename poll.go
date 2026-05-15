package dynatrace

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

// PollingManager drives periodic flag-config polling.
// It calls fetchFlags on a fixed interval and routes outcomes to callbacks.
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

// newPollingManager creates a PollingManager. Call Start to begin polling.
func newPollingManager(
	fetchFlags func() (FetchResult, error),
	storeFlags func(string) ([]string, error),
	emitChange func([]string),
	emitSuccess func(),
	emitFailure func(string),
	interval time.Duration,
) *PollingManager {
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

// Start launches the polling goroutine. Each poll fires after the previous one
// completes plus the configured interval (fixed delay, not fixed rate).
func (pm *PollingManager) Start() {
	go pm.run()
}

// Stop cancels the polling goroutine. Idempotent.
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

	// 429: rate limited; continue serving cached values (providers.md §2.3)
	if result.IsRateLimited() {
		slog.Debug("poll: rate limited; serving cached values")
		return
	}

	if !result.IsSuccess() {
		reason := "HTTP " + itoa(result.Status)
		slog.Warn("poll: non-success response", "status", result.Status)
		pm.emitFailure(reason)
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

	// Always emit a config-change event on a successful 2xx CDN response: if the CDN
	// returned 200 (not 304), the flag configuration has changed. Specific key tracking
	// would require flagd store internals not exposed via IEvaluator; FlagChanges is
	// left nil (meaning "all flags may have changed"), which is valid per the OF spec.
	pm.emitChange(changedKeys)
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
