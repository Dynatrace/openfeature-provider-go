package internal

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultPollInterval is the provider's poll cadence unless overridden. It applies while no SSE stream is
	// connected, so a missed change notification is caught quickly.
	DefaultPollInterval = 10 * time.Second
	// DefaultSSEConnectedPollInterval is the poll cadence while the SSE stream is connected & fetches are
	// successful: notifications indicate a change, so polling is only a safety net (providers.md 2.1).
	DefaultSSEConnectedPollInterval = 10 * time.Minute
	minPollInterval                 = time.Millisecond
)

// Poll launches a goroutine that drives periodic flag-config polling. It runs until ctx is
// cancelled, then returns. Returns a done channel that closes when the goroutine exits; the
// caller owns the context and shuts down by cancelling it then receiving from done.
//
// On each tick the goroutine calls fetchFlags(ctx) and emits to successChan
// or failureChan based on the result. Both channels are closed when polling
// exits, so receivers can detect shutdown via channel close.
//
// The ticker cadence is anchored to request initiation: time.NewTicker sends
// ticks at regular wall-clock intervals regardless of how long fetchFlags takes,
// so the effective cadence is fixed-rate (providers.md 2.1).
//
// fetchFlags is expected to be single-flight and non-blocking under contention
// (CDNFetcher.TryFetch). A tick that finds a fetch already running is skipped and has no
// observable effect: it emits on neither channel, so it cannot reset the caller's staleness
// grace timer (providers.md 2.1.1). In production the 2.1 bound rules out poll-versus-poll
// contention anyway; the skip is what makes a violation of it harmless rather than silently
// wrong.
func Poll(
	ctx context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	successChan chan<- FetchResult,
	failureChan chan<- string,
	interval time.Duration,
) <-chan struct{} {
	return NewPoller(fetchFlags, fetchFlags, successChan, failureChan, interval).Start(ctx)
}

// Poller drives periodic flag-config polling like Poll, and can also be steered while it runs: SetInterval
// changes the cadence and Trigger asks for a fetch right now. The poll goroutine stays the only sender on,
// and the only closer of, successChan and failureChan, so a caller that steers it from another goroutine can
// never send on a closed channel.
type Poller struct {
	tryFetch    func(context.Context) (FetchResult, error)
	fetchNow    func(context.Context) (FetchResult, error)
	successChan chan<- FetchResult
	failureChan chan<- string
	interval    time.Duration

	// intervalMu makes replacing a pending interval atomic, so the latest SetInterval always wins
	intervalMu   sync.Mutex
	intervalChan chan time.Duration
	triggerChan  chan struct{}
}

// NewPoller builds a Poller. tryFetch serves the periodic ticks and must not wait for a fetch already in
// flight (CDNFetcher.TryFetch); fetchNow serves Trigger and may wait for the single-flight slot
// (CDNFetcher.Fetch), since an on-demand fetch must not be skipped.
func NewPoller(
	tryFetch, fetchNow func(context.Context) (FetchResult, error),
	successChan chan<- FetchResult,
	failureChan chan<- string,
	interval time.Duration,
) *Poller {
	return &Poller{
		tryFetch:     tryFetch,
		fetchNow:     fetchNow,
		successChan:  successChan,
		failureChan:  failureChan,
		interval:     clampInterval(interval),
		intervalChan: make(chan time.Duration, 1),
		triggerChan:  make(chan struct{}, 1),
	}
}

// Start launches the poll goroutine and returns a channel that closes when it exits. It runs until ctx is
// cancelled; the caller shuts down by cancelling ctx and then receiving from the returned channel.
func (p *Poller) Start(ctx context.Context) <-chan struct{} {
	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		defer close(p.successChan)
		defer close(p.failureChan)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pollOnce(ctx, p.tryFetch, p.successChan, p.failureChan)
			case d := <-p.intervalChan:
				// Reset re-anchors the cadence: the next tick is one new interval from now
				ticker.Reset(d)
			case <-p.triggerChan:
				pollOnce(ctx, p.fetchNow, p.successChan, p.failureChan)
			case <-ctx.Done():
				return
			}
		}
	}()
	return doneChan
}

// SetInterval changes the poll cadence. It returns at once and the latest value wins; the next tick is one
// new interval away. Safe to call from any goroutine, including after the poller has stopped.
func (p *Poller) SetInterval(d time.Duration) {
	d = clampInterval(d)
	p.intervalMu.Lock()
	defer p.intervalMu.Unlock()
	select {
	case <-p.intervalChan:
	default:
	}
	p.intervalChan <- d
}

// Trigger asks for a fetch right now, outside the cadence. It returns at once; triggers made while one is
// already waiting collapse into that one. Safe to call from any goroutine, including after the poller has
// stopped.
func (p *Poller) Trigger() {
	select {
	case p.triggerChan <- struct{}{}:
	default:
	}
}

func clampInterval(d time.Duration) time.Duration {
	if d < minPollInterval {
		return minPollInterval
	}
	return d
}

func pollOnce(
	ctx context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	successChan chan<- FetchResult,
	failureChan chan<- string,
) {
	result, err := fetchFlags(ctx)

	if ctx.Err() != nil {
		slog.Debug("poll: cancelled; discarding result")
		return
	}

	if errors.Is(err, ErrFetchInFlight) {
		slog.Debug("poll: fetch already in flight; skipping tick")
		return
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Debug("poll: cancelled; discarding result")
			return
		}
		slog.Warn("poll: fetch failed", "error", err)
		emit(ctx, failureChan, err.Error())
		return
	}

	// No request was made, so the tick is neither a success nor a failure (providers.md 2.1.1).
	if result.IsSkipped() {
		slog.Debug("poll: inside Retry-After window; skipping tick")
		return
	}

	if result.IsNotModified() {
		slog.Debug("poll: flags not modified")
		emit(ctx, successChan, result)
		return
	}

	if result.IsRateLimited() {
		slog.Debug("poll: cdn returned 429; serving cached values")
		return
	}

	if !result.IsSuccess() {
		slog.Warn("poll: non-success response", "status", result.Status)
		emit(ctx, failureChan, "HTTP "+strconv.Itoa(result.Status))
		return
	}

	if result.Body == "" {
		slog.Warn("poll: empty response body")
		emit(ctx, failureChan, "empty response body")
		return
	}

	emit(ctx, successChan, result)
}

// pollMsg is the type set emit delivers: a FetchResult on successChan or a reason string on failureChan.
type pollMsg interface{ string | FetchResult }

func emit[T pollMsg](ctx context.Context, targetChan chan<- T, msg T) {
	select {
	case targetChan <- msg:
	case <-ctx.Done():
	}
}
