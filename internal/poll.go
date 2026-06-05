package internal

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"
)

const minPollInterval = time.Millisecond

// Poll launches a goroutine that drives periodic flag-config polling. It
// returns a cancel function that stops polling and a done channel that
// closes when the goroutine exits. Call cancel then read from done to
// shut down cleanly.
//
// On each tick the goroutine calls fetchFlags(ctx) and emits to success or
// failure based on the result. Both channels are closed when polling exits,
// so receivers can detect shutdown via channel close.
//
// The ticker cadence is anchored to request initiation: time.NewTicker sends
// ticks at regular wall-clock intervals regardless of how long fetchFlags takes,
// so the effective cadence is fixed-rate as long as polls finish well within
// the interval (10s interval, 5s per-request timeout, 5 retries ~1.6s worst
// case in production; see providers.md §2.1).
func Poll(
	parent context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	success chan<- string,
	failure chan<- string,
	interval time.Duration,
) (context.CancelFunc, <-chan struct{}) {
	if interval < minPollInterval {
		interval = minPollInterval
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(success)
		defer close(failure)
		defer cancel()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pollOnce(ctx, fetchFlags, success, failure)
			case <-ctx.Done():
				return
			}
		}
	}()
	return cancel, done
}

func pollOnce(
	ctx context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	success chan<- string,
	failure chan<- string,
) {
	result, err := fetchFlags(ctx)

	if ctx.Err() != nil {
		slog.Debug("poll: cancelled; discarding result")
		return
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Debug("poll: cancelled; discarding result")
			return
		}
		slog.Warn("poll: fetch failed", "error", err)
		emit(ctx, failure, err.Error())
		return
	}

	if result.IsNotModified() {
		slog.Debug("poll: flags not modified")
		emit(ctx, success, "")
		return
	}

	if result.IsRateLimited() {
		slog.Debug("poll: rate limited; serving cached values")
		return
	}

	if !result.IsSuccess() {
		slog.Warn("poll: non-success response", "status", result.Status)
		emit(ctx, failure, "HTTP "+strconv.Itoa(result.Status))
		return
	}

	if result.Body == "" {
		slog.Warn("poll: empty response body")
		emit(ctx, failure, "empty response body")
		return
	}

	emit(ctx, success, result.Body)
}

func emit(ctx context.Context, ch chan<- string, msg string) {
	select {
	case ch <- msg:
	case <-ctx.Done():
	}
}
