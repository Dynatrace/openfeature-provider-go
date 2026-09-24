package internal

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"
)

const minPollInterval = time.Millisecond

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
// so the effective cadence is fixed-rate (providers.md §2.1).
//
// fetchFlags is expected to be single-flight and non-blocking under contention
// (CDNFetcher.TryFetch). A tick that finds a fetch already running is skipped and has no
// observable effect: it emits on neither channel, so it cannot reset the caller's staleness
// grace timer (providers.md §2.1.1). In production the §2.1 bound rules out poll-versus-poll
// contention anyway; the skip is what makes a violation of it harmless rather than silently
// wrong.
func Poll(
	ctx context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	successChan chan<- string,
	failureChan chan<- string,
	interval time.Duration,
) <-chan struct{} {
	if interval < minPollInterval {
		interval = minPollInterval
	}
	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		defer close(successChan)
		defer close(failureChan)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pollOnce(ctx, fetchFlags, successChan, failureChan)
			case <-ctx.Done():
				return
			}
		}
	}()
	return doneChan
}

func pollOnce(
	ctx context.Context,
	fetchFlags func(context.Context) (FetchResult, error),
	successChan chan<- string,
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

	// No request was made, so the tick is neither a success nor a failure (providers.md §2.1.1).
	if result.IsSkipped() {
		slog.Debug("poll: inside Retry-After window; skipping tick")
		return
	}

	if result.IsNotModified() {
		slog.Debug("poll: flags not modified")
		emit(ctx, successChan, "")
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

	emit(ctx, successChan, result.Body)
}

func emit(ctx context.Context, targetChan chan<- string, msg string) {
	select {
	case targetChan <- msg:
	case <-ctx.Done():
	}
}
