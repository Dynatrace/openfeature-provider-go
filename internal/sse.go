package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/launchdarkly/eventsource"
)

const (
	// defaultSSEInitialRetry is the delay before the first retry. It doubles after each failure, up to
	// defaultSSEMaxRetry.
	defaultSSEInitialRetry = time.Second
	defaultSSEMaxRetry     = 30 * time.Second
	// sseJitterRatio randomizes retry delays, so many providers do not reconnect at the same moment after an outage.
	sseJitterRatio = 0.5
	// sseReadTimeout is how long nothing at all may arrive before we treat the connection as dead and reconnect.
	sseReadTimeout = 150 * time.Second

	// sseCloseGrace is how long the library gets to drop a cancelled connection before we close the stream.
	sseCloseGrace = 250 * time.Millisecond
	// sseDrainLimit is the longest we wait for the stream to finish closing; after that we give up on it.
	sseDrainLimit = 2 * time.Second

	redactedQuery = "?<redacted>"
)

// SSEOptions tunes RunSSE. The zero value works.
type SSEOptions struct {
	// Client sends the stream request. It must not set Timeout, which would cut the long-lived stream.
	// Nil means a default client.
	Client *http.Client
	// InitialRetry and MaxRetry bound the delay between connection attempts: it starts at InitialRetry and
	// doubles up to MaxRetry.
	InitialRetry time.Duration
	MaxRetry     time.Duration
}

func (o SSEOptions) withDefaults() SSEOptions {
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	if o.InitialRetry <= 0 {
		o.InitialRetry = defaultSSEInitialRetry
	}
	if o.MaxRetry < o.InitialRetry {
		o.MaxRetry = max(defaultSSEMaxRetry, o.InitialRetry)
	}
	return o
}

// RunSSE keeps an SSE connection to rawURL open until ctx is cancelled. To shut down, cancel ctx and wait
// for the returned channel to close.
//
// A connection that cannot be opened is retried with growing delays, because the stream is usually only
// down temporarily. onConnected is called once, on the first successful connection.
//
// rawURL holds the stream credentials in its query, so it is only ever logged in masked form (see MaskURL).
func RunSSE(ctx context.Context, rawURL string, opts SSEOptions, onConnected func()) <-chan struct{} {
	opts = opts.withDefaults()
	done := make(chan struct{})
	go func() {
		defer close(done)
		connLost := make(chan struct{})
		var lostOnce sync.Once
		stream := connectWithRetry(ctx, rawURL, opts, func() { lostOnce.Do(func() { close(connLost) }) })
		if stream == nil {
			return
		}
		slog.Info("sse: connected", "url", MaskURL(rawURL))
		if onConnected != nil {
			onConnected()
		}
		consumeUntilDone(ctx, stream, connLost)
		slog.Info("sse: closed", "url", MaskURL(rawURL))
	}()
	return done
}

// connectWithRetry opens the stream, retrying until it connects or ctx is cancelled (then it returns nil).
// We retry ourselves because the library's own first-connection retry cannot be interrupted.
func connectWithRetry(ctx context.Context, rawURL string, opts SSEOptions, onCancelledConnLost func()) *eventsource.Stream {
	delay := opts.InitialRetry
	for attempt := 1; ; attempt++ {
		stream, err := subscribe(ctx, rawURL, opts, onCancelledConnLost)
		if err == nil {
			return stream
		}
		if ctx.Err() != nil {
			return nil
		}
		wait := withSSEJitter(delay)
		slog.Warn("sse: connection failed; retrying", "url", MaskURL(rawURL), "attempt", attempt,
			"retry_in", wait, "error", describeError(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		delay = min(delay*2, opts.MaxRetry)
	}
}

// withSSEJitter shortens d by a random share of up to sseJitterRatio, like the library does for its own retries,
// because our first-connection retries would otherwise all fire at the same moment after an outage.
func withSSEJitter(d time.Duration) time.Duration {
	spread := int64(float64(d) * sseJitterRatio)
	if spread <= 0 {
		return d
	}
	return d - time.Duration(rand.Int64N(spread))
}

// subscribe opens the stream once; after that the library reconnects on its own. onCancelledConnLost is
// called when the library reports the dropped connection after ctx was cancelled.
func subscribe(ctx context.Context, rawURL string, opts SSEOptions, onCancelledConnLost func()) (*eventsource.Stream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	return eventsource.SubscribeWithRequestAndOptions(req,
		eventsource.StreamOptionHTTPClient(opts.Client),
		eventsource.StreamOptionReadTimeout(sseReadTimeout),
		eventsource.StreamOptionInitialRetry(opts.InitialRetry),
		eventsource.StreamOptionUseBackoff(opts.MaxRetry),
		eventsource.StreamOptionUseJitter(sseJitterRatio),
		// the handler replaces the Errors channel, so nothing needs draining; it only logs
		eventsource.StreamOptionErrorHandler(func(err error) eventsource.StreamErrorHandlerResult {
			if ctx.Err() != nil {
				onCancelledConnLost()
				return eventsource.StreamErrorHandlerResult{}
			}
			slog.Warn("sse: stream error; the stream reconnects on its own", "url", MaskURL(rawURL), "error", describeError(err))
			return eventsource.StreamErrorHandlerResult{}
		}),
	)
}

// consumeUntilDone discards events until ctx is cancelled, then closes the stream. Events must keep being read,
// even after Close, or the library blocks.
//
// Closing the stream right after the cancel can deadlock the library (eventsource v1.9.1). So we first wait
// for it to report the dropped connection, or for a short grace period, and we bound the final wait so
// Shutdown cannot hang.
func consumeUntilDone(ctx context.Context, stream *eventsource.Stream, connLost <-chan struct{}) {
	if drainEvents(stream.Events, ctx.Done(), nil) {
		return // the stream ended by itself
	}
	grace := time.NewTimer(sseCloseGrace)
	defer grace.Stop()
	if drainEvents(stream.Events, connLost, grace.C) {
		return
	}
	stream.Close()
	limit := time.NewTimer(sseDrainLimit)
	defer limit.Stop()
	if !drainEvents(stream.Events, nil, limit.C) {
		slog.Warn("sse: stream did not finish closing; abandoning it")
	}
}

// drainEvents discards events until the channel closes (true), or stop or timeout fires (false).
// A nil channel never fires.
func drainEvents(events <-chan eventsource.Event, stop <-chan struct{}, timeout <-chan time.Time) bool {
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return true
			}
		case <-stop:
			return false
		case <-timeout:
			return false
		}
	}
}

// MaskURL drops the query, fragment and user info, where the credentials are, so the result is safe to
// log. A removed query shows as "?<redacted>".
func MaskURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	masked := u.Scheme + "://" + u.Host + u.EscapedPath()
	if u.RawQuery != "" || u.ForceQuery {
		masked += redactedQuery
	}
	return masked
}

// describeError turns a stream error into a loggable string. net/http errors contain the full URL, so it is
// masked, and an HTTP error response is reduced to its status code.
func describeError(err error) string {
	var statusErr eventsource.SubscriptionError
	if errors.As(err, &statusErr) {
		return fmt.Sprintf("unexpected HTTP status %d", statusErr.Code)
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Sprintf("%s %s: %v", urlErr.Op, MaskURL(urlErr.URL), urlErr.Err)
	}
	return err.Error()
}
