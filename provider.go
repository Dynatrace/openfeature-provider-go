// Package dynatrace provides a Dynatrace OpenFeature provider for Go server-side applications.
// It wraps flagd-core for in-process flag evaluation and fetches configuration from the
// Dynatrace CDN with ETag revalidation, polling, retry, and state management.
package dynatrace

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dynatrace/openfeature-provider-go/internal"
	"github.com/open-feature/flagd/core/pkg/evaluator"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/model"
	"github.com/open-feature/flagd/core/pkg/notifications"
	"github.com/open-feature/flagd/core/pkg/store"
	flagdsync "github.com/open-feature/flagd/core/pkg/sync"
	openfeature "github.com/open-feature/go-sdk/openfeature"
	"go.uber.org/zap"
)

const (
	providerName        = "dynatrace"
	defaultPollInterval = 10 * time.Second
	defaultGracePeriod  = 60 * time.Second
	channelBufferSize   = 8
	// versionMetadataKey is the flag-set metadata key carrying the config version.
	versionMetadataKey = "version"
	// modulePath is this module's import path, used to resolve the SDK version at runtime.
	modulePath = "github.com/Dynatrace/openfeature-provider-go"
	// fallbackSDKVersion is reported when the module version is unavailable; it must be valid semver.
	fallbackSDKVersion = "0.0.0"
)

var (
	_ openfeature.FeatureProvider = (*DynatraceProvider)(nil)
	_ openfeature.StateHandler    = (*DynatraceProvider)(nil)
	_ openfeature.EventHandler    = (*DynatraceProvider)(nil)
)

// DynatraceProvider is a Dynatrace OpenFeature provider for server-side Go applications.
// Use NewProvider to construct one, then register via openfeature.SetProvider.
type DynatraceProvider struct {
	config       providerConfig
	evaluator    evaluator.IEvaluator
	flagStore    *store.Store
	fetcher      *internal.CDNFetcher
	httpClient   *http.Client
	configOrigin string
	eventsChan   chan openfeature.Event
	pollInterval time.Duration
	gracePeriod  time.Duration

	state             atomic.Pointer[openfeature.State]
	successChan       chan string
	failureChan       chan string
	pollCancel        context.CancelFunc
	pollDoneChan      <-chan struct{}
	stateLoopDoneChan chan struct{}
	shutdownOnce      sync.Once

	metricsAggregator    *internal.MetricsAggregator
	metricsFlusher       *internal.MetricsFlusher
	metricsHook          openfeature.Hook
	metricsOrigin        string
	metricsFlushInterval time.Duration
	metricsCancel        context.CancelFunc
	metricsDoneChan      <-chan struct{}
}

// ProviderOption configures a DynatraceProvider.
type ProviderOption func(*DynatraceProvider)

// WithConfigOrigin overrides the config origin (scheme, host, and port). For dev/labs environments.
// The platform and key path segments are appended automatically.
// Production URL is derived automatically from the SDK key.
func WithConfigOrigin(origin string) ProviderOption {
	return func(p *DynatraceProvider) { p.configOrigin = origin }
}

// WithHTTPClient injects a custom HTTP client (e.g. a test server client).
func WithHTTPClient(c *http.Client) ProviderOption {
	return func(p *DynatraceProvider) { p.httpClient = c }
}

// withPollInterval overrides the polling interval. Not user-configurable per spec; exposed for testing.
func withPollInterval(d time.Duration) ProviderOption {
	return func(p *DynatraceProvider) { p.pollInterval = d }
}

// WithEvaluator injects a custom flagd-core evaluator. Exposed for testing.
func WithEvaluator(e evaluator.IEvaluator) ProviderOption {
	return func(p *DynatraceProvider) { p.evaluator = e }
}

// WithMetricsOrigin overrides the metrics origin (scheme, host, and optional port). For
// dev/labs environments. The provider appends /v1/metrics. Production URL is derived
// automatically from the SDK key.
func WithMetricsOrigin(origin string) ProviderOption {
	return func(p *DynatraceProvider) { p.metricsOrigin = origin }
}

// WithMetricsFlushInterval overrides the periodic metrics flush cadence (default 30s).
func WithMetricsFlushInterval(d time.Duration) ProviderOption {
	return func(p *DynatraceProvider) { p.metricsFlushInterval = d }
}

// NewProvider creates a DynatraceProvider for the given SDK key.
// The key is validated immediately; an error is returned if the format is invalid.
func NewProvider(sdkKey string, opts ...ProviderOption) (*DynatraceProvider, error) {
	zapLog := zap.NewNop()
	log := logger.NewLogger(zapLog, false)
	flagStore, _ := store.NewStore(log, nil)
	eval := evaluator.NewJSON(log, flagStore)

	p := &DynatraceProvider{
		evaluator:    eval,
		flagStore:    flagStore,
		eventsChan:   make(chan openfeature.Event, channelBufferSize),
		pollInterval: defaultPollInterval,
		gracePeriod:  defaultGracePeriod,
		successChan:  make(chan string, channelBufferSize),
		failureChan:  make(chan string, channelBufferSize),
	}
	p.setState(openfeature.NotReadyState)
	for _, opt := range opts {
		opt(p)
	}

	cfg, err := parseSDKKey(sdkKey, p.configOrigin, p.metricsOrigin)
	if err != nil {
		return nil, err
	}
	// A fetch must never outlive its poll interval (providers.md §2.1). Only validated when
	// using the default HTTP client; a custom client implies the caller controls the timeout
	// (e.g. tests with a local httptest server).
	if p.httpClient == nil && p.pollInterval <= internal.MaxFetchDuration() {
		return nil, fmt.Errorf(
			"pollInterval (%s) must be greater than the worst-case fetch duration (%s)",
			p.pollInterval, internal.MaxFetchDuration(),
		)
	}
	p.config = cfg
	p.fetcher = internal.NewCDNFetcher(cfg.cdnURL, p.httpClient)

	p.metricsAggregator = internal.NewMetricsAggregator(internal.DefaultMetricsHardCap)
	p.metricsFlusher = internal.NewMetricsFlusher(
		p.metricsAggregator,
		p.httpClient,
		cfg.sdkKey,
		cfg.metricsURL,
		p.metricsFlushInterval,
		internal.DefaultMetricsSoftCap,
		resolveSDKVersion(),
		providerName,
	)
	p.metricsHook = internal.NewMetricsHook(p.metricsAggregator, time.Now, p.metricsFlusher.RecordChan())
	return p, nil
}

// Metadata returns provider metadata.
func (p *DynatraceProvider) Metadata() openfeature.Metadata {
	return openfeature.Metadata{Name: providerName}
}

// Hooks returns the provider hooks. The metrics hook records every evaluation for aggregation.
func (p *DynatraceProvider) Hooks() []openfeature.Hook {
	if p.metricsHook == nil {
		return nil
	}
	return []openfeature.Hook{p.metricsHook}
}

// Init fetches the initial flag configuration and starts polling.
//
// A failed initial fetch is not terminal. The polling timer, state loop and metrics flusher
// start either way, so the provider recovers on a later tick exactly like any other fetch
// failure (providers.md §2.4). Init still returns the error, which the SDK reports as a plain
// ERROR; auth failures are no exception, since 401/403 are recoverable once a key is rotated
// or a backend fault clears (providers.md §4). A malformed key never reaches this point: it is
// rejected in NewProvider, before any network activity.
func (p *DynatraceProvider) Init(_ openfeature.EvaluationContext) error {
	slog.Info("initializing dynatrace provider", "key", p.config.publicKey())

	initErr := p.loadInitialConfig()
	if initErr != nil {
		// The validators are cached as soon as a 2xx lands, before the body is judged. Drop them
		// so the recovering poll fetches unconditionally instead of 304-ing on a config that was
		// never applied.
		p.fetcher.InvalidateCache()
		slog.Error("initial cdn fetch failed; starting in ERROR and recovering on the polling timer",
			"key", p.config.publicKey(), "error", initErr)
	} else {
		p.setState(openfeature.ReadyState)
		p.updateMetricsVersionFromFlagSet()
	}

	p.startBackgroundLoops()

	if initErr != nil {
		return initErr
	}
	slog.Info("dynatrace provider initialized", "key", p.config.publicKey())
	return nil
}

// loadInitialConfig performs the initial fetch and applies the config it returns.
func (p *DynatraceProvider) loadInitialConfig() error {
	result, fetchErr := p.fetcher.Fetch(context.Background())
	if fetchErr != nil {
		return fmt.Errorf("initial cdn fetch failed: %w", fetchErr)
	}
	if result.IsSkipped() {
		return fmt.Errorf("initial cdn fetch was skipped")
	}
	if !result.IsSuccess() {
		return fmt.Errorf("initial cdn fetch returned HTTP %d", result.Status)
	}
	if result.Body == "" {
		return fmt.Errorf("initial cdn response body was empty")
	}
	if setErr := p.evaluator.SetState(flagdsync.DataSync{
		FlagData: result.Body,
		Source:   p.config.cdnURL,
	}); setErr != nil {
		return fmt.Errorf("initial cdn config is invalid: %w", setErr)
	}
	return nil
}

// startBackgroundLoops starts the state loop, the polling timer and the metrics flusher. The
// polling timer starts only once the initial fetch has settled, so it never contends with it
// (providers.md §3.1).
func (p *DynatraceProvider) startBackgroundLoops() {
	p.stateLoopDoneChan = make(chan struct{})
	go p.stateLoop()

	pollCtx, pollCancel := context.WithCancel(context.Background())
	p.pollCancel = pollCancel
	// TryFetch, not Fetch: a tick that contends with an in-flight fetch is skipped rather
	// than queued behind it (providers.md §2.1.1).
	p.pollDoneChan = internal.Poll(pollCtx, p.fetcher.TryFetch, p.successChan, p.failureChan, p.pollInterval)

	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	p.metricsCancel = metricsCancel
	p.metricsDoneChan = p.metricsFlusher.Run(metricsCtx)
}

// Shutdown stops polling and transitions to NOT_READY. Idempotent.
func (p *DynatraceProvider) Shutdown() {
	p.shutdownOnce.Do(func() {
		slog.Info("shutting down dynatrace provider")
		if p.pollCancel != nil {
			p.pollCancel()
			<-p.pollDoneChan
			<-p.stateLoopDoneChan
		}
		if p.metricsCancel != nil {
			p.metricsCancel()
			<-p.metricsDoneChan
		}
		p.setState(openfeature.NotReadyState)
		slog.Info("dynatrace provider shut down")
	})
}

// EventChannel returns the channel on which the provider emits lifecycle events.
func (p *DynatraceProvider) EventChannel() <-chan openfeature.Event { return p.eventsChan }

// BooleanEvaluation resolves a boolean flag.
func (p *DynatraceProvider) BooleanEvaluation(ctx context.Context, flag string, defaultValue bool, flatCtx openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	value, detail := resolve(p, ctx, flag, defaultValue, flatCtx, p.evaluator.ResolveBooleanValue, identity[bool])
	return openfeature.BoolResolutionDetail{Value: value, ProviderResolutionDetail: detail}
}

// StringEvaluation resolves a string flag.
func (p *DynatraceProvider) StringEvaluation(ctx context.Context, flag string, defaultValue string, flatCtx openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	value, detail := resolve(p, ctx, flag, defaultValue, flatCtx, p.evaluator.ResolveStringValue, identity[string])
	return openfeature.StringResolutionDetail{Value: value, ProviderResolutionDetail: detail}
}

// FloatEvaluation resolves a float64 flag.
func (p *DynatraceProvider) FloatEvaluation(ctx context.Context, flag string, defaultValue float64, flatCtx openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	value, detail := resolve(p, ctx, flag, defaultValue, flatCtx, p.evaluator.ResolveFloatValue, identity[float64])
	return openfeature.FloatResolutionDetail{Value: value, ProviderResolutionDetail: detail}
}

// IntEvaluation resolves an int64 flag.
func (p *DynatraceProvider) IntEvaluation(ctx context.Context, flag string, defaultValue int64, flatCtx openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	value, detail := resolve(p, ctx, flag, defaultValue, flatCtx, p.evaluator.ResolveIntValue, identity[int64])
	return openfeature.IntResolutionDetail{Value: value, ProviderResolutionDetail: detail}
}

// ObjectEvaluation resolves an object flag.
func (p *DynatraceProvider) ObjectEvaluation(ctx context.Context, flag string, defaultValue any, flatCtx openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	value, detail := resolve(p, ctx, flag, defaultValue, flatCtx, p.evaluator.ResolveObjectValue, func(m map[string]any) any { return m })
	return openfeature.InterfaceResolutionDetail{Value: value, ProviderResolutionDetail: detail}
}

func identity[T any](v T) T { return v }

func resolve[T, U any](
	p *DynatraceProvider,
	ctx context.Context, flag string, defaultValue U, flatCtx openfeature.FlattenedContext,
	fn func(context.Context, string, string, map[string]any) (T, string, string, model.Metadata, error),
	convert func(T) U,
) (U, openfeature.ProviderResolutionDetail) {
	if !p.isReady() {
		return defaultValue, notReadyDetail()
	}
	value, variant, reason, metadata, err := fn(ctx, "", flag, flatCtx)
	if err != nil {
		return defaultValue, mapError(flag, err)
	}
	// Disabled flags succeed with no value; use the caller's code default.
	if reason == model.DisabledReason {
		return defaultValue, openfeature.ProviderResolutionDetail{
			Reason:       openfeature.Reason(model.DisabledReason),
			FlagMetadata: openfeature.FlagMetadata(metadata),
		}
	}
	if reason == model.FallbackReason {
		return defaultValue, openfeature.ProviderResolutionDetail{
			Reason:       openfeature.DefaultReason,
			FlagMetadata: openfeature.FlagMetadata(metadata),
		}
	}
	return convert(value), openfeature.ProviderResolutionDetail{
		Variant:      variant,
		Reason:       openfeature.Reason(reason),
		FlagMetadata: openfeature.FlagMetadata(metadata),
	}
}

// commitFlags applies a fetched config payload and returns the changed keys. Before applying the
// new config it seals the current metrics batch under the old version and flushes it, so
// pre-rotation counts are reported against the version under which they occurred; the live
// version is then bumped on the now-empty aggregator.
func (p *DynatraceProvider) commitFlags(payload string) ([]string, error) {
	p.metricsFlusher.EnqueueSealedBatch(p.metricsAggregator.Drain())

	before := flagsByKey(p.flagStore)
	if err := p.evaluator.SetState(flagdsync.DataSync{FlagData: payload, Source: p.config.cdnURL}); err != nil {
		// The body was fetched but not applied, so the cached validators describe a config the
		// store never took. Drop them so the next poll refetches it instead of seeing a 304.
		p.fetcher.InvalidateCache()
		return nil, err
	}
	after := flagsByKey(p.flagStore)

	p.updateMetricsVersionFromFlagSet()

	changed := notifications.NewFromFlags(before, after)
	if len(changed) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(changed))
	for k := range changed {
		keys = append(keys, k)
	}
	return keys, nil
}

// flagsByKey returns the store's flags keyed by flag key, or nil on read failure.
func flagsByKey(s *store.Store) map[string]model.Flag {
	if s == nil {
		return nil
	}
	flags, _, err := s.GetAll(context.Background(), nil)
	if err != nil {
		return nil
	}
	out := make(map[string]model.Flag, len(flags))
	for _, f := range flags {
		out[f.Key] = f
	}
	return out
}

// updateMetricsVersionFromFlagSet reads the config version and sets it on the aggregator. It is
// called after each config load; blank or absent versions are ignored.
//
// flagd-core merges flag-set-level metadata (which carries the config "version") into each flag's
// metadata at load time rather than exposing it as a separate flag-set accessor, so the version is
// read from a stored flag's metadata. This is the same source the metrics hook reads per
// evaluation, and avoids evaluating every flag just to read one metadata key.
func (p *DynatraceProvider) updateMetricsVersionFromFlagSet() {
	flags, _, err := p.flagStore.GetAll(context.Background(), nil)
	if err != nil {
		slog.Debug("metrics: reading flag-set metadata reported an error", "error", err)
	}
	for _, f := range flags {
		if version, ok := f.Metadata[versionMetadataKey].(string); ok && strings.TrimSpace(version) != "" {
			p.metricsAggregator.SetVersion(version)
			return
		}
	}
}

// resolveSDKVersion reports this module's version as bare semver (no leading "v"), or
// fallbackSDKVersion when the version is unavailable. The metrics ingest endpoint validates this
// field and rejects non-semver values (a "v"-prefixed tag, "(devel)" from a source build, or
// "unknown") with HTTP 422, so the value must always be normalized.
func resolveSDKVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Path == modulePath {
			if v := normalizeSDKVersion(info.Main.Version); v != "" {
				return v
			}
		}
		for _, dep := range info.Deps {
			if dep.Path == modulePath {
				if v := normalizeSDKVersion(dep.Version); v != "" {
					return v
				}
			}
		}
	}
	return fallbackSDKVersion
}

// normalizeSDKVersion converts a Go module version into the bare-semver form the metrics endpoint
// accepts: it trims the leading "v" (Go tags releases as "v1.2.3") and returns "" for unusable
// values ("(devel)" source builds or an empty version).
func normalizeSDKVersion(v string) string {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" || strings.Contains(v, "devel") {
		return ""
	}
	return v
}

func (p *DynatraceProvider) stateLoop() {
	var grace *time.Timer
	graceC := func() <-chan time.Time {
		if grace == nil {
			return nil
		}
		return grace.C
	}
	stopGrace := func() {
		if grace != nil {
			grace.Stop()
			grace = nil
		}
	}
	defer func() {
		stopGrace()
		p.setState(openfeature.NotReadyState)
		close(p.stateLoopDoneChan)
	}()

	// fail handles a poll failure or a commit failure. From any non-Ready state
	// it just logs; from Ready it transitions to Stale, emits, and arms grace.
	fail := func(reason string) {
		if prev := p.getState(); prev != openfeature.ReadyState {
			slog.Warn("poll failed", "state", prev, "reason", reason)
			return
		}
		p.setState(openfeature.StaleState)
		slog.Warn("poll failed; transitioning to STALE", "reason", reason)
		p.emit(openfeature.ProviderStale, openfeature.ProviderEventDetails{Message: reason})
		stopGrace()
		grace = time.NewTimer(p.gracePeriod)
	}

	for {
		select {
		case body, ok := <-p.successChan:
			if !ok {
				return
			}
			var changed []string
			if body != "" {
				keys, err := p.commitFlags(body)
				if err != nil {
					fail("failed to store flags: " + err.Error())
					continue
				}
				changed = keys
			}
			stopGrace()
			// NotReady covers a failed initialization: the first successful poll is what
			// brings the provider up, so it must emit PROVIDER_READY itself.
			if prev := p.getState(); prev != openfeature.ReadyState {
				p.setState(openfeature.ReadyState)
				slog.Info("dynatrace provider recovered to READY", "previous", prev)
				p.emit(openfeature.ProviderReady, openfeature.ProviderEventDetails{Message: "recovered"})
			}
			if len(changed) > 0 {
				p.emit(openfeature.ProviderConfigChange, openfeature.ProviderEventDetails{FlagChanges: changed})
			}

		case reason, ok := <-p.failureChan:
			if !ok {
				return
			}
			fail(reason)

		case <-graceC():
			grace = nil
			if p.getState() != openfeature.StaleState {
				continue
			}
			p.setState(openfeature.ErrorState)
			slog.Error("dynatrace provider grace period expired; transitioning to ERROR")
			p.emit(openfeature.ProviderError, openfeature.ProviderEventDetails{Message: "grace period expired; CDN unreachable"})
		}
	}
}

func (p *DynatraceProvider) getState() openfeature.State {
	if s := p.state.Load(); s != nil {
		return *s
	}
	return openfeature.NotReadyState
}

func (p *DynatraceProvider) setState(s openfeature.State) {
	p.state.Store(&s)
}

func (p *DynatraceProvider) isReady() bool {
	s := p.getState()
	return s == openfeature.ReadyState || s == openfeature.StaleState || s == openfeature.ErrorState
}

func (p *DynatraceProvider) emit(t openfeature.EventType, d openfeature.ProviderEventDetails) {
	e := openfeature.Event{ProviderName: providerName, EventType: t, ProviderEventDetails: d}
	select {
	case p.eventsChan <- e:
	default:
		slog.Warn("dynatrace provider event channel full; dropping event", "type", t)
	}
}

func mapError(flagKey string, err error) openfeature.ProviderResolutionDetail {
	var resErr openfeature.ResolutionError
	switch err.Error() {
	case model.FlagNotFoundErrorCode:
		resErr = openfeature.NewFlagNotFoundResolutionError(fmt.Sprintf("flag %q not found", flagKey))
	case model.TypeMismatchErrorCode:
		resErr = openfeature.NewTypeMismatchResolutionError(fmt.Sprintf("flag %q type mismatch", flagKey))
	case model.ParseErrorCode:
		resErr = openfeature.NewParseErrorResolutionError(fmt.Sprintf("flag %q parse error", flagKey))
	default:
		resErr = openfeature.NewGeneralResolutionError(fmt.Sprintf("flag %q evaluation error: %s", flagKey, err.Error()))
	}
	return openfeature.ProviderResolutionDetail{ResolutionError: resErr, Reason: openfeature.ErrorReason}
}

func notReadyDetail() openfeature.ProviderResolutionDetail {
	return openfeature.ProviderResolutionDetail{
		ResolutionError: openfeature.NewProviderNotReadyResolutionError("provider not initialized"),
		Reason:          openfeature.ErrorReason,
	}
}
