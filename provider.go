// Package dynatrace provides a Dynatrace OpenFeature provider for Go server-side applications.
// It wraps flagd-core for in-process flag evaluation and fetches configuration from the
// Dynatrace CDN with ETag revalidation, polling, retry, and state management.
package dynatrace

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Dynatrace/openfeature-provider-go/internal"
	"github.com/open-feature/flagd/core/pkg/evaluator"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/model"
	flagdsync "github.com/open-feature/flagd/core/pkg/sync"
	"github.com/open-feature/flagd/core/pkg/store"
	openfeature "github.com/open-feature/go-sdk/openfeature"
	"go.uber.org/zap"
)

const (
	providerName        = "dynatrace"
	defaultPollInterval = 10 * time.Second
	defaultGracePeriod  = 60 * time.Second
)

var (
	_ openfeature.FeatureProvider = (*DynatraceProvider)(nil)
	_ openfeature.StateHandler    = (*DynatraceProvider)(nil)
	_ openfeature.EventHandler    = (*DynatraceProvider)(nil)
)

// DynatraceProvider is a Dynatrace OpenFeature provider for server-side Go applications.
// Use NewProvider to construct one, then register via openfeature.SetProvider.
type DynatraceProvider struct {
	options      Options
	evaluator    evaluator.IEvaluator
	fetcher      *internal.CDNFetcher
	poller       *internal.PollingManager
	httpClient   *http.Client
	events       chan openfeature.Event
	pollInterval time.Duration
	gracePeriod  time.Duration

	mu           sync.RWMutex
	state        openfeature.State
	graceTimer   *time.Timer
	shutdownOnce sync.Once
}

// ProviderOption is a functional option for DynatraceProvider.
type ProviderOption func(*DynatraceProvider)

// WithHTTPClient injects a custom HTTP client (e.g. a test server client).
func WithHTTPClient(c *http.Client) ProviderOption {
	return func(p *DynatraceProvider) { p.httpClient = c }
}

// WithPollInterval overrides the polling interval. Not user-configurable per spec; exposed for testing.
func WithPollInterval(d time.Duration) ProviderOption {
	return func(p *DynatraceProvider) { p.pollInterval = d }
}

// WithGracePeriod overrides the STALE→ERROR grace period. Not user-configurable per spec; exposed for testing.
func WithGracePeriod(d time.Duration) ProviderOption {
	return func(p *DynatraceProvider) { p.gracePeriod = d }
}

// WithEvaluator injects a custom flagd-core evaluator. Exposed for testing.
func WithEvaluator(e evaluator.IEvaluator) ProviderOption {
	return func(p *DynatraceProvider) { p.evaluator = e }
}

// NewProvider creates a DynatraceProvider for the given Options.
func NewProvider(options Options, opts ...ProviderOption) *DynatraceProvider {
	zapLog := zap.NewNop()
	log := logger.NewLogger(zapLog, false)
	flagStore, _ := store.NewStore(log, nil)
	eval := evaluator.NewJSON(log, flagStore)

	p := &DynatraceProvider{
		options:      options,
		evaluator:    eval,
		events:       make(chan openfeature.Event, 8),
		pollInterval: defaultPollInterval,
		gracePeriod:  defaultGracePeriod,
		state:        openfeature.NotReadyState,
	}
	for _, opt := range opts {
		opt(p)
	}
	p.fetcher = internal.NewCDNFetcher(options.CDNURL(), p.httpClient)
	return p
}

// Metadata returns provider metadata.
func (p *DynatraceProvider) Metadata() openfeature.Metadata {
	return openfeature.Metadata{Name: providerName}
}

// Hooks returns an empty hook slice.
func (p *DynatraceProvider) Hooks() []openfeature.Hook { return nil }

// Init fetches the initial flag configuration and starts polling.
func (p *DynatraceProvider) Init(_ openfeature.EvaluationContext) error {
	slog.Info("initializing dynatrace provider", "key", p.options.PublicKey())

	result, fetchErr := p.fetcher.Fetch()
	if fetchErr != nil {
		return fmt.Errorf("initial cdn fetch failed: %w", fetchErr)
	}
	if !result.IsSuccess() {
		return fmt.Errorf("initial cdn fetch returned HTTP %d", result.Status)
	}
	if result.Body == "" {
		return fmt.Errorf("initial cdn response body was empty")
	}

	if setErr := p.evaluator.SetState(flagdsync.DataSync{
		FlagData: result.Body,
		Source:   p.options.CDNURL(),
	}); setErr != nil {
		return fmt.Errorf("initial cdn config is invalid: %w", setErr)
	}

	p.mu.Lock()
	p.state = openfeature.ReadyState
	p.mu.Unlock()

	p.sendEvent(openfeature.Event{
		ProviderName: providerName,
		EventType:    openfeature.ProviderReady,
	})

	p.poller = internal.NewPollingManager(
		p.fetcher.Fetch,
		p.storeFlags,
		p.onConfigChanged,
		p.onFetchSuccess,
		p.onFetchFailure,
		p.pollInterval,
	)
	p.poller.Start()

	slog.Info("dynatrace provider initialized", "key", p.options.PublicKey())
	return nil
}

// Shutdown stops polling and transitions to NOT_READY. Idempotent.
func (p *DynatraceProvider) Shutdown() {
	p.shutdownOnce.Do(func() {
		slog.Info("shutting down dynatrace provider")
		if p.poller != nil {
			p.poller.Stop()
		}
		p.cancelGraceTimer()
		p.mu.Lock()
		p.state = openfeature.NotReadyState
		p.mu.Unlock()
		close(p.events)
		slog.Info("dynatrace provider shut down")
	})
}

// EventChannel returns the channel on which the provider emits lifecycle events.
func (p *DynatraceProvider) EventChannel() <-chan openfeature.Event { return p.events }

// BooleanEvaluation resolves a boolean flag.
func (p *DynatraceProvider) BooleanEvaluation(ctx context.Context, flag string, defaultValue bool, flatCtx openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	if !p.isReady() {
		return openfeature.BoolResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
	}
	value, variant, reason, metadata, err := p.evaluator.ResolveBooleanValue(ctx, "", flag, flatCtx)
	if err != nil {
		return openfeature.BoolResolutionDetail{Value: defaultValue, ProviderResolutionDetail: mapError(flag, err)}
	}
	return openfeature.BoolResolutionDetail{Value: value, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Variant: variant, Reason: openfeature.Reason(reason), FlagMetadata: openfeature.FlagMetadata(metadata),
	}}
}

// StringEvaluation resolves a string flag.
func (p *DynatraceProvider) StringEvaluation(ctx context.Context, flag string, defaultValue string, flatCtx openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	if !p.isReady() {
		return openfeature.StringResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
	}
	value, variant, reason, metadata, err := p.evaluator.ResolveStringValue(ctx, "", flag, flatCtx)
	if err != nil {
		return openfeature.StringResolutionDetail{Value: defaultValue, ProviderResolutionDetail: mapError(flag, err)}
	}
	return openfeature.StringResolutionDetail{Value: value, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Variant: variant, Reason: openfeature.Reason(reason), FlagMetadata: openfeature.FlagMetadata(metadata),
	}}
}

// FloatEvaluation resolves a float64 flag.
func (p *DynatraceProvider) FloatEvaluation(ctx context.Context, flag string, defaultValue float64, flatCtx openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	if !p.isReady() {
		return openfeature.FloatResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
	}
	value, variant, reason, metadata, err := p.evaluator.ResolveFloatValue(ctx, "", flag, flatCtx)
	if err != nil {
		return openfeature.FloatResolutionDetail{Value: defaultValue, ProviderResolutionDetail: mapError(flag, err)}
	}
	return openfeature.FloatResolutionDetail{Value: value, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Variant: variant, Reason: openfeature.Reason(reason), FlagMetadata: openfeature.FlagMetadata(metadata),
	}}
}

// IntEvaluation resolves an int64 flag.
func (p *DynatraceProvider) IntEvaluation(ctx context.Context, flag string, defaultValue int64, flatCtx openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	if !p.isReady() {
		return openfeature.IntResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
	}
	value, variant, reason, metadata, err := p.evaluator.ResolveIntValue(ctx, "", flag, flatCtx)
	if err != nil {
		return openfeature.IntResolutionDetail{Value: defaultValue, ProviderResolutionDetail: mapError(flag, err)}
	}
	return openfeature.IntResolutionDetail{Value: value, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Variant: variant, Reason: openfeature.Reason(reason), FlagMetadata: openfeature.FlagMetadata(metadata),
	}}
}

// ObjectEvaluation resolves an object flag.
func (p *DynatraceProvider) ObjectEvaluation(ctx context.Context, flag string, defaultValue any, flatCtx openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	if !p.isReady() {
		return openfeature.InterfaceResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
	}
	value, variant, reason, metadata, err := p.evaluator.ResolveObjectValue(ctx, "", flag, flatCtx)
	if err != nil {
		return openfeature.InterfaceResolutionDetail{Value: defaultValue, ProviderResolutionDetail: mapError(flag, err)}
	}
	return openfeature.InterfaceResolutionDetail{Value: value, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Variant: variant, Reason: openfeature.Reason(reason), FlagMetadata: openfeature.FlagMetadata(metadata),
	}}
}

func (p *DynatraceProvider) storeFlags(json string) ([]string, error) {
	err := p.evaluator.SetState(flagdsync.DataSync{FlagData: json, Source: p.options.CDNURL()})
	return nil, err
}

func (p *DynatraceProvider) onFetchSuccess() {
	p.cancelGraceTimer()
	p.mu.Lock()
	current := p.state
	if current == openfeature.StaleState || current == openfeature.ErrorState {
		p.state = openfeature.ReadyState
		p.mu.Unlock()
		slog.Info("dynatrace provider recovered to READY", "previous", current)
		p.sendEvent(openfeature.Event{ProviderName: providerName, EventType: openfeature.ProviderReady,
			ProviderEventDetails: openfeature.ProviderEventDetails{Message: "recovered"}})
		return
	}
	p.mu.Unlock()
}

func (p *DynatraceProvider) onFetchFailure(reason string) {
	p.mu.Lock()
	current := p.state
	if current != openfeature.ReadyState {
		p.mu.Unlock()
		slog.Warn("poll failed", "state", current, "reason", reason)
		return
	}
	p.state = openfeature.StaleState
	p.mu.Unlock()
	slog.Warn("poll failed; transitioning to STALE", "reason", reason)
	p.sendEvent(openfeature.Event{ProviderName: providerName, EventType: openfeature.ProviderStale,
		ProviderEventDetails: openfeature.ProviderEventDetails{Message: reason}})
	p.startGraceTimer()
}

func (p *DynatraceProvider) onConfigChanged(keys []string) {
	p.sendEvent(openfeature.Event{ProviderName: providerName, EventType: openfeature.ProviderConfigChange,
		ProviderEventDetails: openfeature.ProviderEventDetails{FlagChanges: keys}})
}

func (p *DynatraceProvider) startGraceTimer() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.graceTimer != nil {
		p.graceTimer.Stop()
	}
	p.graceTimer = time.AfterFunc(p.gracePeriod, p.graceExpired)
}

func (p *DynatraceProvider) cancelGraceTimer() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.graceTimer != nil {
		p.graceTimer.Stop()
		p.graceTimer = nil
	}
}

func (p *DynatraceProvider) graceExpired() {
	p.mu.Lock()
	if p.state != openfeature.StaleState {
		p.mu.Unlock()
		return
	}
	p.state = openfeature.ErrorState
	p.mu.Unlock()
	slog.Error("dynatrace provider grace period expired; transitioning to ERROR")
	p.sendEvent(openfeature.Event{ProviderName: providerName, EventType: openfeature.ProviderError,
		ProviderEventDetails: openfeature.ProviderEventDetails{Message: "grace period expired; CDN unreachable"}})
}

func (p *DynatraceProvider) isReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s := p.state
	return s == openfeature.ReadyState || s == openfeature.StaleState || s == openfeature.ErrorState
}

func (p *DynatraceProvider) sendEvent(e openfeature.Event) {
	defer func() { _ = recover() }()
	select {
	case p.events <- e:
	default:
		slog.Warn("dynatrace provider event channel full; dropping event", "type", e.EventType)
	}
}

func mapError(flagKey string, err error) openfeature.ProviderResolutionDetail {
	var resErr openfeature.ResolutionError
	switch err.Error() {
	case model.FlagNotFoundErrorCode:
		resErr = openfeature.NewFlagNotFoundResolutionError(fmt.Sprintf("flag %q not found", flagKey))
	case model.FlagDisabledErrorCode:
		resErr = openfeature.NewFlagNotFoundResolutionError(fmt.Sprintf("flag %q is disabled", flagKey))
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
