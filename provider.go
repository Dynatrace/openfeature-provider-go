// Package dynatrace provides a Dynatrace OpenFeature provider for Go server-side applications.
package dynatrace

import (
	"context"

	openfeature "github.com/open-feature/go-sdk/openfeature"
)

const providerName = "dynatrace"

// Compile-time interface assertions.
var (
	_ openfeature.FeatureProvider = (*DynatraceProvider)(nil)
	_ openfeature.StateHandler    = (*DynatraceProvider)(nil)
	_ openfeature.EventHandler    = (*DynatraceProvider)(nil)
)

// DynatraceProvider is a Dynatrace OpenFeature provider for server-side Go applications.
type DynatraceProvider struct {
	events chan openfeature.Event
}

// NewProvider creates a stub DynatraceProvider.
func NewProvider() *DynatraceProvider {
	return &DynatraceProvider{
		events: make(chan openfeature.Event, 8),
	}
}

func (p *DynatraceProvider) Metadata() openfeature.Metadata {
	return openfeature.Metadata{Name: providerName}
}

func (p *DynatraceProvider) Hooks() []openfeature.Hook { return nil }

func (p *DynatraceProvider) Init(_ openfeature.EvaluationContext) error { return nil }

func (p *DynatraceProvider) Shutdown() {}

func (p *DynatraceProvider) EventChannel() <-chan openfeature.Event { return p.events }

func (p *DynatraceProvider) BooleanEvaluation(_ context.Context, _ string, defaultValue bool, _ openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	return openfeature.BoolResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
}

func (p *DynatraceProvider) StringEvaluation(_ context.Context, _ string, defaultValue string, _ openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	return openfeature.StringResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
}

func (p *DynatraceProvider) FloatEvaluation(_ context.Context, _ string, defaultValue float64, _ openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	return openfeature.FloatResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
}

func (p *DynatraceProvider) IntEvaluation(_ context.Context, _ string, defaultValue int64, _ openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	return openfeature.IntResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
}

func (p *DynatraceProvider) ObjectEvaluation(_ context.Context, _ string, defaultValue any, _ openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	return openfeature.InterfaceResolutionDetail{Value: defaultValue, ProviderResolutionDetail: notReadyDetail()}
}

func notReadyDetail() openfeature.ProviderResolutionDetail {
	return openfeature.ProviderResolutionDetail{
		ResolutionError: openfeature.NewProviderNotReadyResolutionError("provider not initialized"),
		Reason:          openfeature.ErrorReason,
	}
}
