package internal

import (
	"context"
	"testing"
	"time"

	openfeature "github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
)

func fixedClock(sec int64) func() time.Time {
	return func() time.Time { return time.Unix(sec, 0) }
}

func boolHookContext(flagKey string) openfeature.HookContext {
	return openfeature.NewHookContext(
		flagKey, openfeature.Boolean, false,
		openfeature.ClientMetadata{}, openfeature.Metadata{},
		openfeature.EvaluationContext{},
	)
}

func details(flagKey, variant string, reason openfeature.Reason, errCode openfeature.ErrorCode, version string) openfeature.InterfaceEvaluationDetails {
	md := openfeature.FlagMetadata{}
	if version != "" {
		md["version"] = version
	}
	return openfeature.InterfaceEvaluationDetails{
		EvaluationDetails: openfeature.EvaluationDetails{
			FlagKey:  flagKey,
			FlagType: openfeature.Boolean,
			ResolutionDetail: openfeature.ResolutionDetail{
				Variant:      variant,
				Reason:       reason,
				ErrorCode:    errCode,
				FlagMetadata: md,
			},
		},
	}
}

func TestMetricsHook_NormalizationTable(t *testing.T) {
	cases := []struct {
		name          string
		variant       string
		reason        openfeature.Reason
		errCode       openfeature.ErrorCode
		wantVariant   string
		wantReason    string
		wantErrorCode string
	}{
		{"success", "control", openfeature.TargetingMatchReason, "", "control", "targeting_match", ""},
		{"error blanks variant", "control", openfeature.ErrorReason, openfeature.FlagNotFoundCode, "__default__", "error", "flag_not_found"},
		{"default reason", "control", openfeature.DefaultReason, "", "__default__", "default", ""},
		{"blank variant", "", openfeature.StaticReason, "", "__default__", "static", ""},
		{"blank reason", "control", "", "", "control", "unknown", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := NewMetricsAggregator(DefaultMetricsHardCap)
			a.SetVersion("v1")
			h := NewMetricsHook(a, fixedClock(1741780823), nil)
			h.Finally(context.Background(), boolHookContext("flag"), details("flag", tc.variant, tc.reason, tc.errCode, ""), openfeature.HookHints{})

			batch := a.Drain()
			assert.Equal(t, int64(1), batch.Evaluations["flag"][EvaluationMetricKey{
				Variant:      tc.wantVariant,
				Reason:       tc.wantReason,
				ErrorCode:    tc.wantErrorCode,
				TimestampSec: 1741780820, // floored to the 10s bucket
			}])
		})
	}
}

func TestMetricsHook_ReadsVersionFromMetadata(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v2")
	h := NewMetricsHook(a, fixedClock(100), nil)

	// per-eval version v1 mismatches the aggregator's v2 -> dropped
	h.Finally(context.Background(), boolHookContext("flag"), details("flag", "on", openfeature.StaticReason, "", "v1"), openfeature.HookHints{})
	assert.Equal(t, 0, a.Size())

	// matching version -> recorded
	h.Finally(context.Background(), boolHookContext("flag"), details("flag", "on", openfeature.StaticReason, "", "v2"), openfeature.HookHints{})
	assert.Equal(t, 1, a.Size())
}

func TestMetricsHook_FallsBackToHookContextFlagKey(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	h := NewMetricsHook(a, fixedClock(100), nil)

	d := details("", "on", openfeature.StaticReason, "", "")
	h.Finally(context.Background(), boolHookContext("ctx-flag"), d, openfeature.HookHints{})

	batch := a.Drain()
	assert.Contains(t, batch.Evaluations, "ctx-flag")
}

func TestMetricsHook_OnRecordPanicIsIsolated(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	h := NewMetricsHook(a, fixedClock(100), func() { panic("boom") })

	assert.NotPanics(t, func() {
		h.Finally(context.Background(), boolHookContext("flag"), details("flag", "on", openfeature.StaticReason, "", ""), openfeature.HookHints{})
	})
	assert.Equal(t, 1, a.Size(), "record still happens before the callback panics")
}
