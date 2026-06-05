package internal

import (
	"context"
	"strings"
	"time"

	openfeature "github.com/open-feature/go-sdk/openfeature"
)

const (
	// defaultVariant is the sentinel recorded when a real variant is unavailable.
	defaultVariant = "__default__"
	// versionMetadataKey is the flag-metadata key carrying the per-evaluation config version.
	versionMetadataKey = "version"

	reasonError   = "error"
	reasonUnknown = "unknown"

	// bucketSeconds is the timestamp bucket width; evaluations are floored to this many seconds.
	bucketSeconds = 10
)

// MetricsHook records every flag evaluation into a MetricsAggregator. It uses the OpenFeature
// Finally stage, which runs on both success and error, mirroring the Java provider's
// finallyAfter hook.
type MetricsHook struct {
	openfeature.UnimplementedHook
	aggregator *MetricsAggregator
	clock      func() time.Time
	recordChan chan<- struct{}
}

// NewMetricsHook builds a hook recording into aggregator. clock is injectable for deterministic
// tests (defaults to time.Now). recordChan is an optional coalescing channel signalled after each
// record so the flusher can schedule capacity-triggered flushes; it is nil-safe and the send never
// blocks the evaluation path.
func NewMetricsHook(aggregator *MetricsAggregator, clock func() time.Time, recordChan chan<- struct{}) *MetricsHook {
	if clock == nil {
		clock = time.Now
	}
	return &MetricsHook{aggregator: aggregator, clock: clock, recordChan: recordChan}
}

// Finally records the evaluation and notifies the capacity callback. It never blocks or fails
// the evaluation path.
func (h *MetricsHook) Finally(_ context.Context, hookContext openfeature.HookContext, details openfeature.InterfaceEvaluationDetails, _ openfeature.HookHints) {
	flagKey := strings.TrimSpace(details.FlagKey)
	if flagKey == "" {
		flagKey = strings.TrimSpace(hookContext.FlagKey())
	}
	if flagKey == "" {
		return
	}

	errorCode := normalizeErrorCode(details.ErrorCode)
	h.aggregator.Record(
		flagKey,
		normalizeVariant(details.Variant, details.Reason, errorCode),
		normalizeReason(details.Reason, errorCode),
		errorCode,
		bucketTimestamp(h.clock()),
		readVersion(details.FlagMetadata),
	)

	h.signalRecord()
}

// signalRecord notifies the flusher that a record happened. The send is non-blocking and coalesced
// (buffered cap 1), so it never disrupts the evaluation path.
func (h *MetricsHook) signalRecord() {
	if h.recordChan == nil {
		return
	}
	select {
	case h.recordChan <- struct{}{}:
	default:
	}
}

// normalizeErrorCode lowercases the error code, returning "" when there is no error. The
// lowercase form matches the Java enum-name normalization (e.g. FLAG_NOT_FOUND -> flag_not_found).
func normalizeErrorCode(code openfeature.ErrorCode) string {
	if code == "" {
		return ""
	}
	return strings.ToLower(string(code))
}

// normalizeVariant returns the default sentinel for error, default-reason, or blank-variant
// evaluations; otherwise the variant verbatim.
func normalizeVariant(variant string, reason openfeature.Reason, errorCode string) string {
	if errorCode != "" || isDefaultReason(reason) || strings.TrimSpace(variant) == "" {
		return defaultVariant
	}
	return variant
}

// normalizeReason returns "error" for errored evaluations, "unknown" for a blank reason, and the
// lowercased reason otherwise.
func normalizeReason(reason openfeature.Reason, errorCode string) string {
	if errorCode != "" {
		return reasonError
	}
	if strings.TrimSpace(string(reason)) == "" {
		return reasonUnknown
	}
	return strings.ToLower(string(reason))
}

func isDefaultReason(reason openfeature.Reason) bool {
	return strings.EqualFold(string(reason), string(openfeature.DefaultReason))
}

// bucketTimestamp floors a time to the bucketSeconds boundary (epoch seconds are non-negative).
func bucketTimestamp(t time.Time) int64 {
	sec := t.Unix()
	return sec - (sec % bucketSeconds)
}

// readVersion extracts the per-evaluation config version from flag metadata, returning "" when
// absent or unreadable.
func readVersion(metadata openfeature.FlagMetadata) string {
	if metadata == nil {
		return ""
	}
	v, err := metadata.GetString(versionMetadataKey)
	if err != nil {
		return ""
	}
	return v
}
