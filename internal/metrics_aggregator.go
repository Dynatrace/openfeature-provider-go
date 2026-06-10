package internal

import (
	"strings"
	"sync"
)

// Distinct-flag caps. These are hardcoded to match the Java provider, which posts to the
// same ingest endpoint. The soft cap triggers an out-of-band flush; the hard cap drops
// counting of newly-seen flags (existing flags keep accumulating).
const (
	DefaultMetricsSoftCap = 1000
	DefaultMetricsHardCap = 2000
)

// EvaluationMetricKey is the aggregation bucket key. Its fields are all comparable, so the
// struct is usable directly as a Go map key (value-based equality, no hashing required).
type EvaluationMetricKey struct {
	Variant      string
	Reason       string
	ErrorCode    string
	TimestampSec int64
}

// DrainedBatch is a snapshot of aggregated evaluations captured under a single config version.
type DrainedBatch struct {
	Evaluations map[string]map[EvaluationMetricKey]int64
	Version     string
}

// IsEmpty reports whether the batch holds any aggregated flag.
func (b DrainedBatch) IsEmpty() bool { return len(b.Evaluations) == 0 }

// MetricsAggregator accumulates flag evaluations into compact
// (flagKey -> (variant, reason, errorCode, 10s-bucket) -> count) buckets.
//
// All access is guarded by a single mutex. Records are gated on a config version: nothing
// is counted until a version is observed, and per-evaluation versions that disagree with the
// current batch version are dropped (so counts never straddle a config rotation).
type MetricsAggregator struct {
	mu               sync.Mutex
	metrics          map[string]map[EvaluationMetricKey]int64
	version          string
	maxDistinctFlags int
}

// NewMetricsAggregator creates an aggregator bounded at maxDistinctFlags distinct flag keys.
// A non-positive bound falls back to DefaultMetricsHardCap.
func NewMetricsAggregator(maxDistinctFlags int) *MetricsAggregator {
	if maxDistinctFlags <= 0 {
		maxDistinctFlags = DefaultMetricsHardCap
	}
	return &MetricsAggregator{
		metrics:          make(map[string]map[EvaluationMetricKey]int64),
		maxDistinctFlags: maxDistinctFlags,
	}
}

// Record aggregates a single evaluation. evalVersion is the per-evaluation version read from
// flag metadata (may be empty). The record is dropped when: no config version has been
// observed yet; the per-evaluation version disagrees with the current batch version; or the
// flag is newly-seen and the hard cap is reached.
func (a *MetricsAggregator) Record(flagKey, variant, reason, errorCode string, timestampSec int64, evalVersion string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.version == "" {
		return
	}
	if evalVersion != "" && evalVersion != a.version {
		return
	}

	buckets, exists := a.metrics[flagKey]
	if !exists {
		if len(a.metrics) >= a.maxDistinctFlags {
			return
		}
		buckets = make(map[EvaluationMetricKey]int64)
		a.metrics[flagKey] = buckets
	}
	buckets[EvaluationMetricKey{
		Variant:      variant,
		Reason:       reason,
		ErrorCode:    errorCode,
		TimestampSec: timestampSec,
	}]++
}

// SetVersion sets the current batch version. Blank values are ignored (never clears the version).
func (a *MetricsAggregator) SetVersion(v string) {
	if strings.TrimSpace(v) == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.version = v
}

// Version returns the current batch version.
func (a *MetricsAggregator) Version() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version
}

// Size returns the number of distinct flag keys currently held.
func (a *MetricsAggregator) Size() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.metrics)
}

// Drain atomically swaps in a fresh map and returns the previous contents tagged with the
// version that was active while they were accumulated. The live version is left unchanged.
func (a *MetricsAggregator) Drain() DrainedBatch {
	a.mu.Lock()
	defer a.mu.Unlock()
	drained := DrainedBatch{Evaluations: a.metrics, Version: a.version}
	a.metrics = make(map[string]map[EvaluationMetricKey]int64)
	return drained
}
