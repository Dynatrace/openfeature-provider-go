package internal

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsAggregator_DropsWhenNoVersion(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.Record("flag", "on", "targeting_match", "", 100, "")
	assert.Equal(t, 0, a.Size())
}

func TestMetricsAggregator_RecordsAfterVersionSet(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	a.Record("flag", "on", "targeting_match", "", 100, "")
	a.Record("flag", "on", "targeting_match", "", 100, "")

	batch := a.Drain()
	require.False(t, batch.IsEmpty())
	assert.Equal(t, "v1", batch.Version)
	assert.Equal(t, int64(2), batch.Evaluations["flag"][EvaluationMetricKey{
		Variant: "on", Reason: "targeting_match", TimestampSec: 100,
	}])
}

func TestMetricsAggregator_DropsOnVersionMismatch(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v2")
	a.Record("flag", "on", "targeting_match", "", 100, "v1") // stale per-eval version
	assert.Equal(t, 0, a.Size(), "mismatch should drop even when the map is empty")

	a.Record("flag", "on", "targeting_match", "", 100, "v2")
	assert.Equal(t, 1, a.Size())
}

func TestMetricsAggregator_BlankVersionIgnored(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	a.SetVersion("   ")
	assert.Equal(t, "v1", a.Version())
}

func TestMetricsAggregator_KeyDimensionsAreDistinct(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	a.Record("flag", "on", "targeting_match", "", 100, "")
	a.Record("flag", "off", "targeting_match", "", 100, "")
	a.Record("flag", "on", "default", "", 100, "")
	a.Record("flag", "on", "targeting_match", "", 110, "") // different time bucket

	batch := a.Drain()
	assert.Len(t, batch.Evaluations["flag"], 4)
}

func TestMetricsAggregator_HardCapDropsNewFlagsOnly(t *testing.T) {
	a := NewMetricsAggregator(2)
	a.SetVersion("v1")
	a.Record("flag-a", "on", "static", "", 100, "")
	a.Record("flag-b", "on", "static", "", 100, "")
	a.Record("flag-c", "on", "static", "", 100, "") // new flag past the cap -> dropped
	a.Record("flag-a", "on", "static", "", 100, "") // existing flag keeps counting

	batch := a.Drain()
	assert.Len(t, batch.Evaluations, 2)
	assert.NotContains(t, batch.Evaluations, "flag-c")
	assert.Equal(t, int64(2), batch.Evaluations["flag-a"][EvaluationMetricKey{
		Variant: "on", Reason: "static", TimestampSec: 100,
	}])
}

func TestMetricsAggregator_DrainSwapsAndPreservesVersion(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")
	a.Record("flag", "on", "static", "", 100, "")

	first := a.Drain()
	assert.Equal(t, "v1", first.Version)
	assert.False(t, first.IsEmpty())

	second := a.Drain()
	assert.True(t, second.IsEmpty(), "live map should be empty after the first drain")
	assert.Equal(t, "v1", a.Version(), "drain must not clear the live version")
}

func TestMetricsAggregator_ConcurrentRecordDrain(t *testing.T) {
	a := NewMetricsAggregator(DefaultMetricsHardCap)
	a.SetVersion("v1")

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 1000 {
				a.Record("flag-"+strconv.Itoa(id), "on", "static", "", 100, "")
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			_ = a.Drain()
		}
	}()
	wg.Wait()
	// No assertion on totals: the point is the -race detector observes no data race.
}
