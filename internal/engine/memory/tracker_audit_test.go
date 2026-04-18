package memory

import (
	"sync/atomic"
	"testing"
)

// TestTrackerHonesty_AccumulatorVsActual builds a stand-in for "operator
// holding N bytes" and confirms the tracker reports the same bytes within
// the accuracy tolerance. This is a unit-level smoke test for the tracker
// itself; integration accuracy at SF100 is validated separately by
// TestDistributedTPCHBuildCacheSF100Sample with WADJET_HEAP_PROFILE=1.
func TestTrackerHonesty_AccumulatorVsActual(t *testing.T) {
	tr := NewTracker("audit", 100*1024*1024) // 100 MB budget

	const itemBytes = 1 << 16 // 64 KB
	const items = 256          // 16 MB total
	var actual atomic.Int64

	for i := 0; i < items; i++ {
		tr.ForceReserve(itemBytes)
		actual.Add(itemBytes)
	}

	got := tr.Used()
	want := actual.Load()
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	tolerance := want / 10
	if delta > tolerance {
		t.Errorf("Tracker.Used() = %d, expected within 10%% of %d (delta %d)", got, want, delta)
	}

	for i := 0; i < items/2; i++ {
		tr.Release(itemBytes)
		actual.Add(-itemBytes)
	}

	got = tr.Used()
	want = actual.Load()
	delta = got - want
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		t.Errorf("after release: Tracker.Used() = %d, expected within 10%% of %d (delta %d)", got, want, delta)
	}
}
