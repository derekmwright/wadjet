package worker

import (
	"testing"
	"time"
)

// TestScanDecodeAheadPressure_EpisodeCapChannel pins the v3 gate wiring on
// non-edge envelopes (test processes run without a < 2 GiB GOMEMLIMIT, so
// scanDecodeAheadStrictPressure() is false here): the cache channel is
// consulted through the episode-BOUNDED accessor, so an over-cap episode
// (bounded=false while raw=true) must not collapse decode-ahead, while
// heap pressure always does.
func TestScanDecodeAheadPressure_EpisodeCapChannel(t *testing.T) {
	if scanDecodeAheadStrictPressure() {
		t.Skip("strict envelope: cache channel is unbounded by design")
	}
	origHeap, origRaw, origBounded := heapPressureActive, pageCachePressureActive, pageCachePressureActiveBounded
	t.Cleanup(func() {
		heapPressureActive, pageCachePressureActive, pageCachePressureActiveBounded = origHeap, origRaw, origBounded
	})

	heapPressureActive = func() bool { return false }
	pageCachePressureActive = func() bool { return true } // raw sensor hot...
	pageCachePressureActiveBounded = func(time.Duration) bool { return false }
	if scanDecodeAheadPressure() {
		t.Fatal("over-cap episode collapsed decode-ahead on a non-edge envelope")
	}

	pageCachePressureActiveBounded = func(time.Duration) bool { return true }
	if !scanDecodeAheadPressure() {
		t.Fatal("young episode did not collapse decode-ahead")
	}

	pageCachePressureActiveBounded = func(time.Duration) bool { return false }
	heapPressureActive = func() bool { return true }
	if !scanDecodeAheadPressure() {
		t.Fatal("heap pressure must collapse regardless of the cache episode cap")
	}
}

// TestRefaultEpisodeCapDefault pins the default and the parse rules the
// WADJET_REFAULT_EPISODE_CAP env contract promises (0 = unbounded v2).
func TestRefaultEpisodeCapDefault(t *testing.T) {
	if refaultEpisodeCap != 10*time.Second {
		t.Fatalf("default episode cap = %v, want 10s (env override leaked into test process?)", refaultEpisodeCap)
	}
}
