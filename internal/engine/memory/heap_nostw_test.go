package memory

import (
	"runtime"
	"testing"
)

// TestHeapAllocBytesNoSTW_MatchesMemStats pins the equivalence the
// spill/backpressure paths now rely on: the runtime/metrics
// heap-objects read tracks MemStats.HeapAlloc. Regression test for the
// STW-free swap (frozen-spin residual, 2026-08-14: ShouldSpillFor →
// heapPressureExceeded → ReadMemStats caught stopping the world).
func TestHeapAllocBytesNoSTW_MatchesMemStats(t *testing.T) {
	// Settle the heap so the two reads see similar states.
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	got := heapAllocBytesNoSTW()

	ref := int64(ms.HeapAlloc)
	// Allocations between the two reads make exact equality wrong by
	// design; they must agree within 20% or 8MB, whichever is larger.
	diff := got - ref
	if diff < 0 {
		diff = -diff
	}
	tol := ref / 5
	if tol < 8<<20 {
		tol = 8 << 20
	}
	if diff > tol {
		t.Fatalf("runtime/metrics heap objects %d vs MemStats.HeapAlloc %d: diff %d exceeds tolerance %d",
			got, ref, diff, tol)
	}
}
