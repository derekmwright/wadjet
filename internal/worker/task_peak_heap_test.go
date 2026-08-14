package worker

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestTaskPeakHeapTracker_ObservesGrowth: the tracker must observe a heap
// allocation that appears after start and is retained past at least one
// 50ms tick. This is the regression test for the STW-storm fix (2026-08-13,
// results/20260813-232615): sampling moved from runtime.ReadMemStats
// (stop-the-world, 20 STWs/s per task) to runtime/metrics — the tracker
// must keep reporting correct peaks with the STW-free read.
func TestTaskPeakHeapTracker_ObservesGrowth(t *testing.T) {
	tr := newTaskPeakHeapTracker(context.Background())
	base := tr.PeakMB()

	// Retain ~64MB across several ticks so the sampler must see it.
	slab := make([]byte, 64<<20)
	for i := range slab {
		slab[i] = byte(i)
	}
	time.Sleep(150 * time.Millisecond)
	tr.Stop()
	runtime.KeepAlive(slab)

	if got := tr.PeakMB(); got < base+50 {
		t.Fatalf("peak did not observe 64MB retained alloc: base=%dMB peak=%dMB", base, got)
	}
}

// TestTaskPeakHeapTracker_StopUnblocks: Stop must return promptly and the
// tracker must survive a cancelled context (task teardown paths call Stop
// after ctx cancel).
func TestTaskPeakHeapTracker_StopUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tr := newTaskPeakHeapTracker(ctx)
	cancel()
	done := make(chan struct{})
	go func() { tr.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after context cancel")
	}
}
