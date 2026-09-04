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
//
// Two things make the naive version of this test flaky (#854):
//
//  1. `base` reads live heap bytes, which counts a sibling test's garbage
//     until the GC actually reclaims it — sibling tests in this package
//     ran just before this one and are not required to have been
//     collected yet. If that uncollected garbage is comparable in size to
//     the slab this test retains, and the GC reclaims it WHILE this test
//     is running (a plausible outcome of the extra allocation pressure
//     from the 64MiB slab below), the absolute heap can fail to rise 50MB
//     above `base` even though the tracker correctly observed the
//     allocation: `base` was already inflated by since-collected garbage.
//     runtime.GC() before reading `base` forces that garbage out first,
//     so `base` reflects genuinely live bytes.
//  2. Sleeping a fixed duration and hoping it outlasts the ticker's 50ms
//     cadence is inherently racy under CPU load (CI, or a local run
//     under a scheduler burner): the goroutine running the ticker can be
//     descheduled long enough that zero ticks land in the sleep window.
//     Waiting for tracker.ticks to advance past its pre-allocation value
//     instead synchronizes on the real event (a completed sample), not
//     on wall-clock timing.
func TestTaskPeakHeapTracker_ObservesGrowth(t *testing.T) {
	// Settle the heap before reading a baseline (see mechanism note
	// above): a synchronous runtime.GC() reclaims any garbage left by
	// prior tests in this package so `base` reflects only genuinely
	// live heap.
	runtime.GC()

	tr := newTaskPeakHeapTracker(context.Background())
	t.Cleanup(tr.Stop) // stop the sampling goroutine even if a Fatal below exits early

	// newTaskPeakHeapTracker takes its first sample synchronously in the
	// constructor (before the ticker goroutine starts), so `base` is
	// already a real runtime/metrics read of the GC-settled heap above —
	// no need to wait for a tick to read a meaningful base.
	base := tr.PeakMB()

	// Retain a 64MiB slab. 64<<20 bytes == exactly 64 MiB, and PeakMB
	// truncates bytes/(1024*1024) to whole MiB, so an accurate sample
	// taken after this point should read base+64 (modulo a few MiB of
	// incidental allocation from the test binary itself) — comfortably
	// past the base+50 threshold asserted below.
	slab := make([]byte, 64<<20)
	for i := range slab {
		slab[i] = byte(i)
	}

	// Wait for a tick that lands strictly after the allocation above,
	// deterministically: spin on the tracker's own tick counter rather
	// than sleeping a duration chosen to outlast the ticker.
	startTicks := tr.ticks.Load()
	deadline := time.Now().Add(5 * time.Second)
	for tr.ticks.Load() == startTicks {
		if time.Now().After(deadline) {
			t.Fatal("tracker did not produce a sample within 5s of the allocation")
		}
		time.Sleep(time.Millisecond)
	}

	got := tr.PeakMB()
	runtime.KeepAlive(slab)

	if got < base+50 {
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
