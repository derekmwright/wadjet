package exec

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #326: the heap-backpressure valve must not sleep the consume loop feeding
// a pipeline breaker that holds the dominant share of tracked bytes —
// sleeping the holder of live state reclaims nothing. These tests hold the
// valve open for a whole pipeline run (test seam; the real inputs are
// GOMEMLIMIT and live heap) and A/B the kill switch:
//
//   - disabled (pre-#326 behavior): every batch pays the 50 ms pause —
//     wall ≥ batches × 50 ms, pause count == batches.
//   - enabled: the aggregate answers the valve (drain when the #325 gate
//     admits it, otherwise proceed), pause count 0, wall collapses.
//
// Results must be identical in both arms — this changes scheduling, never
// rows (ADR-0006: degradation is slowdown or spill, never wrong answers).

func pressuredAggPipeline(sm *memory.SpillManager, numBatches, rowsPerBatch int) (*Pipeline, *HashAggregate, map[[2]int64]int64) {
	batches, expected := highCardBatches(numBatches, rowsPerBatch)
	h := NewHashAggregate([]string{"k1", "k2"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	p := &Pipeline{
		Source: &testBatchSource{batches: batches},
		Sink:   h,
	}
	return p, h, expected
}

func drainAggResults(t *testing.T, h *HashAggregate) map[[2]int64]int64 {
	t.Helper()
	got := make(map[[2]int64]int64)
	for {
		out, err := h.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[[2]int64{r["k1"].(int64), r["k2"].(int64)}] = r["total"].(int64)
		}
	}
	return got
}

func TestHeapValve_BreakerDrainsInsteadOfSleeping(t *testing.T) {
	const numBatches = 40
	const rowsPerBatch = 64

	memory.SetHeapBackpressureForTesting(1)
	defer memory.SetHeapBackpressureForTesting(0)

	run := func(t *testing.T, disabled bool) (wall time.Duration, pauses, drains int64, got map[[2]int64]int64) {
		t.Helper()
		defer func(v bool) { pressureDrainDisabled = v }(pressureDrainDisabled)
		pressureDrainDisabled = disabled

		// The aggregate is the sole holder on this tracker, so its share is
		// dominant by construction; the #325 gate (floor = budget/8) refuses
		// per-batch drains because state grows far slower than the floor.
		tracker := memory.NewTracker("test", 64<<20)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		p, h, expected := pressuredAggPipeline(sm, numBatches, rowsPerBatch)
		pauses0, drains0 := heapPauseCount.Load(), heapDrainCount.Load()
		start := time.Now()
		if err := p.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v", err)
		}
		wall = time.Since(start)
		got = drainAggResults(t, h)
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if len(got) != len(expected) {
			t.Fatalf("group count: got %d want %d", len(got), len(expected))
		}
		for k, want := range expected {
			if got[k] != want {
				t.Fatalf("group %v: got %d want %d", k, got[k], want)
			}
		}
		return wall, heapPauseCount.Load() - pauses0, heapDrainCount.Load() - drains0, got
	}

	beforeWall, beforePauses, _, _ := run(t, true)
	afterWall, afterPauses, afterDrains, _ := run(t, false)

	t.Logf("#326 evidence: before (sleep valve): pauses=%d wall=%v; after (drain valve): pauses=%d drains=%d wall=%v",
		beforePauses, beforeWall, afterPauses, afterDrains, afterWall)

	// The serial loop checks the valve before every Source.Next, including
	// the EOF pull — numBatches+1 checks total.
	if beforePauses != numBatches+1 {
		t.Errorf("before-arm pause count: got %d, want %d (one 50ms pause per source pull is "+
			"the pre-#326 behavior this test measures against)", beforePauses, numBatches+1)
	}
	if minWall := time.Duration(numBatches) * memory.HeapBackpressurePauseDuration; beforeWall < minWall {
		t.Errorf("before-arm wall %v < %v — the sleep valve did not engage; the A/B is vacuous", beforeWall, minWall)
	}
	// The one tolerated pause is the check before the first batch: an EMPTY
	// aggregate holds nothing, and sleeping an operator that holds nothing
	// is exactly the behavior that must survive.
	if afterPauses > 1 {
		t.Errorf("after-arm pause count: got %d, want ≤1 — the dominant-holder aggregate "+
			"must answer the valve instead of being slept", afterPauses)
	}
	if afterWall >= beforeWall/4 {
		t.Errorf("after-arm wall %v not clearly below before-arm %v — drain-instead-of-sleep "+
			"did not reclaim the parked wall time", afterWall, beforeWall)
	}
}

// TestHeapValve_DrainPathStaysCorrect drives the valve's DRAIN branch (the
// #325 gate admits every drain via a tiny floor) and asserts the aggregate's
// output is still exact — the valve triggering real spills must never change
// rows.
func TestHeapValve_DrainPathStaysCorrect(t *testing.T) {
	memory.SetHeapBackpressureForTesting(1)
	defer memory.SetHeapBackpressureForTesting(0)
	defer func(v int64) { drainFloorDivisor = v }(drainFloorDivisor)
	drainFloorDivisor = 1 << 40 // floor ≈ 0: every valve fire drains

	tracker := memory.NewTracker("test", 64<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	p, h, expected := pressuredAggPipeline(sm, 40, 64)
	drains0 := heapDrainCount.Load()
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d := heapDrainCount.Load() - drains0; d == 0 {
		t.Fatal("expected the valve to route through the aggregate's drain path at least once")
	}
	got := drainAggResults(t, h)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Fatalf("group %v: got %d want %d", k, got[k], want)
		}
	}
}

// TestHeapValve_SmallHolderStillSleeps pins the boundary: a breaker holding
// a minority of the tracked bytes (a peer owns the pressure) must NOT
// suppress the GC catch-up pause — that pause is the correct response when
// the pressure is not the sink's live state.
func TestHeapValve_SmallHolderStillSleeps(t *testing.T) {
	memory.SetHeapBackpressureForTesting(1)
	defer memory.SetHeapBackpressureForTesting(0)

	tracker := memory.NewTracker("test", 64<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	// Foreign holder pins most of the tracker: the aggregate's share can
	// never be dominant.
	tracker.ForceReserve(48 << 20)

	p, h, expected := pressuredAggPipeline(sm, 4, 64)
	pauses0 := heapPauseCount.Load()
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if pauses := heapPauseCount.Load() - pauses0; pauses != 5 {
		t.Errorf("minority-holder pause count: got %d, want 5 (one per source pull, incl. "+
			"the EOF pull) — the sleep response must survive for operators that do not "+
			"own the pressure", pauses)
	}
	got := drainAggResults(t, h)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
}
