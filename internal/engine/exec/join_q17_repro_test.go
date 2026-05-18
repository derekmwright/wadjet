package exec

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestHashJoin_Q17ShapeRepro is a local test bench for the Q17 SF100 mc=3
// heap-pin failure shape. It runs N concurrent HashJoin builds against a
// shared tracker tighter than their cumulative footprint and surfaces:
//   - whether cooperative spill keeps tracker.Used under budget
//   - peak Go HeapAlloc relative to the tracker budget (the gap is the
//     untracked allocation surface — batch transients, scratch buffers,
//     arena residual)
//   - whether all builds complete or any deadlock
//
// At SF100: 3 concurrent fragment tasks held ~5.7 GB tracked each, 17 GB
// cumulative > 14.8 GB GOMEMLIMIT → worker pinned in GC mark-assist,
// Q17 stalled 14 min (project_q17_sf100_instrumented_2026-05-17.md).
//
// This is NOT a regression gate — it's a probe. Run with -v to inspect
// the heap-vs-budget ratio under different scale/budget settings. Useful
// for evaluating operator-internal fixes (partial-drain, arena reclaim,
// earlier spill trigger) WITHOUT paying for SF100 EC2 deploys.
//
// Set WADJET_Q17_REPRO=1 to enable. Skipped by default because it spawns
// goroutines and exercises spill paths (slow + temp files).
func TestHashJoin_Q17ShapeRepro(t *testing.T) {
	if os.Getenv("WADJET_Q17_REPRO") != "1" {
		t.Skip("set WADJET_Q17_REPRO=1 to enable")
	}
	const concurrent = 3
	const buildN = 80_000

	// Each build holds ~3 MB column data + intHashTable + arena.
	// 3 concurrent × ~3 MB = ~9 MB cumulative. Budget 5 MB forces spill
	// to fire on at least one build (cumulative tracker > 60% of 5 MB
	// triggers SpillCheap).
	budget := int64(5 * 1024 * 1024)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	// Pad the string value so each row is ~30 bytes, giving each build
	// enough state to actually trigger spill under the tight budget.
	pad := strings.Repeat("x", 20)
	makeRows := func(offset int) []map[string]any {
		rows := make([]map[string]any, buildN)
		for i := range rows {
			rows[i] = map[string]any{
				"id":  int64(offset + i),
				"val": pad,
			}
		}
		return rows
	}

	spillDir := t.TempDir()
	sharedTracker := memory.NewTracker("shared", budget)
	sm, err := memory.NewSpillManager(spillDir, sharedTracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	joins := make([]*HashJoin, concurrent)
	for i := range joins {
		hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
		hj.Spill = sm
		hj.MemTracker = sharedTracker
		// Tag the build alias so SpillableName attribution is readable
		// in any future logging of this test bench.
		hj.BuildTableAlias = fmt.Sprintf("build-%d", i)
		joins[i] = hj
	}

	// Background HeapAlloc sampler — captures peak across the run.
	var peakHeap atomic.Uint64
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				for {
					cur := peakHeap.Load()
					if ms.HeapAlloc <= cur {
						break
					}
					if peakHeap.CompareAndSwap(cur, ms.HeapAlloc) {
						break
					}
				}
			}
		}
	}()

	// Launch concurrent builds. The pattern mirrors what fragment runners
	// do at SF100 mc=3 — each fragment goroutine drives one HashJoin.Build
	// against the same shared tracker + spill manager.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, concurrent)
	startBarrier := make(chan struct{})
	for i, hj := range joins {
		wg.Add(1)
		go func(idx int, j *HashJoin) {
			defer wg.Done()
			<-startBarrier // wait so all goroutines race-start concurrently
			errs[idx] = j.Build(ctx, NewSliceSource(schema, makeRows(idx*buildN)))
		}(i, hj)
	}
	close(startBarrier)
	wg.Wait()
	close(stopSampler)
	samplerWG.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Build %d failed: %v", i, err)
		}
	}

	// Capture observations.
	final := peakHeap.Load()
	usedFinal := sharedTracker.Used()
	cumulativeTracked := int64(0)
	spilledCount := 0
	for i, j := range joins {
		tm := j.TrackedMem()
		cumulativeTracked += tm
		t.Logf("join[%d] build=%s trackedMem=%d (%d KB) spillState=%v",
			i, j.BuildTableAlias, tm, tm/1024, j.SpillState() != nil)
		if j.SpillState() != nil {
			spilledCount++
		}
	}

	heapMB := float64(final) / 1024 / 1024
	budgetMB := float64(budget) / 1024 / 1024
	overshoot := float64(final) / float64(budget)

	t.Logf("=== Q17-shape repro summary ===")
	t.Logf("  concurrent builds: %d", concurrent)
	t.Logf("  rows per build:    %d", buildN)
	t.Logf("  tracker budget:    %.2f MB", budgetMB)
	t.Logf("  peak Go heap:      %.2f MB (%.2fx budget)", heapMB, overshoot)
	t.Logf("  final tracker:     %d KB (cumulative %d KB)", usedFinal/1024, cumulativeTracked/1024)
	t.Logf("  spilled:           %d / %d builds", spilledCount, concurrent)

	// CORRECTNESS gate: tracker must stay near budget (cooperative spill
	// works). >2× budget would mean the per-tracker spill threshold is
	// broken — that's a real regression.
	if usedFinal > budget*2 {
		t.Errorf("tracker.Used %d > 2× budget %d — cooperative spill is not closing the gap",
			usedFinal, budget)
	}
	// OBSERVABILITY (not a gate): peak-heap-to-budget ratio surfaces the
	// gap between tracked memory and actual Go heap. At SF100 this ratio
	// was ~1.5× (22 GB heap / 14.8 GB GOMEMLIMIT). At small test scale,
	// fixed-size transients (batch pool, intHashTable grow buffers, hash
	// table scratch) are a larger fraction of a small budget, so the
	// ratio is naturally higher — that's why this is logged not asserted.
	//
	// Operator-internal fixes (HashJoin partial-drain, arena reclaim,
	// earlier coop-spill trigger) should REDUCE this ratio when re-run
	// against the same scale. Compare runs, don't compare to a fixed
	// threshold.
}
