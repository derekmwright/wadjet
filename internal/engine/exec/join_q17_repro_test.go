package exec

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestHashJoin_StateRetentionAcrossQueries probes worker-process state
// retention between queries. SF100 mc=3 shows progressive slowdown across
// queries in a run (Q01-Q05 within band, Q06+ progressively slower, Q09
// frequently times out). The hypothesis is that operator state, batch
// pools, runtime allocator pages, or other persistent structures aren't
// fully released between query boundaries — and the next query pays the
// cost.
//
// This test simulates N sequential "queries" against the same shared
// tracker + spill manager (mimicking a worker process running many
// queries). Between iterations it forces GC, samples HeapInuse and
// HeapIdle, and reports whether heap retains memory across boundaries.
//
// At SF100: 14.8 GB GOMEMLIMIT - cache reservation = ~14 GB pool. If
// each query leaves ~500 MB of irreclaimable state, after 6-8 queries
// the pool is effectively exhausted before the next query even starts —
// matching the Q09 timeout pattern observed in
// project_bufio_fix_sf100_2026-05-18.md.
//
// Set WADJET_Q17_REPRO=1 to enable.
func TestHashJoin_StateRetentionAcrossQueries(t *testing.T) {
	if os.Getenv("WADJET_Q17_REPRO") != "1" {
		t.Skip("set WADJET_Q17_REPRO=1 to enable")
	}
	const queries = 8     // simulate Q01-Q08
	const concurrent = 3  // mc=3 per query
	const buildN = 500_000

	budget := int64(60 * 1024 * 1024)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
	}

	spillDir := t.TempDir()
	sharedTracker := memory.NewTracker("shared", budget)
	sm, err := memory.NewSpillManager(spillDir, sharedTracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Run one "query" = N concurrent HashJoin builds, then close all.
	runQuery := func(qIdx int) (peakHeap uint64, dur time.Duration) {
		start := time.Now()
		joins := make([]*HashJoin, concurrent)
		for i := range joins {
			hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
			hj.Spill = sm
			hj.MemTracker = sharedTracker
			hj.BuildTableAlias = fmt.Sprintf("q%d-build-%d", qIdx, i)
			joins[i] = hj
		}

		var peak atomic.Uint64
		stopSampler := make(chan struct{})
		var sampWG sync.WaitGroup
		sampWG.Add(1)
		go func() {
			defer sampWG.Done()
			tk := time.NewTicker(20 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stopSampler:
					return
				case <-tk.C:
					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					for {
						cur := peak.Load()
						if ms.HeapAlloc <= cur {
							break
						}
						if peak.CompareAndSwap(cur, ms.HeapAlloc) {
							break
						}
					}
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		gate := make(chan struct{})
		errs := make([]error, concurrent)
		for i, hj := range joins {
			wg.Add(1)
			go func(idx int, j *HashJoin) {
				defer wg.Done()
				<-gate
				src := newGeneratedIntSource(schema, buildN, int64(qIdx*concurrent*buildN+idx*buildN))
				errs[idx] = j.Build(ctx, src)
			}(i, hj)
		}
		close(gate)
		wg.Wait()
		close(stopSampler)
		sampWG.Wait()

		// Close all joins to release operator state — same as what happens
		// at end of a real query.
		for _, j := range joins {
			_ = j.Close()
		}

		for i, e := range errs {
			if e != nil {
				t.Fatalf("query %d build %d: %v", qIdx, i, e)
			}
		}
		return peak.Load(), time.Since(start)
	}

	// Drain to a steady state before measuring baseline.
	runtime.GC()
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	baselineInuse := msBefore.HeapInuse
	baselineAlloc := msBefore.HeapAlloc

	t.Logf("=== State retention probe: %d sequential queries, %d concurrent builds each ===", queries, concurrent)
	t.Logf("Baseline (pre-Q01): HeapInuse=%d KB, HeapAlloc=%d KB", baselineInuse/1024, baselineAlloc/1024)

	for q := 1; q <= queries; q++ {
		peakHeap, dur := runQuery(q)

		// Force GC to drain transient state — measure what STAYS resident.
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		t.Logf("Q%02d: dur=%v peak=%dMB | post-GC: HeapInuse=%dMB HeapAlloc=%dMB delta-vs-baseline=+%dMB",
			q, dur,
			peakHeap/1024/1024,
			ms.HeapInuse/1024/1024,
			ms.HeapAlloc/1024/1024,
			(int64(ms.HeapAlloc)-int64(baselineAlloc))/1024/1024)
	}
}

// generatedIntSource emits N rows of synthetic data (int64 key + int64 val)
// in batches of size 2048 directly into RecordBatch columns — no intermediate
// []map[string]any allocation. Eliminates ~1 GB of test-scaffolding heap
// usage in the Q17-shape probe so the measured peak heap reflects operator-
// internal allocation, not the test's input materialization.
type generatedIntSource struct {
	schema   []parquet.Column
	total    int
	offset   int
	keyStart int64
}

func newGeneratedIntSource(schema []parquet.Column, total int, keyStart int64) *generatedIntSource {
	return &generatedIntSource{schema: schema, total: total, keyStart: keyStart}
}

func (s *generatedIntSource) Init(_ context.Context) error { return nil }

func (s *generatedIntSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.offset >= s.total {
		return nil, nil
	}
	n := batch.DefaultBatchSize
	if s.offset+n > s.total {
		n = s.total - s.offset
	}
	rb := batch.NewRecordBatch(s.schema, n)
	for i := 0; i < n; i++ {
		k := s.keyStart + int64(s.offset+i)
		rb.Columns[0].SetValue(i, k)
		rb.Columns[1].SetValue(i, k*7)
	}
	s.offset += n
	return rb, nil
}

func (s *generatedIntSource) Close() error { return nil }

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
	const buildN = 2_000_000

	// Each build holds ~80 MB column data + intHashTable + arena.
	// 3 concurrent × ~80 MB = ~240 MB cumulative. Budget 120 MB forces
	// spill on all builds. Larger scale (2M rows) reduces the relative
	// weight of fixed-size transients (bufio buffers, batch pool entries)
	// in peak heap — bringing the heap-vs-budget ratio closer to the
	// SF100 production observation of ~1.5×.
	budget := int64(120 * 1024 * 1024)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
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
	buildStart := time.Now()
	for i, hj := range joins {
		wg.Add(1)
		go func(idx int, j *HashJoin) {
			defer wg.Done()
			<-startBarrier // wait so all goroutines race-start concurrently
			src := newGeneratedIntSource(schema, buildN, int64(idx*buildN))
			errs[idx] = j.Build(ctx, src)
		}(i, hj)
	}
	close(startBarrier)
	wg.Wait()
	buildElapsed := time.Since(buildStart)
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

	// Sum spill bytes by walking the spill dir directly — HashJoin's
	// spillBatchWriter path doesn't register files via SpillRows, so
	// SpillManager.SpilledFiles misses them.
	var spillBytes int64
	var spillFiles int
	_ = filepath.Walk(spillDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		spillBytes += info.Size()
		spillFiles++
		return nil
	})

	t.Logf("=== Q17-shape repro summary ===")
	t.Logf("  concurrent builds: %d", concurrent)
	t.Logf("  rows per build:    %d", buildN)
	t.Logf("  tracker budget:    %.2f MB", budgetMB)
	t.Logf("  peak Go heap:      %.2f MB (%.2fx budget)", heapMB, overshoot)
	t.Logf("  final tracker:     %d KB (cumulative %d KB)", usedFinal/1024, cumulativeTracked/1024)
	t.Logf("  spilled:           %d / %d builds, %d files, %.2f MB total",
		spilledCount, concurrent, spillFiles, float64(spillBytes)/1024/1024)
	t.Logf("  build wall time:   %v", buildElapsed)

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
