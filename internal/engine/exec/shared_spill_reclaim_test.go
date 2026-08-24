package exec

import (
	"context"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// These tests establish the #324 truth: on the SHARED (worker-injected)
// SpillManager path, plan.Cleanup never calls sm.Cleanup() — doing so would
// unlink concurrent queries' files — so every spill file must be removed by
// the operator that wrote it, on the success path AND on the failure path
// (early Close without Finalize, i.e. cancellation or a peer operator's
// error). The manager here is deliberately reused across "queries" and its
// Cleanup() is never called, exactly like Executor.sharedSpill on a worker.
//
// Failing-first: reverting the RemoveSpilled calls in HashAggregate.Finalize
// (legacy raw-row loop) or HashAggregate.Close / Window.Close makes the
// legacy-path arms fail with files left in the spill dir.

// countSpillFiles returns how many files (not directories) live under the
// manager's spill dir.
func countSpillFiles(t *testing.T, sm *memory.SpillManager) int {
	t.Helper()
	entries, err := os.ReadDir(sm.SpillDir())
	if err != nil {
		t.Fatalf("reading spill dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	return n
}

func assertSpillDirEmpty(t *testing.T, sm *memory.SpillManager, phase string) {
	t.Helper()
	if n := countSpillFiles(t, sm); n != 0 {
		entries, _ := os.ReadDir(sm.SpillDir())
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("%s: %d spill file(s) leaked on the shared manager: %v", phase, n, names)
	}
	if files := sm.SpilledFiles(); len(files) != 0 {
		t.Errorf("%s: SpillManager still tracks %d file path(s): %v — on the shared "+
			"manager this list is never reset and grows for the worker's lifetime",
			phase, len(files), files)
	}
}

// newSharedPressuredManager builds a spill manager whose tracker sits above
// the 40% SpillCheap threshold from the start (a peer holds most of the
// budget), so operators spill from the first batch — the shared-worker
// regime under memory pressure.
func newSharedPressuredManager(t *testing.T) *memory.SpillManager {
	t.Helper()
	tracker := memory.NewTracker("shared", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900_000)
	return sm
}

func aggInputBatches(numBatches, rowsPerBatch int) []*batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	out := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			n := int64(bi*rowsPerBatch + ri)
			rows = append(rows, map[string]any{"k": n % 7, "v": n})
		}
		out = append(out, batch.FromRows(schema, rows))
	}
	return out
}

// legacyAgg builds a HashAggregate that CANNOT use the partial-state
// external merge (STDDEV is not a kernel.Accumulator shape), so pressure
// routes it through the legacy raw-row spill — the SpillRows path whose
// files land in SpillManager.files and, before #324, were only ever removed
// by sm.Cleanup().
func legacyAgg(sm *memory.SpillManager) *HashAggregate {
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggStddev, InputCol: "v", OutputCol: "sd", OutputType: parquet.TypeFloat64},
	})
	h.Spill = sm
	return h
}

func TestSharedSpillManager_LegacyAggSpillReclaimedOnCompletion(t *testing.T) {
	defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
	spillFileTargetBytes = 1024 // force real files, not just the in-memory buffer

	sm := newSharedPressuredManager(t)
	ctx := context.Background()

	// Two consecutive "queries" through the same shared manager: the second
	// proves the first's files were removed by the operator, not left for a
	// Cleanup that never comes.
	for q := 0; q < 2; q++ {
		h := legacyAgg(sm)
		if err := h.Init(ctx); err != nil {
			t.Fatalf("query %d Init: %v", q, err)
		}
		for i, b := range aggInputBatches(20, 64) {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatalf("query %d Consume #%d: %v", q, i, err)
			}
		}
		if countSpillFiles(t, sm) == 0 {
			t.Fatalf("query %d: expected legacy raw-row spill files on disk before Finalize "+
				"(the pressure setup no longer forces the SpillRows path — test is vacuous)", q)
		}
		if err := h.Finalize(ctx); err != nil {
			t.Fatalf("query %d Finalize: %v", q, err)
		}
		rows := 0
		for {
			out, err := h.Next(ctx)
			if err != nil {
				t.Fatalf("query %d Next: %v", q, err)
			}
			if out == nil {
				break
			}
			rows += out.ActiveLen()
		}
		if rows != 7 {
			t.Fatalf("query %d: got %d groups, want 7", q, rows)
		}
		if err := h.Close(); err != nil {
			t.Fatalf("query %d Close: %v", q, err)
		}
		assertSpillDirEmpty(t, sm, "after completed legacy-spill query")
	}
}

func TestSharedSpillManager_LegacyAggSpillReclaimedOnFailure(t *testing.T) {
	defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
	spillFileTargetBytes = 1024

	sm := newSharedPressuredManager(t)
	ctx := context.Background()

	// The failure path: the query dies (peer error / cancellation) after this
	// operator spilled but before Finalize. Close is the only reclamation
	// hook that runs — the error path is where leaks live.
	h := legacyAgg(sm)
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i, b := range aggInputBatches(20, 64) {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
	}
	if countSpillFiles(t, sm) == 0 {
		t.Fatal("expected legacy raw-row spill files on disk before the simulated failure")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertSpillDirEmpty(t, sm, "after failed legacy-spill query")
}

func TestSharedSpillManager_PartialStateAggReclaimed(t *testing.T) {
	// The modern external-merge path (agg-spill-*.bin partial runs) removes
	// its files inline; this arm pins that invariant on the same shared
	// manager, completion and failure both.
	sm := newSharedPressuredManager(t)
	ctx := context.Background()

	for _, arm := range []string{"completed", "failed"} {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		if err := h.Init(ctx); err != nil {
			t.Fatalf("%s Init: %v", arm, err)
		}
		for i, b := range aggInputBatches(20, 64) {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatalf("%s Consume #%d: %v", arm, i, err)
			}
		}
		if arm == "completed" {
			if err := h.Finalize(ctx); err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			for {
				out, err := h.Next(ctx)
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				if out == nil {
					break
				}
			}
		}
		if err := h.Close(); err != nil {
			t.Fatalf("%s Close: %v", arm, err)
		}
		assertSpillDirEmpty(t, sm, "partial-state agg, "+arm)
	}
}

func TestSharedSpillManager_SortRunsReclaimed(t *testing.T) {
	defer func(v int64) { minSortRunBytes = v }(minSortRunBytes)
	minSortRunBytes = 1024

	sm := newSharedPressuredManager(t)
	ctx := context.Background()

	for _, arm := range []string{"completed", "failed"} {
		s := &Sort{Keys: []SortKey{{Column: "v"}}, Limit: -1, Spill: sm}
		if err := s.Init(ctx); err != nil {
			t.Fatalf("%s Init: %v", arm, err)
		}
		for i, b := range aggInputBatches(20, 64) {
			if err := s.Consume(ctx, b); err != nil {
				t.Fatalf("%s Consume #%d: %v", arm, i, err)
			}
		}
		if arm == "completed" {
			if err := s.Finalize(ctx); err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			for {
				out, err := s.Next(ctx)
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				if out == nil {
					break
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("%s Close: %v", arm, err)
		}
		assertSpillDirEmpty(t, sm, "sort runs, "+arm)
	}
}
