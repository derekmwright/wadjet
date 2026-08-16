package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSortSpill_FinalizeMergesSpilledRows exercises the external-merge spill
// path: low memory budget triggers sorted-run spill in Consume, then Finalize
// sets up a streaming k-way merge that Next drains into a globally sorted
// result.
//
// Why this test matters: Sort.runFiles is populated by Consume when
// SpillManager.ShouldSpillFor(SpillCheap) trips. SF100 sort-heavy workloads
// (Q21, Q18) hit this path under memory pressure; without coverage it could
// regress silently. Mirrors HashAggregate's TestHashAggregateSpillBatching
// philosophy — set a tiny budget, push enough rows to force spill, verify
// the final output is correct.
func TestSortSpill_FinalizeMergesSpilledRows(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}

	ctx := context.Background()
	spillDir := t.TempDir()
	// 1 KB budget — every Consume's TrackBatch crosses the SpillCheap threshold
	// (60% of 1KB = 600 bytes) so the second-and-subsequent batches spill.
	tracker := memory.NewTracker("sort-spill-test", 1_000)
	sm, err := memory.NewSpillManager(spillDir, tracker)
	if err != nil {
		t.Fatal(err)
	}

	s := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	s.Spill = sm
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Push values in reverse-sorted order across multiple batches so the
	// final sort has real work to do across spill boundaries.
	const numBatches = 20
	const rowsPerBatch = 10
	totalRows := numBatches * rowsPerBatch
	for i := 0; i < numBatches; i++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for j := 0; j < rowsPerBatch; j++ {
			// Descending order: batch 0 has the largest values, batch N-1 the
			// smallest. Spill files therefore contain reverse-sorted runs and
			// finalize must merge them into ascending order.
			v := int64((numBatches-i)*rowsPerBatch - j)
			rows = append(rows, map[string]any{"val": v})
		}
		b := batch.FromRows(schema, rows)
		if err := s.Consume(ctx, b); err != nil {
			t.Fatalf("Consume batch %d: %v", i, err)
		}
	}

	// Sanity check: spill path actually fired (sorted columnar runs).
	if len(s.runFiles) == 0 {
		t.Fatal("spill path was never exercised; tracker/budget setup is wrong")
	}

	if err := s.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Finalize hands runs to the streaming merger; nothing should remain in
	// the pre-finalize run list.
	if len(s.runFiles) != 0 {
		t.Errorf("Finalize should hand off runFiles to the merger, got %d remaining", len(s.runFiles))
	}

	// Drain the sorted output and verify ascending order across every row.
	got := make([]int64, 0, totalRows)
	for {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			got = append(got, r["val"].(int64))
		}
	}
	if len(got) != totalRows {
		t.Fatalf("got %d rows, want %d", len(got), totalRows)
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("output not sorted ascending at index %d: %d < %d", i, got[i], got[i-1])
		}
	}
	// Smallest possible value emitted from the descending generator is 1; largest is totalRows.
	if got[0] != 1 {
		t.Errorf("first row should be 1 (smallest), got %d", got[0])
	}
	if got[len(got)-1] != int64(totalRows) {
		t.Errorf("last row should be %d (largest), got %d", totalRows, got[len(got)-1])
	}
}

// TestSortSpill_MixedInMemoryAndSpilled covers the case where some batches
// are still in s.batches (in memory) at Finalize time because they arrived
// after the most recent spill flush — finalizeWithSpill must merge both
// in-memory and spilled rows.
func TestSortSpill_MixedInMemoryAndSpilled(t *testing.T) {
	forceTinyRuns(t)
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}

	ctx := context.Background()
	spillDir := t.TempDir()
	// Budget tuned to honest per-batch accounting (RecordBatch.MemBytes): a
	// 5-row int64 batch is ~48 bytes (40 data + one 8-byte null-bitmap word),
	// not the ~240 bytes the old b.Len*48+b.Len*40 estimate reported. At a 200
	// byte budget the 40% SpillCheap threshold (80 B) trips every couple of
	// batches; each spill clears the tracker via ReleaseTracking, so pressure
	// relaxes and later batches re-accumulate — exercising the mixed
	// in-memory + spilled merge path in finalizeWithSpill.
	tracker := memory.NewTracker("sort-spill-mixed-test", 200)
	sm, err := memory.NewSpillManager(spillDir, tracker)
	if err != nil {
		t.Fatal(err)
	}

	s := NewSort([]SortKey{{Column: "val", Order: Descending}})
	s.Spill = sm
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}

	const numBatches = 8
	const rowsPerBatch = 5
	totalRows := numBatches * rowsPerBatch
	for i := 0; i < numBatches; i++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for j := 0; j < rowsPerBatch; j++ {
			rows = append(rows, map[string]any{"val": int64(i*rowsPerBatch + j)})
		}
		b := batch.FromRows(schema, rows)
		if err := s.Consume(ctx, b); err != nil {
			t.Fatalf("Consume batch %d: %v", i, err)
		}
	}

	if len(s.runFiles) == 0 {
		t.Fatal("spill path was never exercised")
	}

	if err := s.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	got := make([]int64, 0, totalRows)
	for {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			got = append(got, r["val"].(int64))
		}
	}
	if len(got) != totalRows {
		t.Fatalf("got %d rows, want %d", len(got), totalRows)
	}
	// Descending order — first row is the largest value (totalRows-1).
	for i := 1; i < len(got); i++ {
		if got[i] > got[i-1] {
			t.Fatalf("output not sorted descending at index %d: %d > %d", i, got[i], got[i-1])
		}
	}
	if got[0] != int64(totalRows-1) {
		t.Errorf("first row should be %d (largest), got %d", totalRows-1, got[0])
	}
	if got[len(got)-1] != 0 {
		t.Errorf("last row should be 0 (smallest), got %d", got[len(got)-1])
	}
}
