package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A morsel-parallel Sort clone owns ONE spill artifact — `runFiles`, its
// sorted columnar runs — and it has to reach the primary at `MergeSink`
// (ADR-0027 decision 1, #864).
//
// The clone's `Close` DELETES its run files (`removeRunFiles(s.runFiles)`), so
// a run the merge does not take is not merely un-merged: it is unlinked, and
// every row in it leaves the answer with no error anywhere. This is the
// operator-level twin of `TestCloneRunListsSurviveEveryMergeBranch`, which
// asserts the same thing for the HashAggregate's four lists.
//
// It drives the merge directly rather than through a pipeline for the reason
// that gate does: whether a clone writes a run is decided by how it was asked
// to spill, and the end-to-end arm (the spill sweep's `forcedRunArm`) proves
// the same claim through the planner.
func TestASortCloneRunFilesSurviveTheMerge(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}
	batchOf := func(start, n int) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, n)
		for i := 0; i < n; i++ {
			b.Columns[0].Int64Data[i] = int64(start + i)
			b.Columns[1].SetValue(i, "pad-value-that-takes-some-bytes")
		}
		b.Len = n
		return b
	}

	ctx := context.Background()
	tracker := memory.NewTracker("query", 8<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	primary := NewSort([]SortKey{{Column: "id", Order: Ascending}})
	primary.Spill = sm
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// The primary consumes one batch of its own, so the merge is a real merge
	// and not a hand-over of the clone's whole state.
	if err := primary.Consume(ctx, batchOf(0, 500)); err != nil {
		t.Fatalf("primary consume: %v", err)
	}

	clone := primary.CloneSink().(*Sort)
	// The clone charges a TRACKING-ONLY view, which is what production gives
	// it (pipeline.wireCloneSinkSpill): its ShouldSpillFor is always false, so
	// the knob below is the only way its runs exist at all — which is exactly
	// why this defect was latent.
	clone.Spill = sm.TrackingOnlyView()
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}

	restore := ForceSortSpillEvery(1)
	runsBefore := SortRunsWritten.Load()
	for i := 0; i < 4; i++ {
		if err := clone.Consume(ctx, batchOf(500+i*500, 500)); err != nil {
			ForceSortSpillEvery(restore)
			t.Fatalf("clone consume %d: %v", i, err)
		}
	}
	ForceSortSpillEvery(restore)

	// ENGAGEMENT: without run files this cell is a no-op (ADR-0027 decision 5).
	if len(clone.runFiles) == 0 {
		t.Fatalf("the clone wrote no run files (%d runs written process-wide), so this cell "+
			"cannot see the transfer", SortRunsWritten.Load()-runsBefore)
	}
	wantRuns := len(clone.runFiles)

	primary.MergeSink(clone)
	if len(clone.runFiles) != 0 {
		t.Errorf("the clone still owns %d run files after the merge; its Close will delete them",
			len(clone.runFiles))
	}
	if len(primary.runFiles) != wantRuns {
		t.Fatalf("the primary owns %d of the clone's %d run files after the merge",
			len(primary.runFiles), wantRuns)
	}
	// Closing the clone is what production does next, and it is what turns a
	// missed transfer into deleted rows.
	if err := clone.Close(); err != nil {
		t.Fatalf("clone close: %v", err)
	}

	if err := primary.Finalize(ctx); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	var got []int64
	for {
		out, err := primary.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if out == nil {
			break
		}
		for i := 0; i < out.ActiveLen(); i++ {
			row := i
			if out.Sel != nil {
				row = int(out.Sel[i])
			}
			got = append(got, out.Columns[0].Int64Data[row])
		}
	}
	if err := primary.Close(); err != nil {
		t.Fatalf("primary close: %v", err)
	}
	// 2,500 rows in, 2,500 out, in key order. A lost run shows up as a short
	// count; a mis-merged one shows up as a break in the sequence.
	if len(got) != 2500 {
		t.Fatalf("got %d rows, want 2500 — the clone's runs did not all reach the merge", len(got))
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("row %d is id %d, want %d: the merge is not in key order", i, v, i)
		}
	}
}
