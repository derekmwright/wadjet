package worker

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestApplyPostFilter_SnapshotsSelAcrossBatches verifies that applyPostFilter
// snapshots each batch's selection vector before retaining it. Without the
// snapshot, a single Filter operator's reused outSel buffer is overwritten
// by the next batch, clobbering the prior batch's Sel — exactly the bug that
// caused Q07 to retain (FRANCE,FRANCE) and (GERMANY,GERMANY) rows the
// OR-WHERE was supposed to reject (Bug C).
//
// Repro: feed two batches through the same applyPostFilter call, where the
// filter accepts only specific rows. Verify the rows actually surviving
// each batch match the predicate, not stale indices from the other batch.
func TestApplyPostFilter_SnapshotsSelAcrossBatches(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
	}

	// Batch A: rows [10, 20, 30, 40, 50] — predicate accepts <30 → expect rows 0, 1.
	bA := batch.NewRecordBatch(schema, 5)
	for i, v := range []int64{10, 20, 30, 40, 50} {
		bA.Columns[0].Int64Data[i] = v
		bA.Columns[0].Nulls.SetValid(i)
	}
	bA.Len = 5

	// Batch B: rows [5, 15, 99] — predicate accepts <30 → expect rows 0, 1.
	bB := batch.NewRecordBatch(schema, 3)
	for i, v := range []int64{5, 15, 99} {
		bB.Columns[0].Int64Data[i] = v
		bB.Columns[0].Nulls.SetValid(i)
	}
	bB.Len = 3

	task := distributed.Task{
		ID:              "test",
		StageID:         "test-stage",
		PostFilterExprs: []string{"k < 30"},
	}
	out, err := applyPostFilter(context.Background(), task, []*batch.RecordBatch{bA, bB})
	if err != nil {
		t.Fatalf("applyPostFilter: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 batches, got %d", len(out))
	}
	// Read the surviving rows from each batch via Sel.
	collect := func(b *batch.RecordBatch) []int64 {
		var vals []int64
		n := b.ActiveLen()
		for i := 0; i < n; i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			vals = append(vals, b.Columns[0].Int64Data[row])
		}
		return vals
	}
	gotA := collect(out[0])
	gotB := collect(out[1])
	wantA := []int64{10, 20}
	wantB := []int64{5, 15}
	if !int64SliceEq(gotA, wantA) {
		t.Errorf("batch A surviving rows: got %v, want %v (Sel was clobbered by next batch's filter call)", gotA, wantA)
	}
	if !int64SliceEq(gotB, wantB) {
		t.Errorf("batch B surviving rows: got %v, want %v", gotB, wantB)
	}
}

func int64SliceEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
