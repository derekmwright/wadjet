package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression test for sweep finding #26: CollectSink.ToRows kept the
// columnar batches alive after boxing, so the full result lived twice
// (columnar + boxed) for the sink's lifetime. ToRows must release the
// batches as it converts, while Schema() keeps the schema available for
// callers that previously recovered it via Batches()[0].Schema.
func TestCollectSink_ToRowsReleasesBatches(t *testing.T) {
	schema := []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	mk := func(rows ...map[string]any) *batch.RecordBatch {
		return batch.FromRows(schema, rows)
	}

	sink := &CollectSink{}
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.Consume(ctx, mk(
		map[string]any{"n": int64(1), "s": "alpha"},
		map[string]any{"n": int64(2), "s": "beta"},
	)); err != nil {
		t.Fatal(err)
	}
	if err := sink.Consume(ctx, mk(
		map[string]any{"n": int64(3), "s": "gamma"},
	)); err != nil {
		t.Fatal(err)
	}

	rows := sink.ToRows()
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	// Boxed values must remain valid after the batches are released.
	if rows[0]["s"] != "alpha" || rows[2]["s"] != "gamma" {
		t.Errorf("boxed string values corrupted: %v / %v", rows[0]["s"], rows[2]["s"])
	}
	if rows[1]["n"] != int64(2) {
		t.Errorf("rows[1][n] = %v, want 2", rows[1]["n"])
	}

	if got := sink.Batches(); got != nil {
		t.Errorf("Batches() = %d batches after ToRows, want nil (released)", len(got))
	}
	if got := sink.Schema(); len(got) != 2 || got[0].Name != "n" {
		t.Errorf("Schema() = %v after ToRows, want the consumed schema", got)
	}

	// Second call must be idempotent, not re-convert or lose rows.
	if again := sink.ToRows(); len(again) != 3 {
		t.Fatalf("second ToRows returned %d rows, want 3", len(again))
	}
}

// SkipFinalizeToRows is the worker-stage contract: Finalize must not box,
// and Batches must stay available.
func TestCollectSink_SkipFinalizeToRows(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	sink := &CollectSink{SkipFinalizeToRows: true}
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.Consume(ctx, batch.FromRows(schema, []map[string]any{{"n": int64(7)}})); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.Rows != nil {
		t.Error("Finalize boxed rows despite SkipFinalizeToRows")
	}
	if got := sink.Batches(); len(got) != 1 {
		t.Fatalf("Batches() = %d, want 1", len(got))
	}
}

// MaxBytes bounds the collected result: Consume must return
// ErrCollectBudget once accumulated batch bytes exceed it, so callers with
// a cheaper home for oversized results (the coordinator's local fast path
// re-dispatches to the DAG) can bail out instead of growing the heap.
func TestCollectSink_MaxBytes(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	mk := func() *batch.RecordBatch {
		return batch.FromRows(schema, []map[string]any{{"n": int64(1)}, {"n": int64(2)}})
	}
	sink := &CollectSink{MaxBytes: 1}
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatal(err)
	}
	err := sink.Consume(ctx, mk())
	if !errors.Is(err, ErrCollectBudget) {
		t.Fatalf("expected ErrCollectBudget, got %v", err)
	}

	unbounded := &CollectSink{}
	if err := unbounded.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := unbounded.Consume(ctx, mk()); err != nil {
			t.Fatalf("unbounded sink errored: %v", err)
		}
	}
}
