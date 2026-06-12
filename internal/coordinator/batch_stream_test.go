package coordinator

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// mustRows drains r.Rows() and fails the test on a stream error. Shared by
// every coordinator test that asserts on materialized result rows.
func mustRows(tb testing.TB, r *SQLResult) []map[string]any {
	tb.Helper()
	rows, err := r.Rows()
	if err != nil {
		tb.Fatalf("SQLResult.Rows: %v", err)
	}
	return rows
}

func intBatch(vals ...int64) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		b.Columns[0].Int64Data[i] = v
		b.Columns[0].Nulls.SetValid(i)
	}
	b.Len = len(vals)
	return b
}

func TestSliceStream_DrainsAndDropsRefs(t *testing.T) {
	ctx := context.Background()
	batches := []*batch.RecordBatch{intBatch(1, 2), nil, intBatch(3)}
	s := newSliceStream(batches)

	b1, err := s.Next(ctx)
	if err != nil || b1 == nil || b1.Len != 2 {
		t.Fatalf("first Next = %v, %v", b1, err)
	}
	if batches[0] != nil {
		t.Fatal("stream did not drop its reference to the yielded batch")
	}
	b2, err := s.Next(ctx)
	if err != nil || b2 == nil || b2.Len != 1 {
		t.Fatalf("second Next = %v, %v (nil entries must be skipped)", b2, err)
	}
	for i := 0; i < 3; i++ {
		b, err := s.Next(ctx)
		if b != nil || err != nil {
			t.Fatalf("exhausted Next = %v, %v, want nil, nil", b, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSQLResult_StreamSingleConsumer(t *testing.T) {
	ctx := context.Background()
	r := &SQLResult{Batches: []*batch.RecordBatch{intBatch(1)}}
	s := r.Stream()
	if r.Batches != nil {
		t.Fatal("Stream() must detach Batches from the result")
	}
	b, err := s.Next(ctx)
	if err != nil || b == nil {
		t.Fatalf("Next = %v, %v", b, err)
	}
	// Second Stream() call yields an empty stream, not the same data.
	s2 := r.Stream()
	if b2, _ := s2.Next(ctx); b2 != nil {
		t.Fatal("second Stream() must be empty")
	}
}

func TestSQLResult_RowsDrainsStream(t *testing.T) {
	r := &SQLResult{stream: newSliceStream([]*batch.RecordBatch{intBatch(7, 8), intBatch(9)})}
	rows := mustRows(t, r)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[2]["x"] != int64(9) {
		t.Fatalf("rows[2] = %v", rows[2])
	}
}

func TestSQLResult_CloseIdempotentNilSafe(t *testing.T) {
	var nilRes *SQLResult
	if err := nilRes.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	r := &SQLResult{stream: newSliceStream([]*batch.RecordBatch{intBatch(1)}), Batches: nil}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if b, _ := r.Stream().Next(context.Background()); b != nil {
		t.Fatal("Stream after Close must be empty")
	}
}
