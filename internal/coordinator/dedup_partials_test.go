package coordinator

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression tests for sweep finding #18: deduplicatePartials boxed the
// entire merged DISTINCT result ×3 (ToRows maps + fmt-serialized key set +
// FromRows re-materialization) and its "%v\x00"-joined key collided for
// values containing NUL bytes. It now dedups columnar via the keys-only
// hash aggregate (GroupByAll) with typed binary keys.

// Two distinct rows whose old fmt keys were identical:
// ("a\x00", "b") → "a\x00\x00b\x00" and ("a", "\x00b") → "a\x00\x00b\x00".
func TestDeduplicatePartials_NulByteKeysDoNotCollide(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeString},
		{Name: "y", Type: parquet.TypeString},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"x": "a\x00", "y": "b"},
		{"x": "a", "y": "\x00b"},
	})

	c := &Coordinator{}
	out, err := c.deduplicatePartials(newSliceStream([]*batch.RecordBatch{b}), []string{"x", "y"})
	if err != nil {
		t.Fatal(err)
	}

	rows := 0
	for _, ob := range out {
		rows += ob.ActiveLen()
	}
	if rows != 2 {
		t.Fatalf("got %d rows, want 2 — NUL-containing values collapsed into one group", rows)
	}
}

func TestDeduplicatePartials_TypedColumns(t *testing.T) {
	schema := []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
	}
	mk := func(rows ...[2]any) *batch.RecordBatch {
		out := make([]map[string]any, len(rows))
		for i, r := range rows {
			out[i] = map[string]any{"n": r[0], "f": r[1]}
		}
		return batch.FromRows(schema, out)
	}
	batches := []*batch.RecordBatch{
		mk([2]any{int64(1), 1.5}, [2]any{int64(2), 2.5}),
		mk([2]any{int64(1), 1.5}, [2]any{int64(2), 99.0}), // (2, 99.0) is distinct from (2, 2.5)
	}

	c := &Coordinator{}
	out, err := c.deduplicatePartials(newSliceStream(batches), []string{"n", "f"})
	if err != nil {
		t.Fatal(err)
	}

	rows := 0
	for _, ob := range out {
		rows += ob.ActiveLen()
		// Output schema must preserve names and types for the downstream
		// sort/limit/colIdx machinery.
		if ob.Schema[0].Name != "n" || ob.Schema[1].Name != "f" {
			t.Fatalf("schema names = %v, want [n f]", ob.Schema)
		}
		if ob.Schema[0].Type != parquet.TypeInt64 || ob.Schema[1].Type != parquet.TypeFloat64 {
			t.Fatalf("schema types not preserved: %v", ob.Schema)
		}
	}
	if rows != 3 {
		t.Fatalf("got %d rows, want 3", rows)
	}
}
