package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestColumnIndexFallback_Bidirectional locks in the bidirectional fallback
// logic. The Q07 self-join fix relies on:
//   - Qualified key ("n1.n_name") lookup falling back to unqualified
//     ("n_name") when the schema is from a raw scan.
//   - Unqualified key ("n_regionkey") lookup falling back to a single
//     suffix-matching qualified column ("n1.n_regionkey") when the schema
//     is from a self-join intermediate.
//   - Returning -1 when an unqualified lookup is ambiguous (multiple
//     suffix matches), refusing to silently pick one.
func TestColumnIndexFallback_Bidirectional(t *testing.T) {
	mk := func(names ...string) *batch.RecordBatch {
		schema := make([]parquet.Column, len(names))
		for i, n := range names {
			schema[i] = parquet.Column{Name: n, Type: parquet.TypeString}
		}
		return batch.NewRecordBatch(schema, 1)
	}

	t.Run("exact match", func(t *testing.T) {
		b := mk("a", "b", "c")
		if got := ColumnIndexFallback(b, "b"); got != 1 {
			t.Errorf("exact: got %d, want 1", got)
		}
	})

	t.Run("qualified to unqualified", func(t *testing.T) {
		b := mk("n_nationkey", "n_name")
		if got := ColumnIndexFallback(b, "n1.n_name"); got != 1 {
			t.Errorf("qual->unqual: got %d, want 1", got)
		}
	})

	t.Run("unqualified to single qualified", func(t *testing.T) {
		b := mk("s_suppkey", "n1.n_name", "n1.n_regionkey")
		if got := ColumnIndexFallback(b, "n_regionkey"); got != 2 {
			t.Errorf("unqual->qual: got %d, want 2", got)
		}
	})

	t.Run("unqualified ambiguous returns -1", func(t *testing.T) {
		b := mk("n1.n_name", "n2.n_name")
		if got := ColumnIndexFallback(b, "n_name"); got != -1 {
			t.Errorf("ambiguous: got %d, want -1 (refuse to guess)", got)
		}
	})

	t.Run("not found", func(t *testing.T) {
		b := mk("a", "b")
		if got := ColumnIndexFallback(b, "z"); got != -1 {
			t.Errorf("missing: got %d, want -1", got)
		}
	})
}
