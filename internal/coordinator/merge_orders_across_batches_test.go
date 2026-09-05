package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestMergeOrdersAcrossBatches is the second half of "the gather orders every
// type" (#644's section), and it is the half that was missing: the comparator
// orders every type and its CALLER declined to order anything at all once the
// input was more than one batch.
//
//	if len(batches) != 1 {
//	    return // reAggregatePartials produces a single batch
//	}
//
// True of the re-aggregating path and of no other. A merge with no aggregate
// and no DISTINCT to collapse it takes `drainStream`, which yields one batch
// per partial, and the ORDER BY was dropped on the floor — silently, in
// whatever order the tasks happened to finish in.
//
// It drives `sortBatches` and `topKBatches` directly, for the reason
// `TestSortBatchesOrdersMissingTypes` (#548) and
// `TestF1ADistributedOrderByOrdersEveryContainerType` (#644) do: a gate whose
// trigger is a plan SHAPE cannot be relied on to fire, and the shapes that
// reach this function are narrow.
func TestMergeOrdersAcrossBatches(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	}
	// Three batches whose rows interleave under the ordering: no
	// concatenation of them in any order is already sorted, so a comparator
	// that never runs cannot pass by luck.
	mk := func(rows ...[2]any) *batch.RecordBatch {
		out := make([]map[string]any, len(rows))
		for i, r := range rows {
			out[i] = map[string]any{"id": r[0], "k": r[1]}
		}
		return batch.FromRows(schema, out)
	}
	build := func() []*batch.RecordBatch {
		return []*batch.RecordBatch{
			mk([2]any{int64(1), int64(30)}, [2]any{int64(2), int64(10)}),
			mk([2]any{int64(3), int64(40)}, [2]any{int64(4), int64(20)}),
			mk([2]any{int64(5), int64(50)}, [2]any{int64(6), nil}),
		}
	}
	colIdx := map[string]int{"id": 0, "k": 1}
	cols := []string{"id", "k"}
	read := func(batches []*batch.RecordBatch) []int64 {
		var got []int64
		for _, b := range batches {
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				got = append(got, b.Columns[0].Int64Data[row])
			}
		}
		return got
	}
	same := func(t *testing.T, got, want []int64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("id order = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("id order = %v, want %v", got, want)
			}
		}
	}

	c := &Coordinator{}
	t.Run("sort ascending, NULLS LAST", func(t *testing.T) {
		got := read(c.sortBatches(build(), cols, colIdx, []logical.OrderExpr{{Column: "k"}}))
		same(t, got, []int64{2, 4, 1, 3, 5, 6})
	})
	t.Run("sort descending, NULLS FIRST", func(t *testing.T) {
		got := read(c.sortBatches(build(), cols, colIdx, []logical.OrderExpr{{Column: "k", Desc: true}}))
		same(t, got, []int64{6, 5, 3, 1, 4, 2})
	})
	t.Run("top-K across batches", func(t *testing.T) {
		got := read(c.topKBatches(build(), cols, colIdx, []logical.OrderExpr{{Column: "k"}}, 3))
		same(t, got, []int64{2, 4, 1})
	})
	t.Run("one batch is untouched", func(t *testing.T) {
		one := []*batch.RecordBatch{mk([2]any{int64(9), int64(1)}, [2]any{int64(8), int64(2)})}
		out := c.sortBatches(one, cols, colIdx, []logical.OrderExpr{{Column: "k"}})
		if len(out) != 1 || out[0] != one[0] {
			t.Fatalf("a single-batch input was replaced; it must be sorted in place")
		}
		same(t, read(out), []int64{9, 8})
	})
	t.Run("a mismatched schema declines rather than corrupts", func(t *testing.T) {
		other := batch.FromRows([]parquet.Column{{Name: "z", Type: parquet.TypeInt64}},
			[]map[string]any{{"z": int64(1)}})
		in := append(build(), other)
		out := coalesceForOrdering(in)
		if len(out) != len(in) {
			t.Fatalf("coalesced %d batches of differing schema into %d — it must decline",
				len(in), len(out))
		}
	})
}
