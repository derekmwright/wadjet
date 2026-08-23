package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCountSharingGenericSoAMerge is the regression test for #402.
//
// Two COUNTs of the same column share one count[] array: the second keeps
// count nil and resolves through countArrayOf. materializeFlatAccums — the
// generic/string-key merge path, reached from mergeSinkState via
// migrateToGenericMap — measured each aggregate with flatAccumLen, which
// probes only that aggregate's OWN arrays. A shared COUNT owns none, so it
// measured 0, the bound check skipped it for every group, and it emitted 0
// while its partner emitted the right number.
//
// The shape needed to reach it: a group key that takes the generic SoA path
// (one FLOAT64 column — int/packed/compact/string keys all take a different
// merge), a duplicate COUNT, and a MergeSink of a clone that does NOT
// qualify for adoption or for the external-merge drain, which is what makes
// mergeSinkState fall through to migrateToGenericMap. An EMPTY clone is the
// simplest such clone, and is exactly what the pipeline merges when a worker
// lane consumed nothing.
//
// Downstream this decided a top-N: ORDER BY <the zeroed COUNT> DESC picked a
// different pair of rows under a total order.
func TestCountSharingGenericSoAMerge(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeFloat64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	parent := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, InputCol: "v", OutputCol: "c2", OutputType: parquet.TypeInt64},
		{Func: AggCount, InputCol: "v", OutputCol: "c3", OutputType: parquet.TypeInt64},
	})
	ctx := context.Background()
	if err := parent.Init(ctx); err != nil {
		t.Fatal(err)
	}

	if err := parent.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": 18.5, "v": int64(1)},
		{"k": 53.5, "v": int64(1)},
		{"k": 18.5, "v": nil},
		{"k": 80.5, "v": int64(1)},
	})); err != nil {
		t.Fatal(err)
	}
	// An EMPTY clone. The primary is non-empty so the adoption shortcut
	// does not apply, and a clone that never consumed cannot drain to
	// runs — so the merge falls through to migrateToGenericMap and
	// materializes the PRIMARY's flat accumulators into boxed ones.
	empty := parent.CloneSink().(*HashAggregate)
	if err := empty.Init(ctx); err != nil {
		t.Fatal(err)
	}
	parent.MergeSink(empty)

	rows := aggRows(t, parent)
	if len(rows) != 3 {
		t.Fatalf("groups = %d, want 3: %v", len(rows), rows)
	}
	want := map[string]int64{"18.5": 1, "53.5": 1, "80.5": 1}
	for _, got := range rows {
		key := keyString(got["k"])
		w, ok := want[key]
		if !ok {
			t.Fatalf("unexpected group %q: %v", key, got)
		}
		c2, c3 := got["c2"], got["c3"]
		if !valuesEqual(c2, w) {
			t.Errorf("group %s: c2 = %v, want %v", key, c2, w)
		}
		// The one that matters: c3 is the SAME expression as c2, so any
		// difference between them is the shared count array being lost.
		if !valuesEqual(c3, w) {
			t.Errorf("group %s: c3 = %v, want %v (c2 answered %v for the identical expression)",
				key, c3, w, c2)
		}
	}
}
