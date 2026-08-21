package exec

import (
	"context"
	"testing"
)

// Regression for #378: in a parallel pipeline the primary Sort can finish
// having consumed NOTHING itself — the warmup batch fully filtered out and
// every source batch claimed by a clone worker, which is a scheduling race.
// MergeSink handed the primary the clones' BATCHES but not their SCHEMA, so
// finalize gathered the merged rows into zero output columns: the row count
// was right and every row had no columns at all, varying run to run with
// goroutine scheduling. The client-visible shape was `SELECT x AS c6 ...
// ORDER BY x` returning `map[]` rows.
func TestSortMergeSinkInheritsCloneSchema(t *testing.T) {
	ctx := context.Background()
	primary := NewSort([]SortKey{{Column: "v", Order: Ascending}})
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}

	clone := primary.CloneSink().(*Sort)
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Only the CLONE sees data; the primary consumes nothing.
	if err := clone.Consume(ctx, sortBatchInt64(t, 30, 10, 20)); err != nil {
		t.Fatal(err)
	}
	primary.MergeSink(clone)

	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var got []int64
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		if len(b.Schema) == 0 {
			t.Fatalf("sorted output batch has no columns (len=%d) — the #378 failure shape", b.Len)
		}
		for _, r := range b.ToRows() {
			v, ok := r["v"].(int64)
			if !ok {
				t.Fatalf("row %v carries no int64 column %q", r, "v")
			}
			got = append(got, v)
		}
	}
	want := []int64{10, 20, 30}
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %d, want %d", i, got[i], want[i])
		}
	}
}
