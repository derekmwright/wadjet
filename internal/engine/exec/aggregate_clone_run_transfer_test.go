package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A morsel-parallel clone's spill runs must reach the primary — ALL of them
// (#790).
//
// Mechanism. A clone drains two different ways into two different lists.
// drainedRuns is the PartialDrainBytes bound, the one it is designed to take;
// partialSpillFiles is where spillPartialState appends, and a clone reaches
// THAT through SpillSome — it registers as an accounted operator like any
// other aggregate (Consume does so whenever Spill is non-nil and
// canUseExternalMerge holds, and a tracking-only view is still non-nil), so
// SpillManager.RequestRelief can ask it for bytes on a peer's behalf and it
// writes a whole run in answer.
//
// mergeSinkState transferred only the first list. The clone's Close then
// dropped the second, and every group in those runs left the answer with no
// error anywhere: 5000 input rows became 1100 output rows over 1091 groups,
// each with the count it would have had from the last batch alone.
//
// The gate drives the merge directly rather than through a pipeline, because
// WHICH list a clone fills is a property of how it was asked to spill, and a
// pipeline reaches only one of the two. Reverting the transfer fails it with
// "merged 3 groups, want 6" — the clone's drained half missing entirely.
func TestCloneSpillRunsAllReachThePrimary(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	mkBatch := func(lo, hi int64) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, int(hi-lo))
		for i := lo; i < hi; i++ {
			b.Columns[0].Int64Data[i-lo] = i
			b.Columns[0].Nulls.SetValid(int(i - lo))
			b.Columns[1].Int64Data[i-lo] = i * 10
			b.Columns[1].Nulls.SetValid(int(i - lo))
		}
		b.Len = int(hi - lo)
		return b
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		return h
	}

	ctx := context.Background()
	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	// The clone: consume keys 0..2, then answer a relief request the way
	// SpillManager.RequestRelief would, which is what fills partialSpillFiles
	// rather than drainedRuns.
	clone := mk(sm)
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, mkBatch(0, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := clone.SpillSome(1 << 20); err != nil {
		t.Fatalf("SpillSome: %v", err)
	}
	if len(clone.partialSpillFiles) == 0 {
		t.Fatal("the clone wrote no partial-state run: this fixture no longer reaches the list the bug dropped")
	}
	if len(clone.drainedRuns) != 0 {
		t.Fatalf("the clone filled drainedRuns (%d) — that is the list the merge always transferred, "+
			"so this fixture would pass with the bug present", len(clone.drainedRuns))
	}

	// The primary: keys 3..5, then the merge.
	primary := mk(sm)
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := primary.Consume(ctx, mkBatch(3, 6)); err != nil {
		t.Fatal(err)
	}
	primary.MergeSink(clone)
	if err := primary.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := map[int64]int64{}
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			got[r["k"].(int64)] = r["s"].(int64)
		}
	}
	if err := primary.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(got) != 6 {
		t.Fatalf("merged %d groups, want 6 — the clone's drained runs did not reach the primary (got %v)", len(got), fmt.Sprint(got))
	}
	for k := int64(0); k < 6; k++ {
		if want := k * 10; got[k] != want {
			t.Errorf("key %d: merged %d, want %d", k, got[k], want)
		}
	}
}
