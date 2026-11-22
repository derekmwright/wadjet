// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// lateralBodyBatch is #1706's grouped LATERAL body as the aggregate sees it:
// a STRING key beside the INT64 correlation key (the typed-generic path), two
// rows, two groups.
func lateralBodyBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "product", Type: parquet.TypeString},
		{Name: "order_id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	return batch.FromRows(schema, []map[string]any{
		{"product": "Widget", "order_id": int64(1), "amount": 50.0},
		{"product": "Gadget", "order_id": int64(1), "amount": 100.0},
	})
}

// TestABudgetedAggregatePresizesNoMoreThanItsInputCanFill is #1706's third
// site. Under a 512 KiB budget each clone of a grouped LATERAL body charged
// 458,796 bytes of forced "spill tracking" on its first two-row batch — the
// 4096-slot default presize — and two such aggregates' six clones held
// 2.75 MB at once, drained it to run files and released it. A hash join build
// whose first reservation landed inside that window refused
// (`used=1376347, requested=24692, ... forced=1376347 by "spill tracking"`);
// one that landed after it answered. Under a budget the initial capacity is
// now bounded by the input estimate, so the charge follows the groups and the
// window is gone. Without a budget the presize is unchanged, which the last
// arm holds.
func TestABudgetedAggregatePresizesNoMoreThanItsInputCanFill(t *testing.T) {
	ctx := context.Background()
	const budget = 512 * 1024
	newAgg := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"product", "order_id"}, []AggColumn{
			{Func: AggSum, InputCol: "amount", OutputCol: "sm", OutputType: parquet.TypeFloat64},
		})
		h.InputRowHint = 4 // the planner's estimate for #1706's lat_item
		h.Spill = sm
		return h
	}
	// The bound under which the charge is "proportional to two groups" rather
	// than to a presized table: well under one percent of the old 458,796.
	const groupsBound = 16 * 1024

	t.Run("the primary", func(t *testing.T) {
		tr := memory.NewTracker("query", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		h := newAgg(sm)
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		if err := h.Consume(ctx, lateralBodyBatch()); err != nil {
			t.Fatal(err)
		}
		if got := tr.Used(); got > groupsBound {
			t.Fatalf("a two-group aggregate under a %d-byte budget charged %d bytes (want <= %d): "+
				"its presize is charging capacity its 4-row input cannot fill (#1706)", budget, got, groupsBound)
		}
	})

	t.Run("a partitioned clone does not drain two groups to disk", func(t *testing.T) {
		tr := memory.NewTracker("query", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		prim := newAgg(sm)
		if err := prim.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer prim.Close()
		// The pipeline's wiring for a partitioned clone (pipeline.go): a
		// tracking-only view and a drain bound of 3/4 of the budget over the
		// workers, at the three workers #1706's trace showed.
		const workers = 3
		cl := prim.CloneSink().(*HashAggregate)
		cl.Spill = sm.TrackingOnlyView()
		cl.PartitionedDisjoint = true
		cl.PartialDrainBytes = budget * 3 / (4 * workers)
		if err := cl.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		if err := cl.Consume(ctx, lateralBodyBatch()); err != nil {
			t.Fatal(err)
		}
		if got := tr.Used(); got > groupsBound {
			t.Fatalf("a two-group clone charged %d bytes (want <= %d) (#1706)", got, groupsBound)
		}
		if len(cl.drainedRuns) != 0 {
			t.Fatalf("a two-group clone drained %d run file(s): its empty presize crossed its own drain bound "+
				"(%d bytes) (#1706)", len(cl.drainedRuns), cl.PartialDrainBytes)
		}
	})

	t.Run("without a budget the presize is unchanged", func(t *testing.T) {
		tr := memory.NewTracker("query", 0)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		h := newAgg(sm)
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		if err := h.Consume(ctx, lateralBodyBatch()); err != nil {
			t.Fatal(err)
		}
		// The 4096-slot default: an unbudgeted engine allocates what it did.
		if got := h.groupMemoryUsage(); got < 400*1024 {
			t.Fatalf("an unbudgeted aggregate's group state is %d bytes; the 4096-slot default presize "+
				"(about 458 KB on this path) moved, which this change must not do", got)
		}
	})
}
