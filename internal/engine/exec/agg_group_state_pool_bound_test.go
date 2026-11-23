// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// correlatedCountBatch is the body of #1706's `CROSS JOIN LATERAL (SELECT
// count(*) ... WHERE s.c = p.c)` as one partitioned clone of its aggregate
// sees it: the nullable correlation key. A NULL key moves the aggregate off
// the integer fast path onto the generic map, whose group states come from
// the pool — the path the sightings' trace shows.
func correlatedCountBatch(keys ...any) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64, Nullable: true}}
	rows := make([]map[string]any, len(keys))
	for i, k := range keys {
		rows[i] = map[string]any{"k": k}
	}
	return batch.FromRows(schema, rows)
}

func newCorrelatedCountAgg(sm *memory.SpillManager, hint int64) *HashAggregate {
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
	})
	h.InputRowHint = hint
	h.Spill = sm
	return h
}

// TestABudgetedAggregatesGroupStatePoolIsBoundedByItsInput is #1706's fourth
// site. After the presize cap (daad0ddd) a budgeted aggregate's hash tables
// and key mirrors followed its input estimate, but the group-state pool did
// not: its first chunk was always 4096 groupStates (98,304 bytes), charged in
// full as forced "spill tracking" the moment the first group was allocated.
// A correlated LATERAL count(*) or DISTINCT body runs partitioned, one clone
// per worker; every clone that received a row charged about 107 KB for ONE
// group, so six or seven clones held 535-643 KB of a 512 KiB budget, and a
// hash join build whose first reservation landed before enough of them had
// closed refused (`used=535461, requested=24632, ... forced=535461 by "spill
// tracking"`) — which clones had closed first was the scheduler's choice.
// Under a budget the pool's first chunk now takes the same input bound.
func TestABudgetedAggregatesGroupStatePoolIsBoundedByItsInput(t *testing.T) {
	ctx := context.Background()
	const budget = 512 * 1024
	// The pipeline's clone count on the host the sightings came from
	// (GOMAXPROCS 24). Each clone's charge must leave every clone together,
	// plus the downstream join's first 32,824-byte arrival, inside the budget
	// — whichever of them has closed when the join reserves.
	const workers = 24
	const joinArrival = 32824
	const perClone = (budget - joinArrival) / workers

	t.Run("a partitioned clone of two groups", func(t *testing.T) {
		tr := memory.NewTracker("query", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		prim := newCorrelatedCountAgg(sm, 16) // the planner's estimate for nr_s
		if err := prim.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer prim.Close()
		cl := prim.CloneSink().(*HashAggregate)
		cl.Spill = sm.TrackingOnlyView()
		cl.PartitionedDisjoint = true
		cl.PartialDrainBytes = budget * 3 / (4 * workers)
		if err := cl.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		if err := cl.Consume(ctx, correlatedCountBatch(int64(1), nil)); err != nil {
			t.Fatal(err)
		}
		if got := tr.Used(); got > perClone {
			t.Fatalf("a two-group clone under a %d-byte budget charged %d bytes (want <= %d, so %d clones and "+
				"a join's first arrival fit together): its group-state pool is charging capacity a 16-row "+
				"input cannot fill (#1706)", budget, got, perClone, workers)
		}
	})

	t.Run("a low estimate still holds every group", func(t *testing.T) {
		tr := memory.NewTracker("query", 64<<20)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		h := newCorrelatedCountAgg(sm, 16)
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		const groups = 10000
		keys := []any{nil, nil}
		for i := 1; i < groups; i++ {
			keys = append(keys, int64(i), int64(i))
		}
		for off := 0; off < len(keys); off += batch.DefaultBatchSize {
			end := min(off+batch.DefaultBatchSize, len(keys))
			if err := h.Consume(ctx, correlatedCountBatch(keys[off:end]...)); err != nil {
				t.Fatal(err)
			}
		}
		// The pool grew from the estimate by doubling, never past the
		// 4096-slot chunk, and lost no group on the way.
		if first := len(h.gsPool.chunks[0]); first != 16 {
			t.Fatalf("first group-state chunk is %d slots, want the 16-row estimate", first)
		}
		for i, c := range h.gsPool.chunks {
			if len(c) > groupStateChunkSize {
				t.Fatalf("chunk %d is %d slots, past the %d-slot chunk", i, len(c), groupStateChunkSize)
			}
		}
		rows := drainAgg(t, h)
		if len(rows) != groups {
			t.Fatalf("%d groups, want %d", len(rows), groups)
		}
		for _, r := range rows {
			if r["n"] != int64(2) {
				t.Fatalf("group %v counted %v, want 2", r["k"], r["n"])
			}
		}
	})

	t.Run("without a budget the pool's chunk is unchanged", func(t *testing.T) {
		tr := memory.NewTracker("query", 0)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		h := newCorrelatedCountAgg(sm, 16)
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		if err := h.Consume(ctx, correlatedCountBatch(int64(1), nil)); err != nil {
			t.Fatal(err)
		}
		if len(h.gsPool.chunks) == 0 || len(h.gsPool.chunks[0]) != groupStateChunkSize {
			t.Fatalf("an unbudgeted aggregate's first group-state chunk moved from %d slots, which this "+
				"change must not do", groupStateChunkSize)
		}
	})
}
