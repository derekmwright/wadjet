package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// #279 regression: a primary whose warmup batch was fully filtered never
// runs resolveIndices. MergeSink's empty-primary adoption (adoptStateFrom)
// then copies the clone's resolution — including resolved=true — but the
// per-batch batchUpdaters scratch stayed nil, so the first batch consumed
// AFTER the merge (pressure-collapse serial continuation, spilled-partition
// replay) panicked with index-out-of-range in consumeBatch's updater
// selection (SF100 Q18 join-8 under fuseJoinShuffle, 2026-08-02).
func TestMergeSinkAdopt_ConsumeAfterMerge(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()

	clone := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(2), "v": int64(20)},
	})); err != nil {
		t.Fatal(err)
	}

	// Primary: same spec, never consumed a batch (warmup fully filtered).
	primary := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	primary.MergeSink(clone)

	// Pre-fix: index out of range [0] with length 0 (nil batchUpdaters).
	if err := primary.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int64(1), "v": int64(5)},
		{"k": int64(3), "v": int64(7)},
	})); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, primary)
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3: %v", len(rows), rows)
	}
	sums := sumByKey(rows, "k")
	if sums["1"] != int64(15) {
		t.Errorf("group 1 sum = %v, want 15 (adopted + post-merge rows)", sums["1"])
	}
	if sums["2"] != int64(20) {
		t.Errorf("group 2 sum = %v, want 20 (adopted state)", sums["2"])
	}
	if sums["3"] != int64(7) {
		t.Errorf("group 3 sum = %v, want 7 (post-merge row)", sums["3"])
	}
}

// #279 sibling for the scalar fast path: mergeSinkState's scalar adoption
// copies kernels and merges scalarAccs but does not set resolved, so a
// post-merge Consume re-enters resolveIndices — which used to remake
// scalarAccs unconditionally, silently discarding the merged clone
// partials (wrong results, no panic).
func TestMergeSinkScalarAdopt_ConsumeAfterMergeKeepsPartials(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()

	clone := NewHashAggregate(nil, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"v": int64(10)},
		{"v": int64(20)},
	})); err != nil {
		t.Fatal(err)
	}

	primary := NewHashAggregate(nil, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	primary.MergeSink(clone)

	if err := primary.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"v": int64(5)},
	})); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, primary)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1: %v", len(rows), rows)
	}
	if got := rows[0]["s"]; got != int64(35) {
		t.Errorf("scalar sum = %v, want 35 (merged partials 30 + post-merge 5)", got)
	}
}
