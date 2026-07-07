package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression tests for the unguarded NoGrow-insert loops (found 2026-07-07 by
// the SortMergeJoin equivalence test): strHashTable.PutNoGrow/GetOrInsertNoGrow
// probe for an empty slot and never re-check the load factor, so a call site
// that inserts more distinct keys than the table's remaining headroom without
// EnsureCapacity spins forever once the table fills. Before the fix these
// tests HUNG (10-minute test-binary timeout) rather than failing.

// TestHashJoinBuildFromRows_ManyDistinctStringKeys: BuildFromRows seeded its
// string index at 64 buckets and inserted every row before the deferred
// CheckGrow — >64 distinct string keys in one call filled the table mid-loop.
func TestHashJoinBuildFromRows_ManyDistinctStringKeys(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const n = 500 // well past the 64-bucket seed
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"k": fmt.Sprintf("key-%04d", i), "v": int64(i)}
	}
	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.BuildFromRows(schema, rows)
	// Repeat call: the int-index sibling hazard was capacity sized for the
	// FIRST call only; a second call must extend it, not fill it.
	rows2 := make([]map[string]any, n)
	for i := range rows2 {
		rows2[i] = map[string]any{"k": fmt.Sprintf("key2-%04d", i), "v": int64(n + i)}
	}
	hj.BuildFromRows(schema, rows2)

	probeRows := []map[string]any{
		{"k": "key-0007", "v": int64(-1)},
		{"k": "key2-0499", "v": int64(-2)},
		{"k": "absent", "v": int64(-3)},
	}
	sink := &CollectSink{}
	pipe := &Pipeline{Source: NewSliceSource(schema, probeRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(sink.Rows), sink.Rows)
	}
}

// TestHashJoinBuildFromRows_ManyDistinctIntKeysRepeatCall: the int index is
// pre-sized by tryEnableIntKey from the first call's BuildRowHint; a second
// BuildFromRows call with fresh keys previously relied on post-loop CheckGrow
// alone and could fill the table mid-loop.
func TestHashJoinBuildFromRows_ManyDistinctIntKeysRepeatCall(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	mkRows := func(base, n int) []map[string]any {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"k": int64(base + i), "v": int64(i)}
		}
		return rows
	}
	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.BuildFromRows(schema, mkRows(0, 100))
	// 4× the first call: exceeds any headroom the first sizing left behind.
	hj.BuildFromRows(schema, mkRows(100, 400))

	probeRows := []map[string]any{{"k": int64(499), "v": int64(-1)}}
	sink := &CollectSink{}
	pipe := &Pipeline{Source: NewSliceSource(schema, probeRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 match, got %d", len(sink.Rows))
	}
}

// TestHashJoinKeyOnlyBuild_ManyDistinctStringKeys: the semi/anti key-only
// build paths (serial and per-worker parallel) created their string index at
// 64 buckets AFTER the batch's EnsureCapacity guard had skipped the nil
// index — a first batch with >64 distinct string keys filled it mid-loop.
func TestHashJoinKeyOnlyBuild_ManyDistinctStringKeys(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
	}
	const n = 2048 // one full batch of distinct keys through SliceSource
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"k": fmt.Sprintf("key-%05d", i)}
	}
	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiKeyOnly = true
	if err := hj.Build(context.Background(), NewSliceSource(schema, rows)); err != nil {
		t.Fatal(err)
	}

	probeRows := []map[string]any{
		{"k": "key-00042"},
		{"k": "not-there"},
	}
	sink := &CollectSink{}
	pipe := &Pipeline{Source: NewSliceSource(schema, probeRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.Rows) != 1 {
		t.Fatalf("expected 1 semi-join match, got %d", len(sink.Rows))
	}
}
