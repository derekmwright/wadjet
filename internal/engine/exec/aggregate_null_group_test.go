package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression tests for the NULL-group-key family (sweep follow-up finding,
// 2026-06-12): the single-int and dual-int GROUP BY fast paths diverted
// null-key rows into strGroupStates via processRow, but Next(), the SoA
// merges, and the migrations all read only intGroupStates — so the NULL
// group silently vanished from results. The fix migrates the aggregate to
// the generic path on the first batch whose key column contains nulls, and
// re-encodes migrated keys in the generic path's binary format so
// post-migration inserts (and mixed-mode MergeSink pairs) match.

func aggRows(tb testing.TB, h *HashAggregate) []map[string]any {
	tb.Helper()
	if err := h.Finalize(context.Background()); err != nil {
		tb.Fatal(err)
	}
	var rows []map[string]any
	for {
		b, err := h.Next(context.Background())
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

func sumByKey(rows []map[string]any, key string) map[any]any {
	out := make(map[any]any, len(rows))
	for _, r := range rows {
		k := r[key]
		out[fmt.Sprintf("%v", k)] = r["s"]
	}
	return out
}

func TestIntGroupKey_NullGroupEmitted(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// First batch has no nulls — the int fast path engages.
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int64(7), "v": int64(2)},
		{"k": int64(8), "v": int64(5)},
	})); err != nil {
		t.Fatal(err)
	}
	// Second batch introduces a NULL key mid-stream — must migrate, keep
	// the existing groups, and keep matching key 7 across the migration.
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": nil, "v": int64(1)},
		{"k": int64(7), "v": int64(10)},
		{"k": nil, "v": int64(3)},
	})); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, h)
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3 (NULL, 7, 8): %v", len(rows), rows)
	}
	sums := sumByKey(rows, "k")
	if sums["<nil>"] != int64(4) {
		t.Errorf("NULL group sum = %v, want 4", sums["<nil>"])
	}
	if sums["7"] != int64(12) {
		t.Errorf("group 7 sum = %v, want 12 (pre- and post-migration rows merged)", sums["7"])
	}
	if sums["8"] != int64(5) {
		t.Errorf("group 8 sum = %v, want 5", sums["8"])
	}
}

func TestDualIntGroupKey_NullGroupEmitted(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	h := NewHashAggregate([]string{"a", "b"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": int64(1), "b": int64(2), "v": int64(10)},
	})); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": nil, "b": int64(2), "v": int64(1)},
		{"a": int64(1), "b": nil, "v": int64(2)},
		{"a": nil, "b": nil, "v": int64(3)},
		{"a": int64(1), "b": int64(2), "v": int64(20)},
	})); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, h)
	if len(rows) != 4 {
		t.Fatalf("got %d groups, want 4 ((1,2), (NULL,2), (1,NULL), (NULL,NULL)): %v", len(rows), rows)
	}
	for _, r := range rows {
		if fmt.Sprintf("%v|%v", r["a"], r["b"]) == "1|2" && r["s"] != int64(30) {
			t.Errorf("group (1,2) sum = %v, want 30", r["s"])
		}
	}
}

// Mixed-mode MergeSink: one clone saw nulls (generic, binary keys), the
// other stayed on the int fast path. The migration triggered inside
// MergeSink must produce keys in the same binary format the generic side
// used, or shared groups silently duplicate.
func TestMergeSink_MixedModeAfterNullMigration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	parent := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := parent.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	withNulls := parent.CloneSink().(*HashAggregate)
	if err := withNulls.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := withNulls.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": nil, "v": int64(1)},
		{"k": int64(7), "v": int64(2)},
	})); err != nil {
		t.Fatal(err)
	}

	intOnly := parent.CloneSink().(*HashAggregate)
	if err := intOnly.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := intOnly.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int64(7), "v": int64(40)},
		{"k": int64(9), "v": int64(5)},
	})); err != nil {
		t.Fatal(err)
	}

	parent.MergeSink(withNulls)
	parent.MergeSink(intOnly)

	rows := aggRows(t, parent)
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3 (NULL, 7, 9) — duplicates mean the migrated key format diverged: %v", len(rows), rows)
	}
	sums := sumByKey(rows, "k")
	if sums["7"] != int64(42) {
		t.Errorf("group 7 sum = %v, want 42 (merged across both clones)", sums["7"])
	}
	if sums["<nil>"] != int64(1) {
		t.Errorf("NULL group sum = %v, want 1", sums["<nil>"])
	}
}

func TestIntGroupKey_Int32NullMigrationKeysMatch(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64},
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": int32(3), "v": int64(1)},
	})); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"k": nil, "v": int64(2)},
		{"k": int32(3), "v": int64(4)}, // must merge with the migrated group, not duplicate
	})); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, h)
	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2 (NULL, 3): %v", len(rows), rows)
	}
	sums := sumByKey(rows, "k")
	if sums["3"] != int64(5) {
		t.Errorf("group 3 sum = %v, want 5 (int32 key format mismatch across migration)", sums["3"])
	}
}

// Regression test for the single-STRING fast path's NULL group (issue #144
// suite finding): null-key rows were diverted to processRow, whose groups
// skip intFlatAccs.appendGroup — strGroupStates and the flat accumulators
// went out of alignment and the NULL group emitted with ZEROED aggregates
// (COUNT(*)=0, SUM=NULL over hundreds of rows) while real groups stayed
// correct. The NULL group must be a first-class flat-accumulator slot.
func TestStringGroupKey_NullGroupAggregates(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Interleave null keys with real ones, across two batches so the NULL
	// group accumulates after other groups exist on both sides.
	mk := func(vals ...any) *batch.RecordBatch {
		rows := make([]map[string]any, 0, len(vals)/2)
		for i := 0; i < len(vals); i += 2 {
			rows = append(rows, map[string]any{"k": vals[i], "v": vals[i+1]})
		}
		return batch.FromRows(schema, rows)
	}
	ctx := context.Background()
	if err := h.Consume(ctx, mk("a", int64(1), nil, int64(10), "b", int64(2), nil, int64(20))); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, mk(nil, int64(30), "a", int64(3), "\x01", int64(100))); err != nil {
		t.Fatal(err)
	}

	rows := aggRows(t, h)
	if len(rows) != 4 {
		t.Fatalf("groups = %d (%v), want 4 (a, b, \\x01, NULL)", len(rows), rows)
	}
	byKey := map[string]map[string]any{}
	for _, r := range rows {
		k := "<null>"
		if r["k"] != nil {
			k = r["k"].(string)
		}
		byKey[k] = r
	}
	if got := byKey["<null>"]; got == nil {
		t.Fatal("NULL group missing")
	} else {
		if fmt.Sprintf("%v", got["c"]) != "3" {
			t.Fatalf("NULL group COUNT = %v, want 3 (flat-accumulator misalignment)", got["c"])
		}
		if fmt.Sprintf("%v", got["s"]) != "60" {
			t.Fatalf("NULL group SUM = %v, want 60", got["s"])
		}
	}
	// A real 1-byte "\x01" string key must remain distinct from the NULL
	// group (no in-band sentinel collision in the shared hash table).
	if got := byKey["\x01"]; got == nil || fmt.Sprintf("%v", got["c"]) != "1" {
		t.Fatalf("\\x01 key group = %v, want count 1 distinct from NULL group", got)
	}
	if got := byKey["a"]; got == nil || fmt.Sprintf("%v", got["s"]) != "4" {
		t.Fatalf("group a = %v, want sum 4", got)
	}
	if got := byKey["b"]; got == nil || fmt.Sprintf("%v", got["s"]) != "2" {
		t.Fatalf("group b = %v, want sum 2", got)
	}
}
