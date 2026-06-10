package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// newWindowSpillHarness builds a Window whose tracker budget forces the
// run-spill path within a few batches.
func newWindowSpillHarness(tb testing.TB, cols []WindowColumn, budget int64) *Window {
	tb.Helper()
	tracker := memory.NewTracker("window-ext-test", budget)
	sm, err := memory.NewSpillManager(tb.TempDir(), tracker)
	if err != nil {
		tb.Fatal(err)
	}
	w := NewWindow(cols)
	w.Spill = sm
	if err := w.Init(context.Background()); err != nil {
		tb.Fatal(err)
	}
	return w
}

func drainWindowRows(tb testing.TB, w *Window) []map[string]any {
	tb.Helper()
	var rows []map[string]any
	for {
		b, err := w.Next(context.Background())
		if err != nil {
			tb.Fatalf("Next: %v", err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// canonicalRows sorts rows by their full string representation. Window output
// row ORDER is not part of the operator contract (the planner adds an ORDER
// BY when one is required), and the external path emits in partition-sorted
// order while the in-memory path emits in the last window column's order —
// so equivalence is on the row multiset.
func canonicalRows(rows []map[string]any, cols []string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		s := ""
		for _, c := range cols {
			s += fmt.Sprintf("%s=%v;", c, r[c])
		}
		out[i] = s
	}
	sort.Strings(out)
	return out
}

// runWindowBothPaths feeds identical batches through an in-memory Window and
// a spilling Window and asserts the row multisets match exactly.
func runWindowBothPaths(t *testing.T, schema []parquet.Column, cols []WindowColumn, allRows []map[string]any, rowsPerBatch int, compareCols []string) {
	t.Helper()
	forceTinyRuns(t)
	ctx := context.Background()

	ref := NewWindow(cols)
	if err := ref.Init(ctx); err != nil {
		t.Fatal(err)
	}
	w := newWindowSpillHarness(t, cols, 512)

	for i := 0; i < len(allRows); i += rowsPerBatch {
		end := i + rowsPerBatch
		if end > len(allRows) {
			end = len(allRows)
		}
		if err := ref.Consume(ctx, batch.FromRows(schema, allRows[i:end])); err != nil {
			t.Fatal(err)
		}
		if err := w.Consume(ctx, batch.FromRows(schema, allRows[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.runFiles) == 0 {
		t.Fatal("window run-spill path was never exercised; budget/floor setup is wrong")
	}
	if err := ref.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	want := canonicalRows(drainWindowRows(t, ref), compareCols)
	got := canonicalRows(drainWindowRows(t, w), compareCols)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d differs:\n got  %s\n want %s", i, got[i], want[i])
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWindowExternal_MatchesInMemory: one spec group covering ranking,
// running aggregate, and whole-partition aggregate, with enough duplicate
// partition keys that partitions span batch boundaries in the merged stream.
func TestWindowExternal_MatchesInMemory(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	cols := []WindowColumn{
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
		{Func: WinSum, InputCol: "amount", OutputCol: "running_sum", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}
	rng := rand.New(rand.NewSource(99))
	var rows []map[string]any
	for i := 0; i < 240; i++ {
		rows = append(rows, map[string]any{
			"grp":    int64(rng.Intn(7)),
			"ts":     int64(i), // unique → deterministic running values
			"amount": float64(rng.Intn(100)),
		})
	}
	runWindowBothPaths(t, schema, cols, rows, 16,
		[]string{"grp", "ts", "amount", "rn", "running_sum"})
}

// TestWindowExternal_MultiSpecGroups: two different (PARTITION BY, ORDER BY)
// specs force a disk-to-disk pass plus a re-sort between groups, and the
// output columns must come back in declared order.
func TestWindowExternal_MultiSpecGroups(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	cols := []WindowColumn{
		{Func: WinRank, OutputCol: "rank_a", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"a"}, OrderBy: []SortKey{{Column: "v", Order: Descending}}},
		{Func: WinCount, OutputCol: "cnt_b", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"b"}},
		{Func: WinMax, InputCol: "v", OutputCol: "max_a", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"a"}, OrderBy: []SortKey{{Column: "v", Order: Descending}}},
	}
	rng := rand.New(rand.NewSource(5))
	var rows []map[string]any
	for i := 0; i < 200; i++ {
		rows = append(rows, map[string]any{
			"a": int64(rng.Intn(5)),
			"b": int64(rng.Intn(4)),
			"v": float64(i), // unique → deterministic rank/max
		})
	}
	runWindowBothPaths(t, schema, cols, rows, 16,
		[]string{"a", "b", "v", "rank_a", "cnt_b", "max_a"})
}

// TestWindowExternal_EmptyPartitionBy: no PARTITION BY means one partition
// spanning the whole input — the documented full-materialization residual.
// Must still be correct.
func TestWindowExternal_EmptyPartitionBy(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	cols := []WindowColumn{
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}
	var rows []map[string]any
	for i := 0; i < 150; i++ {
		rows = append(rows, map[string]any{"ts": int64(150 - i), "v": float64(i)})
	}
	runWindowBothPaths(t, schema, cols, rows, 10, []string{"ts", "v", "rn"})
}

// TestWindowExternal_NullPartitionKeys: null partition keys must group into
// one partition on the external path (kernel null==null compares equal),
// matching the in-memory path's sameColumnar semantics.
func TestWindowExternal_NullPartitionKeys(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString, Nullable: true},
		{Name: "ts", Type: parquet.TypeInt64},
	}
	cols := []WindowColumn{
		{Func: WinCount, OutputCol: "cnt", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}},
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}
	rng := rand.New(rand.NewSource(13))
	var rows []map[string]any
	for i := 0; i < 160; i++ {
		var g any
		if rng.Intn(3) == 0 {
			g = nil
		} else {
			g = fmt.Sprintf("g%d", rng.Intn(3))
		}
		rows = append(rows, map[string]any{"grp": g, "ts": int64(i)})
	}
	runWindowBothPaths(t, schema, cols, rows, 12, []string{"grp", "ts", "cnt", "rn"})
}

// TestWindowConcat_NullableStringCorruption is the regression test for a
// windowCopyVectorRange bug found while building the external path: the
// nullable-bytes branch skipped null rows without advancing the BytesColumn
// offset slot, so every row AFTER a null in a concatenated batch read back as
// concatenated garbage ("g0g1g2..."), silently corrupting partition keys and
// window results on the NO-SPILL path. Multiple small batches with a nullable
// string partition column reproduce it; fails before the Set(di, nil) fix.
func TestWindowConcat_NullableStringCorruption(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString, Nullable: true},
		{Name: "ts", Type: parquet.TypeInt64},
	}
	cols := []WindowColumn{
		{Func: WinCount, OutputCol: "cnt", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}},
	}
	ctx := context.Background()
	w := NewWindow(cols) // no SpillManager: pure in-memory columnar path
	if err := w.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Three batches, each with a leading null — the desync trigger.
	groups := []string{"a", "b"}
	want := map[string]int64{"<nil>": 3, "a": 6, "b": 6}
	for b := 0; b < 3; b++ {
		rows := []map[string]any{{"grp": nil, "ts": int64(b * 10)}}
		for j := 0; j < 4; j++ {
			rows = append(rows, map[string]any{
				"grp": groups[j%2],
				"ts":  int64(b*10 + j + 1),
			})
		}
		if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	seen := map[string]bool{}
	for _, r := range drainWindowRows(t, w) {
		g := fmt.Sprint(r["grp"])
		got[g] = r["cnt"].(int64)
		seen[g] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("partition keys corrupted: got %v", got)
	}
	for g, n := range want {
		if got[g] != n {
			t.Fatalf("count for %q: got %d want %d (full: %v)", g, got[g], n, got)
		}
	}
}
