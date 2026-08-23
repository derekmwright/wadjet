package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// TestWindowExternal_NestedPayload: ARRAY and ROW payload columns ride
// through the columnar run path (write -> resort -> merge -> partition
// walk -> concat -> output chunk) and must come back value-identical to
// the in-memory path.
func TestWindowExternal_NestedPayload(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "tags", Type: parquet.TypeArray, ElementType: &elem, Nullable: true},
		{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeString, Nullable: true},
		}},
	}
	cols := []WindowColumn{
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}
	rng := rand.New(rand.NewSource(13))
	var rows []map[string]any
	for i := 0; i < 180; i++ {
		var tags any
		switch i % 4 {
		case 0:
			tags = nil
		case 1:
			tags = []any{}
		default:
			tags = []any{int64(i), int64(i % 9)}
		}
		var b any
		if i%5 == 0 {
			b = nil
		} else {
			b = fmt.Sprintf("b%d", i)
		}
		rows = append(rows, map[string]any{
			"grp":  int64(rng.Intn(6)),
			"ts":   int64(i),
			"tags": tags,
			"rec":  map[string]any{"a": int64(i * 3), "b": b},
		})
	}
	runWindowBothPaths(t, schema, cols, rows, 16,
		[]string{"grp", "ts", "tags", "rec", "rn"})
}

// TestWindowGlobal_AllFunctions: empty PARTITION BY with ORDER BY containing
// duplicate keys (real peer groups), covering every window function through
// the two-pass streaming path against the in-memory reference.
func TestWindowGlobal_AllFunctions(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	ob := []SortKey{{Column: "ts", Order: Ascending}}
	cols := []WindowColumn{
		{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64, OrderBy: ob},
		{Func: WinRank, OutputCol: "rk", OutputType: parquet.TypeInt64, OrderBy: ob},
		{Func: WinDenseRank, OutputCol: "drk", OutputType: parquet.TypeInt64, OrderBy: ob},
		{Func: WinPercentRank, OutputCol: "prk", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinCumeDist, OutputCol: "cd", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinSum, InputCol: "v", OutputCol: "rsum", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinCount, OutputCol: "rcnt", OutputType: parquet.TypeInt64, OrderBy: ob},
		{Func: WinAvg, InputCol: "v", OutputCol: "ravg", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinMin, InputCol: "v", OutputCol: "rmin", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinMax, InputCol: "v", OutputCol: "rmax", OutputType: parquet.TypeFloat64, OrderBy: ob},
		{Func: WinLag, InputCol: "s", OutputCol: "lag2", OutputType: parquet.TypeString, OrderBy: ob, LagLeadOffset: 2, LagLeadDefault: "DFLT"},
		{Func: WinLead, InputCol: "s", OutputCol: "lead3", OutputType: parquet.TypeString, OrderBy: ob, LagLeadOffset: 3},
		{Func: WinFirstValue, InputCol: "s", OutputCol: "fv", OutputType: parquet.TypeString, OrderBy: ob},
		{Func: WinLastValue, InputCol: "s", OutputCol: "lv", OutputType: parquet.TypeString, OrderBy: ob},
		{Func: WinNthValue, InputCol: "s", OutputCol: "nth5", OutputType: parquet.TypeString, OrderBy: ob, NthValueN: 5},
		{Func: WinNtile, OutputCol: "nt", OutputType: parquet.TypeInt64, OrderBy: ob, NtileBuckets: 7},
	}
	rng := rand.New(rand.NewSource(21))
	var rows []map[string]any
	for i := 0; i < 260; i++ {
		var sv any
		if i%7 == 0 {
			sv = nil
		} else {
			sv = fmt.Sprintf("s%d", i)
		}
		rows = append(rows, map[string]any{
			"ts": int64(i / 3), // duplicates → 3-row peer groups
			"v":  float64(rng.Intn(1000)),
			"s":  sv,
		})
	}
	runWindowBothPaths(t, schema, cols, rows, 16, []string{
		"ts", "v", "s", "rn", "rk", "drk", "prk", "cd", "rsum", "rcnt",
		"ravg", "rmin", "rmax", "lag2", "lead3", "fv", "lv", "nth5", "nt"})
}

// TestWindowGlobal_NoOrderBy: whole-partition aggregates with neither
// PARTITION BY nor ORDER BY — pass-1 scalars broadcast to every row.
func TestWindowGlobal_NoOrderBy(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	cols := []WindowColumn{
		{Func: WinSum, InputCol: "v", OutputCol: "tsum", OutputType: parquet.TypeFloat64},
		{Func: WinCount, OutputCol: "tcnt", OutputType: parquet.TypeInt64},
		{Func: WinAvg, InputCol: "v", OutputCol: "tavg", OutputType: parquet.TypeFloat64},
		{Func: WinMin, InputCol: "v", OutputCol: "tmin", OutputType: parquet.TypeFloat64},
		{Func: WinMax, InputCol: "v", OutputCol: "tmax", OutputType: parquet.TypeFloat64},
		{Func: WinLastValue, InputCol: "v", OutputCol: "tlast", OutputType: parquet.TypeFloat64},
	}
	var rows []map[string]any
	for i := 0; i < 200; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "v": float64((i * 37) % 211)})
	}
	runWindowBothPaths(t, schema, cols, rows, 16,
		[]string{"id", "v", "tsum", "tcnt", "tavg", "tmin", "tmax", "tlast"})
}

// TestWindowGlobal_MixedWithPartitioned: a partitioned group and a global
// group in both orders, exercising the global path as a non-final disk pass
// and as the final streaming pass (plus the re-sort between groups).
func TestWindowGlobal_MixedWithPartitioned(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	rng := rand.New(rand.NewSource(31))
	var rows []map[string]any
	for i := 0; i < 220; i++ {
		rows = append(rows, map[string]any{
			"grp": int64(rng.Intn(5)),
			"ts":  int64(i),
			"v":   float64(rng.Intn(500)),
		})
	}
	// Global group declared first → runs as a non-final disk pass.
	colsGlobalFirst := []WindowColumn{
		{Func: WinRank, OutputCol: "grank", OutputType: parquet.TypeInt64,
			OrderBy: []SortKey{{Column: "v", Order: Descending}}},
		{Func: WinSum, InputCol: "v", OutputCol: "psum", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}
	runWindowBothPaths(t, schema, colsGlobalFirst, rows, 16,
		[]string{"grp", "ts", "v", "grank", "psum"})

	// Partitioned group first → global group is the final streaming pass.
	colsGlobalLast := []WindowColumn{
		{Func: WinRowNumber, OutputCol: "prn", OutputType: parquet.TypeInt64,
			PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
		{Func: WinCumeDist, OutputCol: "gcd", OutputType: parquet.TypeFloat64,
			OrderBy: []SortKey{{Column: "v", Order: Ascending}}},
	}
	runWindowBothPaths(t, schema, colsGlobalLast, rows, 16,
		[]string{"grp", "ts", "v", "prn", "gcd"})
}

// TestWindowGlobal_StreamingBoundsMemory drives the pass-2 streamer directly
// over real run files and asserts pending-batch charge stays bounded by a
// few batches while the input is far larger — the property the global path
// exists to provide (the walker it replaces accumulated the whole input).
func TestWindowGlobal_StreamingBoundsMemory(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	dir := t.TempDir()

	const nBatches, rowsPer = 40, 1024
	var batches []*batch.RecordBatch
	var totalBytes, maxBatchBytes int64
	for bi := 0; bi < nBatches; bi++ {
		rows := make([]map[string]any, rowsPer)
		for i := range rows {
			rows[i] = map[string]any{"ts": int64(bi*rowsPer + i), "v": float64(i)}
		}
		b := batch.FromRows(schema, rows)
		batches = append(batches, b)
		mb := b.MemBytes()
		totalBytes += mb
		if mb > maxBatchBytes {
			maxBatchBytes = mb
		}
	}
	keys := []SortKey{{Column: "ts", Order: Ascending}}
	run, err := sortBatchesToRun(dir, schema, batches, nBatches*rowsPer, keys, 0)
	if err != nil {
		t.Fatal(err)
	}

	g := windowSpecGroup{
		orderBy:  keys,
		sortKeys: keys,
		cols: []WindowColumn{
			{Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64, OrderBy: keys},
			{Func: WinSum, InputCol: "v", OutputCol: "rsum", OutputType: parquet.TypeFloat64, OrderBy: keys},
		},
	}

	m1, runs, err := openRunMerger(dir, schema, keys, []string{run})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := collectGlobalWindowStats(m1, schema, g)
	m1.close()
	if err != nil {
		t.Fatal(err)
	}
	if stats.n != nBatches*rowsPer {
		t.Fatalf("stats.n = %d, want %d", stats.n, nBatches*rowsPer)
	}

	m2, runs, err := openRunMerger(dir, schema, keys, runs)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.close()
	defer removeRunFiles(runs)

	var cur, peak int64
	charge := func(d int64) {
		cur += d
		if cur > peak {
			peak = cur
		}
	}
	st := newGlobalWindowStreamer(m2, schema, g, stats, charge)
	var rows int64
	for {
		b, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += int64(b.Len)
	}
	if rows != stats.n {
		t.Fatalf("streamed %d rows, want %d", rows, stats.n)
	}
	if cur != 0 {
		t.Fatalf("outstanding charge after drain: %d, want 0", cur)
	}
	// Streaming bound: a handful of batches, never the whole input.
	//
	// The group's SUM has an ORDER BY, so its frame ends at the end of each
	// row's peer group and the streamer holds the OPEN group back until it
	// closes (#350). Emission is batch-granular, so even with the unique
	// keys used here — where the open group is a single row — the batch
	// holding that row waits for the next one. That is one extra batch on
	// top of the augmented batch already in flight, not a step toward
	// accumulating the input, which is what this bound is here to catch.
	if peak > 8*maxBatchBytes {
		t.Fatalf("peak pending charge %d exceeds 8x max batch (%d) — not streaming (total input %d)", peak, maxBatchBytes, totalBytes)
	}
}

// TestPartitionWalker_ReleaseCurrent: an error mid-accumulation must not
// strand the walker's in-flight partition charge on the shared tracker.
func TestPartitionWalker_ReleaseCurrent(t *testing.T) {
	var outstanding int64
	charge := func(d int64) { outstanding += d }
	w := &partitionWalker{charge: charge}

	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	b := batch.FromRows(schema, []map[string]any{{"k": int64(1)}, {"k": int64(1)}})
	w.appendSegment(b, 0, b.Len)
	if outstanding <= 0 {
		t.Fatal("appendSegment did not charge")
	}
	w.releaseCurrent()
	if outstanding != 0 {
		t.Fatalf("outstanding charge after releaseCurrent: %d, want 0", outstanding)
	}
	if len(w.cur) != 0 || w.curBytes != 0 {
		t.Fatal("walker state not cleared")
	}
}

// F5 regression: a PARTITION BY key whose column type resolves to no
// comparator must fail loudly, not silently merge every partition on that
// key into one — the same failure mode a dropped GROUP BY key has. Before
// the fix, partitionWalker.resolve set a nil compare entry and sameRow
// silently skipped it instead of erroring, exactly the bug class
// sort_merge_join.go's resolveCompareKernels already refuses for join keys.
func TestPartitionWalkerResolve_UnsupportedTypeErrors(t *testing.T) {
	v := batch.NewVector(batch.TypeID(200), 3)
	b := &batch.RecordBatch{
		Columns: []*batch.Vector{v},
		Schema:  []parquet.Column{{Name: "p", Type: parquet.TypeID(200)}},
		Len:     3,
	}
	w := newPartitionWalker(nil, []string{"p"}, nil)
	if err := w.resolve(b); err == nil {
		t.Fatal("expected an error for an unsupported PARTITION BY type, got nil")
	}
}
