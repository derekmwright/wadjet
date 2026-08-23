package exec

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// These tests guard the incremental state-byte counters against rot: the
// SF100 Q17 postmortem (2026-07-03, morsel-agg-partials-v2.md §1-2) traced a
// worker death to groupMemoryUsage under-reporting live aggregate state by
// 41-100% on high-cardinality non-int-SoA GROUP BYs — the spill threshold
// never tripped while the process crossed GOMEMLIMIT. Each test asserts an
// analytic per-group floor that the pre-counter accounting could not reach.

func strKeyBatch(tb testing.TB, base, n, keyLen int) *batch.RecordBatch {
	tb.Helper()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		key := fmt.Sprintf("key-%0*d", keyLen-4, base+i)
		rows[i] = map[string]any{"g": key, "v": float64(i)}
	}
	return batch.FromRows(schema, rows)
}

// TestGroupMemoryUsageTruth_StringKeys: on the single-string fast path a group
// carries its key bytes ONCE — in the hash table's key arena, which
// serializedKeys aliases rather than copying — plus a 16-byte string header
// and (at the 70% load factor) at least one 16-byte table entry. The floor
// below is that per-group truth; the arena term is what the pre-arena
// accounting could reach only through the serializedKeyBytes counter, and
// MemoryUsage() must keep charging it now that the counter no longer does.
func TestGroupMemoryUsageTruth_StringKeys(t *testing.T) {
	const n = 5000
	const keyLen = 32
	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, strKeyBatch(t, 0, n, keyLen)); err != nil {
		t.Fatal(err)
	}
	got := h.groupMemoryUsage()
	floor := int64(n) * (keyLen + 16 + 16)
	if got < floor {
		t.Fatalf("groupMemoryUsage = %d, want >= %d (key bytes + string header + table entry per group)",
			got, floor)
	}
	// The keys are the arena's contents: dropping the table from the estimate
	// would silently un-count every group key.
	if arena := h.strGroupIndex.arenaCap; arena < int64(n)*keyLen {
		t.Fatalf("key arena = %d bytes, want >= %d (one copy of every key)", arena, n*keyLen)
	}
}

// TestGroupMemoryUsageTruth_GenericStringKeys: the multi-column generic path
// serializes its keys into freshly allocated strings (no arena aliasing —
// those keys exist nowhere else), so serializedKeyBytes remains the ONLY
// accounting for them. Three string columns keeps the key wider than the
// 8-byte compact path.
func TestGroupMemoryUsageTruth_GenericStringKeys(t *testing.T) {
	const n = 5000
	const keyLen = 32
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeString},
		{Name: "b", Type: parquet.TypeString},
		{Name: "c", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		key := fmt.Sprintf("key-%0*d", keyLen-4, i)
		rows[i] = map[string]any{"a": key, "b": key, "c": key, "v": float64(i)}
	}
	h := &HashAggregate{
		GroupByCols: []string{"a", "b", "c"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	if h.serializedKeyBytes < int64(n)*3*keyLen {
		t.Fatalf("serializedKeyBytes = %d, want >= %d (3 columns × %d bytes per group)",
			h.serializedKeyBytes, n*3*keyLen, keyLen)
	}
	got := h.groupMemoryUsage()
	floor := int64(n) * 3 * keyLen
	if got < floor {
		t.Fatalf("groupMemoryUsage = %d, want >= %d (serialized key bytes per group)", got, floor)
	}
}

// TestGroupMemoryUsageTruth_CountDistinct: COUNT(DISTINCT) set contents were
// entirely invisible to the pre-counter accounting.
func TestGroupMemoryUsageTruth_CountDistinct(t *testing.T) {
	const groups = 16
	const perGroup = 1000
	const valLen = 16
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeString},
	}
	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggCountDistinct, InputCol: "v", OutputCol: "cnt", OutputType: parquet.TypeInt64}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, groups*perGroup)
	for g := 0; g < groups; g++ {
		for i := 0; i < perGroup; i++ {
			rows = append(rows, map[string]any{
				"g": fmt.Sprintf("group-%04d", g),
				"v": fmt.Sprintf("val-%0*d", valLen-4, i),
			})
		}
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	got := h.groupMemoryUsage()
	// Encoded distinct keys carry the value bytes plus per-entry map overhead.
	floor := int64(groups) * int64(perGroup) * (valLen + 48)
	if got < floor {
		t.Fatalf("groupMemoryUsage = %d, want >= %d (distinct set contents; pre-counter accounting saw none of it)", got, floor)
	}
}

// TestGroupMemoryUsageTruth_ResetAfterSpill: a whole-state spill drops every
// counted structure, so the counters must return to (near) zero with it —
// otherwise the tracker double-charges the next run.
func TestGroupMemoryUsageTruth_ResetAfterSpill(t *testing.T) {
	tracker := memory.NewTracker("test", 1<<30)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
		Spill:       spill,
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, strKeyBatch(t, 0, 2000, 32)); err != nil {
		t.Fatal(err)
	}
	h.reconcileGroupMemory()
	before := h.groupMemoryUsage()
	if err := h.spillFullState(); err != nil {
		t.Fatal(err)
	}
	// Fresh (empty) replacement hash tables legitimately retain some bytes;
	// the incremental counters must be exactly zero.
	if h.serializedKeyBytes != 0 || h.compactKeyBytes != 0 || h.distinctBytes != 0 ||
		h.extraStateBytes != 0 || h.extrasAccsCount != 0 {
		t.Fatalf("state byte counters not reset with the state: serialized=%d compact=%d distinct=%d extraState=%d accs=%d",
			h.serializedKeyBytes, h.compactKeyBytes, h.distinctBytes, h.extraStateBytes, h.extrasAccsCount)
	}
	if after := h.groupMemoryUsage(); after > before/2 {
		t.Fatalf("groupMemoryUsage after spillFullState = %d (before %d)", after, before)
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestGroupMemoryUsageTruth_MergeAdopt: groups adopted wholesale from a merged
// clone (generic path) must land in the primary's counters — the barrier
// recharge (reconcileGroupMemory at MergeSink) reads groupMemoryUsage.
func TestGroupMemoryUsageTruth_MergeAdopt(t *testing.T) {
	ctx := context.Background()
	mk := func() *HashAggregate {
		h := &HashAggregate{
			GroupByCols: []string{"g"},
			Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
		}
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		return h
	}
	const n = 3000
	const keyLen = 32
	primary := mk()
	clone := mk()
	if err := primary.Consume(ctx, strKeyBatch(t, 0, n, keyLen)); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, strKeyBatch(t, n, n, keyLen)); err != nil { // disjoint keys → all adopted
		t.Fatal(err)
	}
	primary.MergeSink(clone)
	got := primary.groupMemoryUsage()
	floor := int64(2*n) * (2*keyLen + 96)
	if got < floor {
		t.Fatalf("post-merge groupMemoryUsage = %d, want >= %d (both sides' groups)", got, floor)
	}
}

// TestGroupMemoryUsageTruth_VsHeap compares the estimate against measured
// live-heap growth for the string-keyed shape. Generous one-sided bound
// (estimate >= half of measured growth) because heap measurement is noisy;
// the pre-counter accounting sat at ~15-25% of measured on this shape, so
// even the loose bound catches a regression to that state.
func TestGroupMemoryUsageTruth_VsHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("heap measurement is noisy; skipped in -short")
	}
	const n = 100_000
	const keyLen = 32
	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Build the input first so its allocations don't pollute the window.
	batches := make([]*batch.RecordBatch, 0, n/5000)
	for base := 0; base < n; base += 5000 {
		batches = append(batches, strKeyBatch(t, base, 5000, keyLen))
	}
	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)
	for _, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&ms1)
	grown := int64(ms1.HeapAlloc) - int64(ms0.HeapAlloc)
	got := h.groupMemoryUsage()
	if grown > 0 && got < grown/2 {
		t.Fatalf("groupMemoryUsage = %d but live heap grew %d — estimate below 50%% of measured (pre-counter behavior)", got, grown)
	}
}

// containerArrayBatch builds n rows of (g, c_arr) where c_arr is an
// 8-element STRING array whose elements are elemLen bytes wide — one group
// per row, so groupMemoryUsage's container charge is exactly n retained
// arrays, no MIN comparisons overwriting one another. Built directly rather
// than through FromRows so the nested column's shape comes from the schema,
// the same reason agg_container_minmax_test.go does.
func containerArrayBatch(tb testing.TB, base, n, elemLen int) *batch.RecordBatch {
	tb.Helper()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int32(base+i))
		arr := make([]any, 8)
		for j := range arr {
			arr[j] = fmt.Sprintf("v-%0*d", elemLen, base+i)
		}
		b.Columns[1].SetValue(i, arr)
	}
	return b
}

// TestGroupMemoryUsageTruth_ContainerMinMax: a boxed container MIN/MAX
// retains a whole copied Vector per group, not a scalar slot — the flat
// per-agg charge (extraStateBytes += len(h.Aggs) * 80, sized for a scalar
// box) undercounted it by an order of magnitude once the payload had any
// size to it (measured 1336-1807 B/group against 484 reported, #F3). This
// is the deterministic half of that guard: no GC noise, just the counter
// against its own known floor and its own growth with payload size.
func TestGroupMemoryUsageTruth_ContainerMinMax(t *testing.T) {
	const n = 10_000
	run := func(elemLen int) int64 {
		h := &HashAggregate{
			GroupByCols: []string{"g"},
			Aggs:        []AggColumn{{Func: AggMin, InputCol: "c_arr", OutputCol: "lo", OutputType: parquet.TypeArray}},
		}
		ctx := context.Background()
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Consume(ctx, containerArrayBatch(t, 0, n, elemLen)); err != nil {
			t.Fatal(err)
		}
		return h.extraStateBytes
	}
	small := run(8)
	large := run(64)

	flatOnly := int64(n) * 80
	if small <= flatOnly {
		t.Fatalf("extraStateBytes = %d, want > the flat per-agg figure %d — the retained container's own Vector.MemBytes() must be charged on top",
			small, flatOnly)
	}
	if large <= small {
		t.Fatalf("extraStateBytes did not grow with payload size: short-element run=%d, long-element run=%d", small, large)
	}
}

// TestGroupMemoryUsageTruth_ContainerMinMaxVsHeap compares the container
// charge against measured live-heap growth, the same one-sided (noise-
// tolerant) bound TestGroupMemoryUsageTruth_VsHeap uses for string keys.
func TestGroupMemoryUsageTruth_ContainerMinMaxVsHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("heap measurement is noisy; skipped in -short")
	}
	const n = 10_000
	const elemLen = 32
	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggMin, InputCol: "c_arr", OutputCol: "lo", OutputType: parquet.TypeArray}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Build the input first so its allocations don't pollute the window.
	b := containerArrayBatch(t, 0, n, elemLen)
	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)
	if err := h.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&ms1)
	grown := int64(ms1.HeapAlloc) - int64(ms0.HeapAlloc)
	got := h.groupMemoryUsage()
	if grown > 0 && got < grown/2 {
		t.Fatalf("groupMemoryUsage = %d but live heap grew %d — estimate below 50%% of measured", got, grown)
	}
}

// TestGroupMemoryUsageTruth_MinByContainer checks the same hole in
// minMaxByState.bestVal: MIN_BY/MAX_BY's return column can be a container
// too (aggSpecOutputType declares MIN_BY's output as its input's own type,
// with no restriction to scalars), and bestVal then retains the same
// GetValue box a container MIN/MAX retains as a Vector. A scalar bestVal
// must stay on the flat charge alone (already measured close: a STRING
// return column ran 487 B/group against 484 reported) — only the container
// shape should push extraStateBytes past it.
func TestGroupMemoryUsageTruth_MinByContainer(t *testing.T) {
	const n = 5_000
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "k", Type: parquet.TypeFloat64},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}
	b := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, int32(i))
		b.Columns[1].SetValue(i, float64(i))
		arr := make([]any, 8)
		for j := range arr {
			arr[j] = fmt.Sprintf("v-%032d", i)
		}
		b.Columns[2].SetValue(i, arr)
	}

	h := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs: []AggColumn{{Func: AggMinBy, InputCol: "c_arr", InputCol2: "k",
			OutputCol: "lo", OutputType: parquet.TypeArray}},
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}
	flatOnly := int64(n) * 80
	if h.extraStateBytes <= flatOnly {
		t.Fatalf("extraStateBytes = %d, want > the flat per-agg figure %d — MIN_BY's retained container bestVal must be charged too",
			h.extraStateBytes, flatOnly)
	}
}
