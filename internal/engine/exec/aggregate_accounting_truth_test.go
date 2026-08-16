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

// TestGroupMemoryUsageTruth_StringKeys: every string-keyed group carries (at
// least) the arena copy of its key, a second key copy behind
// serializedKeys/keyValues, and a 96-byte groupStateExtras. The pre-counter
// accounting saw only the arena copy (+ table/pool overhead ≈ 84 B/group for
// 32-byte keys), so a floor of 2×keyLen+96 per group fails without the
// counters.
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
	floor := int64(n) * (2*keyLen + 96)
	if got < floor {
		t.Fatalf("groupMemoryUsage = %d, want >= %d (2×key bytes + extras per group; pre-counter accounting reported ~%d)",
			got, floor, n*84)
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
