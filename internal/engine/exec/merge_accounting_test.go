package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestSortMergeSink_TransfersAccounting verifies the morsel-clone rule:
// when a clone Sort tracked its buffered batches (tracking-only SpillManager
// view against a shared tracker), MergeSink moves the reservation to the
// primary along with the batches — charge-primary-first, release-clone — so
// tracker.Used never dips and the merged bytes stay visible to the
// primary's spill trigger until Finalize/Close.
func TestSortMergeSink_TransfersAccounting(t *testing.T) {
	tracker := memory.NewTracker("test", 1<<30)
	real, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	view := real.TrackingOnlyView()

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	mkBatch := func(base int) *batch.RecordBatch {
		rows := make([]map[string]any, 100)
		for i := range rows {
			rows[i] = map[string]any{"k": int64(base + i), "v": float64(i)}
		}
		return batch.FromRows(schema, rows)
	}

	primary := &Sort{Keys: []SortKey{{Column: "k"}}, Spill: real}
	clone := primary.CloneSink().(*Sort)
	clone.Spill = view

	ctx := context.Background()
	if err := primary.Consume(ctx, mkBatch(0)); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, mkBatch(100)); err != nil {
		t.Fatal(err)
	}
	preMerge := tracker.Used()
	if preMerge <= 0 {
		t.Fatalf("expected both sides to have charged the shared tracker, Used = %d", preMerge)
	}

	primary.MergeSink(clone)
	if got := tracker.Used(); got != preMerge {
		t.Fatalf("tracker.Used changed across MergeSink: %d -> %d (transfer must be net-zero)", preMerge, got)
	}
	// The clone must hold nothing after merge: its Close releases nothing.
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != preMerge {
		t.Fatalf("clone.Close released tracked bytes it no longer owns: %d -> %d", preMerge, got)
	}
	// The primary owns everything: its Close returns the tracker to zero.
	if err := primary.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used after primary.Close = %d, want 0", got)
	}
}

// TestHashAggregateFinalize_MixedSpillFormats is the regression test for
// the orphaned-legacy-spill bug: an aggregate that (1) partial-drains
// int-keyed runs under pressure, then (2) migrates to the generic map
// (here via MergeSink into the post-spill reset state; a mid-stream null
// group key triggers the same demotion), then (3) buffers more input on
// the legacy raw-row spill path (canUseExternalMerge is false after the
// migration) must fold BOTH spill formats into Finalize. The old Finalize
// returned early on partialSpillFiles and silently dropped the legacy
// rows.
func TestHashAggregateFinalize_MixedSpillFormats(t *testing.T) {
	// Budget 1 with tracked state > 0 keeps ShouldSpillFor(SpillCheap)
	// permanently true — every Consume takes the pressure branch.
	tracker := memory.NewTracker("test", 1)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	mkBatch := func(base, n int) *batch.RecordBatch {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"g": int64(base + i), "v": float64(base + i)}
		}
		return batch.FromRows(schema, rows)
	}

	agg := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
		Spill:       spill,
	}
	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// (1) Int-keyed consume under pressure → partial-state run + reset.
	// The first Consume tracks state (Used goes 0→positive); the second
	// sees ShouldSpillFor true and partial-drains.
	if err := agg.Consume(ctx, mkBatch(0, 256)); err != nil {
		t.Fatal(err)
	}
	if err := agg.Consume(ctx, mkBatch(256, 256)); err != nil {
		t.Fatal(err)
	}
	if len(agg.partialSpillFiles) == 0 {
		t.Fatal("test setup: expected a partial-state spill run after the second pressured consume")
	}
	// (2) Merge an int-keyed clone into the reset (empty, flat-accs-nil)
	// primary → mergeSinkState migrates both to the generic map.
	clone := agg.CloneSink().(*HashAggregate)
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, mkBatch(1000, 512)); err != nil {
		t.Fatal(err)
	}
	agg.MergeSink(clone)
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	// (3) More input under pressure on the now-generic aggregate → legacy
	// raw-row spill path.
	if err := agg.Consume(ctx, mkBatch(2000, 512)); err != nil {
		t.Fatal(err)
	}
	if len(agg.spillBuffer) == 0 && len(agg.spillFiles) == 0 {
		t.Fatal("test setup: expected legacy raw-row spill after the post-migration pressured consume")
	}

	if err := agg.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]float64{}
	for {
		b, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		gIdx, tIdx := -1, -1
		for i, c := range b.Schema {
			switch c.Name {
			case "g":
				gIdx = i
			case "total":
				tIdx = i
			}
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			var g int64
			switch b.Columns[gIdx].Type {
			case batch.TypeInt64:
				g = b.Columns[gIdx].Int64Data[row]
			default:
				t.Fatalf("unexpected group key type %v", b.Columns[gIdx].Type)
			}
			seen[g] += b.Columns[tIdx].Float64Data[row]
		}
	}
	if len(seen) != 1536 {
		t.Fatalf("groups = %d, want 1536 (512 spilled-run + 512 merged + 512 legacy-spilled)", len(seen))
	}
	for _, base := range []int{0, 1000, 2000} {
		for i := 0; i < 512; i += 511 { // spot-check ends of each range
			g := int64(base + i)
			if seen[g] != float64(g) {
				t.Fatalf("group %d: sum %v, want %v", g, seen[g], float64(g))
			}
		}
	}
	if err := agg.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHashAggregateMergeSink_RechargesPrimary verifies that merging a
// clone's group state into a spill-armed primary recharges the primary's
// tracked footprint (reconcileGroupMemory runs inside MergeSink) and that
// the clone's release at Close leaves the shared tracker consistent —
// ending at exactly the primary's tracked group memory.
func TestHashAggregateMergeSink_RechargesPrimary(t *testing.T) {
	tracker := memory.NewTracker("test", 1<<30)
	real, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	view := real.TrackingOnlyView()

	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	mkBatch := func(base int) *batch.RecordBatch {
		rows := make([]map[string]any, 512)
		for i := range rows {
			rows[i] = map[string]any{"g": int64(base + i), "v": float64(i)}
		}
		return batch.FromRows(schema, rows)
	}

	primary := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeFloat64}},
		Spill:       real,
	}
	ctx := context.Background()
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	clone := primary.CloneSink().(*HashAggregate)
	clone.Spill = view
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}

	if err := primary.Consume(ctx, mkBatch(0)); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, mkBatch(10000)); err != nil {
		t.Fatal(err)
	}
	if tracker.Used() <= 0 {
		t.Fatalf("expected group state charges on the shared tracker, Used = %d", tracker.Used())
	}

	primary.MergeSink(clone)
	afterMerge := tracker.Used()
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	afterClose := tracker.Used()
	if afterClose >= afterMerge {
		t.Fatalf("clone.Close must release the clone's charge: %d -> %d", afterMerge, afterClose)
	}
	if afterClose != primary.trackedGroupMem {
		t.Fatalf("tracker.Used (%d) != primary.trackedGroupMem (%d) after merge+clone-close — merged state under- or over-accounted",
			afterClose, primary.trackedGroupMem)
	}
	// The merged footprint must cover both sides' groups (1024 distinct).
	if primary.trackedGroupMem <= 0 {
		t.Fatal("primary tracked footprint is zero after absorbing clone state")
	}
	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	total := 0
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.ActiveLen()
	}
	if total != 1024 {
		t.Fatalf("merged aggregate produced %d groups, want 1024", total)
	}
	if err := primary.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used after primary.Close = %d, want 0", got)
	}
}
