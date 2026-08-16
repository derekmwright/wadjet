package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	// (2) Demote to the generic map via a NULL group key. (This used to be
	// staged by merging a clone into the post-spill reset primary, but
	// mergeSinkState no longer migrates — an empty primary adopts the
	// clone's SoA state and a non-empty one receives partial-state runs;
	// see morsel-agg-partials-v2.md §3.C. The null-key demotion is the
	// remaining organic route to the generic path for a simple-agg
	// aggregate.)
	nullRows := make([]map[string]any, 64)
	for i := range nullRows {
		nullRows[i] = map[string]any{"g": int64(1000 + i), "v": float64(i)}
	}
	nullRows[0] = map[string]any{"g": nil, "v": 1.0}
	if err := agg.Consume(ctx, batch.FromRows(schema, nullRows)); err != nil {
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
				if b.Columns[gIdx].Nulls.IsNullFast(row) {
					g = -999 // NULL group sentinel for the seen map
				} else {
					g = b.Columns[gIdx].Int64Data[row]
				}
			default:
				t.Fatalf("unexpected group key type %v", b.Columns[gIdx].Type)
			}
			seen[g] += b.Columns[tIdx].Float64Data[row]
		}
	}
	if len(seen) != 1088 {
		t.Fatalf("groups = %d, want 1088 (512 spilled-run + 63 post-demotion + 1 NULL + 512 legacy-spilled)", len(seen))
	}
	for _, base := range []int{0, 2000} {
		for i := 0; i < 512; i += 511 { // spot-check ends of each range
			g := int64(base + i)
			if seen[g] != float64(g) {
				t.Fatalf("group %d: sum %v, want %v", g, seen[g], float64(g))
			}
		}
	}
	// Step-2 rows carry v = i (0-indexed offset), not v = g.
	if seen[1001] != 1.0 {
		t.Fatalf("group 1001: sum %v, want 1", seen[1001])
	}
	if seen[-999] != 1.0 {
		t.Fatalf("NULL group: sum %v, want 1 (the demotion row)", seen[-999])
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

// TestHashAggregateMergeSink_DrainsCloneToRuns: when the primary is
// NON-empty and the sides cannot merge on an SoA fast path (compact-key mode
// has none), mergeSinkState must hand the clone's state to the primary as
// canonical partial-state run files — not materialize both sides into the
// generic map (the SF100 Q17 barrier blowup, morsel-agg-partials-v2.md §3.C).
func TestHashAggregateMergeSink_DrainsCloneToRuns(t *testing.T) {
	tracker := memory.NewTracker("test", 1<<30)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	// Two group columns of mixed type land on the compact-key path (binary
	// serialized keys, no SoA-SoA merge).
	schema := []parquet.Column{
		{Name: "y", Type: parquet.TypeInt64},
		{Name: "r", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	mk := func(base, n int) *batch.RecordBatch {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{
				"y": int64(base + i),
				"r": fmt.Sprintf("r%03d", (base+i)%7),
				"v": float64(base + i),
			}
		}
		return batch.FromRows(schema, rows)
	}
	newAgg := func() *HashAggregate {
		h := &HashAggregate{
			GroupByCols: []string{"y", "r"},
			Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
			Spill:       spill,
		}
		if err := h.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		return h
	}
	ctx := context.Background()
	primary := newAgg()
	if err := primary.Consume(ctx, mk(0, 500)); err != nil {
		t.Fatal(err)
	}
	clone := primary.CloneSink().(*HashAggregate)
	clone.Spill = spill.TrackingOnlyView()
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(ctx, mk(500, 500)); err != nil { // disjoint keys
		t.Fatal(err)
	}

	if primary.groupCount() == 0 {
		t.Fatal("test setup: primary must be non-empty so adoption does not fire")
	}
	runsBefore := len(primary.partialSpillFiles)
	primary.MergeSink(clone)
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	if len(primary.partialSpillFiles) <= runsBefore {
		t.Fatalf("expected clone state handed over as partial-state runs; files %d -> %d",
			runsBefore, len(primary.partialSpillFiles))
	}
	if primary.groupCount() != 500 {
		t.Fatalf("primary in-memory groups = %d, want 500 (untouched by the merge)", primary.groupCount())
	}

	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	sum := 0.0
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		ti := -1
		for i, c := range b.Schema {
			if c.Name == "total" {
				ti = i
			}
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			rows++
			sum += b.Columns[ti].Float64Data[row]
		}
	}
	if rows != 1000 {
		t.Fatalf("output groups = %d, want 1000", rows)
	}
	if want := float64(999*1000) / 2; sum != want {
		t.Fatalf("total sum = %v, want %v", sum, want)
	}
	if err := primary.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHashAggregateClonePartialDrain: a clone with PartialDrainBytes set
// must bound its in-memory state by self-draining to run files mid-consume,
// hand every run to the primary at MergeSink, and lose no groups. This is
// the §3.A never-OOM bound for morsel-parallel high-NDV aggregation
// (morsel-agg-partials-v2.md): clones run on a tracking-only view whose
// ShouldSpillFor is unconditionally false, so this threshold is their only
// pressure valve.
func TestHashAggregateClonePartialDrain(t *testing.T) {
	tracker := memory.NewTracker("test", 1<<30)
	spill, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	mk := func(base, n int) *batch.RecordBatch {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"g": int64(base + i), "v": float64(base + i)}
		}
		return batch.FromRows(schema, rows)
	}
	ctx := context.Background()
	primary := &HashAggregate{
		GroupByCols: []string{"g"},
		Aggs:        []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeFloat64}},
		Spill:       spill,
	}
	if err := primary.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := primary.Consume(ctx, mk(0, 1000)); err != nil {
		t.Fatal(err)
	}

	clone := primary.CloneSink().(*HashAggregate)
	clone.Spill = spill.TrackingOnlyView()
	clone.PartialDrainBytes = 64 << 10 // tiny bound → several drains
	if err := clone.Init(ctx); err != nil {
		t.Fatal(err)
	}
	const cloneGroups = 20_000
	for base := 1000; base < 1000+cloneGroups; base += 2000 {
		if err := clone.Consume(ctx, mk(base, 2000)); err != nil {
			t.Fatal(err)
		}
	}
	if len(clone.drainedRuns) < 2 {
		t.Fatalf("expected multiple self-drains under a 64KB bound, got %d runs", len(clone.drainedRuns))
	}
	// The bound held: in-memory state stayed near the threshold, not
	// proportional to the 20k groups consumed.
	if got := clone.groupMemoryUsage(); got > 8*(64<<10) {
		t.Fatalf("clone in-memory state = %d bytes, want bounded near 64KB", got)
	}

	runs := len(clone.drainedRuns)
	primary.MergeSink(clone)
	if len(clone.drainedRuns) != 0 {
		t.Fatal("MergeSink must take ownership of the clone's drained runs")
	}
	if len(primary.partialSpillFiles) < runs {
		t.Fatalf("primary received %d runs, clone drained %d", len(primary.partialSpillFiles), runs)
	}
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}

	if err := primary.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	groups := 0
	sum := 0.0
	for {
		b, err := primary.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		ti := -1
		for i, c := range b.Schema {
			if c.Name == "total" {
				ti = i
			}
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			groups++
			sum += b.Columns[ti].Float64Data[row]
		}
	}
	want := 1000 + cloneGroups
	if groups != want {
		t.Fatalf("output groups = %d, want %d", groups, want)
	}
	n := float64(want)
	if wantSum := n * (n - 1) / 2; sum != wantSum {
		t.Fatalf("total sum = %v, want %v", sum, wantSum)
	}
	if err := primary.Close(); err != nil {
		t.Fatal(err)
	}
}
