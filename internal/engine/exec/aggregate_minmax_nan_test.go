package exec

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression coverage for #457 (MIN/MAX aggregate over a float column
// ignoring the NaN total order) at the HashAggregate/pipeline level. The
// kernel-level accumulator updaters are pinned directly in
// internal/engine/exec/kernel/agg_nan_test.go; these tests exercise the
// SAME defect through every consumption shape HashAggregate offers: the
// int-keyed SoA scatter path (agg_scatter.go), the generic per-group path
// (kernel.ResolveRowMin/Max), the scalar (no GROUP BY) batch path, the
// morsel-parallel cross-unit merge (mergeFlatAccumRow), and a forced
// external-merge spill (Accumulator.Merge via the k-way drain merger) — the
// same PostgreSQL answer (verified live against postgres:17-alpine) must
// come out of every one of them: MAX over a group containing a NaN is NaN,
// MIN is the smallest non-NaN value unless every value is NaN, and NULLs
// are skipped as always.

func nanMinMaxSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
}

func nanMinMaxAggs() []AggColumn {
	return []AggColumn{
		{Func: AggMin, InputCol: "v", OutputCol: "lo", OutputType: parquet.TypeFloat64},
		{Func: AggMax, InputCol: "v", OutputCol: "hi", OutputType: parquet.TypeFloat64},
	}
}

// checkMinMaxRow asserts a {lo, hi} output row against expected values,
// treating math.NaN() as a sentinel meaning "must be NaN".
func checkMinMaxRow(t *testing.T, label string, row map[string]any, wantLo, wantHi float64) {
	t.Helper()
	lo, ok := row["lo"].(float64)
	if !ok {
		t.Fatalf("%s: lo = %#v (%T), want float64", label, row["lo"], row["lo"])
	}
	hi, ok := row["hi"].(float64)
	if !ok {
		t.Fatalf("%s: hi = %#v (%T), want float64", label, row["hi"], row["hi"])
	}
	if math.IsNaN(wantLo) {
		if !math.IsNaN(lo) {
			t.Errorf("%s: MIN = %v, want NaN", label, lo)
		}
	} else if lo != wantLo {
		t.Errorf("%s: MIN = %v, want %v", label, lo, wantLo)
	}
	if math.IsNaN(wantHi) {
		if !math.IsNaN(hi) {
			t.Errorf("%s: MAX = %v, want NaN", label, hi)
		}
	} else if hi != wantHi {
		t.Errorf("%s: MAX = %v, want %v", label, hi, wantHi)
	}
}

// TestHashAggregate_GroupedMinMaxFloatNaN_SoAScatterPath drives the int-keyed
// SoA scatter path (scatterMinFloat/scatterMaxFloat, agg_scatter.go) with
// NaN at different arrival positions, an all-NaN group and a NaN+NULL group,
// each split across several Consume() batches so the scatter kernel — not
// just the single-row updater — sees the arrival order.
func TestHashAggregate_GroupedMinMaxFloatNaN_SoAScatterPath(t *testing.T) {
	nan := math.NaN()
	groups := map[int64][]any{ // nil entry = SQL NULL
		1: {nan, 5.0, 3.0, -100.0},          // NaN first
		2: {5.0, 3.0, -100.0, nan},          // NaN last
		3: {5.0, nan, 3.0, -100.0},          // NaN middle
		4: {nan, nan, nan},                  // all NaN
		5: {nan, nil, nil},                  // NaN + NULLs only
		6: {nil, nan, 2.0},                  // NULL, NaN, finite
		7: {1.0, math.Inf(-1), math.Inf(1)}, // sanity: no NaN
	}
	want := map[int64][2]float64{
		1: {-100.0, nan}, 2: {-100.0, nan}, 3: {-100.0, nan},
		4: {nan, nan}, 5: {nan, nan}, 6: {2.0, nan},
		7: {math.Inf(-1), math.Inf(1)},
	}

	ctx := context.Background()
	h := NewHashAggregate([]string{"k"}, nanMinMaxAggs())
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// One row per Consume call per group, round-robining groups, so each
	// group's values arrive one Consume() (and therefore one scatter call)
	// apart — the shape that would let arrival order leak into the answer.
	maxLen := 0
	for _, vals := range groups {
		if len(vals) > maxLen {
			maxLen = len(vals)
		}
	}
	for i := 0; i < maxLen; i++ {
		var rows []map[string]any
		for k, vals := range groups {
			if i >= len(vals) {
				continue
			}
			rows = append(rows, map[string]any{"k": k, "v": vals[i]})
		}
		if len(rows) == 0 {
			continue
		}
		if err := h.Consume(ctx, batch.FromRows(nanMinMaxSchema(), rows)); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[int64]map[string]any{}
	for {
		b, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, row := range b.ToRows() {
			got[row["k"].(int64)] = row
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	for k, w := range want {
		row, ok := got[k]
		if !ok {
			t.Fatalf("group %d missing from output", k)
		}
		checkMinMaxRow(t, fmt.Sprintf("group %d", k), row, w[0], w[1])
	}
}

// TestHashAggregate_GroupedMinMaxFloatNaN_GenericPath forces the generic
// per-group path (a STRING group-by key never takes the int/packed SoA fast
// paths), which updates each group's Accumulator through
// kernel.ResolveRowMin/ResolveRowMax directly.
func TestHashAggregate_GroupedMinMaxFloatNaN_GenericPath(t *testing.T) {
	nan := math.NaN()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
	rows := []map[string]any{
		{"k": "a", "v": nan}, {"k": "a", "v": 5.0}, {"k": "a", "v": -100.0},
		{"k": "b", "v": 5.0}, {"k": "b", "v": -100.0}, {"k": "b", "v": nan},
		{"k": "c", "v": nan}, {"k": "c", "v": nan},
	}
	want := map[string][2]float64{
		"a": {-100.0, nan}, "b": {-100.0, nan}, "c": {nan, nan},
	}

	ctx := context.Background()
	h := NewHashAggregate([]string{"k"}, nanMinMaxAggs())
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[string]any{}
	for {
		b, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, row := range b.ToRows() {
			got[row["k"].(string)] = row
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	for k, w := range want {
		row, ok := got[k]
		if !ok {
			t.Fatalf("group %q missing from output", k)
		}
		checkMinMaxRow(t, "group "+k, row, w[0], w[1])
	}
}

// TestHashAggregate_ScalarMinMaxFloatNaN drives the scalar (no GROUP BY)
// batch aggregate fast path (kernel.ResolveBatchMin/Max via minSliceFloat64/
// maxSliceFloat64), NaN arriving in the middle of the only batch.
func TestHashAggregate_ScalarMinMaxFloatNaN(t *testing.T) {
	nan := math.NaN()
	rows := []map[string]any{
		{"k": int64(1), "v": 5.0}, {"k": int64(1), "v": nan}, {"k": int64(1), "v": -100.0}, {"k": int64(1), "v": 3.0},
	}
	ctx := context.Background()
	h := NewHashAggregate(nil, nanMinMaxAggs())
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(nanMinMaxSchema(), rows)); err != nil {
		t.Fatal(err)
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := h.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected one output row")
	}
	out := b.ToRows()
	if len(out) != 1 {
		t.Fatalf("got %d rows, want 1", len(out))
	}
	checkMinMaxRow(t, "scalar", out[0], -100.0, nan)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHashAggregate_MergeSink_MinMaxFloatNaN drives the morsel-parallel
// cross-unit merge (mergeFlatAccumRow, aggregate.go) directly: two units
// consume the SAME group key with disjoint value sets — one unit sees only
// NaN, the other a finite range — and MergeSink combines them. Checked in
// both merge directions (which unit is the primary matters for the exact
// code path but must not matter for the answer).
func TestHashAggregate_MergeSink_MinMaxFloatNaN(t *testing.T) {
	nan := math.NaN()
	for _, dir := range []string{"nan_unit_primary", "finite_unit_primary"} {
		t.Run(dir, func(t *testing.T) {
			ctx := context.Background()
			prim := NewHashAggregate([]string{"k"}, nanMinMaxAggs())
			if err := prim.Init(ctx); err != nil {
				t.Fatal(err)
			}
			clone := prim.CloneSink().(*HashAggregate)
			if err := clone.Init(ctx); err != nil {
				t.Fatal(err)
			}

			nanRows := []map[string]any{{"k": int64(1), "v": nan}, {"k": int64(1), "v": nan}}
			finiteRows := []map[string]any{{"k": int64(1), "v": -5.0}, {"k": int64(1), "v": 5.0}}

			var nanUnit, finiteUnit *HashAggregate
			if dir == "nan_unit_primary" {
				nanUnit, finiteUnit = prim, clone
			} else {
				nanUnit, finiteUnit = clone, prim
			}
			if err := nanUnit.Consume(ctx, batch.FromRows(nanMinMaxSchema(), nanRows)); err != nil {
				t.Fatal(err)
			}
			if err := finiteUnit.Consume(ctx, batch.FromRows(nanMinMaxSchema(), finiteRows)); err != nil {
				t.Fatal(err)
			}

			prim.MergeSink(clone)
			if err := prim.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := prim.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				t.Fatal("expected one output row")
			}
			out := b.ToRows()
			if len(out) != 1 {
				t.Fatalf("got %d rows, want 1", len(out))
			}
			// The union of {NaN, NaN} and {-5, 5}: MIN must be the finite
			// -5 (NaN never wins MIN while a finite value exists anywhere),
			// MAX must be NaN (NaN always wins MAX).
			checkMinMaxRow(t, dir, out[0], -5.0, nan)
			if err := prim.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestExternalMergeSpill_FloatMinMaxNaNSurvivesDrain forces a spill so equal
// keys land in different spilled runs and are combined by the k-way merger's
// Accumulator.Merge (kWayMerger.Next, aggregate_partial_spill.go) rather
// than by an in-memory scatter/row update. Mirrors
// TestExternalMergeSpill_FloatNegZeroCollapsesOnDrain's shape (#446's
// -0.0/+0.0 drain regression) for #457's NaN MIN/MAX.
func TestExternalMergeSpill_FloatMinMaxNaNSurvivesDrain(t *testing.T) {
	nan := math.NaN()
	const numBatches = 20
	const rowsPerBatch = 50
	batches := make([]*batch.RecordBatch, 0, numBatches)
	// Group 0 gets a NaN on an EARLY batch and finite values on LATER
	// batches (spanning many forced spills), group 1 gets NaN on a LATE
	// batch — exercising both merge directions across the drain.
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			g := int64((bi*rowsPerBatch + ri) % 2)
			var v float64
			switch {
			case g == 0 && bi == 0:
				v = nan
			case g == 1 && bi == numBatches-1:
				v = nan
			default:
				v = float64(bi*1000+ri) - 5000 // finite range, some negative
			}
			rows = append(rows, map[string]any{"k": g, "v": v})
		}
		batches = append(batches, batch.FromRows(nanMinMaxSchema(), rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, nanMinMaxAggs())
		h.Spill = spill
		return h
	}

	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := mkAgg(sm)
	tracker.ForceReserve(900) // tight budget: forces repeated spill+drain
	rows := runHashAggToMap(t, h, batches)

	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2", len(rows))
	}
	for _, row := range rows {
		k, _ := row["k"].(int64)
		// Both groups contain exactly one NaN and a finite range that
		// includes negative values — MIN must be finite (the smallest
		// finite value present), MAX must be NaN, regardless of which
		// batch (and therefore which spilled run) carried the NaN.
		lo, hi := row["lo"], row["hi"]
		loF, ok := lo.(float64)
		if !ok || math.IsNaN(loF) {
			t.Errorf("group %d: MIN = %#v, want a finite value (NaN never wins MIN while a finite "+
				"value is present, regardless of which spilled run carried it)", k, lo)
		}
		hiF, ok := hi.(float64)
		if !ok || !math.IsNaN(hiF) {
			t.Errorf("group %d: MAX = %#v, want NaN", k, hi)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// --- MIN_BY/MAX_BY float NaN ordering key (fold-in to #457) ----------------
//
// MIN_BY/MAX_BY's accumulator (updateGroup's AggMinBy/AggMaxBy case,
// aggregate.go) and its cross-unit merge (mergeExtraState's *minMaxByState
// case) compared the ordering column with raw `<`/`>` instead of
// kernel.CompareFloat64, the same arrival-order-dependent defect #457 fixed
// for bare MIN/MAX. Reviewer's probe: rows {a: NaN, b: 1.0, c: -5.0,
// d: 3.0} — under PostgreSQL's float order (NaN sorts greatest, ADR-0012
// item 8), MIN_BY must return the row carrying the smallest non-NaN
// comparison value ("c") and MAX_BY must return the row whose comparison
// value is NaN ("a"), in every arrival order and whichever unit merges into
// the other.

func minByMaxBySchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64, Nullable: true},
	}
}

func minByMaxByAggs() []AggColumn {
	return []AggColumn{
		{Func: AggMinBy, InputCol: "name", InputCol2: "val", OutputCol: "min_name", OutputType: parquet.TypeString},
		{Func: AggMaxBy, InputCol: "name", InputCol2: "val", OutputCol: "max_name", OutputType: parquet.TypeString},
	}
}

// checkMinByMaxByRow asserts the {min_name, max_name} output row against the
// reviewer's probe's expected winners.
func checkMinByMaxByRow(t *testing.T, label string, row map[string]any) {
	t.Helper()
	if minName, _ := row["min_name"].(string); minName != "c" {
		t.Errorf("%s: MIN_BY = %q, want %q", label, minName, "c")
	}
	if maxName, _ := row["max_name"].(string); maxName != "a" {
		t.Errorf("%s: MAX_BY = %q, want %q", label, maxName, "a")
	}
}

// TestHashAggregate_MinByMaxByFloatNaN_ArrivalOrder drives the grouped
// accumulator (updateGroup's AggMinBy/AggMaxBy case) with the reviewer's
// probe row set delivered in several different arrival orders, each as its
// own group key so a single Consume call's per-row order is what's under
// test.
func TestHashAggregate_MinByMaxByFloatNaN_ArrivalOrder(t *testing.T) {
	nan := math.NaN()
	// base[0]=a (NaN), base[1]=b (1.0), base[2]=c (-5.0), base[3]=d (3.0).
	type probeRow struct {
		name string
		val  float64
	}
	base := []probeRow{{"a", nan}, {"b", 1.0}, {"c", -5.0}, {"d", 3.0}}
	orders := []struct {
		name string
		perm []int
	}{
		{"natural", []int{0, 1, 2, 3}},               // a, b, c, d — NaN first
		{"nan_last", []int{1, 2, 3, 0}},              // b, c, d, a — NaN last
		{"min_first", []int{2, 0, 1, 3}},             // c, a, b, d — winner arrives first
		{"reverse", []int{3, 2, 1, 0}},               // d, c, b, a
		{"nan_then_min_adjacent", []int{2, 3, 1, 0}}, // c, d, b, a
	}

	ctx := context.Background()
	h := NewHashAggregate([]string{"k"}, minByMaxByAggs())
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for i, o := range orders {
		k := int64(i)
		rows := make([]map[string]any, 0, len(o.perm))
		for _, idx := range o.perm {
			rows = append(rows, map[string]any{"k": k, "name": base[idx].name, "val": base[idx].val})
		}
		if err := h.Consume(ctx, batch.FromRows(minByMaxBySchema(), rows)); err != nil {
			t.Fatalf("%s: Consume: %v", o.name, err)
		}
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[int64]map[string]any{}
	for {
		b, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, row := range b.ToRows() {
			got[row["k"].(int64)] = row
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	for i, o := range orders {
		row, ok := got[int64(i)]
		if !ok {
			t.Fatalf("%s: group missing from output", o.name)
		}
		checkMinByMaxByRow(t, o.name, row)
	}
}

// TestHashAggregate_MinByMaxByFloatNaN_Scalar drives the same probe row set
// through the no-GROUP-BY scalar path, in several arrival orders.
func TestHashAggregate_MinByMaxByFloatNaN_Scalar(t *testing.T) {
	nan := math.NaN()
	orders := map[string][]map[string]any{
		"natural": {
			{"k": int64(1), "name": "a", "val": nan},
			{"k": int64(1), "name": "b", "val": 1.0},
			{"k": int64(1), "name": "c", "val": -5.0},
			{"k": int64(1), "name": "d", "val": 3.0},
		},
		"min_first": {
			{"k": int64(1), "name": "c", "val": -5.0},
			{"k": int64(1), "name": "a", "val": nan},
			{"k": int64(1), "name": "d", "val": 3.0},
			{"k": int64(1), "name": "b", "val": 1.0},
		},
		"nan_last": {
			{"k": int64(1), "name": "b", "val": 1.0},
			{"k": int64(1), "name": "d", "val": 3.0},
			{"k": int64(1), "name": "c", "val": -5.0},
			{"k": int64(1), "name": "a", "val": nan},
		},
	}
	for name, rows := range orders {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			h := NewHashAggregate(nil, minByMaxByAggs())
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h.Consume(ctx, batch.FromRows(minByMaxBySchema(), rows)); err != nil {
				t.Fatal(err)
			}
			if err := h.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := h.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				t.Fatal("expected one output row")
			}
			out := b.ToRows()
			if len(out) != 1 {
				t.Fatalf("got %d rows, want 1", len(out))
			}
			checkMinByMaxByRow(t, name, out[0])
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestHashAggregate_MergeSink_MinByMaxByFloatNaN drives mergeExtraState's
// *minMaxByState case directly: one unit sees only the NaN-keyed row, the
// other sees the three finite-keyed rows, merged in both directions (which
// unit is primary must not change the answer).
func TestHashAggregate_MergeSink_MinByMaxByFloatNaN(t *testing.T) {
	nan := math.NaN()
	for _, dir := range []string{"nan_unit_primary", "finite_unit_primary"} {
		t.Run(dir, func(t *testing.T) {
			ctx := context.Background()
			prim := NewHashAggregate([]string{"k"}, minByMaxByAggs())
			if err := prim.Init(ctx); err != nil {
				t.Fatal(err)
			}
			clone := prim.CloneSink().(*HashAggregate)
			if err := clone.Init(ctx); err != nil {
				t.Fatal(err)
			}

			nanRows := []map[string]any{{"k": int64(1), "name": "a", "val": nan}}
			finiteRows := []map[string]any{
				{"k": int64(1), "name": "b", "val": 1.0},
				{"k": int64(1), "name": "c", "val": -5.0},
				{"k": int64(1), "name": "d", "val": 3.0},
			}

			var nanUnit, finiteUnit *HashAggregate
			if dir == "nan_unit_primary" {
				nanUnit, finiteUnit = prim, clone
			} else {
				nanUnit, finiteUnit = clone, prim
			}
			if err := nanUnit.Consume(ctx, batch.FromRows(minByMaxBySchema(), nanRows)); err != nil {
				t.Fatal(err)
			}
			if err := finiteUnit.Consume(ctx, batch.FromRows(minByMaxBySchema(), finiteRows)); err != nil {
				t.Fatal(err)
			}

			prim.MergeSink(clone)
			if err := prim.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := prim.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				t.Fatal("expected one output row")
			}
			out := b.ToRows()
			if len(out) != 1 {
				t.Fatalf("got %d rows, want 1", len(out))
			}
			checkMinByMaxByRow(t, dir, out[0])
			if err := prim.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
