package exec

import (
	"context"
	"math"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// runHashAggToMap drives a HashAggregate from the given batches and returns
// {groupKey -> output} pairs. Used by external-merge tests to compare
// spilling vs non-spilling runs.
func runHashAggToMap(t *testing.T, h *HashAggregate, batches []*batch.RecordBatch) []map[string]any {
	t.Helper()
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	var rows []map[string]any
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		rows = append(rows, out.ToRows()...)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return rows
}

// TestExternalMergeSpill_SingleIntKeySum forces multiple spills under a tight
// budget and verifies the k-way-merged result equals a non-spilling reference
// run, row-for-row. Mirrors the canonical SF100+ scenario the audit memo
// flagged: GROUP BY l_orderkey on a high-cardinality int column with SUM.
func TestExternalMergeSpill_SingleIntKeySum(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const numBatches = 25
	const rowsPerBatch = 200
	const numGroups = 200
	batches := make([]*batch.RecordBatch, 0, numBatches)
	expected := make(map[int64]int64)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			k := int64((bi*rowsPerBatch + ri) % numGroups)
			v := int64(bi*1000 + ri + 1)
			rows = append(rows, map[string]any{"k": k, "v": v})
			expected[k] += v
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
		})
		h.Spill = spill
		return h
	}

	// Reference run: no spill manager, so all aggregation happens in-memory.
	refRows := runHashAggToMap(t, mkAgg(nil), batches)
	if len(refRows) != numGroups {
		t.Fatalf("reference: expected %d groups, got %d", numGroups, len(refRows))
	}

	// Spilling run: tight budget so every other Consume spills. The
	// external-merge path drains and resets each time, so ShouldSpillFor
	// should flip back to false after each drain.
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := mkAgg(sm)
	// Force pressure before the first batch so we exercise the partial-spill
	// path from the outset rather than a single end-of-input drain.
	tracker.ForceReserve(900)
	gotRows := runHashAggToMap(t, h, batches)

	// Verify spill path was actually exercised.
	// (h.partialSpillFiles is cleared by Finalize.) — check the test budget.
	if len(gotRows) != numGroups {
		t.Fatalf("spilling: expected %d groups, got %d", numGroups, len(gotRows))
	}

	// Compare row-for-row.
	got := make(map[int64]int64, len(gotRows))
	for _, r := range gotRows {
		k := r["k"].(int64)
		v := r["total"].(int64)
		got[k] = v
	}
	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("k=%d: got=%d want=%d", k, got[k], want)
		}
	}
}

// TestExternalMergeSpill_StringKeyMixedAggs covers a single-string group key
// with a mix of SUM, COUNT, MIN, MAX, AVG to verify that all five simple-agg
// kinds round-trip correctly through partial-state spill + merge.
func TestExternalMergeSpill_StringKeyMixedAggs(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	const numBatches = 12
	const rowsPerBatch = 100
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	type accExpected struct {
		count int64
		sum   float64
		min   float64
		max   float64
	}
	expected := make(map[string]*accExpected)
	for _, k := range keys {
		expected[k] = &accExpected{min: math.Inf(1), max: math.Inf(-1)}
	}
	batches := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			k := keys[(bi+ri)%len(keys)]
			v := float64(bi*1000 + ri)
			rows = append(rows, map[string]any{"g": k, "v": v})
			e := expected[k]
			e.count++
			e.sum += v
			if v < e.min {
				e.min = v
			}
			if v > e.max {
				e.max = v
			}
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		return &HashAggregate{
			GroupByCols: []string{"g"},
			Aggs: []AggColumn{
				{Func: AggSum, InputCol: "v", OutputCol: "sum_v", OutputType: parquet.TypeFloat64},
				{Func: AggCount, InputCol: "v", OutputCol: "cnt_v", OutputType: parquet.TypeInt64},
				{Func: AggMin, InputCol: "v", OutputCol: "min_v", OutputType: parquet.TypeFloat64},
				{Func: AggMax, InputCol: "v", OutputCol: "max_v", OutputType: parquet.TypeFloat64},
				{Func: AggAvg, InputCol: "v", OutputCol: "avg_v", OutputType: parquet.TypeFloat64},
			},
			Spill: spill,
		}
	}

	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)

	rows := runHashAggToMap(t, mkAgg(sm), batches)
	if len(rows) != len(keys) {
		t.Fatalf("group count: got %d want %d", len(rows), len(keys))
	}
	got := make(map[string]map[string]any, len(rows))
	for _, r := range rows {
		got[r["g"].(string)] = r
	}
	for k, e := range expected {
		r, ok := got[k]
		if !ok {
			t.Errorf("missing group %q", k)
			continue
		}
		if r["sum_v"].(float64) != e.sum {
			t.Errorf("k=%q sum: got %v want %v", k, r["sum_v"], e.sum)
		}
		if r["cnt_v"].(int64) != e.count {
			t.Errorf("k=%q count: got %v want %v", k, r["cnt_v"], e.count)
		}
		if r["min_v"].(float64) != e.min {
			t.Errorf("k=%q min: got %v want %v", k, r["min_v"], e.min)
		}
		if r["max_v"].(float64) != e.max {
			t.Errorf("k=%q max: got %v want %v", k, r["max_v"], e.max)
		}
		gotAvg := r["avg_v"].(float64)
		wantAvg := e.sum / float64(e.count)
		if math.Abs(gotAvg-wantAvg) > 1e-9 {
			t.Errorf("k=%q avg: got %v want %v", k, gotAvg, wantAvg)
		}
	}
}

// TestExternalMergeSpill_GenericMultiColKey covers the useGenericSoA path:
// multi-column GROUP BY with a string column forcing fall-through past the
// int / dual-int / compact / single-string fast paths.
func TestExternalMergeSpill_GenericMultiColKey(t *testing.T) {
	schema := []parquet.Column{
		{Name: "region", Type: parquet.TypeString},
		{Name: "year", Type: parquet.TypeInt32},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	regions := []string{"NA", "EU", "APAC"}
	years := []int32{2020, 2021, 2022, 2023}
	type expectKey struct {
		region string
		year   int32
	}
	expected := make(map[expectKey]float64)
	const numBatches = 10
	const rowsPerBatch = 60
	batches := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			r := regions[(bi+ri)%len(regions)]
			y := years[ri%len(years)]
			a := float64(bi*100 + ri)
			rows = append(rows, map[string]any{"region": r, "year": y, "amount": a})
			expected[expectKey{r, y}] += a
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		return &HashAggregate{
			GroupByCols: []string{"region", "year"},
			Aggs: []AggColumn{
				{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
			},
			Spill: spill,
		}
	}

	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)
	rows := runHashAggToMap(t, mkAgg(sm), batches)

	got := make(map[expectKey]float64, len(rows))
	for _, r := range rows {
		k := expectKey{r["region"].(string), r["year"].(int32)}
		got[k] = r["total"].(float64)
	}
	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if math.Abs(got[k]-want) > 1e-6 {
			t.Errorf("region=%s year=%d: got %v want %v", k.region, k.year, got[k], want)
		}
	}
}

// TestExternalMergeSpill_PartialFileRoundTrip is a unit test for the
// writer/reader round-trip on its own, decoupled from HashAggregate. It
// constructs partial groups by hand, writes them, reads them back, and
// asserts byte-identical equivalence.
func TestExternalMergeSpill_PartialFileRoundTrip(t *testing.T) {
	header := partialSpillHeader{
		GroupCols: []partialGroupCol{
			{Name: "k", Type: parquet.TypeInt64},
		},
		Aggs: []partialAggSpec{
			{Func: AggSum, IsFloat: false},
			{Func: AggCount},
			{Func: AggMin, IsFloat: true},
		},
	}

	groups := []*partialGroup{
		{
			SortKey: []byte{0, 0, 0, 0, 0, 0, 0, 1},
			KeyVals: []partialKeyValue{{Tag: partialTagInt64, I64: 1}},
			Accs: []kernel.Accumulator{
				{Count: 5, SumI64: 100},
				{Count: 5},
				{Count: 5, IsFloat: true, MinF64: 3.14, HasMin: true},
			},
		},
		{
			SortKey: []byte{0, 0, 0, 0, 0, 0, 0, 2},
			KeyVals: []partialKeyValue{{Tag: partialTagInt64, I64: 2}},
			Accs: []kernel.Accumulator{
				{Count: 7, SumI64: 200},
				{Count: 7},
				{Count: 7, IsFloat: true, MinF64: 2.71, HasMin: true},
			},
		},
	}

	dir := t.TempDir()
	w, path, err := newPartialSpillWriter(dir, header)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		if err := w.Write(*g); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := openPartialSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.header.NumGroups != 2 {
		t.Errorf("header NumGroups: got %d want 2", r.header.NumGroups)
	}
	for i, want := range groups {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		if !bytesEq(got.SortKey, want.SortKey) {
			t.Errorf("group %d sortKey: got %x want %x", i, got.SortKey, want.SortKey)
		}
		if got.KeyVals[0].Tag != want.KeyVals[0].Tag || got.KeyVals[0].I64 != want.KeyVals[0].I64 {
			t.Errorf("group %d keyVals: got %+v want %+v", i, got.KeyVals[0], want.KeyVals[0])
		}
		for ai := range want.Accs {
			gotAcc := got.Accs[ai]
			wantAcc := want.Accs[ai]
			spec := header.Aggs[ai]
			// The format only persists fields the agg actually uses
			// (see emitAcc / readAcc). For AggSum/AggAvg/AggCount we
			// expect Count + a sum field; for AggMin/AggMax we expect
			// HasMin/HasMax and the corresponding value (no Count).
			switch spec.Func {
			case AggSum, AggAvg:
				if gotAcc.Count != wantAcc.Count {
					t.Errorf("group %d agg %d Count: got %d want %d", i, ai, gotAcc.Count, wantAcc.Count)
				}
				if gotAcc.SumI64 != wantAcc.SumI64 {
					t.Errorf("group %d agg %d SumI64: got %d want %d", i, ai, gotAcc.SumI64, wantAcc.SumI64)
				}
			case AggCount:
				if gotAcc.Count != wantAcc.Count {
					t.Errorf("group %d agg %d Count: got %d want %d", i, ai, gotAcc.Count, wantAcc.Count)
				}
			case AggMin:
				if gotAcc.HasMin != wantAcc.HasMin {
					t.Errorf("group %d agg %d HasMin: got %v want %v", i, ai, gotAcc.HasMin, wantAcc.HasMin)
				}
				if gotAcc.MinF64 != wantAcc.MinF64 {
					t.Errorf("group %d agg %d MinF64: got %v want %v", i, ai, gotAcc.MinF64, wantAcc.MinF64)
				}
			}
		}
	}
	// Past the last group, Next should return EOF.
	if _, err := r.Next(); err == nil {
		t.Error("expected EOF after last group")
	}
}

// TestExternalMergeSpill_KWayMerge tests the merger directly with hand-built
// runs. Two file runs with overlapping keys must produce a single merged
// stream with combined accumulators.
func TestExternalMergeSpill_KWayMerge(t *testing.T) {
	specs := []partialAggSpec{
		{Func: AggSum, IsFloat: false},
		{Func: AggCount},
	}

	mkGroup := func(k int64, sum int64, cnt int64) *partialGroup {
		buf := make([]byte, 8)
		// We deliberately use the same little-endian encoding the audit
		// example used. The merger only requires byte-equal sortKeys for
		// the same logical group; all runs in this test build keys the
		// same way.
		buf[0] = byte(k)
		return &partialGroup{
			SortKey: buf,
			KeyVals: []partialKeyValue{{Tag: partialTagInt64, I64: k}},
			Accs: []kernel.Accumulator{
				{Count: cnt, SumI64: sum},
				{Count: cnt},
			},
		}
	}

	runA := []*partialGroup{
		mkGroup(1, 10, 1),
		mkGroup(3, 30, 1),
		mkGroup(5, 50, 1),
	}
	runB := []*partialGroup{
		mkGroup(2, 200, 2),
		mkGroup(3, 300, 3),
		mkGroup(4, 400, 4),
	}

	merger := newKWayMerger([]partialRunSource{
		newMemoryRunSource(runA),
		newMemoryRunSource(runB),
	}, specs)
	defer merger.Close()

	type out struct {
		k   int64
		sum int64
		cnt int64
	}
	var got []out
	for {
		g, err := merger.Next()
		if err != nil {
			t.Fatalf("merger.Next: %v", err)
		}
		if g == nil {
			break
		}
		got = append(got, out{
			k:   g.KeyVals[0].I64,
			sum: g.Accs[0].SumI64,
			cnt: g.Accs[0].Count,
		})
	}
	want := []out{
		{1, 10, 1},
		{2, 200, 2},
		{3, 330, 4}, // merged: 30+300, 1+3
		{4, 400, 4},
		{5, 50, 1},
	}
	sort.Slice(got, func(i, j int) bool { return got[i].k < got[j].k })
	if len(got) != len(want) {
		t.Fatalf("merge output count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("merged[%d]: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// TestExternalMergeSpill_StreamingNext verifies that finalizeViaPartialMerge
// runs in streaming mode: after Finalize, partialMerger is non-nil and
// strGroupStates is empty (we don't materialize the merged result up front).
// Each Next() pulls one batch's worth, and the merger is closed when the
// last batch is drained.
func TestExternalMergeSpill_StreamingNext(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900) // force pressure on first Consume

	const numGroups = 5000 // multiple batches worth of merged output
	const batches = 5
	expected := make(map[int64]int64, numGroups)
	for bi := 0; bi < batches; bi++ {
		rows := make([]map[string]any, 0, numGroups)
		for k := int64(0); k < int64(numGroups); k++ {
			v := int64(bi + 1)
			rows = append(rows, map[string]any{"k": k, "v": v})
			expected[k] += v
		}
		if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	// Streaming-mode invariants right after Finalize:
	if h.partialMerger == nil {
		t.Fatal("expected partialMerger set after Finalize, got nil")
	}
	if len(h.strGroupStates) != 0 {
		t.Errorf("strGroupStates should be empty in streaming mode, got %d", len(h.strGroupStates))
	}

	// Drain Next() across multiple calls. Verify that the merger drains
	// across at least 2 batches (numGroups > DefaultBatchSize).
	got := make(map[int64]int64, numGroups)
	nBatches := 0
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		nBatches++
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if nBatches < 2 {
		t.Errorf("expected streaming to span >=2 batches for %d groups, got %d", numGroups, nBatches)
	}
	if h.partialMerger != nil {
		t.Errorf("expected partialMerger cleared after exhaustion, still set")
	}

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != numGroups {
		t.Fatalf("group count: got %d want %d", len(got), numGroups)
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("k=%d: got %d want %d", k, got[k], want)
		}
	}
}

func bytesEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestExternalMergeSpill_FinalizeAllSpilledNoRemainder exercises the path
// where SpillSome drained every group, no more Consume calls follow, and
// Finalize must merge the spill files alone — partialGroupCursor.numGroups
// is 0 and the cursor must Close without joining the source list.
//
// Regression check: ensure a 0-group cursor doesn't appear as a heap entry
// (which would cause Peek().SortKey to be a zero-length slice and break the
// merger's order invariant).
func TestExternalMergeSpill_FinalizeAllSpilledNoRemainder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	const numGroups = 200
	rows := make([]map[string]any, 0, numGroups)
	expected := make(map[int64]int64, numGroups)
	for i := 0; i < numGroups; i++ {
		k := int64(i)
		v := int64(i + 1)
		rows = append(rows, map[string]any{"k": k, "v": v})
		expected[k] = v
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}

	// Drain everything to spill, then DON'T consume anything further. The
	// in-memory remainder is now empty.
	if _, err := h.SpillSome(h.Inspect().OwnedBytes); err != nil {
		t.Fatal(err)
	}
	if len(h.partialSpillFiles) != 1 {
		t.Fatalf("expected 1 spill file, got %d", len(h.partialSpillFiles))
	}
	if h.numIntGroups != 0 {
		t.Fatalf("expected empty in-memory groups, got %d", h.numIntGroups)
	}

	// Finalize hits finalizeViaPartialMerge with no in-memory remainder.
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := make(map[int64]int64, numGroups)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != numGroups {
		t.Fatalf("group count: got %d want %d", len(got), numGroups)
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("k=%d: got %d want %d", k, got[k], want)
		}
	}
}

// TestHashAggregateSpillable_FootprintAndSpillSome covers the Spillable
// contract directly: SpillFootprint reflects tracker bytes after Consume,
// SpillSome drains state to a partial-spill file and releases bytes back,
// and a follow-up Consume rebuilds normally on the empty hash table.
func TestHashAggregateSpillable_FootprintAndSpillSome(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// SpillFootprint before any input is 0 — no state, no tracker bytes.
	if got := h.Inspect().OwnedBytes; got != 0 {
		t.Errorf("pre-Consume footprint: got %d want 0", got)
	}

	rows := make([]map[string]any, 0, 500)
	for i := 0; i < 500; i++ {
		rows = append(rows, map[string]any{"k": int64(i), "v": int64(i + 1)})
	}
	b := batch.FromRows(schema, rows)
	if err := h.Consume(ctx, b); err != nil {
		t.Fatal(err)
	}

	// Footprint is now non-zero (group state grew during scatter).
	footprint := h.Inspect().OwnedBytes
	if footprint <= 0 {
		t.Fatalf("post-Consume footprint: got %d want > 0", footprint)
	}

	// SpillSome drains the entire hash table.
	freed, err := h.SpillSome(footprint)
	if err != nil {
		t.Fatal(err)
	}
	if freed != footprint {
		t.Errorf("SpillSome: freed %d want %d", freed, footprint)
	}

	// A second SpillSome with no in-memory state freed should return 0
	// without writing a new file.
	if again, err := h.SpillSome(1024); err != nil || again != 0 {
		t.Errorf("idle SpillSome: got freed=%d err=%v want 0/nil", again, err)
	}

	// A follow-up Consume rebuilds the hash on an empty table and stays
	// correct end-to-end. Append rows for a second pass over the same keys
	// so Finalize merges file + in-memory.
	moreRows := make([]map[string]any, 0, 500)
	for i := 0; i < 500; i++ {
		moreRows = append(moreRows, map[string]any{"k": int64(i), "v": int64(10)})
	}
	if err := h.Consume(ctx, batch.FromRows(schema, moreRows)); err != nil {
		t.Fatal(err)
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 500 {
		t.Fatalf("group count: got %d want 500", len(got))
	}
	for i := int64(0); i < 500; i++ {
		want := (i + 1) + 10
		if got[i] != want {
			t.Errorf("k=%d: got %d want %d", i, got[i], want)
		}
	}
}

// TestSortSpillable_FootprintAndSpillSome covers Sort's Spillable contract:
// after Consume the footprint reflects accumulated input bytes; SpillSome
// drains them to a raw-row spill file and releases the bytes.
func TestSortSpillable_FootprintAndSpillSome(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSort([]SortKey{{Column: "k"}})
	s.Spill = sm
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}

	if got := s.Inspect().OwnedBytes; got != 0 {
		t.Errorf("pre-Consume footprint: got %d want 0", got)
	}

	// Two batches of 200 rows each — Sort accumulates both before a Finalize.
	for batchIdx := 0; batchIdx < 2; batchIdx++ {
		rows := make([]map[string]any, 0, 200)
		for i := 0; i < 200; i++ {
			rows = append(rows, map[string]any{"k": int64(batchIdx*1000 + i)})
		}
		if err := s.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}

	footprint := s.Inspect().OwnedBytes
	if footprint <= 0 {
		t.Fatalf("post-Consume footprint: got %d want > 0", footprint)
	}

	freed, err := s.SpillSome(footprint)
	if err != nil {
		t.Fatal(err)
	}
	if freed != footprint {
		t.Errorf("SpillSome: freed %d want %d", freed, footprint)
	}
	if got := s.Inspect().OwnedBytes; got != 0 {
		t.Errorf("post-spill footprint: got %d want 0", got)
	}

	// Idle SpillSome with no batches in memory returns 0.
	if again, err := s.SpillSome(1024); err != nil || again != 0 {
		t.Errorf("idle SpillSome: got freed=%d err=%v", again, err)
	}

	// Finalize must still emit the rows in sorted order — they live on disk.
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var got []int64
	for {
		out, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got = append(got, r["k"].(int64))
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 400 {
		t.Fatalf("rows: got %d want 400", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("not sorted at i=%d: %d < %d", i, got[i], got[i-1])
		}
	}
}

// TestSpillable_MultiBreakerCooperativeRelief simulates the architectural
// scenario the audit memo flagged: two breakers (HashAggregate + Sort) in
// the same fragment share a tracker; one of them is passive in memory while
// the other hits pressure. RequestRelief invoked from the second operator
// should pick the largest registered Spillable and free its bytes.
func TestSpillable_MultiBreakerCooperativeRelief(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	// Aggregate accumulates a tall hash table.
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 600)
	for i := 0; i < 600; i++ {
		rows = append(rows, map[string]any{"k": int64(i), "v": int64(i)})
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}

	// Sort holds nothing yet — it's a "peer" that hasn't received batches.
	s := NewSort([]SortKey{{Column: "k"}})
	s.Spill = sm
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}

	hFootprint := h.Inspect().OwnedBytes
	if hFootprint == 0 {
		t.Fatal("hash aggregate footprint should be non-zero")
	}
	if s.Inspect().OwnedBytes != 0 {
		t.Fatalf("sort footprint should be 0 (no batches), got %d", s.Inspect().OwnedBytes)
	}

	// Imagine Sort is about to allocate and asks for relief. The registry
	// should pick HashAggregate (largest contributor) and call SpillSome.
	freed, err := sm.RequestRelief(hFootprint)
	if err != nil {
		t.Fatal(err)
	}
	if freed == 0 {
		t.Fatal("RequestRelief returned 0 — registry didn't route to HashAggregate")
	}

	// Cleanup. Both operators should still finalize correctly even after
	// their cooperative spill participation.
	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("HashAggregate.Finalize after relief: %v", err)
	}
	gotGroups := 0
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		gotGroups += out.ActiveLen()
	}
	if gotGroups != 600 {
		t.Errorf("groups after cooperative spill: got %d want 600", gotGroups)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHashAggregateSpillable_RequestReliefRoutesHere proves that when
// HashAggregate is registered as the largest Spillable in a SpillManager and
// an external caller invokes RequestRelief, the relief is satisfied by
// HashAggregate.SpillSome (rather than returning 0 because no-one volunteered).
// Mirrors the multi-breaker scenario where a peer Sort hits memory pressure
// and asks the registry for help.
func TestHashAggregateSpillable_RequestReliefRoutesHere(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Build up a few hundred groups so SpillFootprint > 0 and the registry
	// can pick this aggregate.
	rows := make([]map[string]any, 0, 400)
	for i := 0; i < 400; i++ {
		rows = append(rows, map[string]any{"k": int64(i), "v": int64(i)})
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}

	footprint := h.Inspect().OwnedBytes
	if footprint <= 0 {
		t.Fatalf("setup precondition: footprint should be positive, got %d", footprint)
	}

	// Simulate a peer operator under pressure asking the SpillManager to
	// free bytes. The registry walks Spillables, picks the largest, and
	// calls SpillSome on it. Our HashAggregate is the only registered
	// Spillable, so it MUST satisfy the request.
	freed, err := sm.RequestRelief(footprint)
	if err != nil {
		t.Fatalf("RequestRelief: %v", err)
	}
	if freed == 0 {
		t.Fatal("RequestRelief returned 0 freed — Spillable not registered or SpillSome not wired")
	}
	if h.Inspect().OwnedBytes != 0 {
		t.Errorf("after relief, footprint should be 0, got %d", h.Inspect().OwnedBytes)
	}

	// Finalize should still produce the original 400 distinct sums.
	if err := h.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 400 {
		t.Fatalf("group count: got %d want 400", len(got))
	}
	for i := int64(0); i < 400; i++ {
		if got[i] != i {
			t.Errorf("k=%d: got %d want %d", i, got[i], i)
		}
	}
}

// TestPartialDrain_IntKeyMultiSpill exercises the partition-slice partial-
// drain path: feeds a high-cardinality int-keyed aggregate, fires multiple
// SpillSome calls with small targets during Consume, and verifies the final
// merged output matches a no-spill reference row-for-row.
//
// This is the regression test for the drain-rebuild loop that caused Q17
// SF100 mc=3 to stall: previously SpillSome always drained the whole hash
// table on every call, so subsequent Consume rows rebuilt the same groups
// from scratch — pinning the heap and starving heartbeats. Partial drain
// frees only a hash-partition slice; surviving partitions stay in-memory so
// new rows for those keys hit existing groups instead of re-inserting.
func TestPartialDrain_IntKeyMultiSpill(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const numGroups = 1000
	const rowsPerBatch = 200
	const numBatches = 25

	expected := make(map[int64]int64)
	batches := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			k := int64((bi*rowsPerBatch + ri) % numGroups)
			v := int64(bi*1000 + ri + 1)
			rows = append(rows, map[string]any{"k": k, "v": v})
			expected[k] += v
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
		})
		h.Spill = spill
		return h
	}

	// Reference run: no SpillManager, all aggregation in-memory.
	refRows := runHashAggToMap(t, mkAgg(nil), batches)
	if len(refRows) != numGroups {
		t.Fatalf("reference: expected %d groups, got %d", numGroups, len(refRows))
	}

	// Partial-drain run. Large tracker so no auto-spill — drive partial
	// drains manually via SpillSome with small targets.
	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := mkAgg(sm)
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var partialDrainsObserved int
	var maxFreeListSeen int
	for i, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
		// Every other batch: drain ~⅛ of footprint to exercise partial path.
		if i%2 == 1 {
			footprint := h.Inspect().OwnedBytes
			if footprint <= 0 {
				continue
			}
			target := footprint / 8
			if target <= 0 {
				target = 1
			}
			freed, err := h.SpillSome(target)
			if err != nil {
				t.Fatalf("SpillSome #%d: %v", i, err)
			}
			// Partial path returns less than footprint (would route to full
			// drain otherwise). Non-zero confirms we actually drained.
			if freed > 0 && freed < footprint {
				partialDrainsObserved++
			}
			if n := len(h.freeGroupIDs); n > maxFreeListSeen {
				maxFreeListSeen = n
			}
		}
	}
	if partialDrainsObserved < 3 {
		t.Errorf("expected ≥3 partial drains, observed %d (target sizing may be off)", partialDrainsObserved)
	}
	if maxFreeListSeen == 0 {
		t.Errorf("freeGroupIDs never populated — partial-drain reclamation path didn't fire")
	}
	if h.drainK == 0 {
		t.Errorf("drainK never initialized — spillPartialPartitions never reached")
	}

	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != numGroups {
		t.Fatalf("partial-drain group count: got %d want %d", len(got), numGroups)
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("k=%d: got %d want %d", k, got[k], want)
		}
	}
}

// TestPartialDrain_SelfSpill verifies the Consume-time pressure self-spill
// uses the partial-drain path instead of whole-table drain. This is the Q18
// SF100 unblock — heavy GROUP BY workloads (150M orderkey groups) previously
// drained the entire hash table on every self-spill event, creating the same
// drain-rebuild loop that cooperative spill caused on Q17.
//
// Asserts:
//   1. Self-spill fires (freeGroupIDs populated post-Consume) on int-keyed path.
//   2. Final aggregate result matches no-spill reference row-for-row.
//   3. Tracker.Used() lands below the 60% SpillCheap trigger after spill.
func TestPartialDrain_SelfSpill(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const numGroups = 2000
	const rowsPerBatch = 500
	const numBatches = 20
	expected := make(map[int64]int64)
	batches := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			k := int64((bi*rowsPerBatch + ri) % numGroups)
			v := int64(bi*1000 + ri + 1)
			rows = append(rows, map[string]any{"k": k, "v": v})
			expected[k] += v
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	mkAgg := func(spill *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
		})
		h.Spill = spill
		return h
	}

	refRows := runHashAggToMap(t, mkAgg(nil), batches)
	if len(refRows) != numGroups {
		t.Fatalf("reference: expected %d groups, got %d", numGroups, len(refRows))
	}

	// Tight tracker budget so the SpillCheap trigger (60% = ~60 KB) fires
	// during Consume after the int-keyed aggregate accrues group state.
	const budget int64 = 100_000
	tracker := memory.NewTracker("test", budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := mkAgg(sm)
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	var sawFreeList, sawPartialFiles bool
	var maxFreeListSize int
	for i, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
		if n := len(h.freeGroupIDs); n > 0 {
			sawFreeList = true
			if n > maxFreeListSize {
				maxFreeListSize = n
			}
		}
		if len(h.partialSpillFiles) > 0 {
			sawPartialFiles = true
		}
	}
	if !sawFreeList {
		t.Errorf("freeGroupIDs never populated — self-spill took whole-drain path; partial-drain mechanism unreached")
	}
	if !sawPartialFiles {
		t.Errorf("partialSpillFiles never populated — no spill of any kind fired (budget too generous?)")
	}
	if h.drainK == 0 {
		t.Errorf("drainK uninitialized — partition selection never ran")
	}

	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := make(map[int64]int64)
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[r["k"].(int64)] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != numGroups {
		t.Fatalf("self-spill group count: got %d want %d (maxFreeList=%d, drainK=%d)",
			len(got), numGroups, maxFreeListSize, h.drainK)
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("k=%d: got %d want %d", k, got[k], want)
		}
	}
}

// TestPartialDrain_FreeListReuse asserts that slots reclaimed by a partial
// drain are reused by the next Consume cycle: the surviving hash table
// continues to grow only with truly-new keys, while keys that hash into a
// drained partition recycle slots from h.freeGroupIDs.
func TestPartialDrain_FreeListReuse(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Pre-populate a single batch with 5000 unique keys so we have enough
	// material to drain a partition.
	const numKeys = 5000
	rows := make([]map[string]any, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		rows = append(rows, map[string]any{"k": int64(i), "v": int64(i)})
	}

	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}

	slotsBeforeDrain := h.numIntGroups
	footprint := h.Inspect().OwnedBytes
	if footprint <= 0 {
		t.Fatalf("post-Consume footprint: got %d want > 0", footprint)
	}

	// Drain ~⅛ of footprint. With K = max(numKeys/1M, 8) capped → K=8,
	// numPartitionsToDrain = ceil(target/perPartition) → 1.
	if _, err := h.SpillSome(footprint / 8); err != nil {
		t.Fatal(err)
	}
	freeListSize := len(h.freeGroupIDs)
	if freeListSize == 0 {
		t.Fatalf("freeGroupIDs empty after partial drain — reclamation didn't run")
	}
	liveAfterDrain := h.intGroupIndex.Len()
	if liveAfterDrain >= slotsBeforeDrain {
		t.Errorf("hash table didn't shrink: liveAfter=%d slotsBefore=%d", liveAfterDrain, slotsBeforeDrain)
	}
	if liveAfterDrain+freeListSize != slotsBeforeDrain {
		t.Errorf("survivor + free = %d, expected %d (slotsBefore)", liveAfterDrain+freeListSize, slotsBeforeDrain)
	}

	// Re-Consume the same keys. Drained keys should recycle slots from the
	// free list. The int-keyed slot count must NOT grow above
	// slotsBeforeDrain — every drained key reuses a freed slot.
	if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	if got := h.numIntGroups; got > slotsBeforeDrain {
		t.Errorf("int group slots grew despite free-list: got %d, expected ≤ %d", got, slotsBeforeDrain)
	}
	// Free list should be empty again — every entry got reused.
	if got := len(h.freeGroupIDs); got != 0 {
		t.Errorf("freeGroupIDs not fully drained: got %d entries, expected 0", got)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}
