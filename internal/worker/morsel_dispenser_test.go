package worker

import (
	"context"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// stubBatchSource yields a fixed slice of batches, then EOF.
type stubBatchSource struct {
	batches []*batch.RecordBatch
	idx     int
}

func (s *stubBatchSource) Init(context.Context) error { return nil }
func (s *stubBatchSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}
func (s *stubBatchSource) Close() error { return nil }

// makeMorselTestBatch builds an (id int64, name string) batch with
// id = base..base+n-1 and name = "r<id>".
func makeMorselTestBatch(tb testing.TB, base, n int) *batch.RecordBatch {
	tb.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"id":   int64(base + i),
			"name": "r" + strconv.Itoa(base+i),
		}
	}
	return batch.FromRows(schema, rows)
}

// collectMorsels runs the dispenser producer over src and drains every
// morsel into a slice WITHOUT retiring them; the caller retires.
func collectMorsels(tb testing.TB, d *morselDispenser, src *stubBatchSource) []morsel {
	tb.Helper()
	done := make(chan error, 1)
	go func() { done <- d.run(context.Background(), src) }()
	var got []morsel
	for m := range d.ch {
		got = append(got, m)
	}
	if err := <-done; err != nil {
		tb.Fatalf("dispenser run: %v", err)
	}
	return got
}

// activeIDs extracts the id column values of a batch's active rows.
func activeIDs(b *batch.RecordBatch) []int64 {
	idCol := b.ColumnByName("id")
	out := make([]int64, 0, b.ActiveLen())
	for i := 0; i < b.ActiveLen(); i++ {
		row := i
		if b.Sel != nil {
			row = int(b.Sel[i])
		}
		out = append(out, idCol.Int64Data[row])
	}
	return out
}


// newSplitTestDispenser builds a dispenser whose bytes gate is disabled
// (budget 0 → splitMinCost 0) so tests with small in-memory parents still
// exercise the view-splitting machinery. admit() always admits when nothing
// is in flight, so a zero budget only affects the gate.
func newSplitTestDispenser(k int, split bool) *morselDispenser {
	d := newMorselDispenser(k, split)
	d.budget = 0
	return d
}

func TestMorselDispenser_SplitViews(t *testing.T) {
	const n = 2*batch.DefaultBatchSize + 100
	parent := makeMorselTestBatch(t, 0, n)
	d := newSplitTestDispenser(2, true)
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})

	if len(got) != 3 {
		t.Fatalf("morsels = %d, want 3", len(got))
	}
	var ids []int64
	for _, m := range got {
		if m.b.ActiveLen() > batch.DefaultBatchSize {
			t.Fatalf("view ActiveLen = %d, want <= %d", m.b.ActiveLen(), batch.DefaultBatchSize)
		}
		// Zero-copy: views share the parent's vectors, not copies.
		if m.b.Columns[0] != parent.Columns[0] {
			t.Fatal("view column vector is not the parent's (copied, not zero-copy)")
		}
		// A view's Sel must be capped at its own region so an append can
		// never spill into a sibling's slice of the shared backing array.
		if cap(m.b.Sel) != len(m.b.Sel) {
			t.Fatalf("view Sel cap = %d, len = %d — must be three-index capped", cap(m.b.Sel), len(m.b.Sel))
		}
		ids = append(ids, activeIDs(m.b)...)
	}
	if len(ids) != n {
		t.Fatalf("total active rows across views = %d, want %d", len(ids), n)
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("row %d: id = %d, want %d (views must cover the parent exactly, in order)", i, id, i)
		}
	}

	// Refcounted retirement: budget bytes release only when the LAST
	// sibling view retires.
	cost := parent.MemBytes()
	if got := d.inFlight.Load(); got != cost {
		t.Fatalf("inFlight before retire = %d, want %d", got, cost)
	}
	got[0].retire()
	got[1].retire()
	if got := d.inFlight.Load(); got != cost {
		t.Fatalf("inFlight after partial retire = %d, want %d (parent must stay charged)", got, cost)
	}
	got[2].retire()
	if got := d.inFlight.Load(); got != 0 {
		t.Fatalf("inFlight after full retire = %d, want 0", got)
	}
}

func TestMorselDispenser_ParentSelRespected(t *testing.T) {
	const n = 3 * batch.DefaultBatchSize
	parent := makeMorselTestBatch(t, 0, n)
	// Parent arrives with a selection already applied (even ids only).
	sel := make([]uint32, 0, n/2)
	for i := 0; i < n; i += 2 {
		sel = append(sel, uint32(i))
	}
	parent.Sel = sel

	d := newSplitTestDispenser(2, true)
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})

	var ids []int64
	for _, m := range got {
		ids = append(ids, activeIDs(m.b)...)
		m.retire()
	}
	if len(ids) != n/2 {
		t.Fatalf("active rows = %d, want %d", len(ids), n/2)
	}
	for i, id := range ids {
		if id != int64(2*i) {
			t.Fatalf("row %d: id = %d, want %d (views must honor the parent's Sel)", i, id, 2*i)
		}
	}
}

func TestMorselDispenser_NoSplitPassthrough(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 3*batch.DefaultBatchSize)
	d := newMorselDispenser(2, false)
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})
	if len(got) != 1 {
		t.Fatalf("morsels = %d, want 1 (split disabled)", len(got))
	}
	if got[0].b != parent {
		t.Fatal("no-split morsel must carry the original batch")
	}
	got[0].retire()
	if got := d.inFlight.Load(); got != 0 {
		t.Fatalf("inFlight after retire = %d, want 0", got)
	}
}

func TestMorselDispenser_SmallBatchNotSplit(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, batch.DefaultBatchSize)
	d := newMorselDispenser(2, true)
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})
	if len(got) != 1 {
		t.Fatalf("morsels = %d, want 1 (at-threshold batch must pass through)", len(got))
	}
	if got[0].b != parent {
		t.Fatal("unsplit morsel must carry the original batch")
	}
	got[0].retire()
}

// TestMorselDispenser_ByteBudgetBlocksProducer proves the budget bounds
// in-flight bytes: with budget = one batch's cost, the producer cannot admit
// batch 2 until batch 1 fully retires, so peak in-flight equals one batch,
// never two.
func TestMorselDispenser_ByteBudgetBlocksProducer(t *testing.T) {
	b1 := makeMorselTestBatch(t, 0, 100)
	b2 := makeMorselTestBatch(t, 100, 100)
	cost := b1.MemBytes()

	d := newMorselDispenser(1, true)
	d.budget = cost // exactly one batch fits

	done := make(chan error, 1)
	go func() { done <- d.run(context.Background(), &stubBatchSource{batches: []*batch.RecordBatch{b1, b2}}) }()

	var seen []morsel
	for m := range d.ch {
		seen = append(seen, m)
		m.retire() // retire immediately so the producer can admit the next
	}
	if err := <-done; err != nil {
		t.Fatalf("dispenser run: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("morsels = %d, want 2", len(seen))
	}
	if peak := d.peakInFlight.Load(); peak > cost {
		t.Fatalf("peak in-flight = %d, want <= %d (budget must serialize admissions)", peak, cost)
	}
}

// TestMorselDispenser_OversizeAdmitted proves a batch larger than the whole
// budget is admitted when nothing is in flight — degrade, don't deadlock.
func TestMorselDispenser_OversizeAdmitted(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 100)
	d := newMorselDispenser(1, true)
	d.budget = 1

	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})
	if len(got) != 1 {
		t.Fatalf("morsels = %d, want 1", len(got))
	}
	got[0].retire()
	if got := d.inFlight.Load(); got != 0 {
		t.Fatalf("inFlight after retire = %d, want 0", got)
	}
}

// TestMorselDispenser_SiblingSelIsolation verifies the audited op contract
// views rely on: reassigning one view's Sel (what exec.Filter and
// HashJoinProbe do) and writing into a view's own Sel region must not change
// a sibling's active rows.
func TestMorselDispenser_SiblingSelIsolation(t *testing.T) {
	const n = 2 * batch.DefaultBatchSize
	parent := makeMorselTestBatch(t, 0, n)
	d := newSplitTestDispenser(2, true)
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})
	if len(got) != 2 {
		t.Fatalf("morsels = %d, want 2", len(got))
	}
	wantSibling := append([]int64(nil), activeIDs(got[1].b)...)

	// Simulate a filter on view 0: in-place compaction into its own Sel
	// region (the KernelFilter shape) followed by pointer reassignment (the
	// exec.Filter shape).
	v0 := got[0].b
	for i := 0; i < 10; i++ {
		v0.Sel[i] = v0.Sel[i*2]
	}
	v0.Sel = v0.Sel[:10]

	if gotSibling := activeIDs(got[1].b); len(gotSibling) != len(wantSibling) {
		t.Fatalf("sibling active rows changed: %d, want %d", len(gotSibling), len(wantSibling))
	} else {
		for i := range wantSibling {
			if gotSibling[i] != wantSibling[i] {
				t.Fatalf("sibling row %d changed after view-0 Sel mutation: %d, want %d", i, gotSibling[i], wantSibling[i])
			}
		}
	}
	got[0].retire()
	got[1].retire()
}

// TestMorselDispenser_AdaptiveViewSize: the split targets ~4 views per
// consumer (with a DefaultBatchSize floor and 64k cap), not unconditional
// 2048-row views — v1.5's fixed-size split put ~500 channel handoffs per
// row group on the critical path (SF10 suite +15%).
func TestMorselDispenser_AdaptiveViewSize(t *testing.T) {
	cases := []struct {
		k, n, want int
	}{
		{2, 4196, batch.DefaultBatchSize},          // small parent → floor
		{8, 1_000_000, 31250},                      // 1M rows, k=8 → n/32
		{4, 40_000, 2500},                          // 40k rows, k=4 → n/16
		{2, 2_000_000, morselMaxViewRows},          // huge parent → cap
		{8, 8 * batch.DefaultBatchSize, batch.DefaultBatchSize}, // exactly 4·k floor-sized views
	}
	for _, c := range cases {
		d := newMorselDispenser(c.k, true)
		if got := d.viewRowsFor(c.n); got != c.want {
			t.Errorf("viewRowsFor(n=%d, k=%d) = %d, want %d", c.n, c.k, got, c.want)
		}
	}
}

// TestMorselDispenser_AdaptiveSplitCounts: dispatch produces ceil(n/viewRows)
// views covering the parent exactly.
func TestMorselDispenser_AdaptiveSplitCounts(t *testing.T) {
	const n = 40_000
	parent := makeMorselTestBatch(t, 0, n)
	d := newSplitTestDispenser(4, true) // viewRows = 2500 → 16 views
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{parent}})
	if len(got) != 16 {
		t.Fatalf("morsels = %d, want 16", len(got))
	}
	var ids []int64
	for _, m := range got {
		ids = append(ids, activeIDs(m.b)...)
		m.retire()
	}
	if len(ids) != n {
		t.Fatalf("total rows = %d, want %d", len(ids), n)
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("row %d: id = %d, want %d", i, id, i)
		}
	}
	if got := d.inFlight.Load(); got != 0 {
		t.Fatalf("inFlight after retire = %d, want 0", got)
	}
}

// TestMorselDispenser_BytesGate: parents small relative to the byte budget
// pass through unsplit (2k of them fit in flight — splitting is pure
// overhead, measured +10-45% on SF10 join fragments); parents above
// budget/(2k) split.
func TestMorselDispenser_BytesGate(t *testing.T) {
	const n = 3 * batch.DefaultBatchSize
	small := makeMorselTestBatch(t, 0, n)
	cost := small.MemBytes()

	// Gate open: parent cost ≤ budget/(2k) → no split despite n > viewRows.
	d := newMorselDispenser(2, true)
	d.budget = cost * 4 // splitMinCost = cost*4/(2*2) = cost ≥ cost → unsplit
	got := collectMorsels(t, d, &stubBatchSource{batches: []*batch.RecordBatch{small}})
	if len(got) != 1 {
		t.Fatalf("morsels = %d, want 1 (small parent must not split)", len(got))
	}
	if got[0].b != small {
		t.Fatal("gated morsel must carry the original batch")
	}
	got[0].retire()

	// Gate closed: same parent with a tight budget splits.
	big := makeMorselTestBatch(t, 0, n)
	d2 := newMorselDispenser(2, true)
	d2.budget = cost // splitMinCost = cost/4 < cost → split
	got2 := collectMorsels(t, d2, &stubBatchSource{batches: []*batch.RecordBatch{big}})
	if len(got2) != 3 {
		t.Fatalf("morsels = %d, want 3 (oversized parent must split)", len(got2))
	}
	for _, m := range got2 {
		m.retire()
	}
}
