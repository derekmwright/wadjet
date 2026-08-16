package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// fakeProbe is a hand-driven GuardProbe.
type fakeProbe struct {
	bloom []uint64
	mask  uint64
	ok    bool
	done  bool
}

func (p *fakeProbe) TryResolve() ([]uint64, uint64, bool) {
	if p.done && p.ok {
		return p.bloom, p.mask, true
	}
	return nil, 0, false
}
func (p *fakeProbe) Done() bool { return p.done }
func (p *fakeProbe) Wait(ctx context.Context) ([]uint64, uint64, bool) {
	if p.done && p.ok {
		return p.bloom, p.mask, true
	}
	return nil, 0, false
}

func guardBloomOf(keys ...int64) ([]uint64, uint64) {
	bloom, mask := NewBloomSized(len(keys))
	for _, k := range keys {
		h := BloomHashInt(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	return bloom, mask
}

// twoColBatch builds a batch with int64 columns "sk" (emit key) and "nk"
// (guard column). nullNK marks rows whose nk is NULL.
func twoColBatch(t *testing.T, sk, nk []int64, nullNK ...int) *batch.RecordBatch {
	t.Helper()
	if len(sk) != len(nk) {
		t.Fatal("bad fixture")
	}
	b := &batch.RecordBatch{
		Schema: []parquet.Column{
			{Name: "sk", Type: parquet.TypeInt64},
			{Name: "nk", Type: parquet.TypeInt64},
		},
		Len: len(sk),
	}
	vs := batch.NewVector(parquet.TypeInt64, len(sk))
	vs.Int64Data = append(vs.Int64Data[:0], sk...)
	vn := batch.NewVector(parquet.TypeInt64, len(nk))
	vn.Int64Data = append(vn.Int64Data[:0], nk...)
	for i := range sk {
		vs.Nulls.SetValid(i)
		vn.Nulls.SetValid(i)
	}
	for _, i := range nullNK {
		vn.Nulls.SetNull(i)
	}
	b.Columns = []*batch.Vector{vs, vn}
	return b
}

// snapshotKeys reports which of the given keys the snapshot bloom contains.
func snapshotContains(t *testing.T, op *DynamicFilterEmitOp, key int64) bool {
	t.Helper()
	snap := op.Snapshot()
	return bloomContains(snap.Bloom, snap.BloomMask, bloomHashInt(key))
}

// Guard pending → rows buffer; guard settles mid-scan → buffer retro-
// filters (non-matching dropped) and later batches insert directly.
func TestGuardedEmitRetroFilterMidScan(t *testing.T) {
	op := NewDynamicFilterEmitOp("f-emit", "sk", "int64", 1024)
	probe := &fakeProbe{}
	op.AddGuard("f-guard", "nk", probe)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Batch 1 arrives while the guard is pending: sk 10 (nk 1, in guard),
	// sk 20 (nk 2, NOT in guard).
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{10, 20}, []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	if op.rowCount != 0 {
		t.Fatalf("buffered rows must not touch accumulators, rowCount=%d", op.rowCount)
	}
	// Guard settles: bloom contains nk=1 only.
	probe.bloom, probe.mask = guardBloomOf(1)
	probe.ok, probe.done = true, true
	// Batch 2: sk 30 (nk 1) — post-settle, but the SOURCE would have
	// filtered it; the emit op inserts whatever it sees now.
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{30}, []int64{1})); err != nil {
		t.Fatal(err)
	}
	op.FinalizeGuards(context.Background())
	if !snapshotContains(t, op, 10) {
		t.Fatal("guard-passing buffered key 10 missing from bloom")
	}
	if snapshotContains(t, op, 20) {
		t.Fatal("guard-dropped key 20 leaked into bloom")
	}
	if !snapshotContains(t, op, 30) {
		t.Fatal("post-settle direct key 30 missing")
	}
	snap := op.Snapshot()
	if snap.RowCount != 2 {
		t.Fatalf("rowCount must count survivors only, got %d", snap.RowCount)
	}
	if snap.Min != 10 || snap.Max != 30 {
		t.Fatalf("range must reflect survivors: [%d,%d]", snap.Min, snap.Max)
	}
	_, buffered, dropped, overflowed, _ := op.GuardStats()
	if buffered != 2 || dropped != 1 || overflowed {
		t.Fatalf("stats: buffered=%d dropped=%d overflowed=%v", buffered, dropped, overflowed)
	}
}

// Guard resolves only at finalize: buffer flushes there, filtered.
func TestGuardedEmitFinalizeFlush(t *testing.T) {
	op := NewDynamicFilterEmitOp("f-emit", "sk", "int64", 1024)
	probe := &fakeProbe{}
	op.AddGuard("f-guard", "nk", probe)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{10, 20}, []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	probe.bloom, probe.mask = guardBloomOf(2)
	probe.ok, probe.done = true, true
	op.FinalizeGuards(context.Background())
	if snapshotContains(t, op, 10) || !snapshotContains(t, op, 20) {
		t.Fatal("finalize flush filtered wrong rows")
	}
}

// Guard terminated without a bloom (withheld filter): every buffered row
// passes — matches the consume side's no-filter degradation.
func TestGuardedEmitMissingGuardPassesAll(t *testing.T) {
	op := NewDynamicFilterEmitOp("f-emit", "sk", "int64", 1024)
	probe := &fakeProbe{done: true} // done, no bloom
	op.AddGuard("f-guard", "nk", probe)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{10, 20}, []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	op.FinalizeGuards(context.Background())
	if !snapshotContains(t, op, 10) || !snapshotContains(t, op, 20) {
		t.Fatal("missing guard must pass every buffered key")
	}
}

// Null guard value: dropped when the guard bloom resolved (BloomFilterOp
// parity — null keys never pass a bloom).
func TestGuardedEmitNullGuardValueDropped(t *testing.T) {
	op := NewDynamicFilterEmitOp("f-emit", "sk", "int64", 1024)
	probe := &fakeProbe{}
	op.AddGuard("f-guard", "nk", probe)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{10, 20}, []int64{1, 1}, 1)); err != nil {
		t.Fatal(err)
	}
	probe.bloom, probe.mask = guardBloomOf(1)
	probe.ok, probe.done = true, true
	op.FinalizeGuards(context.Background())
	if !snapshotContains(t, op, 10) {
		t.Fatal("valid-guard key 10 missing")
	}
	if snapshotContains(t, op, 20) {
		t.Fatal("null-guard key 20 must be dropped when the bloom resolved")
	}
}

// Buffer overflow degrades to unguarded inserts: all keys land in the
// bloom (wider, drop-only correct) and the overflow flag reports it.
func TestGuardedEmitOverflowDegradesUnguarded(t *testing.T) {
	op := NewDynamicFilterEmitOp("f-emit", "sk", "int64", 1024)
	probe := &fakeProbe{}
	op.AddGuard("f-guard", "nk", probe)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Pre-fill the buffer to cap-1 so the next 2-row batch trips the
	// upfront capacity check (no test hook needed for the real cap).
	op.bufKeys = make([]int64, emitGuardBufferCap-1)
	g := op.guards[0]
	g.vals = make([]int64, emitGuardBufferCap-1)
	g.nulls = make([]bool, emitGuardBufferCap-1)
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{10, 20}, []int64{1, 2})); err != nil {
		t.Fatal(err)
	}
	if _, _, _, overflowed, _ := op.GuardStats(); !overflowed {
		t.Fatal("cap breach must set overflowed")
	}
	// The tripping batch's keys must NOT be lost — they insert directly.
	if !snapshotContains(t, op, 10) || !snapshotContains(t, op, 20) {
		t.Fatal("overflow batch keys lost — would falsely reject rows downstream")
	}
	// Post-overflow batches insert directly, unguarded.
	if _, err := op.Execute(context.Background(), twoColBatch(t, []int64{30}, []int64{2})); err != nil {
		t.Fatal(err)
	}
	if !snapshotContains(t, op, 30) {
		t.Fatal("post-overflow keys must insert directly")
	}
	op.FinalizeGuards(context.Background())
}
