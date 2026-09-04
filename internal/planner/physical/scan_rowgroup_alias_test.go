package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The lifetime contract, at the granularity the row-group read introduced.
//
// A row group's buffer goes back to the pool the moment THAT row group has
// been decoded — not, as before, when the file's LAST row group has. So a
// buffer is recycled while OTHER row groups of the same file are still being
// decoded and while batches decoded from it are still travelling downstream.
// If any decoded value aliased that buffer, the next row group to take it out
// of the pool would rewrite somebody's rows in place, and the query would
// return wrong values with no error anywhere.
//
// It does not alias — every fixed-width and bytes payload is copied into the
// vector's own arena — and `TestReadRowGroupNative_StagedNoAliasing` asserts
// that for the staged path. This asserts it for THIS path, at the granularity
// where the recycle happens MID-FILE.
//
// Two things make the assertion able to fail, and an earlier version of this
// test had neither:
//
//   - The snapshot is taken AS EACH BATCH ARRIVES, not after the scan. A
//     snapshot taken at the end records whatever a mid-scan recycle already
//     did, and then compares it with itself.
//   - The recycle happens inside ONE scan source. Buffers are pooled per
//     source and per file shape, so a second source's scan can never be handed
//     the first's buffers; an arm built that way is vacuous by type.
//
// And PoisonReusedSlabs makes the recycle visible: every buffer handed back
// from the pool is overwritten first, so an aliased value becomes 0xEE rather
// than "whatever the next row group happened to put there".

// fatRows makes row groups big enough to be worth pooling and gives every row
// a distinct value, so one rewritten row is visible.
func fatRows(n, base int, fill byte) []map[string]any {
	const pad = 512
	rows := make([]map[string]any, n)
	for i := range rows {
		v := make([]byte, pad)
		for j := range v {
			v[j] = fill
		}
		copy(v, fmt.Sprintf("%c%08d", fill, base+i))
		rows[i] = map[string]any{"id": int64(base + i), "value": string(v)}
	}
	return rows
}

func TestABatchOutlivesItsRowGroupsBuffer(t *testing.T) {
	const rgs, rowsPerRG = 8, 150
	prevPoison := PoisonReusedSlabs(true)
	defer PoisonReusedSlabs(prevPoison)

	sets := make([][]map[string]any, rgs)
	for i := range sets {
		sets[i] = fatRows(rowsPerRG, i*rowsPerRG, 'a')
	}
	cat, entry := scanAccountingFixture(t, sets...)
	tracker := memory.NewTracker("scan-alias", 1<<30)
	inner := &scanSourceInner{
		cat: cat, memTracker: tracker,
		schema:  scanAccountingSchemaCols(),
		batchCh: make(chan *batch.RecordBatch, 2), // small: batches queue behind the consumer
		errCh:   make(chan error, 1),
	}
	slot := rowGroupSlot(t, inner, cat, entry, rgs)
	reusesBefore := RowGroupSlabReuses()

	var rowOff int64
	for i := 0; i < rgs; i++ {
		inner.rgUnits = append(inner.rgUnits, rgUnit{
			slot: slot, rgIndex: i, rgRowOffset: rowOff, numRows: rowsPerRG})
		rowOff += rowsPerRG
	}
	inner.wg.Add(2)
	go inner.rgWorker(context.Background())
	go inner.rgWorker(context.Background())
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	// Snapshot AS EACH BATCH ARRIVES, and hold the batch. Row groups later in
	// the file are still loading — into buffers this batch's row group just
	// returned to the pool, overwritten with 0xEE on the way out.
	var held []*batch.RecordBatch
	var want [][]string
	ctx := context.Background()
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		snap := make([]string, b.Len)
		for r := 0; r < b.Len; r++ {
			snap[r] = b.Columns[1].BytesData.StringValue(r)
		}
		held = append(held, b)
		want = append(want, snap)
	}
	if len(held) < rgs/2 {
		t.Fatalf("%d batches for %d row groups — too few to have overlapped a recycle", len(held), rgs)
	}
	if reuses := RowGroupSlabReuses() - reusesBefore; reuses == 0 {
		t.Fatal("this scan never took a buffer back out of the pool, so no batch was ever live " +
			"across a recycle and nothing below is being tested")
	}

	for i, b := range held {
		for r := 0; r < b.Len; r++ {
			got := b.Columns[1].BytesData.StringValue(r)
			if got != want[i][r] {
				t.Fatalf("batch %d row %d read %q, but decoded as %q: a row group's buffer was "+
					"recycled — and overwritten — while a batch decoded from it was still live",
					i, r, got, want[i][r])
			}
			if len(got) > 0 && got[0] == 0xEE {
				t.Fatalf("batch %d row %d is poison: it aliases a recycled row-group buffer", i, r)
			}
		}
	}
}

// TestAPoisonedSlabIsActuallyReused: the gate above proves nothing unless the
// pool really hands a buffer back during that scan. This is its precondition,
// asserted rather than assumed — with poisoning on, a reused buffer arrives
// full of 0xEE, so a scan that never reuses one would leave this at zero.
func TestAPoisonedSlabIsActuallyReused(t *testing.T) {
	prevPoison := PoisonReusedSlabs(true)
	defer PoisonReusedSlabs(prevPoison)
	inner := &scanSourceInner{}
	const n = 4096
	first := inner.getSlab(n)
	for i := range first[:cap(first)] {
		first[:cap(first)][i] = 0x01
	}
	inner.putSlab(first)
	again := inner.getSlab(n)
	if cap(again) != cap(first) {
		t.Fatalf("the pool did not hand the buffer back (cap %d then %d)", cap(first), cap(again))
	}
	for i, c := range again[:cap(again)] {
		if c != 0xEE {
			t.Fatalf("byte %d of a reused buffer is %#x, want the poison — PoisonReusedSlabs is not "+
				"reaching the reuse path, so the aliasing gate cannot fail", i, c)
		}
	}
}

// TestARowGroupErrorMidDecodeLeavesNothingCharged: a row group demanded after
// its release — the shape a decode error takes at this seam — must be an error,
// and the slot must still give back every buffer and every charge. A leaking
// error path is a phantom reservation on the worker-lifetime tracker.
func TestARowGroupErrorMidDecodeLeavesNothingCharged(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(200, 0), testRows(200, 200), testRows(200, 400))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 3)
	inner.rgUnits = []rgUnit{{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1}, {slot: slot, rgIndex: 2}}

	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	slot.releaseRG(inner, 0)
	if _, _, err := slot.rgs.RowGroupBytes(0); err == nil {
		t.Fatal("a released row group was served again")
	}
	inner.drainSlotCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after an error and a drain, want 0", used)
	}
}

// TestACancelledScanMidFileLeavesNothingCharged: the context dies with the
// stream open and row groups resident. Every buffer and charge must come back.
func TestACancelledScanMidFileLeavesNothingCharged(t *testing.T) {
	cat, entry := scanAccountingFixture(t,
		testRows(200, 0), testRows(200, 200), testRows(200, 400), testRows(200, 600))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}

	ctx, cancel := context.WithCancel(context.Background())
	primeFooterCache(t, cat, entry)
	slot := &fileSlot{entry: entry, wantRG: []int{0, 1, 2, 3}}
	slot.rgRemaining.Store(4)
	if _, err := slot.ensureLoaded(inner, ctx); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if slot.rgs == nil {
		t.Fatal("not in row-group mode")
	}
	inner.rgUnits = []rgUnit{
		{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1},
		{slot: slot, rgIndex: 2}, {slot: slot, rgIndex: 3},
	}
	for rg := 0; rg < 2; rg++ {
		if _, _, err := slot.rgs.RowGroupBytes(rg); err != nil {
			t.Fatalf("row group %d: %v", rg, err)
		}
	}
	if used := tracker.Used(); used <= 0 {
		t.Fatal("two row groups resident and nothing charged")
	}
	cancel() // the scan dies mid-file, two row groups resident, stream open

	inner.drainSlotCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after a cancelled scan was drained, want 0", used)
	}
	if n := inner.residentSlabs.Load(); n != 0 {
		t.Fatalf("%d row-group buffers still counted resident after the drain", n)
	}
}

// scanAccountingSchemaCols is the fixture's schema, which the scan source
// needs to decode with.
func scanAccountingSchemaCols() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}
}
