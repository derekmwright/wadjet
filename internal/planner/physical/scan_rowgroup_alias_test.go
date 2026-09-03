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
// A row group's buffer goes back to readBufPool the moment that row group has
// been decoded — not, as before, when the file's LAST row group has. So a
// buffer is recycled while OTHER row groups of the same file are still being
// decoded and while batches decoded from it are still travelling downstream.
// If any decoded value aliased that buffer, the next file to take it out of
// the pool would rewrite somebody's rows in place, and the query would return
// wrong values with no error anywhere.
//
// It does not alias — every fixed-width and bytes payload is copied into the
// vector's own arena — and `TestReadRowGroupNative_StagedNoAliasing` asserts
// that for the staged path. This asserts it for THIS path, at this
// granularity, which is the one where a recycle happens mid-file.

// scanAllHolding runs a whole scan through the rg workers and returns every
// batch, still held. Nothing releases them, which is the point: the buffers
// they were decoded from are back in the pool by the time this returns.
func scanAllHolding(t *testing.T, inner *scanSourceInner, slot *fileSlot, rgs int) []*batch.RecordBatch {
	t.Helper()
	var rowOff int64
	for i := 0; i < rgs; i++ {
		n := int64(200)
		inner.rgUnits = append(inner.rgUnits, rgUnit{slot: slot, rgIndex: i, rgRowOffset: rowOff, numRows: n})
		rowOff += n
	}
	inner.wg.Add(2)
	go inner.rgWorker(context.Background())
	go inner.rgWorker(context.Background())
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	var held []*batch.RecordBatch
	ctx := context.Background()
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		held = append(held, b)
	}
	return held
}

// fatRows makes a row group big enough to be POOLED. putReadBuf keeps only
// buffers of 64 KiB and up, so a fixture with small row groups would recycle
// nothing and the aliasing assertion below would be vacuous.
func fatRows(n, base int, fill byte) []map[string]any {
	const pad = 512
	rows := make([]map[string]any, n)
	for i := range rows {
		v := make([]byte, pad)
		for j := range v {
			v[j] = fill
		}
		// A distinct prefix per row, so a rewritten row is visible even when
		// the fill byte matches.
		copy(v, fmt.Sprintf("%c%08d", fill, base+i))
		rows[i] = map[string]any{"id": int64(base + i), "value": string(v)}
	}
	return rows
}

func TestABatchOutlivesItsRowGroupsBuffer(t *testing.T) {
	const rgs, rowsPerRG = 4, 200

	// File A, held. Every value is distinct so a rewritten byte shows up.
	sets := make([][]map[string]any, rgs)
	for i := range sets {
		sets[i] = fatRows(rowsPerRG, i*rowsPerRG, 'a')
	}
	catA, entryA := scanAccountingFixture(t, sets...)
	trackerA := memory.NewTracker("scan-a", 1<<30)
	innerA := &scanSourceInner{
		cat: catA, memTracker: trackerA,
		schema:  scanAccountingSchemaCols(),
		batchCh: make(chan *batch.RecordBatch, rgs+1),
		errCh:   make(chan error, 1),
	}
	slotA := rowGroupSlot(t, innerA, catA, entryA, rgs)
	if peak := peakRowGroupBytes(slotA.rgs.starts, slotA.rgs.ends); peak < 64*1024 {
		t.Fatalf("the fixture's largest row group is %d bytes, under putReadBuf's 64 KiB floor — "+
			"its buffers are never pooled, so nothing below could observe a recycle", peak)
	}
	held := scanAllHolding(t, innerA, slotA, rgs)
	if len(held) == 0 {
		t.Fatal("no batches")
	}

	// What A decoded, snapshotted as strings BEFORE anything else runs.
	want := make([][]string, len(held))
	for i, b := range held {
		want[i] = make([]string, b.Len)
		for r := 0; r < b.Len; r++ {
			want[i][r] = b.Columns[1].BytesData.StringValue(r)
		}
	}

	// File B: same shape and the same row-group byte sizes, DIFFERENT bytes.
	// Scanning it hands its row groups buffers out of the pool — including
	// the ones A's row groups were decoded from and released.
	otherSets := make([][]map[string]any, rgs)
	for i := range otherSets {
		otherSets[i] = fatRows(rowsPerRG, i*rowsPerRG, 'Z')
	}
	for pass := 0; pass < 3; pass++ { // several passes: sync.Pool is per-P
		catB, entryB := scanAccountingFixture(t, otherSets...)
		trackerB := memory.NewTracker("scan-b", 1<<30)
		innerB := &scanSourceInner{
			cat: catB, memTracker: trackerB,
			schema:  scanAccountingSchemaCols(),
			batchCh: make(chan *batch.RecordBatch, rgs+1),
			errCh:   make(chan error, 1),
		}
		slotB := rowGroupSlot(t, innerB, catB, entryB, rgs)
		for _, b := range scanAllHolding(t, innerB, slotB, rgs) {
			_ = b
		}
	}

	for i, b := range held {
		for r := 0; r < b.Len; r++ {
			if got := b.Columns[1].BytesData.StringValue(r); got != want[i][r] {
				t.Fatalf("batch %d row %d read %q, decoded as %q: a row group's buffer was "+
					"recycled while a batch decoded from it was still live",
					i, r, got, want[i][r])
			}
		}
	}
}

// TestARowGroupErrorMidDecodeLeavesNothingCharged: the decode of a row group
// fails (its bytes are not the file's), and the slot must still give back
// every buffer and every charge. An error path that leaks is a phantom
// reservation on the worker-lifetime tracker.
func TestARowGroupErrorMidDecodeLeavesNothingCharged(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(200, 0), testRows(200, 200), testRows(200, 400))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 3)
	inner.rgUnits = []rgUnit{{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1}, {slot: slot, rgIndex: 2}}

	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	// The loader's stream is now past row group 0. Ask for a row group it has
	// already passed, which is the shape a decode error takes here.
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
