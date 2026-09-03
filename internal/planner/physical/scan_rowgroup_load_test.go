package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// primeFooterCache decodes the file's footer under the identity the scan uses,
// which is what buildRGUnits' pruning pass does for every file the catalog has
// no persisted row-group metadata for. Row-group mode needs the footer to be
// there already: reading it from the object would be a second request per
// file, which is the decision this design keeps.
func primeFooterCache(t *testing.T, cat *catalog.Catalog, entry catalog.FileEntry) {
	t.Helper()
	ras, ok := cat.Store().(objstore.ReaderAtStore)
	if !ok {
		t.Fatal("fixture store has no ReaderAt")
	}
	ra, size, err := ras.GetReaderAt(context.Background(), cat.Bucket(), entry.Path)
	if err != nil {
		t.Fatalf("get reader: %v", err)
	}
	defer ra.Close()
	if _, err := parquet.OpenFileReaderMetadataCached(ra, size,
		footerCacheIdentity(cat, entry, entry.SizeBytes)); err != nil {
		t.Fatalf("prime footer cache: %v", err)
	}
}

// rowGroupSlot builds a lazy fileSlot over the fixture, primed the way
// buildRGUnits primes one, and asserts it took the row-group path.
func rowGroupSlot(t *testing.T, inner *scanSourceInner, cat *catalog.Catalog, entry catalog.FileEntry, numRGs int) *fileSlot {
	t.Helper()
	primeFooterCache(t, cat, entry)
	slot := &fileSlot{entry: entry}
	for i := 0; i < numRGs; i++ {
		slot.wantRG = append(slot.wantRG, i)
	}
	slot.rgRemaining.Store(int64(numRGs))
	before, _ := RowGroupLoadStats()
	if _, err := slot.ensureLoaded(inner, context.Background()); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if after, _ := RowGroupLoadStats(); after != before+1 {
		t.Fatal("the slot did not take the row-group path — every assertion below would be about the whole-file read")
	}
	return slot
}

// TestAScansFileBytesAreChargedAndReleasedPerRowGroup is #789's mechanism
// turned into an assertion. The whole-file read charges the file at first
// touch and releases it when the LAST row group has been decoded, so a query's
// admission floor carries the whole file for the length of the scan. Row-group
// mode charges a row group when its bytes land and releases it when that row
// group is done.
//
// Its contrast pair is TestAFileWithNoDecodedFooterKeepsTheWholeFileRead
// below, which asserts the OTHER behaviour on the same fixture: the whole-file
// read charges entry.SizeBytes before a single row group has been demanded and
// releases none of it until the last one is done. Both run on every build, so
// the difference between the two paths is asserted rather than described. With
// WADJET_SCAN_RG_BUFFERS=0 this test fails at its engagement check, which is
// the same statement from the other side.
func TestAScansFileBytesAreChargedAndReleasedPerRowGroup(t *testing.T) {
	cat, entry := scanAccountingFixture(t,
		testRows(400, 0), testRows(400, 400), testRows(400, 800), testRows(400, 1200))

	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 4)

	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d before any row group was demanded, want 0 — opening the file must not "+
			"charge bytes nobody has read (the file is %d bytes)", used, entry.SizeBytes)
	}

	// Demanding one row group charges that row group, not the file.
	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	oneRG := tracker.Used()
	t.Logf("one of four row groups resident: used=%d, file=%d bytes", oneRG, entry.SizeBytes)
	if oneRG <= 0 {
		t.Fatal("used = 0 with a row group resident — its bytes are not on the ledger")
	}
	if oneRG >= entry.SizeBytes {
		t.Fatalf("used = %d with one of four row groups resident, want well under the file's %d bytes",
			oneRG, entry.SizeBytes)
	}

	// Releasing it releases its charge — without waiting for the file's last.
	slot.releaseRG(inner, 0)
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after row group 0 was released, want 0: its bytes are gone, so the charge "+
			"must be too. The whole-file read released nothing here", used)
	}

	// And the rest behave the same, one at a time.
	for rg := 1; rg < 4; rg++ {
		if _, _, err := slot.rgs.RowGroupBytes(rg); err != nil {
			t.Fatalf("row group %d: %v", rg, err)
		}
		if used := tracker.Used(); used >= entry.SizeBytes {
			t.Fatalf("used = %d with row group %d resident, want under the file's %d bytes",
				used, rg, entry.SizeBytes)
		}
		slot.releaseRG(inner, rg)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after the last row group, want 0", used)
	}
}

// TestAScanNeverHoldsMoreThanItsBudgetOfFileBytes: with a budget too small for
// the file, the peak charge over a full scan stays near one row group. This is
// the property #789 needs — what a scan holds follows the budget, not how far
// the scheduler let it run ahead.
func TestAScanNeverHoldsMoreThanItsBudgetOfFileBytes(t *testing.T) {
	cat, entry := scanAccountingFixture(t,
		testRows(400, 0), testRows(400, 400), testRows(400, 800), testRows(400, 1200))

	// A budget under the file, over a row group.
	budget := entry.SizeBytes / 2
	tracker := memory.NewTracker("scan-test", budget)
	inner := &scanSourceInner{
		cat:        cat,
		memTracker: tracker,
		schema:     []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "value", Type: parquet.TypeString}},
		batchCh:    make(chan *batch.RecordBatch, 8),
		errCh:      make(chan error, 1),
	}
	slot := rowGroupSlot(t, inner, cat, entry, 4)
	var rowOff int64
	for i := 0; i < 4; i++ {
		inner.rgUnits = append(inner.rgUnits, rgUnit{slot: slot, rgIndex: i, rgRowOffset: rowOff, numRows: 400})
		rowOff += 400
	}

	inner.wg.Add(2)
	go inner.rgWorker(context.Background())
	go inner.rgWorker(context.Background())
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	ctx := context.Background()
	var rows int64
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		rows += int64(b.ActiveLen())
	}
	if rows != 1600 {
		t.Fatalf("rows = %d, want 1600", rows)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after the scan drained, want 0", used)
	}
	if peak := tracker.Peak(); peak >= entry.SizeBytes {
		t.Fatalf("peak = %d over a scan of a %d-byte file, want under the file: the scan held the "+
			"whole file at once", peak, entry.SizeBytes)
	}
}

// TestAFileWithNoDecodedFooterKeepsTheWholeFileRead: row-group mode needs the
// byte ranges the footer carries, and reading the footer from the object here
// would be a second request per file. Without a decoded footer the slot must
// fall back, not read one.
func TestAFileWithNoDecodedFooterKeepsTheWholeFileRead(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(200, 0), testRows(200, 200))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}

	slot := &fileSlot{entry: entry, wantRG: []int{0, 1}}
	slot.rgRemaining.Store(2)
	// No primeFooterCache: nothing in this process has decoded this file's
	// footer, which is the catalog-rgmeta case.
	_, whole := RowGroupLoadStats()
	if _, err := slot.ensureLoaded(inner, context.Background()); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if _, after := RowGroupLoadStats(); after != whole+1 {
		t.Fatal("a file with no decoded footer did not take the whole-file read")
	}
	if slot.rgs != nil {
		t.Fatal("row-group mode engaged without a decoded footer")
	}
	if used := tracker.Used(); used < entry.SizeBytes {
		t.Fatalf("used = %d on the whole-file path, want >= %d", used, entry.SizeBytes)
	}
	slot.releaseRG(inner, 0)
	slot.releaseRG(inner, 1)
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after the last row group, want 0", used)
	}
}

// TestARowGroupsBytesAreNotHandedOutAfterTheyAreReleased: the buffer goes back
// to readBufPool on release and may already hold another file's bytes, so a
// late demand must be an ERROR, never those bytes.
func TestARowGroupsBytesAreNotHandedOutAfterTheyAreReleased(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(200, 0), testRows(200, 200))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 2)

	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	slot.releaseRG(inner, 0)
	if _, _, err := slot.rgs.RowGroupBytes(0); err == nil {
		t.Fatal("a released row group's buffer was handed out again")
	}
	slot.releaseRG(inner, 1)
}

// TestAnAbandonedRowGroupScanLeavesNothingOnTheTracker: a scan torn down
// before its last row group (LIMIT, cancel, error) must release every buffer
// and charge it still holds. The charge lives on a worker-lifetime SHARED
// tracker, where a leak is a permanent phantom reservation.
func TestAnAbandonedRowGroupScanLeavesNothingOnTheTracker(t *testing.T) {
	cat, entry := scanAccountingFixture(t,
		testRows(200, 0), testRows(200, 200), testRows(200, 400))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 3)
	inner.rgUnits = []rgUnit{
		{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1}, {slot: slot, rgIndex: 2},
	}

	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	if _, _, err := slot.rgs.RowGroupBytes(1); err != nil {
		t.Fatalf("row group 1: %v", err)
	}
	slot.releaseRG(inner, 0) // one consumed, then the scan is abandoned
	if used := tracker.Used(); used <= 0 {
		t.Fatal("nothing charged with a row group still resident")
	}
	inner.drainSlotCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after drainSlotCharges, want 0 (phantom reservation)", used)
	}
}
