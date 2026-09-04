package physical

import (
	"bytes"
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

// scanAccountingTwoFiles puts TWO files with different row-group SIZES in one
// table, which is what a table looks like after a compaction lands beside a
// fresh ingest. One scan source reads both.
func scanAccountingTwoFiles(t *testing.T, big, small [][]map[string]any) (*catalog.Catalog, catalog.FileEntry, catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}}
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	put := func(name string, sets [][]map[string]any) catalog.FileEntry {
		data := writeTestParquetMultiRG(t, schema, sets...)
		path := "tables/items/" + name
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
		var rows int64
		for _, rs := range sets {
			rows += int64(len(rs))
		}
		e := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: rows}
		if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{e}); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		return e
	}
	return cat, put("chunk_big.parquet", big), put("chunk_small.parquet", small)
}

// TestOneSourceWithTwoRowGroupSizesChargesEachRowGroupItsOwnBytes.
//
// A scan source reads a TABLE, not a file, and a table's files do not share a
// row-group size — a compacted file beside a freshly ingested one is the
// ordinary case. A buffer pool over one source with a single "big enough" rule
// therefore hands the small file's row group a buffer the big file left
// behind, and cap() is what this path charges: measured at 105,900 bytes for a
// 332-byte row group, 319x the row group and 53x the whole small file. That is
// a bigger charge for one row group than the whole-file read it replaced made
// for the entire file.
//
// The pool is bucketed by the FILE's largest row group, out of the footer, so
// a row group is charged at most its own file's largest — which is what
// ADR-0006's producer row 2 says. This asserts it with the big file drained
// FIRST, so its buffers are in the source's pool when the small file loads.
func TestOneSourceWithTwoRowGroupSizesChargesEachRowGroupItsOwnBytes(t *testing.T) {
	bigSets := make([][]map[string]any, 4)
	for i := range bigSets {
		bigSets[i] = fatRows(200, i*200, 'b') // ~100 KB of row group
	}
	smallSets := make([][]map[string]any, 4)
	for i := range smallSets {
		rows := make([]map[string]any, 4)
		for r := range rows {
			rows[r] = map[string]any{"id": int64(i*4 + r), "value": "s"}
		}
		smallSets[i] = rows // a few hundred bytes of row group
	}
	cat, bigEntry, smallEntry := scanAccountingTwoFiles(t, bigSets, smallSets)

	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}

	// Drain the big file first: its buffers end up in the source's pool.
	bigSlot := rowGroupSlot(t, inner, cat, bigEntry, 4)
	for rg := 0; rg < 4; rg++ {
		if _, _, err := bigSlot.rgs.RowGroupBytes(rg); err != nil {
			t.Fatalf("big row group %d: %v", rg, err)
		}
		bigSlot.releaseRG(inner, rg)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after the big file drained, want 0", used)
	}

	// Now the small file, through the SAME scan source and the same pool.
	smallSlot := rowGroupSlot(t, inner, cat, smallEntry, 4)
	smallPeak := peakRowGroupBytes(smallSlot.rgs.starts, smallSlot.rgs.ends)
	if _, _, err := smallSlot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("small row group 0: %v", err)
	}
	charge := tracker.Used()
	t.Logf("small file: %d bytes, largest row group %d, charge for one resident row group %d",
		smallEntry.SizeBytes, smallPeak, charge)

	if charge > smallPeak {
		t.Fatalf("one row group of a %d-byte file whose largest row group is %d bytes is charged "+
			"%d — %.0fx the row group. A row group must be charged at most its own file's largest, "+
			"which is what ADR-0006's producer row 2 says; this is another file's buffer",
			smallEntry.SizeBytes, smallPeak, charge, float64(charge)/float64(smallPeak))
	}
	if charge > smallEntry.SizeBytes {
		t.Fatalf("one row group is charged %d for a %d-byte file — more than the whole-file read "+
			"this replaced charged for the entire file", charge, smallEntry.SizeBytes)
	}
	for rg := 0; rg < 4; rg++ {
		smallSlot.releaseRG(inner, rg)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after both files drained, want 0", used)
	}
}

// TestARowGroupIsHeldInABufferAtMostTwiceItsSize is the slab pool's stated
// invariant, and it is stated rather than preferred because every wrong pool
// this arc tried broke it at one end or the other: the process-wide pool held
// a 100 KiB row group in whatever the largest file left behind; the parquet
// chunk pool's 64 KiB FLOOR held a 5 KiB row group in 64 KiB; a bucket keyed
// on the file's exact largest row group held nothing twice, because
// compression makes that number different for every file.
//
// The bound has no floor and no chosen number: the class is the row group's
// own size rounded up to a power of two, so the buffer is never more than
// twice what the row group needs, at any size.
func TestARowGroupIsHeldInABufferAtMostTwiceItsSize(t *testing.T) {
	inner := &scanSourceInner{}
	for _, n := range []int{1, 2, 3, 100, 332, 4095, 4096, 4097, 82_000, 1 << 20, (1 << 20) + 1} {
		buf := inner.getSlab(n)
		if len(buf) != n {
			t.Fatalf("getSlab(%d) has len %d", n, len(buf))
		}
		if cap(buf) < n {
			t.Fatalf("getSlab(%d) has cap %d, which cannot hold it", n, cap(buf))
		}
		if cap(buf) > 2*n {
			t.Fatalf("a %d-byte row group is held in a %d-byte buffer — %.1fx. The bound is twice "+
				"the row group's own byte range, with no floor; a pool that exceeds it is charging "+
				"the budget for a shape that is not this row group's",
				n, cap(buf), float64(cap(buf))/float64(n))
		}
		inner.putSlab(buf)
	}
}

// TestSlabsAreReusedAcrossFilesOfSimilarShape: the bound above is worth
// nothing if it costs the reuse. Two files whose row groups differ by a few
// percent — which is what compression does to one table's files — must share a
// bucket, or every row group allocates. Keying the bucket on the file's EXACT
// largest row group did exactly that and cost +29.2% heap over TPC-H SF1.
//
// The property is "a row group no larger than one this scan has already read,
// in its own size class, is served from the pool". A LARGER one allocates once
// and then serves the rest, which is the pool converging on the biggest row
// group the scan sees; one in a SMALLER class allocates rather than being held
// in a buffer more than twice its size, which is the bound above. Buffers are
// allocated at the row group's own size, not the class's, because rounding
// every fresh allocation up to a power of two measured +10.9% suite heap.
func TestSlabsAreReusedAcrossFilesOfSimilarShape(t *testing.T) {
	inner := &scanSourceInner{}
	// One file's row group, then another file's, a few percent smaller: same
	// class, so the second is served from the first's buffer.
	inner.putSlab(inner.getSlab(100_000))
	before := RowGroupSlabReuses()
	for _, n := range []int{97_400, 99_800, 70_000, 66_000} {
		inner.putSlab(inner.getSlab(n))
	}
	if got := RowGroupSlabReuses() - before; got != 4 {
		t.Fatalf("%d of 4 row groups no bigger than one already read were served from the pool — "+
			"the bucket key is discriminating shapes that should share a buffer", got)
	}
}
