package physical

import (
	"bytes"
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// scanAccountingFixture builds a MemStore-backed catalog holding one parquet
// file with the given row sets (one row group each) and returns the catalog
// and the file's entry.
func scanAccountingFixture(t *testing.T, rowSets ...[]map[string]any) (*catalog.Catalog, catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "value", Type: parquet.TypeString},
		},
	}
	data := writeTestParquetMultiRG(t, schema, rowSets...)

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	path := "tables/items/chunk_001.parquet"
	if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("put file: %v", err)
	}
	var rows int64
	for _, rs := range rowSets {
		rows += int64(len(rs))
	}
	entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: rows}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{entry}); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat, entry
}

func testRows(n, base int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(base + i), "value": "v"}
	}
	return rows
}

// TestScanAccounting_FileChargeLifecycle exercises the reservation made by
// fileSlot.ensureLoaded: the file's bytes must be charged to the tracker
// while the buffer is held and fully released when the last row group is
// consumed.
func TestScanAccounting_FileChargeLifecycle(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(10, 0), testRows(10, 10))

	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}

	slot := &fileSlot{entry: entry}
	slot.rgRemaining.Store(2)

	if _, err := slot.ensureLoaded(inner, context.Background()); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if used := tracker.Used(); used < entry.SizeBytes {
		t.Fatalf("used = %d while file held, want >= %d", used, entry.SizeBytes)
	}

	slot.releaseRG(inner) // rg 1 of 2 — buffer still held
	if used := tracker.Used(); used < entry.SizeBytes {
		t.Fatalf("used = %d after first releaseRG, want >= %d (buffer still held)", used, entry.SizeBytes)
	}
	slot.releaseRG(inner) // last rg — buffer and charge released
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after last releaseRG, want 0", used)
	}
}

// TestScanAccounting_FileChargeReleasedOnGetError verifies the reservation
// does not leak when the object fetch fails.
func TestScanAccounting_FileChargeReleasedOnGetError(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(10, 0))
	entry.Path = "tables/items/missing.parquet" // force a Get error

	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := &fileSlot{entry: entry}
	slot.rgRemaining.Store(1)

	if _, err := slot.ensureLoaded(inner, context.Background()); err == nil {
		t.Fatal("expected error for missing object")
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after failed load, want 0", used)
	}
}

// runRGWorkerScan starts n rgWorkers over preloaded slots for the given
// parquet bytes and returns the inner plus the per-file FileReader.
func runRGWorkerScan(t *testing.T, tracker *memory.Tracker, deleteAll bool, rowSets ...[]map[string]any) (*scanSourceInner, int64) {
	t.Helper()
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "value", Type: parquet.TypeString},
		},
	}
	data := writeTestParquetMultiRG(t, schema, rowSets...)
	fr := openNativeReader(t, data)
	numRGs := fr.NumRowGroups()

	const path = "f.parquet"
	slot := newPreloadedFileSlot(catalog.FileEntry{Path: path}, fr)
	slot.rgRemaining.Store(int64(numRGs))
	units := make([]rgUnit, numRGs)
	var totalRows int64
	for i := 0; i < numRGs; i++ {
		rgRows := fr.RowGroupNumRows(i)
		units[i] = rgUnit{slot: slot, rgIndex: i, rgRowOffset: totalRows, numRows: rgRows}
		totalRows += rgRows
	}

	inner := &scanSourceInner{
		schema:     schema.Columns,
		rgUnits:    units,
		batchCh:    make(chan *batch.RecordBatch, numRGs+1),
		errCh:      make(chan error, 1),
		memTracker: tracker,
	}
	if deleteAll {
		del := make(map[int64]bool, totalRows)
		for i := int64(0); i < totalRows; i++ {
			del[i] = true
		}
		inner.deleteMarkers = map[string]map[int64]bool{path: del}
	}

	inner.wg.Add(2)
	go inner.rgWorker(context.Background())
	go inner.rgWorker(context.Background())
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()
	return inner, totalRows
}

// TestScanAccounting_BatchChargesReleasedOnNext verifies decoded row-group
// batches are charged while held by the scan source and released as they
// leave through next().
func TestScanAccounting_BatchChargesReleasedOnNext(t *testing.T) {
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner, totalRows := runRGWorkerScan(t, tracker, false, testRows(10, 0), testRows(10, 10), testRows(10, 20))

	ctx := context.Background()
	var gotRows int64
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		gotRows += int64(b.ActiveLen())
	}
	if gotRows != totalRows {
		t.Fatalf("rows = %d, want %d", gotRows, totalRows)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after consuming all batches, want 0", used)
	}
	if tracker.Peak() <= 0 {
		t.Fatal("peak = 0 — decoded batches were never charged")
	}
}

// TestScanAccounting_DropPathReleases verifies batches fully eliminated by
// delete markers release their charge instead of leaking it.
func TestScanAccounting_DropPathReleases(t *testing.T) {
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner, _ := runRGWorkerScan(t, tracker, true, testRows(10, 0), testRows(10, 10))

	ctx := context.Background()
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		t.Fatalf("expected no batches past full delete markers, got %d rows", b.ActiveLen())
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after drop-path scan, want 0", used)
	}
	if tracker.Peak() <= 0 {
		t.Fatal("peak = 0 — decoded batches were never charged")
	}
}

// TestScanAccounting_DrainOnClose verifies charges for batches stranded in
// batchCh (consumer never drained them) are released by the Close-time
// drain instead of leaking on the shared tracker.
func TestScanAccounting_DrainOnClose(t *testing.T) {
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner, _ := runRGWorkerScan(t, tracker, false, testRows(10, 0), testRows(10, 10), testRows(10, 20))

	// Consume nothing. Workers finish (batchCh has capacity for all RGs),
	// then Close-style teardown drains the outstanding charges.
	inner.wg.Wait()
	if used := tracker.Used(); used <= 0 {
		t.Fatal("expected outstanding charges for stranded batches")
	}
	inner.drainBatchCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after drain, want 0", used)
	}
}
