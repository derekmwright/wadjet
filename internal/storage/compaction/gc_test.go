package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// testGCSchema returns a simple schema for GC tests.
func testGCSchema() parquet.Schema {
	return parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
}

// setupGCTable creates a test catalog with a table and some files + delete markers.
func setupGCTable(t *testing.T, markerAge time.Duration) (*catalog.Catalog, context.Context) {
	t.Helper()
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		rows := []map[string]any{
			{"id": int64(i*3 + 1), "name": fmt.Sprintf("a_%d", i)},
			{"id": int64(i*3 + 2), "name": fmt.Sprintf("b_%d", i)},
			{"id": int64(i*3 + 3), "name": fmt.Sprintf("c_%d", i)},
		}
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Add delete markers with a specific age
	markers := []catalog.DeleteMarker{
		{FilePath: "tables/events/data/chunk_0000.parquet", RowIndices: []int64{0}, CreatedAt: time.Now().Add(-markerAge)},
		{FilePath: "tables/events/data/chunk_0001.parquet", RowIndices: []int64{1, 2}, CreatedAt: time.Now().Add(-markerAge)},
	}
	if err := cat.AddDeleteMarkers(ctx, "events", markers); err != nil {
		t.Fatal(err)
	}

	return cat, ctx
}

func TestGCDeleteMarkers_AgedRewrite(t *testing.T) {
	// Markers are 20 minutes old, GC min age is 10 minutes → should trigger
	cat, ctx := setupGCTable(t, 20*time.Minute)

	rewrite, orphan, err := cat.GCDeleteMarkers(ctx, "events", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphan) != 0 {
		t.Errorf("expected 0 orphans, got %d", len(orphan))
	}
	if len(rewrite) != 2 {
		t.Fatalf("expected 2 rewrite targets, got %d", len(rewrite))
	}

	// Rewrite markers should still be in manifest (ForceCompactFile needs them)
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 2 {
		t.Errorf("expected 2 markers still present for rewrite, got %d", len(manifest.DeleteMarkers))
	}

	// After ForceCompactFile, markers should be cleaned via SwapFileForGC
	c := New(cat, nil, DefaultConfig())
	for fp, indices := range rewrite {
		gcSet := make(map[int64]bool, len(indices))
		for _, idx := range indices {
			gcSet[idx] = true
		}
		if err := c.ForceCompactFile(ctx, "events", fp, gcSet); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after ForceCompactFile, got %d", len(manifest.DeleteMarkers))
	}
}

func TestGCDeleteMarkers_FreshMarkersKept(t *testing.T) {
	// Markers are 5 minutes old, GC min age is 10 minutes → should NOT trigger
	cat, ctx := setupGCTable(t, 5*time.Minute)

	rewrite, orphan, err := cat.GCDeleteMarkers(ctx, "events", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 0 {
		t.Errorf("expected 0 rewrite paths for fresh markers, got %d", len(rewrite))
	}
	if len(orphan) != 0 {
		t.Errorf("expected 0 orphans for fresh markers, got %d", len(orphan))
	}

	// Markers should still be present
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 2 {
		t.Errorf("expected 2 markers kept, got %d", len(manifest.DeleteMarkers))
	}
}

func TestGCDeleteMarkers_OrphanCleanup(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Add a file
	partPath := "tables/logs/data"
	path := partPath + "/chunk_0000.parquet"
	size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
		{"id": int64(1), "name": "a"},
	})
	if err := cat.AddFiles(ctx, "logs", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add an aged marker for a file that does NOT exist
	markers := []catalog.DeleteMarker{
		{FilePath: "tables/logs/data/nonexistent.parquet", RowIndices: []int64{0, 1}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}
	if err := cat.AddDeleteMarkers(ctx, "logs", markers); err != nil {
		t.Fatal(err)
	}

	rewrite, orphan, err := cat.GCDeleteMarkers(ctx, "logs", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 0 {
		t.Errorf("expected 0 rewrite paths, got %d", len(rewrite))
	}
	if len(orphan) != 1 {
		t.Fatalf("expected 1 orphan path, got %d", len(orphan))
	}
	if orphan[0] != "tables/logs/data/nonexistent.parquet" {
		t.Errorf("unexpected orphan path: %s", orphan[0])
	}

	// Marker should be removed
	manifest, err := cat.GetManifest(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after orphan GC, got %d", len(manifest.DeleteMarkers))
	}
}

func TestForceCompactFile_RewritesWithDeleteMarkers(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
	}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Delete row 1 (bob)
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{1}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.DeleteGrace = time.Nanosecond // defer, but eligible to flush immediately
	c := New(cat, nil, cfg)
	gcSet := map[int64]bool{1: true}
	if err := c.ForceCompactFile(ctx, "events", path, gcSet); err != nil {
		t.Fatal(err)
	}

	// Original file should be gone, replaced by rewrite
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(manifest.Partitions))
	}
	files := manifest.Partitions[0].Files
	if len(files) != 1 {
		t.Fatalf("expected 1 file after rewrite, got %d", len(files))
	}

	// New file should have 2 rows (alice, carol — bob was deleted)
	if files[0].NumRows != 2 {
		t.Errorf("expected 2 rows after rewrite, got %d", files[0].NumRows)
	}
	if files[0].Path == path {
		t.Errorf("expected new file path, got original: %s", files[0].Path)
	}

	// The old file's BYTES must survive the manifest swap: in-flight tasks
	// dispatched against the pre-rewrite manifest still read them. (The
	// pre-DeleteGrace behavior deleted here immediately, which raced every
	// running query against the compactor — 2026-06-11 SF10 edge run.)
	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Fatalf("old file must remain in store until DeleteGrace expires: %v", err)
	}

	// After the grace expires, the background flush deletes it physically.
	time.Sleep(2 * time.Millisecond)
	if n := c.FlushDeferredDeletes(ctx); n != 1 {
		t.Fatalf("expected 1 deferred deletion flushed, got %d", n)
	}
	if _, _, err := store.Get(ctx, "test-bucket", path); err == nil {
		t.Error("expected old file to be deleted from object store after flush")
	}
}

func TestForceCompactFile_AllRowsDeleted(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Delete all rows
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{0, 1}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	c := New(cat, nil, DefaultConfig())
	gcSet := map[int64]bool{0: true, 1: true}
	if err := c.ForceCompactFile(ctx, "events", path, gcSet); err != nil {
		t.Fatal(err)
	}

	// File should be gone entirely
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles != 0 {
		t.Errorf("expected 0 files after all-deleted rewrite, got %d", totalFiles)
	}
}

func TestForceCompactFile_MissingFile(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()

	if err := cat.CreateTable(ctx, "events", testGCSchema(), nil); err != nil {
		t.Fatal(err)
	}

	c := New(cat, nil, DefaultConfig())
	// Should be a no-op, not an error
	if err := c.ForceCompactFile(ctx, "events", "nonexistent.parquet", map[int64]bool{0: true}); err != nil {
		t.Errorf("ForceCompactFile on missing file should be no-op, got error: %v", err)
	}
}

func TestBackgroundSweep_GCDeleteMarkers(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
	}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add aged delete marker
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{1}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	// Run background sweep with GC enabled (1 minute min age to catch 20-min-old markers)
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:    true,
		Compaction: DefaultConfig(),
		GCMinAge:   1 * time.Minute,
	}, nil)
	bc.sweep(ctx)

	// After sweep: marker should be gone, file should be rewritten with 2 rows
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after sweep GC, got %d", len(manifest.DeleteMarkers))
	}

	totalFiles := 0
	var totalRows int64
	for _, p := range manifest.Partitions {
		for _, f := range p.Files {
			totalFiles++
			totalRows += f.NumRows
		}
	}
	if totalFiles != 1 {
		t.Errorf("expected 1 file after sweep, got %d", totalFiles)
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows after sweep (bob deleted), got %d", totalRows)
	}
}

// TestBackgroundSweep_FlushesDroppedTableFiles is a #494 regression: with
// ReclaimDroppedTables enabled, the background compaction sweep drives
// catalog.Catalog.FlushDroppedTableFiles (DropTable itself never deletes
// physical bytes — see its doc comment), so a dropped table's files must
// actually disappear once the sweep runs past their DropGrace.
func TestBackgroundSweep_FlushesDroppedTableFiles(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_0000.parquet"
	rows := []map[string]any{{"id": int64(1), "name": "alice"}}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// The compactor is constructed BEFORE the drop, which is also the
	// production order (cmd/wadjet/main.go builds it at startup): with
	// ReclaimDroppedTables set it declares itself the flusher on this
	// catalog, and DropTable only records where a flusher exists.
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:              true,
		Compaction:           DefaultConfig(),
		DropGrace:            time.Nanosecond,
		ReclaimDroppedTables: true,
	}, nil)

	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Fatalf("file must survive DropTable itself: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	bc.sweep(ctx)

	if _, _, err := store.Get(ctx, "test-bucket", path); err == nil {
		t.Error("expected dropped table's file to be physically deleted after the sweep")
	}
}

// TestBackgroundSweep_ReclaimDroppedTablesDefaultsOff is a #494 regression
// for the wiring decision itself: ReclaimDroppedTables must default to
// false, so a sweep that doesn't opt in never calls
// FlushDroppedTableFiles even though a drop is well past its grace.
func TestBackgroundSweep_ReclaimDroppedTablesDefaultsOff(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_0000.parquet"
	rows := []map[string]any{{"id": int64(1), "name": "alice"}}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond)
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:    true,
		Compaction: DefaultConfig(),
		DropGrace:  time.Nanosecond,
		// ReclaimDroppedTables left at its zero value (false).
	}, nil)
	// Holds twice over now: the sweep never calls FlushDroppedTableFiles,
	// AND the drop above recorded nothing, because no flusher was ever
	// declared on this catalog (catalog.Catalog.EnableDropReclaim).
	bc.sweep(ctx)

	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Errorf("dropped table's file must survive a sweep with ReclaimDroppedTables unset: %v", err)
	}
}

// TestReclaimNotWiredWhenBackgroundCompactionIsDisabled is a review
// regression for the wiring gate itself: NewBackgroundCompactor used to
// declare the drop-reclaim flusher on ReclaimDroppedTables alone, so a
// compactor built with Enabled:false — whose Start never launches the
// sweep loop, and whose sweep is never called on any timer — still left
// DropTable recording every drop's files into pendingDrops. Nothing was
// ever going to consume that list, so it only grew toward
// maxPendingDropPaths and evicted (leaked) instead of costing nothing,
// which is the entire point of declaring the flusher lazily.
func TestReclaimNotWiredWhenBackgroundCompactionIsDisabled(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	// Constructed before the drop — the production order, and exactly
	// what makes the flusher wire itself prematurely without the gate.
	_ = NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:              false,
		Compaction:           DefaultConfig(),
		DropGrace:            time.Nanosecond,
		ReclaimDroppedTables: true,
	}, nil)

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_0000.parquet"
	rows := []map[string]any{{"id": int64(1), "name": "alice"}}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	if n := cat.PendingDropCount(); n != 0 {
		t.Errorf("DropTable recorded %d pending entries on a catalog whose compactor is disabled (Enabled:false); a sweep that never runs will never consume them", n)
	}
}

// TestReviewRepro_DropThenReregisterSameFilesLosesThem is a permanent
// #494 regression, promoted from the adversarial review's reproducer: the
// documented re-registration workflow (#278 — harness / bench loaders /
// Iceberg discovery re-register the SAME object paths into a table)
// combined with the drop grace-delete used to lose data outright — DROP
// the table, re-register the same files (they are still physically
// present, which is the whole point of the grace), and the background
// sweep deleted them out from under the LIVE table. The live-manifest
// guard in catalog.Catalog.FlushDroppedTableFiles is what fixes this.
func TestReviewRepro_DropThenReregisterSameFilesLosesThem(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Declaring the flusher first is the production order, and it is what
	// makes the drop below recordable at all.
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:              true,
		Compaction:           DefaultConfig(),
		DropGrace:            time.Nanosecond,
		ReclaimDroppedTables: true,
	}, nil)

	// AddNewFiles, so this is an OWNED file: the ownership marker would
	// otherwise keep it out of pendingDrops entirely and this test would
	// pass without ever reaching the guard it is about.
	path := "tables/events/chunk_0198ff00-0000-7000-8000-000000000001.parquet"
	rows := []map[string]any{{"id": int64(1), "name": "alice"}}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Operator drops the table...
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	// ...then re-runs the loader, which re-creates the table and
	// re-registers the very same object paths (AddFiles is deliberately
	// idempotent for exactly this, per #278).
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// The table is live and queryable right now.
	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Fatalf("precondition: live table's file must exist: %v", err)
	}

	// The background sweep runs after DropGrace elapses.
	time.Sleep(2 * time.Millisecond)
	bc.sweep(ctx)

	man, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	refs := 0
	for _, p := range man.Partitions {
		refs += len(p.Files)
	}
	if refs != 1 {
		t.Fatalf("live manifest should still reference 1 file, got %d", refs)
	}
	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Errorf("DATA LOSS: the LIVE table's manifest still references %q but the sweep deleted it: %v", path, err)
	}
}

func TestAddDeleteMarkers_PreservesCreatedAt(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "events", nil, "data/", []catalog.FileEntry{
		{Path: "data/file.parquet", SizeBytes: 100, NumRows: 10, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// First marker with explicit old timestamp
	oldTime := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Millisecond)
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: "data/file.parquet", RowIndices: []int64{0}, CreatedAt: oldTime},
	}); err != nil {
		t.Fatal(err)
	}

	// Add more indices to the same file — CreatedAt should stay as the earliest
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: "data/file.parquet", RowIndices: []int64{5}},
	}); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 merged marker, got %d", len(manifest.DeleteMarkers))
	}

	dm := manifest.DeleteMarkers[0]
	// CreatedAt should be preserved from the first (older) marker
	if dm.CreatedAt.After(oldTime.Add(time.Second)) {
		t.Errorf("expected CreatedAt ~%v (preserved from first marker), got %v", oldTime, dm.CreatedAt)
	}
}

// TestForceCompactFile_ConcurrentDeletePreserved verifies that delete markers
// added concurrently between GC scan and ForceCompactFile are not lost.
// TestForceCompactFile_ConcurrentDeletePreserved is #894's rule at the GC
// door: a DELETE that commits after the GC scan is APPLIED by the rewrite,
// not left behind it.
//
// The test this replaced asserted the opposite, and asserted it on the
// metadata rather than on the rows: it required the concurrent marker to
// SURVIVE the swap, still naming the old file path. That is the shape #894
// reproduced. A marker pointing at a file that no longer exists is a marker
// no reader can apply — the rewrite output already contains the row — and the
// next GC sweep removes it as an orphan, so the deleted row comes back for
// good. "Preserved" has to mean the deleted ROW stays deleted, which is what
// this asserts.
func TestForceCompactFile_ConcurrentDeletePreserved(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
		{"id": int64(4), "name": "dave"},
	}
	size := writeTestFile(t, store, "test-bucket", path, schema, rows)
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 4, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add aged marker for row 1 (bob) — this will be picked up by GC
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{1}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	// Run GC scan — returns rewrite paths
	rewrite, _, err := cat.GCDeleteMarkers(ctx, "events", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 1 {
		t.Fatalf("expected 1 rewrite target, got %d", len(rewrite))
	}

	// CONCURRENT DELETE: a new marker for row 3 (dave), committed AFTER the
	// GC scan and before the rewrite reads the manifest.
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{3}, CreatedAt: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	c := New(cat, nil, DefaultConfig())
	for fp, indices := range rewrite {
		gcSet := make(map[int64]bool, len(indices))
		for _, idx := range indices {
			gcSet[idx] = true
		}
		if err := c.ForceCompactFile(ctx, "events", fp, gcSet); err != nil {
			t.Fatal(err)
		}
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	// The rewrite applied BOTH markers, so nothing is left dangling.
	if len(manifest.DeleteMarkers) != 0 {
		t.Fatalf("a rewrite applies all of a file's markers or none: got %+v", manifest.DeleteMarkers)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected one replacement file, got %+v", manifest.Partitions)
	}
	newPath := manifest.Partitions[0].Files[0].Path
	if newPath == path {
		t.Fatal("the file was not rewritten")
	}

	// And the rows: bob and dave are gone, alice and carol survive.
	got := readTestFileNames(t, store, "test-bucket", newPath, schema)
	want := []string{"alice", "carol"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rewritten rows = %v; want %v (a committed DELETE must not be resurrected)", got, want)
	}
}

// readTestFileNames reads a rewritten file's name column, in row order.
func readTestFileNames(t *testing.T, store objstore.Store, bucket, path string, schema parquet.Schema) []string {
	t.Helper()
	rc, _, err := store.Get(context.Background(), bucket, path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRowsAs(schema.Columns, schema.ColumnNames())
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row["name"].(string))
	}
	return out
}

// TestGCDeleteMarkers_ZeroCreatedAtBackfill verifies that pre-existing markers
// with zero-value CreatedAt are backfilled and become GC-eligible next cycle.
func TestGCDeleteMarkers_ZeroCreatedAtBackfill(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	})
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add marker with zero CreatedAt (simulating pre-GC-feature marker)
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{0}},
	}); err != nil {
		t.Fatal(err)
	}

	// First GC pass: zero-value marker should be backfilled but NOT trigger rewrite
	rewrite, _, err := cat.GCDeleteMarkers(ctx, "events", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 0 {
		t.Errorf("first GC pass: expected 0 rewrite paths (just backfilled), got %d", len(rewrite))
	}

	// Verify the marker now has a non-zero CreatedAt
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 marker, got %d", len(manifest.DeleteMarkers))
	}
	if manifest.DeleteMarkers[0].CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt after backfill")
	}
}

// TestForceCompactFile_DoubleGCSkipped verifies that the per-file lock
// prevents concurrent GC rewrites of the same file.
func TestForceCompactFile_DoubleGCSkipped(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	})
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{0}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	c := New(cat, nil, DefaultConfig())

	// Manually acquire the lock to simulate another goroutine in-flight
	if !c.tryAcquireGCLock(path) {
		t.Fatal("expected to acquire lock")
	}

	gcSet := map[int64]bool{0: true}

	// Second call should be a no-op (lock held)
	if err := c.ForceCompactFile(ctx, "events", path, gcSet); err != nil {
		t.Errorf("expected no-op when GC lock held, got error: %v", err)
	}

	// File should still be unchanged (rewrite was skipped)
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	files := manifest.Partitions[0].Files
	if len(files) != 1 || files[0].Path != path {
		t.Errorf("expected original file unchanged, got %v", files)
	}
	if len(manifest.DeleteMarkers) != 1 {
		t.Errorf("expected marker still present (skipped), got %d", len(manifest.DeleteMarkers))
	}

	// Release lock and retry — now it should work
	c.releaseGCLock(path)
	if err := c.ForceCompactFile(ctx, "events", path, gcSet); err != nil {
		t.Fatal(err)
	}

	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after successful GC, got %d", len(manifest.DeleteMarkers))
	}
}

// TestGCDeleteMarkers_ZeroCreatedAtTwoSweeps verifies that a zero-CreatedAt marker
// is backfilled on the first sweep and becomes GC-eligible on the second sweep.
func TestGCDeleteMarkers_ZeroCreatedAtTwoSweeps(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	path := partPath + "/chunk_0000.parquet"
	size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	})
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add marker with zero CreatedAt (simulating a pre-GC-feature marker)
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{0}},
	}); err != nil {
		t.Fatal(err)
	}

	// First sweep: marker gets backfilled to now, but is skipped this cycle
	rewrite, _, err := cat.GCDeleteMarkers(ctx, "events", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 0 {
		t.Errorf("first sweep: expected 0 rewrite paths (just backfilled), got %d", len(rewrite))
	}

	// Second sweep with minAge=0: backfilled marker is now past the cutoff
	rewrite, _, err = cat.GCDeleteMarkers(ctx, "events", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewrite) != 1 {
		t.Fatalf("second sweep: expected 1 rewrite path, got %d", len(rewrite))
	}

	// Complete the GC via ForceCompactFile
	c := New(cat, nil, DefaultConfig())
	for fp, indices := range rewrite {
		gcSet := make(map[int64]bool, len(indices))
		for _, idx := range indices {
			gcSet[idx] = true
		}
		if err := c.ForceCompactFile(ctx, "events", fp, gcSet); err != nil {
			t.Fatal(err)
		}
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after GC, got %d", len(manifest.DeleteMarkers))
	}
	var totalRows int64
	for _, p := range manifest.Partitions {
		for _, f := range p.Files {
			totalRows += f.NumRows
		}
	}
	// Row 0 (alice) deleted, row 1 (bob) remains
	if totalRows != 1 {
		t.Errorf("expected 1 row after GC, got %d", totalRows)
	}
}

// faultInjectKV wraps a MemKV and injects ErrRevisionMismatch on the first N Update calls.
type faultInjectKV struct {
	inner     *catalog.MemKV
	failsLeft int
}

func (f *faultInjectKV) Get(key string) ([]byte, uint64, error) { return f.inner.Get(key) }
func (f *faultInjectKV) Put(key string, value []byte) (uint64, error) {
	return f.inner.Put(key, value)
}
func (f *faultInjectKV) Update(key string, value []byte, expectedRev uint64) (uint64, error) {
	if f.failsLeft > 0 {
		f.failsLeft--
		return 0, catalog.ErrRevisionMismatch
	}
	return f.inner.Update(key, value, expectedRev)
}
func (f *faultInjectKV) Delete(key string) error              { return f.inner.Delete(key) }
func (f *faultInjectKV) List(prefix string) ([]string, error) { return f.inner.List(prefix) }

// TestGCDeleteMarkers_OrphanCASRetry simulates ErrRevisionMismatch during
// orphan marker cleanup and verifies the CAS retry loop recovers correctly.
func TestGCDeleteMarkers_OrphanCASRetry(t *testing.T) {
	store := objstore.NewMemStore()
	memKV := catalog.NewMemKV()
	fkv := &faultInjectKV{inner: memKV, failsLeft: 1}
	cat := catalog.New(fkv, store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := testGCSchema()
	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Add a real file
	partPath := "tables/logs/data"
	path := partPath + "/chunk_0000.parquet"
	size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
		{"id": int64(1), "name": "a"},
	})
	if err := cat.AddFiles(ctx, "logs", nil, partPath, []catalog.FileEntry{
		{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// Add an aged orphan marker — references a file that doesn't exist
	if err := cat.AddDeleteMarkers(ctx, "logs", []catalog.DeleteMarker{
		{FilePath: partPath + "/gone.parquet", RowIndices: []int64{0}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	// First Update call will return ErrRevisionMismatch; retry should succeed
	_, orphan, err := cat.GCDeleteMarkers(ctx, "logs", 10*time.Minute)
	if err != nil {
		t.Fatalf("expected retry to succeed, got error: %v", err)
	}
	if len(orphan) != 1 {
		t.Fatalf("expected 1 orphan cleaned up, got %d", len(orphan))
	}

	// Orphan marker should be gone from the manifest
	manifest, err := cat.GetManifest(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 markers after orphan GC retry, got %d", len(manifest.DeleteMarkers))
	}
}

// TestSwapFileForGC_AtomicSwap verifies the swap's two halves: it publishes
// the removal, the replacement and the marker cleanup in one transaction, and
// it REFUSES a partial application.
//
// The refusal half is #894. This test used to assert the opposite — swap with
// three of the file's four marked indices "applied" and expect the fourth to
// survive — and that surviving marker is the defect: it names a row of a file
// the swap just removed, so no reader can apply it, and the replacement
// carries the row forever.
func TestSwapFileForGC_AtomicSwap(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	schema := testGCSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	partPath := "tables/events/data"
	oldPath := partPath + "/old.parquet"
	if err := cat.AddFiles(ctx, "events", nil, partPath, []catalog.FileEntry{
		{Path: oldPath, SizeBytes: 100, NumRows: 5, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: oldPath, RowIndices: []int64{0, 1, 2, 3}, CreatedAt: time.Now().Add(-20 * time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	newEntry := catalog.FileEntry{
		Path:      partPath + "/new.parquet",
		SizeBytes: 50,
		NumRows:   2,
		CreatedAt: time.Now().UTC(),
	}

	// A rewrite that applied only 0,1,2 does not reflect the marker on 3.
	// Publishing it would put row 3 back.
	partial := map[int64]bool{0: true, 1: true, 2: true}
	err := cat.SwapFileForGC(ctx, "events", oldPath, &newEntry, nil, partPath, partial)
	if !errors.Is(err, catalog.ErrCompactionDeletesAdvanced) {
		t.Fatalf("a partially-applied rewrite must be refused, got %v", err)
	}
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions[0].Files) != 1 || manifest.Partitions[0].Files[0].Path != oldPath {
		t.Fatalf("a refused swap must leave the old file in place, got %+v", manifest.Partitions)
	}
	if len(manifest.DeleteMarkers) != 1 || len(manifest.DeleteMarkers[0].RowIndices) != 4 {
		t.Fatalf("a refused swap must leave the markers alone, got %+v", manifest.DeleteMarkers)
	}

	// The whole set: removal, addition and marker cleanup, in one transaction.
	full := map[int64]bool{0: true, 1: true, 2: true, 3: true}
	if err := cat.SwapFileForGC(ctx, "events", oldPath, &newEntry, nil, partPath, full); err != nil {
		t.Fatal(err)
	}

	manifest, err = cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	files := manifest.Partitions[0].Files
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != newEntry.Path {
		t.Errorf("expected new file path %q, got %q", newEntry.Path, files[0].Path)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Fatalf("every applied marker must be gone, got %+v", manifest.DeleteMarkers)
	}
}
