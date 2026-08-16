package compaction

import (
	"context"
	"fmt"
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
// This is the TOCTOU regression test Sara flagged.
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

	// CONCURRENT DELETE: add a new marker for row 3 (dave) AFTER GC scan
	// This simulates a DELETE happening between GC and ForceCompactFile
	if err := cat.AddDeleteMarkers(ctx, "events", []catalog.DeleteMarker{
		{FilePath: path, RowIndices: []int64{3}, CreatedAt: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	// ForceCompactFile should apply the aged marker (row 1) but preserve
	// the concurrent marker (row 3)
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

	// The concurrent delete marker for row 3 should still be present
	// (pointing at the OLD file path since the new file has different rows)
	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 remaining marker (concurrent delete), got %d", len(manifest.DeleteMarkers))
	}
	dm := manifest.DeleteMarkers[0]
	if len(dm.RowIndices) != 1 || dm.RowIndices[0] != 3 {
		t.Errorf("expected remaining marker for row 3, got %v", dm.RowIndices)
	}
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
func (f *faultInjectKV) Delete(key string) error          { return f.inner.Delete(key) }
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

// TestSwapFileForGC_AtomicSwap verifies that SwapFileForGC atomically removes
// the old file, adds the new file, and removes only applied markers.
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

	// Add markers: indices 0,1,2 are "applied", index 3 is "concurrent"
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

	// Swap: applied indices are 0,1,2 — index 3 should survive
	applied := map[int64]bool{0: true, 1: true, 2: true}
	if err := cat.SwapFileForGC(ctx, "events", oldPath, &newEntry, nil, partPath, applied); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	// Old file gone, new file present
	files := manifest.Partitions[0].Files
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != newEntry.Path {
		t.Errorf("expected new file path %q, got %q", newEntry.Path, files[0].Path)
	}

	// Only the non-applied index (3) should remain
	if len(manifest.DeleteMarkers) != 1 {
		t.Fatalf("expected 1 remaining marker, got %d", len(manifest.DeleteMarkers))
	}
	dm := manifest.DeleteMarkers[0]
	if len(dm.RowIndices) != 1 || dm.RowIndices[0] != 3 {
		t.Errorf("expected remaining index [3], got %v", dm.RowIndices)
	}
}
