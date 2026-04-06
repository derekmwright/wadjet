package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
		t.Fatalf("expected 2 rewrite paths, got %d", len(rewrite))
	}

	// Rewrite markers should still be in manifest (ForceCompactFile needs them)
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.DeleteMarkers) != 2 {
		t.Errorf("expected 2 markers still present for rewrite, got %d", len(manifest.DeleteMarkers))
	}

	// After ForceCompactFile, markers should be cleaned via RemoveFiles
	c := New(cat, nil, DefaultConfig())
	for _, fp := range rewrite {
		if err := c.ForceCompactFile(ctx, "events", fp); err != nil {
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

	c := New(cat, nil, DefaultConfig())
	if err := c.ForceCompactFile(ctx, "events", path); err != nil {
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

	// Old file should not exist in object store
	_, _, err = store.Get(ctx, "test-bucket", path)
	if err == nil {
		t.Error("expected old file to be deleted from object store")
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
	if err := c.ForceCompactFile(ctx, "events", path); err != nil {
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
	if err := c.ForceCompactFile(ctx, "events", "nonexistent.parquet"); err != nil {
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
