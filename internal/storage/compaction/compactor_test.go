package compaction

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestCompactTable_UnpartitionedTableOutputPath is a regression test for a
// bug where the compactor built the merged output path as
// "<part.Path>/compacted_X.parquet" — with `part.Path` empty for
// unpartitioned tables, the key began with "/" and the compacted file
// landed at the bucket root while the old chunks were deleted, orphaning
// the data (no catalog entry points to it). Repeated compactor passes
// silently emptied customer/part at SF10. The fix prepends the table
// prefix so unpartitioned output lands under tables/<name>/.
func TestCompactTable_UnpartitionedTableOutputPath(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "customer", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Ingester convention for unpartitioned tables: files live directly at
	// tables/<name>/chunk_X.parquet and the PartitionEntry.Path is "".
	const partPath = ""
	var partValues map[string]string
	for i := 0; i < 15; i++ {
		path := fmt.Sprintf("tables/customer/chunk_%04d.parquet", i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i*10 + 1)},
			{"id": int64(i*10 + 2)},
		})
		if err := cat.AddFiles(ctx, "customer", partValues, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 5
	c := New(cat, nil, cfg)
	if _, err := c.CompactTable(ctx, "customer"); err != nil {
		t.Fatal(err)
	}

	manifest, _ := cat.GetManifest(ctx, "customer")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(manifest.Partitions[0].Files))
	}
	compactedPath := manifest.Partitions[0].Files[0].Path

	// Regression: must live under tables/customer/, not at the bucket root.
	if strings.HasPrefix(compactedPath, "/") {
		t.Fatalf("compacted file has leading slash — would land at bucket root: %q", compactedPath)
	}
	if !strings.HasPrefix(compactedPath, "tables/customer/") {
		t.Fatalf("compacted file not under tables/customer/: %q", compactedPath)
	}

	// Verify the data is actually reachable in the object store at that path.
	rc, _, err := store.Get(ctx, "test-bucket", compactedPath)
	if err != nil {
		t.Fatalf("compacted file not reachable at %q: %v", compactedPath, err)
	}
	rc.Close()
}

func setupTestCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func writeTestFile(t *testing.T, store objstore.Store, bucket, path string, schema parquet.Schema, rows []map[string]any) int64 {
	t.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	size := int64(buf.Len())
	_, err = store.Put(context.Background(), bucket, path, bytes.NewReader(buf.Bytes()), size, "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	return size
}

func TestCompactTable_MergesSmallFiles(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Create 15 small files in one partition
	partValues := map[string]string{"region": "us"}
	partPath := "tables/events/region=us"
	for i := 0; i < 15; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		rows := []map[string]any{
			{"id": int64(i*10 + 1), "name": fmt.Sprintf("row_%d_a", i)},
			{"id": int64(i*10 + 2), "name": fmt.Sprintf("row_%d_b", i)},
		}
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		if err := cat.AddFiles(ctx, "events", partValues, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Verify pre-compaction state
	manifest, _ := cat.GetManifest(ctx, "events")
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 15 {
		t.Fatalf("expected 1 partition with 15 files, got %d partitions",
			len(manifest.Partitions))
	}

	// Run compaction
	cfg := DefaultConfig()
	cfg.MinFiles = 5 // lower threshold for test
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	if result.PartitionsCompacted != 1 {
		t.Errorf("partitions compacted: got %d, want 1", result.PartitionsCompacted)
	}
	if result.FilesRemoved != 15 {
		t.Errorf("files removed: got %d, want 15", result.FilesRemoved)
	}
	if result.FilesCreated != 1 {
		t.Errorf("files created: got %d, want 1", result.FilesCreated)
	}
	if result.RowsMerged != 30 {
		t.Errorf("rows merged: got %d, want 30", result.RowsMerged)
	}

	// Verify post-compaction manifest
	manifest, _ = cat.GetManifest(ctx, "events")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("expected 1 file after compaction, got %d", len(manifest.Partitions[0].Files))
	}

	// Read back the compacted file and verify data
	compactedPath := manifest.Partitions[0].Files[0].Path
	rc, _, err := store.Get(ctx, "test-bucket", compactedPath)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := readAll(rc)
	rc.Close()

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 30 {
		t.Fatalf("compacted file: expected 30 rows, got %d", len(rows))
	}
}

func TestCompactTable_AppliesDeleteMarkers(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "logs", schema, nil); err != nil {
		t.Fatal(err)
	}

	partValues := map[string]string{}
	partPath := "tables/logs/data"
	var allFiles []catalog.FileEntry
	for i := 0; i < 12; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		rows := []map[string]any{
			{"id": int64(i*3 + 1)},
			{"id": int64(i*3 + 2)},
			{"id": int64(i*3 + 3)},
		}
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		fe := catalog.FileEntry{Path: path, SizeBytes: size, NumRows: 3, CreatedAt: time.Now().UTC()}
		allFiles = append(allFiles, fe)
		if err := cat.AddFiles(ctx, "logs", partValues, partPath, []catalog.FileEntry{fe}); err != nil {
			t.Fatal(err)
		}
	}

	// Delete row 0 from file 0 and row 1 from file 5
	if err := cat.AddDeleteMarkers(ctx, "logs", []catalog.DeleteMarker{
		{FilePath: allFiles[0].Path, RowIndices: []int64{0}},
		{FilePath: allFiles[5].Path, RowIndices: []int64{1}},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 5
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "logs")
	if err != nil {
		t.Fatal(err)
	}

	// 12 files × 3 rows = 36, minus 2 deleted = 34
	if result.RowsMerged != 34 {
		t.Errorf("rows merged: got %d, want 34", result.RowsMerged)
	}

	// Delete markers should be cleaned
	manifest, _ := cat.GetManifest(ctx, "logs")
	if len(manifest.DeleteMarkers) != 0 {
		t.Errorf("expected 0 delete markers after compaction, got %d", len(manifest.DeleteMarkers))
	}
}

func TestCompactTable_SkipsBelowThreshold(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "small", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Only 3 files — below MinFiles threshold
	partPath := "tables/small/data"
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/chunk_%d.parquet", partPath, i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{{"id": int64(i)}})
		if err := cat.AddFiles(ctx, "small", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	c := New(cat, nil, DefaultConfig()) // MinFiles=10
	result, err := c.CompactTable(ctx, "small")
	if err != nil {
		t.Fatal(err)
	}

	if result.PartitionsCompacted != 0 {
		t.Errorf("expected no compaction, got %d partitions compacted", result.PartitionsCompacted)
	}
}

// TestCompactTable_MultiPassCompaction verifies that when a partition has more
// files than MaxFilesPerPass, multiple passes run back-to-back within a single
// CompactTable call until the partition is fully compacted.
func TestCompactTable_MultiPassCompaction(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "multipass", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Create 30 files — with MaxFilesPerPass=10, needs 3+ passes
	const numFiles = 30
	partPath := "tables/multipass/data"
	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
		size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
			{"id": int64(i)},
		})
		if err := cat.AddFiles(ctx, "multipass", nil, partPath, []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := Config{MinFiles: 5, MaxFileSizeBytes: 32 * 1024 * 1024, MaxFilesPerPass: 10}
	c := New(cat, nil, cfg)
	result, err := c.CompactTable(ctx, "multipass")
	if err != nil {
		t.Fatal(err)
	}

	// All 30 files should be compacted (multi-pass)
	if result.FilesRemoved != numFiles {
		t.Errorf("files removed: got %d, want %d", result.FilesRemoved, numFiles)
	}
	if result.RowsMerged != numFiles {
		t.Errorf("rows merged: got %d, want %d", result.RowsMerged, numFiles)
	}

	// Verify single file remains
	manifest, _ := cat.GetManifest(ctx, "multipass")
	if len(manifest.Partitions[0].Files) != 1 {
		t.Errorf("expected 1 file after multi-pass compaction, got %d",
			len(manifest.Partitions[0].Files))
	}
}

// TestAdaptivePassSize verifies that small files get a larger pass size.
func TestAdaptivePassSize(t *testing.T) {
	c := &Compactor{config: Config{MaxFilesPerPass: 50}}

	// 1000 tiny files at 1KB each → target 256MB / 1KB = 262144 max per pass
	files := make([]catalog.FileEntry, 1000)
	for i := range files {
		files[i] = catalog.FileEntry{SizeBytes: 1024}
	}
	part := catalog.PartitionEntry{Files: files}
	got := c.adaptivePassSize(part)
	if got < 1000 {
		t.Errorf("tiny files: expected pass size >= 1000 (all files), got %d", got)
	}

	// 100 files at 10MB each → target 256MB / 10MB = 25, but floor is MaxFilesPerPass=50
	files = make([]catalog.FileEntry, 100)
	for i := range files {
		files[i] = catalog.FileEntry{SizeBytes: 10 * 1024 * 1024}
	}
	part = catalog.PartitionEntry{Files: files}
	got = c.adaptivePassSize(part)
	if got != 50 {
		t.Errorf("10MB files: expected pass size = 50 (floor), got %d", got)
	}
}

func readAll(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(rc.(interface{ Read([]byte) (int, error) }))
	return buf.Bytes(), err
}

func TestBackgroundCompactor_SweepsAllTables(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}

	// Create two tables, each with enough files to trigger compaction.
	for _, table := range []string{"alpha", "beta"} {
		if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatal(err)
		}
		partPath := fmt.Sprintf("tables/%s/data", table)
		for i := 0; i < 12; i++ {
			path := fmt.Sprintf("%s/chunk_%04d.parquet", partPath, i)
			size := writeTestFile(t, store, "test-bucket", path, schema, []map[string]any{
				{"id": int64(i)},
			})
			if err := cat.AddFiles(ctx, table, nil, partPath, []catalog.FileEntry{
				{Path: path, SizeBytes: size, NumRows: 1, CreatedAt: time.Now().UTC()},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Run the sweep directly (don't start the ticker-based loop).
	bc := NewBackgroundCompactor(cat, BackgroundConfig{
		Enabled:    true,
		Compaction: Config{MinFiles: 5, MaxFileSizeBytes: 32 * 1024 * 1024, MaxFilesPerPass: 50},
	}, nil)
	bc.sweep(ctx)

	// Both tables should now have 1 file each.
	for _, table := range []string{"alpha", "beta"} {
		manifest, err := cat.GetManifest(ctx, table)
		if err != nil {
			t.Fatal(err)
		}
		if len(manifest.Partitions) != 1 {
			t.Fatalf("table %s: expected 1 partition, got %d", table, len(manifest.Partitions))
		}
		if len(manifest.Partitions[0].Files) != 1 {
			t.Errorf("table %s: expected 1 file after compaction, got %d",
				table, len(manifest.Partitions[0].Files))
		}
	}
}
