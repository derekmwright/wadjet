package compaction

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

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

func readAll(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(rc.(interface{ Read([]byte) (int, error) }))
	return buf.Bytes(), err
}
