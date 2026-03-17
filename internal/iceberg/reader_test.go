package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// putObj is a helper to store a JSON-serializable object in the MemStore.
func putObj(t *testing.T, store *objstore.MemStore, bucket, key string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/json"); err != nil {
		t.Fatal(err)
	}
}

// putRaw stores raw bytes in the MemStore.
func putRaw(t *testing.T, store *objstore.MemStore, bucket, key string, data []byte) {
	t.Helper()
	if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/json"); err != nil {
		t.Fatal(err)
	}
}

func setupTestTable(t *testing.T) (*objstore.MemStore, string) {
	t.Helper()
	store := objstore.NewMemStore()
	bucket := "test-bucket"
	ctx := context.Background()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	// Create data file entries
	dataFiles := []DataFileEntry{
		{FilePath: "data/part-00000.parquet", FileFormat: "PARQUET", RecordCount: 1000, FileSizeBytes: 50000},
		{FilePath: "data/part-00001.parquet", FileFormat: "PARQUET", RecordCount: 500, FileSizeBytes: 25000},
	}

	// Create manifest
	putObj(t, store, bucket, "warehouse/db/t1/metadata/manifest-1.json", dataFiles)

	// Create manifest list
	manifestList := []ManifestEntry{
		{ManifestPath: "metadata/manifest-1.json", ManifestLength: 1024, Content: 0, AddedSnapshotID: 100},
	}
	putObj(t, store, bucket, "warehouse/db/t1/metadata/snap-100-manifest-list.json", manifestList)

	// Create table metadata
	snapID := int64(100)
	meta := TableMetadata{
		FormatVersion: 1,
		TableUUID:     "test-uuid-123",
		Location:      "warehouse/db/t1",
		Schema: Schema{
			SchemaID: 0,
			Type:     "struct",
			Fields: []Field{
				{ID: 1, Name: "id", Required: true, Type: "long"},
				{ID: 2, Name: "name", Required: false, Type: "string"},
				{ID: 3, Name: "ts", Required: false, Type: "timestamptz"},
			},
		},
		PartitionSpec: []PartitionField{
			{SourceID: 1, FieldID: 1000, Name: "id_identity", Transform: "identity"},
		},
		Snapshots: []Snapshot{
			{
				SnapshotID:   100,
				TimestampMs:  1700000000000,
				ManifestList: "metadata/snap-100-manifest-list.json",
				Summary:      map[string]string{"operation": "append"},
			},
		},
		CurrentSnapshotID: &snapID,
	}
	putObj(t, store, bucket, "warehouse/db/t1/metadata/v1.metadata.json", meta)

	return store, bucket
}

func TestTableReaderReadTable(t *testing.T) {
	store, bucket := setupTestTable(t)
	reader := NewTableReader(store, bucket)

	info, err := reader.ReadTable(context.Background(), "warehouse/db/t1/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}

	if info.Metadata.TableUUID != "test-uuid-123" {
		t.Errorf("table uuid = %q", info.Metadata.TableUUID)
	}
	if len(info.Schema.Columns) != 3 {
		t.Errorf("columns = %d, want 3", len(info.Schema.Columns))
	}
	if len(info.DataFiles) != 2 {
		t.Errorf("data files = %d, want 2", len(info.DataFiles))
	}
	if info.DataFiles[0].RecordCount != 1000 {
		t.Errorf("record count = %d, want 1000", info.DataFiles[0].RecordCount)
	}
	if len(info.PartitionKeys) != 1 || info.PartitionKeys[0] != "id" {
		t.Errorf("partition keys = %v, want [id]", info.PartitionKeys)
	}
}

func TestTableReaderNoSnapshot(t *testing.T) {
	store := objstore.NewMemStore()
	bucket := "test-bucket"
	ctx := context.Background()
	store.MakeBucket(ctx, bucket)

	meta := TableMetadata{
		FormatVersion: 1,
		TableUUID:     "empty-table",
		Location:      "warehouse/db/empty",
		Schema: Schema{
			Fields: []Field{
				{ID: 1, Name: "col1", Required: false, Type: "string"},
			},
		},
		Snapshots: []Snapshot{},
	}
	putObj(t, store, bucket, "warehouse/db/empty/metadata/v1.metadata.json", meta)

	reader := NewTableReader(store, bucket)
	info, err := reader.ReadTable(ctx, "warehouse/db/empty/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DataFiles) != 0 {
		t.Errorf("expected no data files, got %d", len(info.DataFiles))
	}
	if len(info.Schema.Columns) != 1 {
		t.Errorf("schema columns = %d, want 1", len(info.Schema.Columns))
	}
}

func TestTableReaderDiscoverTable(t *testing.T) {
	store, bucket := setupTestTable(t)
	reader := NewTableReader(store, bucket)

	info, err := reader.DiscoverTable(context.Background(), "warehouse/db/t1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Metadata.TableUUID != "test-uuid-123" {
		t.Errorf("table uuid = %q", info.Metadata.TableUUID)
	}
	if len(info.DataFiles) != 2 {
		t.Errorf("data files = %d, want 2", len(info.DataFiles))
	}
}

func TestTableReaderDiscoverTableWithVersionHint(t *testing.T) {
	store, bucket := setupTestTable(t)
	ctx := context.Background()

	// Add version-hint.text
	putRaw(t, store, bucket, "warehouse/db/t1/metadata/version-hint.text", []byte("1"))

	reader := NewTableReader(store, bucket)
	info, err := reader.DiscoverTable(ctx, "warehouse/db/t1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Metadata.TableUUID != "test-uuid-123" {
		t.Errorf("table uuid = %q", info.Metadata.TableUUID)
	}
}

func TestTableReaderDiscoverNotFound(t *testing.T) {
	store := objstore.NewMemStore()
	bucket := "test-bucket"
	store.MakeBucket(context.Background(), bucket)

	reader := NewTableReader(store, bucket)
	_, err := reader.DiscoverTable(context.Background(), "nonexistent/table")
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestTableReaderSkipsDeleteManifests(t *testing.T) {
	store := objstore.NewMemStore()
	bucket := "test-bucket"
	ctx := context.Background()
	store.MakeBucket(ctx, bucket)

	// Data file in data manifest
	dataFiles := []DataFileEntry{
		{FilePath: "data/part-00000.parquet", FileFormat: "PARQUET", RecordCount: 100},
	}
	putObj(t, store, bucket, "warehouse/t/metadata/data-manifest.json", dataFiles)

	// Delete manifest entries (should be skipped)
	deleteFiles := []DataFileEntry{
		{FilePath: "data/delete-00000.parquet", FileFormat: "PARQUET", RecordCount: 5},
	}
	putObj(t, store, bucket, "warehouse/t/metadata/delete-manifest.json", deleteFiles)

	manifestList := []ManifestEntry{
		{ManifestPath: "metadata/data-manifest.json", Content: 0},
		{ManifestPath: "metadata/delete-manifest.json", Content: 1}, // delete manifest
	}
	putObj(t, store, bucket, "warehouse/t/metadata/snap-1-ml.json", manifestList)

	snapID := int64(1)
	meta := TableMetadata{
		FormatVersion: 1,
		Location:      "warehouse/t",
		Schema:        Schema{Fields: []Field{{ID: 1, Name: "id", Type: "long"}}},
		Snapshots:     []Snapshot{{SnapshotID: 1, ManifestList: "metadata/snap-1-ml.json"}},
		CurrentSnapshotID: &snapID,
	}
	putObj(t, store, bucket, "warehouse/t/metadata/v1.metadata.json", meta)

	reader := NewTableReader(store, bucket)
	info, err := reader.ReadTable(ctx, "warehouse/t/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	// Only data files, not delete files
	if len(info.DataFiles) != 1 {
		t.Errorf("data files = %d, want 1 (should skip delete manifest)", len(info.DataFiles))
	}
}

func TestResolvePathAbsolute(t *testing.T) {
	reader := &TableReader{}

	tests := []struct {
		name     string
		location string
		filePath string
		want     string
	}{
		{"s3 path", "warehouse/t", "s3://mybucket/warehouse/t/data/file.parquet", "warehouse/t/data/file.parquet"},
		{"gs path", "warehouse/t", "gs://mybucket/data/file.parquet", "data/file.parquet"},
		{"s3a path", "warehouse/t", "s3a://mybucket/path/file.parquet", "path/file.parquet"},
		{"absolute path", "warehouse/t", "/data/file.parquet", "data/file.parquet"},
		{"relative path", "warehouse/t", "data/file.parquet", "warehouse/t/data/file.parquet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reader.resolvePath(tt.location, tt.filePath)
			if got != tt.want {
				t.Errorf("resolvePath(%q, %q) = %q, want %q", tt.location, tt.filePath, got, tt.want)
			}
		})
	}
}

func TestTableReaderV2Schema(t *testing.T) {
	store := objstore.NewMemStore()
	bucket := "test-bucket"
	ctx := context.Background()
	store.MakeBucket(ctx, bucket)

	meta := TableMetadata{
		FormatVersion: 2,
		TableUUID:     "v2-table",
		Location:      "warehouse/db/v2t",
		Schemas: []Schema{
			{SchemaID: 0, Fields: []Field{{ID: 1, Name: "old_col", Type: "string"}}},
			{SchemaID: 1, Fields: []Field{
				{ID: 1, Name: "id", Required: true, Type: "long"},
				{ID: 2, Name: "val", Required: false, Type: "double"},
			}},
		},
		CurrentSchema: 1,
		PartitionSpecs: []PartitionSpec{
			{SpecID: 0, Fields: []PartitionField{}},
		},
		DefaultSpecID: 0,
		Snapshots:     []Snapshot{},
	}
	putObj(t, store, bucket, "warehouse/db/v2t/metadata/v1.metadata.json", meta)

	reader := NewTableReader(store, bucket)
	info, err := reader.ReadTable(ctx, "warehouse/db/v2t/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Schema.Columns) != 2 {
		t.Errorf("columns = %d, want 2 (should use schema-id 1)", len(info.Schema.Columns))
	}
	if info.Schema.Columns[0].Name != "id" {
		t.Errorf("first column = %q, want %q", info.Schema.Columns[0].Name, "id")
	}
}
