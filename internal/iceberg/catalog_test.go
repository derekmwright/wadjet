package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func setupCatalogTest(t *testing.T) (*catalog.Catalog, *objstore.MemStore) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func putJSON(t *testing.T, store *objstore.MemStore, bucket, key string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), "application/json"); err != nil {
		t.Fatal(err)
	}
}

func seedIcebergTable(t *testing.T, store *objstore.MemStore, bucket string) {
	t.Helper()

	// Data files
	dataFiles := []DataFileEntry{
		{FilePath: "data/year=2024/part-00000.parquet", FileFormat: "PARQUET", RecordCount: 5000, FileSizeBytes: 250000, Partition: map[string]any{"year": "2024"}},
		{FilePath: "data/year=2024/part-00001.parquet", FileFormat: "PARQUET", RecordCount: 3000, FileSizeBytes: 150000, Partition: map[string]any{"year": "2024"}},
		{FilePath: "data/year=2025/part-00000.parquet", FileFormat: "PARQUET", RecordCount: 2000, FileSizeBytes: 100000, Partition: map[string]any{"year": "2025"}},
	}
	putJSON(t, store, bucket, "warehouse/events/metadata/manifest-1.json", dataFiles)

	manifestList := []ManifestEntry{
		{ManifestPath: "metadata/manifest-1.json", Content: 0, AddedSnapshotID: 1},
	}
	putJSON(t, store, bucket, "warehouse/events/metadata/snap-1-ml.json", manifestList)

	snapID := int64(1)
	meta := TableMetadata{
		FormatVersion: 2,
		TableUUID:     "ice-events-001",
		Location:      "warehouse/events",
		Schemas: []Schema{
			{SchemaID: 0, Fields: []Field{
				{ID: 1, Name: "event_id", Required: true, Type: "long"},
				{ID: 2, Name: "event_type", Required: false, Type: "string"},
				{ID: 3, Name: "timestamp", Required: false, Type: "timestamptz"},
				{ID: 4, Name: "year", Required: false, Type: "int"},
				{ID: 5, Name: "payload", Required: false, Type: "string"},
			}},
		},
		CurrentSchema: 0,
		PartitionSpecs: []PartitionSpec{
			{SpecID: 0, Fields: []PartitionField{
				{SourceID: 4, FieldID: 1000, Name: "year_identity", Transform: "identity"},
			}},
		},
		DefaultSpecID: 0,
		Snapshots: []Snapshot{
			{SnapshotID: 1, TimestampMs: 1700000000000, ManifestList: "metadata/snap-1-ml.json"},
		},
		CurrentSnapshotID: &snapID,
	}
	putJSON(t, store, bucket, "warehouse/events/metadata/v1.metadata.json", meta)
}

func TestCatalogIntegrationRegisterTable(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	seedIcebergTable(t, store, "test-bucket")

	ci := NewCatalogIntegration(cat)
	info, err := ci.RegisterTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}

	if info.Metadata.TableUUID != "ice-events-001" {
		t.Errorf("table uuid = %q", info.Metadata.TableUUID)
	}
	if len(info.DataFiles) != 3 {
		t.Errorf("data files = %d, want 3", len(info.DataFiles))
	}

	// Verify table exists in catalog
	tables, err := cat.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tn := range tables {
		if tn == "events" {
			found = true
		}
	}
	if !found {
		t.Error("table 'events' not found in catalog")
	}

	// Verify schema
	tableMeta, err := cat.GetTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(tableMeta.Schema.Columns) != 5 {
		t.Errorf("schema columns = %d, want 5", len(tableMeta.Schema.Columns))
	}

	// Verify partition keys
	if len(tableMeta.PartitionKeys) != 1 || tableMeta.PartitionKeys[0] != "year" {
		t.Errorf("partition keys = %v, want [year]", tableMeta.PartitionKeys)
	}

	// Verify manifest has file entries
	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles != 3 {
		t.Errorf("total manifest files = %d, want 3", totalFiles)
	}
}

func TestCatalogIntegrationDiscoverAndRegister(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	seedIcebergTable(t, store, "test-bucket")

	ci := NewCatalogIntegration(cat)
	info, err := ci.DiscoverAndRegister(ctx, "events_discovered", "warehouse/events")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DataFiles) != 3 {
		t.Errorf("data files = %d, want 3", len(info.DataFiles))
	}

	tableMeta, err := cat.GetTable(ctx, "events_discovered")
	if err != nil {
		t.Fatal(err)
	}
	if tableMeta.Name != "events_discovered" {
		t.Errorf("table name = %q", tableMeta.Name)
	}
}

func TestCatalogIntegrationRefreshTable(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	seedIcebergTable(t, store, "test-bucket")

	ci := NewCatalogIntegration(cat)

	// First register
	_, err := ci.RegisterTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}

	// Refresh — simulates Iceberg table update
	info, err := ci.RefreshTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DataFiles) != 3 {
		t.Errorf("data files = %d, want 3", len(info.DataFiles))
	}

	// Table should still exist with correct schema
	tableMeta, err := cat.GetTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(tableMeta.Schema.Columns) != 5 {
		t.Errorf("columns after refresh = %d, want 5", len(tableMeta.Schema.Columns))
	}
}

func TestCatalogIntegrationEmptyTable(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	// Iceberg table with no snapshots
	meta := TableMetadata{
		FormatVersion: 1,
		TableUUID:     "empty-001",
		Location:      "warehouse/empty",
		Schema: Schema{
			Fields: []Field{
				{ID: 1, Name: "id", Required: true, Type: "long"},
			},
		},
	}
	putJSON(t, store, "test-bucket", "warehouse/empty/metadata/v1.metadata.json", meta)

	ci := NewCatalogIntegration(cat)
	info, err := ci.RegisterTable(ctx, "empty_table", "warehouse/empty/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DataFiles) != 0 {
		t.Errorf("expected 0 data files, got %d", len(info.DataFiles))
	}

	tableMeta, err := cat.GetTable(ctx, "empty_table")
	if err != nil {
		t.Fatal(err)
	}
	if len(tableMeta.Schema.Columns) != 1 {
		t.Errorf("columns = %d, want 1", len(tableMeta.Schema.Columns))
	}
}

func TestCatalogIntegrationDuplicateRegistration(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	seedIcebergTable(t, store, "test-bucket")

	ci := NewCatalogIntegration(cat)

	_, err := ci.RegisterTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json")
	if err != nil {
		t.Fatal(err)
	}

	// Second registration should fail
	_, err = ci.RegisterTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json")
	if err == nil {
		t.Fatal("expected error for duplicate registration")
	}
}
