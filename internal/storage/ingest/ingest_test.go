package ingest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

var testSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "name", Type: parquet.TypeString},
	{Name: "year", Type: parquet.TypeString},
}}

const testBucket = "test-bucket"
const testTable = "events"

func setupCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, testTable, testSchema, []string{"year"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return cat, store
}

func TestNew(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}
	if ing.tableName != testTable {
		t.Fatalf("expected table %q, got %q", testTable, ing.tableName)
	}
	if ing.buffers == nil {
		t.Fatal("expected buffers map to be initialized")
	}
}

func TestIngestAndFlushAll(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2025"},
		{"id": int64(2), "name": "bob", "year": "2025"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify parquet files were written to the store.
	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	parquetCount := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetCount++
			if obj.Size == 0 {
				t.Errorf("parquet file %s has zero size", obj.Key)
			}
		}
	}
	if parquetCount == 0 {
		t.Fatal("expected at least one parquet file in the store")
	}
}

func TestIngestPartitionDistribution(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2024"},
		{"id": int64(2), "name": "bob", "year": "2025"},
		{"id": int64(3), "name": "carol", "year": "2024"},
		{"id": int64(4), "name": "dave", "year": "2026"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Each distinct year value should produce a separate parquet file under its partition path.
	years := map[string]bool{"2024": false, "2025": false, "2026": false}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		for y := range years {
			if strings.Contains(obj.Key, "year="+y) {
				years[y] = true
			}
		}
	}

	for y, found := range years {
		if !found {
			t.Errorf("expected parquet file for partition year=%s", y)
		}
	}
}

func TestFlushAllUpdatesManifest(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2025"},
		{"id": int64(2), "name": "bob", "year": "2025"},
		{"id": int64(3), "name": "carol", "year": "2026"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, testTable)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	if len(manifest.Partitions) != 2 {
		t.Fatalf("expected 2 partitions in manifest, got %d", len(manifest.Partitions))
	}

	totalFiles := 0
	var totalRows int64
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
		for _, f := range p.Files {
			totalRows += f.NumRows
			if f.SizeBytes == 0 {
				t.Errorf("file %s has zero size in manifest", f.Path)
			}
			if f.Path == "" {
				t.Error("file entry has empty path")
			}
		}
	}

	if totalFiles != 2 {
		t.Fatalf("expected 2 file entries in manifest, got %d", totalFiles)
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 total rows across manifest entries, got %d", totalRows)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxBufferSize != 128*1024*1024 {
		t.Errorf("expected MaxBufferSize 128 MB, got %d", cfg.MaxBufferSize)
	}
	if cfg.MaxBufferRows != 1_000_000 {
		t.Errorf("expected MaxBufferRows 1M, got %d", cfg.MaxBufferRows)
	}
	if cfg.FlushInterval != 60*time.Second {
		t.Errorf("expected FlushInterval 60s, got %v", cfg.FlushInterval)
	}
	if cfg.RowGroupSize != 128*1024 {
		t.Errorf("expected RowGroupSize 128K, got %d", cfg.RowGroupSize)
	}
}

func TestIngestValidation_MissingRequiredColumn(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "strict", schema, []string{"year"})

	ing := New(cat, "strict", schema, []string{"year"}, DefaultConfig())

	// Missing required column "id"
	err := ing.Ingest(ctx, []map[string]any{
		{"name": "alice", "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for missing NOT NULL column")
	}
	if !strings.Contains(err.Error(), "missing required column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_NullInNotNullColumn(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "strict2", schema, []string{"year"})

	ing := New(cat, "strict2", schema, []string{"year"}, DefaultConfig())

	// Null value for NOT NULL column
	err := ing.Ingest(ctx, []map[string]any{
		{"id": nil, "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for null in NOT NULL column")
	}
	if !strings.Contains(err.Error(), "cannot be null") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_WrongType(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "typed", schema, []string{"year"})

	ing := New(cat, "typed", schema, []string{"year"}, DefaultConfig())

	// Wrong type: string where int64 expected
	err := ing.Ingest(ctx, []map[string]any{
		{"id": "not-a-number", "name": "alice", "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	if !strings.Contains(err.Error(), "expected integer") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_NullableColumnMissing(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "nullable", schema, []string{"year"})

	ing := New(cat, "nullable", schema, []string{"year"}, DefaultConfig())

	// Missing nullable column "name" should be OK
	err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "year": "2025"},
	})
	if err != nil {
		t.Fatalf("unexpected error for missing nullable column: %v", err)
	}
}

func TestIngestValidation_NetworkTypes(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: false},
		{Name: "dst_ip", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "network", schema, []string{"year"})

	ing := New(cat, "network", schema, []string{"year"}, DefaultConfig())

	// Valid: string values for network types
	err := ing.Ingest(ctx, []map[string]any{
		{"src_ip": "192.168.1.1", "dst_ip": "::1", "mac": "aa:bb:cc:dd:ee:ff", "year": "2025"},
	})
	if err != nil {
		t.Fatalf("unexpected error for valid network types: %v", err)
	}

	// Invalid: integer where string expected for IPV4
	err = ing.Ingest(ctx, []map[string]any{
		{"src_ip": 12345, "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for integer in IPV4 column")
	}
	if !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("unexpected error: %v", err)
	}
}
