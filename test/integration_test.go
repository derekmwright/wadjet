package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestFullPipeline(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create table
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString, Nullable: false},
			{Name: "amount", Type: parquet.TypeFloat64, Nullable: false},
			{Name: "year", Type: parquet.TypeString, Nullable: false},
			{Name: "month", Type: parquet.TypeString, Nullable: false},
		},
	}

	err = db.CreateTable(ctx, "events", schema, []string{"year", "month"})
	if err != nil {
		t.Fatal(err)
	}

	// Verify table exists
	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || tables[0] != "events" {
		t.Fatalf("expected [events], got %v", tables)
	}

	// Ingest data
	ing := db.NewIngester("events", schema, []string{"year", "month"}, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  10,
		FlushInterval: 0, // no periodic flush
	})

	rows := []map[string]any{
		{"user_id": "alice", "amount": 100.0, "year": "2026", "month": "03"},
		{"user_id": "bob", "amount": 200.0, "year": "2026", "month": "03"},
		{"user_id": "alice", "amount": 150.0, "year": "2026", "month": "03"},
		{"user_id": "carol", "amount": 300.0, "year": "2026", "month": "02"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify manifest has files
	manifest, err := db.Catalog().GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) == 0 {
		t.Fatal("expected partitions in manifest")
	}

	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles == 0 {
		t.Fatal("expected files in manifest")
	}

	t.Logf("Ingested into %d partitions, %d files", len(manifest.Partitions), totalFiles)
}

func TestCatalogOperations(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}

	// Create table
	if err := db.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Create duplicate should fail
	if err := db.CreateTable(ctx, "users", schema, nil); err == nil {
		t.Fatal("expected error creating duplicate table")
	}

	// List tables
	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}

	// Drop table
	if err := db.DropTable(ctx, "users"); err != nil {
		t.Fatal(err)
	}

	tables, err = db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(tables))
	}
}
