package wadjet

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestDescribeCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	// DESCRIBE with mixed case should still find the table
	result, err := db.Query(ctx, "DESCRIBE Events")
	if err != nil {
		t.Fatalf("DESCRIBE Events (mixed case) failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}

	// SHOW COLUMNS FROM with uppercase should also work
	result, err = db.Query(ctx, "SHOW COLUMNS FROM EVENTS")
	if err != nil {
		t.Fatalf("SHOW COLUMNS FROM EVENTS (uppercase) failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestDescribeParquetFallback(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Create table schema and write some data
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "name": "xss"},
		{"id": int64(2), "name": "sqli"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify normal DESCRIBE works
	result, err := db.Query(ctx, "DESCRIBE findings")
	if err != nil {
		t.Fatalf("DESCRIBE findings failed: %v", err)
	}
	if len(result.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(result.Rows))
	}

	// Now create a second DB with the SAME store but fresh catalog (simulates restart)
	db2, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// DESCRIBE should fall back to Parquet file schema discovery
	result, err = db2.Query(ctx, "DESCRIBE findings")
	if err != nil {
		t.Fatalf("DESCRIBE findings (parquet fallback) failed: %v", err)
	}
	if len(result.Rows) < 2 {
		t.Fatalf("expected at least 2 rows from parquet fallback, got %d", len(result.Rows))
	}

	// Verify column names are correct
	foundID, foundName := false, false
	for _, row := range result.Rows {
		if row["column_name"] == "id" {
			foundID = true
		}
		if row["column_name"] == "name" {
			foundName = true
		}
	}
	if !foundID || !foundName {
		t.Errorf("expected id and name columns, got %v", result.Rows)
	}
}
