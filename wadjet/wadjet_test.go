package wadjet

import (
	"context"
	"strings"
	"testing"
	"time"

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

// Regression test for GitHub issue #7: CURRENT_DATE returns NULL.
// Table-less SELECT must work and return correct values.
func TestCurrentDateNotNull(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	today := time.Now().Format("2006-01-02")

	tests := []struct {
		sql    string
		colKey string // column name in result
		check  func(val any) bool
	}{
		{
			sql:    "SELECT CURRENT_DATE",
			colKey: "current_date()",
			check:  func(val any) bool { return val == today },
		},
		{
			sql:    "SELECT CURRENT_TIMESTAMP",
			colKey: "current_timestamp()",
			check: func(val any) bool {
				s, ok := val.(string)
				return ok && strings.HasPrefix(s, today[:10])
			},
		},
		{
			sql:    "SELECT NOW()",
			colKey: "now()",
			check: func(val any) bool {
				s, ok := val.(string)
				return ok && strings.HasPrefix(s, today[:10])
			},
		},
		{
			sql:    "SELECT 1 + 1 AS result",
			colKey: "result",
			check:  func(val any) bool { return val == float64(2) || val == int64(2) || val == 2 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			val, ok := result.Rows[0][tt.colKey]
			if !ok {
				t.Fatalf("column %q not found in result: %v", tt.colKey, result.Rows[0])
			}
			if val == nil {
				t.Fatalf("%s returned NULL", tt.sql)
			}
			if !tt.check(val) {
				t.Errorf("%s returned unexpected value: %v", tt.sql, val)
			}
		})
	}
}
