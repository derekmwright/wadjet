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

// Regression test for GitHub issue #9: SELECT col1, col2 returns NULL values
// when using column projection instead of SELECT *.
func TestColumnProjectionNotNull(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "severity", Type: parquet.TypeString, Nullable: true},
			{Name: "title", Type: parquet.TypeString, Nullable: true},
			{Name: "status", Type: parquet.TypeString, Nullable: true},
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
		{"id": int64(1), "severity": "high", "title": "Public bucket", "status": "open"},
		{"id": int64(2), "severity": "low", "title": "Missing tag", "status": "closed"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// SELECT * should work
	star, err := db.Query(ctx, "SELECT * FROM findings")
	if err != nil {
		t.Fatalf("SELECT * failed: %v", err)
	}
	if len(star.Rows) != 2 {
		t.Fatalf("SELECT * expected 2 rows, got %d", len(star.Rows))
	}
	for i, row := range star.Rows {
		if row["severity"] == nil {
			t.Errorf("SELECT * row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("SELECT * row %d: title is nil", i)
		}
	}

	// SELECT col1, col2 should also work — this is the regression
	proj, err := db.Query(ctx, "SELECT severity, title FROM findings")
	if err != nil {
		t.Fatalf("SELECT severity, title failed: %v", err)
	}
	if len(proj.Rows) != 2 {
		t.Fatalf("SELECT severity, title expected 2 rows, got %d", len(proj.Rows))
	}
	if len(proj.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %v", len(proj.Columns), proj.Columns)
	}
	for i, row := range proj.Rows {
		sev := row["severity"]
		title := row["title"]
		if sev == nil {
			t.Errorf("row %d: severity is nil (expected non-nil)", i)
		}
		if title == nil {
			t.Errorf("row %d: title is nil (expected non-nil)", i)
		}
		t.Logf("row %d: severity=%v title=%v (keys: %v)", i, sev, title, mapKeys(row))
	}

	// Column projection with WHERE clause
	filtered, err := db.Query(ctx, "SELECT severity, title FROM findings WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT with WHERE failed: %v", err)
	}
	if len(filtered.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(filtered.Rows))
	}
	if filtered.Rows[0]["severity"] == nil {
		t.Errorf("filtered row: severity is nil")
	}
	if filtered.Rows[0]["title"] == nil {
		t.Errorf("filtered row: title is nil")
	}

	// Table-qualified column reference
	qualified, err := db.Query(ctx, "SELECT findings.severity, findings.title FROM findings")
	if err != nil {
		t.Fatalf("SELECT findings.col failed: %v", err)
	}
	if len(qualified.Rows) != 2 {
		t.Fatalf("qualified: expected 2 rows, got %d", len(qualified.Rows))
	}
	for i, row := range qualified.Rows {
		t.Logf("qualified row %d: keys=%v", i, mapKeys(row))
		// Check both qualified and unqualified keys
		sev := row["severity"]
		if sev == nil {
			sev = row["findings.severity"]
		}
		if sev == nil {
			t.Errorf("qualified row %d: severity is nil (keys: %v)", i, mapKeys(row))
		}
	}

	// Single column projection
	single, err := db.Query(ctx, "SELECT title FROM findings")
	if err != nil {
		t.Fatalf("SELECT title failed: %v", err)
	}
	if len(single.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(single.Rows))
	}
	for i, row := range single.Rows {
		if row["title"] == nil {
			t.Errorf("single col row %d: title is nil", i)
		}
	}
}

// Same test but with a partitioned table (matches issue #9 exactly)
func TestColumnProjectionPartitioned(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "tenant_id", Type: parquet.TypeString},
			{Name: "scan_date", Type: parquet.TypeString},
			{Name: "severity", Type: parquet.TypeString, Nullable: true},
			{Name: "title", Type: parquet.TypeString, Nullable: true},
			{Name: "status", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, []string{"tenant_id", "scan_date"}); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, []string{"tenant_id", "scan_date"}, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"tenant_id": "acme", "scan_date": "2026-03-01", "severity": "high", "title": "Public bucket", "status": "open"},
		{"tenant_id": "acme", "scan_date": "2026-03-01", "severity": "low", "title": "Missing tag", "status": "closed"},
		{"tenant_id": "globex", "scan_date": "2026-03-02", "severity": "critical", "title": "SQL injection", "status": "open"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Column projection with partition filter
	result, err := db.Query(ctx, "SELECT severity, title FROM findings WHERE tenant_id = 'acme'")
	if err != nil {
		t.Fatalf("partitioned SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		t.Logf("part row %d: severity=%v title=%v keys=%v", i, row["severity"], row["title"], mapKeys(row))
		if row["severity"] == nil {
			t.Errorf("part row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("part row %d: title is nil", i)
		}
	}

	// Column projection without partition filter
	all, err := db.Query(ctx, "SELECT severity, title FROM findings")
	if err != nil {
		t.Fatalf("unfiltered SELECT failed: %v", err)
	}
	if len(all.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(all.Rows))
	}
	for i, row := range all.Rows {
		if row["severity"] == nil {
			t.Errorf("all row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("all row %d: title is nil", i)
		}
	}

	// Verify column list is correct and matches row keys
	if len(result.Columns) != 2 || result.Columns[0] != "severity" || result.Columns[1] != "title" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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
