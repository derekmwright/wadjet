package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

func TestCurrentDateMinusInterval(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Bare SELECT: CURRENT_DATE - INTERVAL '30 days'
	result, err := db.Query(ctx, "SELECT CURRENT_DATE - INTERVAL '30 days'")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows returned")
	}
	colName := result.Columns[0]
	val := result.Rows[0][colName]
	t.Logf("CURRENT_DATE - INTERVAL '30 days' = %v (type: %T)", val, val)

	expected := time.Now().AddDate(0, 0, -30).Format("2006-01-02")
	valStr := fmt.Sprintf("%v", val)
	if valStr != expected {
		t.Errorf("got %q, want %q", valStr, expected)
	}

	// Addition variant
	result2, err := db.Query(ctx, "SELECT CURRENT_DATE + INTERVAL '7 days'")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	col2 := result2.Columns[0]
	val2 := fmt.Sprintf("%v", result2.Rows[0][col2])
	expected2 := time.Now().AddDate(0, 0, 7).Format("2006-01-02")
	if val2 != expected2 {
		t.Errorf("CURRENT_DATE + INTERVAL '7 days': got %q, want %q", val2, expected2)
	}
}

func TestIntervalInWhereClause(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "scan_date", Type: parquet.TypeString},
			{Name: "finding", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Insert rows: some within 30 days, some older
	now := time.Now()
	rows := []map[string]any{
		{"id": int64(1), "scan_date": now.AddDate(0, 0, -5).Format("2006-01-02"), "finding": "recent"},
		{"id": int64(2), "scan_date": now.AddDate(0, 0, -10).Format("2006-01-02"), "finding": "recent"},
		{"id": int64(3), "scan_date": now.AddDate(0, 0, -60).Format("2006-01-02"), "finding": "old"},
		{"id": int64(4), "scan_date": now.AddDate(0, 0, -90).Format("2006-01-02"), "finding": "old"},
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// The Scout AI pattern: filter on CURRENT_DATE - INTERVAL '30 days'
	result, err := db.Query(ctx, "SELECT id, scan_date, finding FROM findings WHERE scan_date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY id")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	t.Logf("rows returned: %d", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  id=%v scan_date=%v finding=%v", row["id"], row["scan_date"], row["finding"])
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 recent rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row["finding"] != "recent" {
			t.Errorf("expected only 'recent' findings, got %v", row["finding"])
		}
	}
}
