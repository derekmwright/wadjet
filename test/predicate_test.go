package test

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
	"github.com/derekmwright/caelum/pkg/caelum"
)

func setupPredicateTable(t *testing.T) *caelum.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := caelum.Open(ctx, caelum.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "status", Type: parquet.TypeString},
			{Name: "score", Type: parquet.TypeFloat64, Nullable: true},
			{Name: "year", Type: parquet.TypeString},
		},
	}

	if err := db.CreateTable(ctx, "items", schema, []string{"year"}); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int64(1), "status": "active", "score": 10.0, "year": "2026"},
		{"id": int64(2), "status": "inactive", "score": 20.0, "year": "2026"},
		{"id": int64(3), "status": "active", "score": 30.0, "year": "2026"},
		{"id": int64(4), "status": "pending", "score": 40.0, "year": "2026"},
		{"id": int64(5), "status": "active", "score": 50.0, "year": "2026"},
		{"id": int64(6), "status": "inactive", "score": nil, "year": "2026"},
		{"id": int64(7), "status": "active", "score": 70.0, "year": "2026"},
		{"id": int64(8), "status": "pending", "score": 80.0, "year": "2026"},
		{"id": int64(9), "status": "active", "score": 90.0, "year": "2026"},
		{"id": int64(10), "status": "active", "score": 100.0, "year": "2026"},
	}

	ing := db.NewIngester("items", schema, []string{"year"}, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestPredicateBetween(t *testing.T) {
	db := setupPredicateTable(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM items WHERE score between 20 and 50")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("BETWEEN 20 AND 50: %d rows", len(result.Rows))
	for _, row := range result.Rows {
		score := row["score"]
		t.Logf("  id=%v score=%v", row["id"], score)
		if score != nil {
			s := score.(float64)
			if s < 20 || s > 50 {
				t.Fatalf("score %v outside BETWEEN range", s)
			}
		}
	}

	// ids 2(20), 3(30), 4(40), 5(50) = 4 rows
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
}

func TestPredicateIn(t *testing.T) {
	db := setupPredicateTable(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM items WHERE status in ('active', 'pending')")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("IN ('active', 'pending'): %d rows", len(result.Rows))
	for _, row := range result.Rows {
		status := row["status"]
		t.Logf("  id=%v status=%v", row["id"], status)
		if status != "active" && status != "pending" {
			t.Fatalf("unexpected status: %v", status)
		}
	}

	// active: 1,3,5,7,9,10; pending: 4,8 = 8 rows
	if len(result.Rows) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(result.Rows))
	}
}

func TestPredicateComparison(t *testing.T) {
	db := setupPredicateTable(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM items WHERE score >= 70")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("score >= 70: %d rows", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  id=%v score=%v", row["id"], row["score"])
	}

	// 70, 80, 90, 100 = 4 rows
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
}

func TestPredicateEquality(t *testing.T) {
	db := setupPredicateTable(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM items WHERE status = 'inactive'")
	if err != nil {
		t.Fatal(err)
	}

	// ids 2, 6
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row["status"] != "inactive" {
			t.Fatalf("expected status=inactive, got %v", row["status"])
		}
	}
}
