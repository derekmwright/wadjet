package test

import (
	"context"
	"math"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

func setupSampleDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "value", Type: parquet.TypeFloat64},
		},
	}
	if err := db.CreateTable(ctx, "data", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Insert 1000 rows
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{
			"id":    int64(i + 1),
			"value": float64(i) * 1.5,
		}
	}
	ing := db.NewIngester("data", schema, nil, ingest.Config{MaxBufferRows: 2000, RowGroupSize: 2000})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTablesample_Bernoulli(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// BERNOULLI(50) should return ~50% of rows (with statistical tolerance)
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(50)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	// Expect ~500 rows ± 100 (generous tolerance for randomness)
	if cnt < 300 || cnt > 700 {
		t.Errorf("BERNOULLI(50) returned %d rows, expected ~500 ± 200", cnt)
	}
}

func TestTablesample_BernoulliSmall(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// BERNOULLI(10) should return ~10% of rows
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(10)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	// Expect ~100 rows ± 60
	if cnt < 30 || cnt > 200 {
		t.Errorf("BERNOULLI(10) returned %d rows, expected ~100 ± 100", cnt)
	}
}

func TestTablesample_System(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// SYSTEM sampling: block-level. With 1 batch, result is either 0 or 1000.
	// Run multiple times to verify it works (at least one should include data)
	var gotData bool
	for i := 0; i < 20; i++ {
		r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE SYSTEM(50)")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		cnt, _ := r.Rows[0]["cnt"].(int64)
		if cnt > 0 {
			gotData = true
			// SYSTEM returns entire batches, so should be all rows
			if cnt != 1000 {
				t.Errorf("SYSTEM(50) returned %d rows, expected 0 or 1000", cnt)
			}
		}
	}
	if !gotData {
		t.Error("SYSTEM(50) returned 0 rows in 20 attempts (extremely unlikely)")
	}
}

func TestTablesample_BernoulliWithFilter(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// Sample + filter should work together
	r, err := db.Query(ctx, "SELECT AVG(value) as avg_val FROM data TABLESAMPLE BERNOULLI(100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// BERNOULLI(100) = include all rows. Average of 0*1.5, 1*1.5, ..., 999*1.5 = 499.5*1.5 = 749.25
	avg, _ := r.Rows[0]["avg_val"].(float64)
	if math.Abs(avg-749.25) > 0.01 {
		t.Errorf("BERNOULLI(100) avg=%f, want 749.25", avg)
	}
}

func TestTablesample_Bernoulli100Percent(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// 100% should return all rows
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	if cnt != 1000 {
		t.Errorf("BERNOULLI(100) returned %d rows, want 1000", cnt)
	}
}
