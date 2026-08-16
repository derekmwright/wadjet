package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestUnnest_IntValues(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	r, err := db.Query(ctx, "SELECT * FROM unnest(1, 2, 3) AS u(val)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	for i, row := range r.Rows {
		val, ok := row["val"].(int64)
		if !ok {
			t.Errorf("row %d: expected int64, got %T (%v)", i, row["val"], row["val"])
			continue
		}
		if val != int64(i+1) {
			t.Errorf("row %d: val=%d, want %d", i, val, i+1)
		}
	}
}

func TestUnnest_StringValues(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	r, err := db.Query(ctx, "SELECT * FROM unnest('alpha', 'beta', 'gamma') AS u(tag)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	expected := []string{"alpha", "beta", "gamma"}
	for i, row := range r.Rows {
		tag, _ := row["tag"].(string)
		if tag != expected[i] {
			t.Errorf("row %d: tag=%q, want %q", i, tag, expected[i])
		}
	}
}

func TestUnnest_WithOrdinality(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	r, err := db.Query(ctx, "SELECT * FROM unnest(10, 20, 30) WITH ORDINALITY AS u(val, idx)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	for i, row := range r.Rows {
		val, _ := row["val"].(int64)
		idx, _ := row["idx"].(int64)
		expectedVal := int64((i + 1) * 10)
		expectedIdx := int64(i + 1)
		if val != expectedVal {
			t.Errorf("row %d: val=%d, want %d", i, val, expectedVal)
		}
		if idx != expectedIdx {
			t.Errorf("row %d: idx=%d, want %d", i, idx, expectedIdx)
		}
	}
}

func TestUnnest_CrossJoinWithTable(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Create a simple table
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "people", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("people", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"name": "Alice"},
		{"name": "Bob"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// CROSS JOIN with unnest: each person gets every tag
	r, err := db.Query(ctx, `
		SELECT p.name, u.tag
		FROM people p
		CROSS JOIN unnest('admin', 'user') AS u(tag)
		ORDER BY p.name, u.tag
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// 2 people × 2 tags = 4 rows
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	expected := []struct {
		name string
		tag  string
	}{
		{"Alice", "admin"},
		{"Alice", "user"},
		{"Bob", "admin"},
		{"Bob", "user"},
	}
	for i, exp := range expected {
		row := r.Rows[i]
		if row["name"] != exp.name {
			t.Errorf("row %d: name=%v, want %s", i, row["name"], exp.name)
		}
		if row["tag"] != exp.tag {
			t.Errorf("row %d: tag=%v, want %s", i, row["tag"], exp.tag)
		}
	}
}

func TestUnnest_DefaultColumnName(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Without column alias — default column name is "unnest"
	r, err := db.Query(ctx, "SELECT * FROM unnest(42, 99)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	// Default column name should be "unnest"
	val, ok := r.Rows[0]["unnest"].(int64)
	if !ok {
		t.Fatalf("expected column 'unnest' with int64, got %v", r.Rows[0])
	}
	if val != 42 {
		t.Errorf("first row: unnest=%d, want 42", val)
	}
}
