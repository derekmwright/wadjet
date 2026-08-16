package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupMergeDB(t *testing.T) *wadjet.DB {
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
			{Name: "name", Type: parquet.TypeString},
			{Name: "price", Type: parquet.TypeFloat64},
		},
	}

	// Create target table (products)
	if err := db.CreateTable(ctx, "products", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("products", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "name": "Widget", "price": 10.0},
		{"id": int64(2), "name": "Gadget", "price": 20.0},
		{"id": int64(3), "name": "Doohickey", "price": 30.0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Create source table (updates)
	if err := db.CreateTable(ctx, "updates", schema, nil); err != nil {
		t.Fatal(err)
	}
	uing := db.NewIngester("updates", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := uing.Ingest(ctx, []map[string]any{
		{"id": int64(2), "name": "Gadget Pro", "price": 25.0}, // existing: update
		{"id": int64(4), "name": "Thingamajig", "price": 40.0}, // new: insert
	}); err != nil {
		t.Fatal(err)
	}
	if err := uing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestMerge_UpdateAndInsert(t *testing.T) {
	db := setupMergeDB(t)
	ctx := context.Background()

	// MERGE: update existing, insert new
	r, err := db.Query(ctx, `
		MERGE INTO products t
		USING updates s
		ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET name = s.name, price = s.price
		WHEN NOT MATCHED THEN INSERT (id, name, price) VALUES (s.id, s.name, s.price)
	`)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}

	// Should affect 2 rows (1 update + 1 insert)
	if r.Rows[0]["result"] != "MERGE 2" {
		t.Fatalf("expected MERGE 2, got %v", r.Rows[0]["result"])
	}

	// Verify final state
	result, err := db.Query(ctx, "SELECT id, name, price FROM products ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 products, got %d: %v", len(result.Rows), result.Rows)
	}

	expected := map[int64]string{
		1: "Widget",      // unchanged
		2: "Gadget Pro",  // updated
		3: "Doohickey",   // unchanged
		4: "Thingamajig", // inserted
	}
	for _, row := range result.Rows {
		id, _ := row["id"].(int64)
		name, _ := row["name"].(string)
		if want, ok := expected[id]; ok && name != want {
			t.Errorf("id=%d: name=%q, want %q", id, name, want)
		}
	}
}

func TestMerge_MatchedDelete(t *testing.T) {
	db := setupMergeDB(t)
	ctx := context.Background()

	// MERGE with DELETE for matched rows
	_, err := db.Query(ctx, `
		MERGE INTO products t
		USING updates s
		ON t.id = s.id
		WHEN MATCHED THEN DELETE
	`)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}

	// Verify: id=2 (Gadget) should be deleted
	result, err := db.Query(ctx, "SELECT id FROM products ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 products after MERGE DELETE, got %d: %v", len(result.Rows), result.Rows)
	}

	ids := make(map[int64]bool)
	for _, row := range result.Rows {
		id, _ := row["id"].(int64)
		ids[id] = true
	}
	if ids[2] {
		t.Error("id=2 should have been deleted")
	}
}

func TestMerge_InsertOnly(t *testing.T) {
	db := setupMergeDB(t)
	ctx := context.Background()

	// MERGE with only NOT MATCHED clause
	r, err := db.Query(ctx, `
		MERGE INTO products t
		USING updates s
		ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT (id, name, price) VALUES (s.id, s.name, s.price)
	`)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}

	// Only 1 row inserted (id=4, new); id=2 already exists
	if r.Rows[0]["result"] != "MERGE 1" {
		t.Fatalf("expected MERGE 1, got %v", r.Rows[0]["result"])
	}

	result, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM products")
	if err != nil {
		t.Fatal(err)
	}
	if cnt, _ := result.Rows[0]["cnt"].(int64); cnt != 4 {
		t.Fatalf("expected 4 products, got %d", cnt)
	}
}
