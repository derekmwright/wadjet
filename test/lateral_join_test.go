package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupOrdersDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Create orders table
	orderSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "customer", Type: parquet.TypeString},
			{Name: "total", Type: parquet.TypeFloat64},
		},
	}
	if err := db.CreateTable(ctx, "orders", orderSchema, nil); err != nil {
		t.Fatal(err)
	}

	// Create line_items table
	itemSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "order_id", Type: parquet.TypeInt64},
			{Name: "product", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
		},
	}
	if err := db.CreateTable(ctx, "line_items", itemSchema, nil); err != nil {
		t.Fatal(err)
	}

	// Insert orders
	orders := []map[string]any{
		{"id": int64(1), "customer": "Alice", "total": 150.0},
		{"id": int64(2), "customer": "Bob", "total": 200.0},
		{"id": int64(3), "customer": "Carol", "total": 0.0},
	}
	oing := db.NewIngester("orders", orderSchema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := oing.Ingest(ctx, orders); err != nil {
		t.Fatal(err)
	}
	if err := oing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Insert line items
	items := []map[string]any{
		{"id": int64(1), "order_id": int64(1), "product": "Widget", "amount": 50.0},
		{"id": int64(2), "order_id": int64(1), "product": "Gadget", "amount": 100.0},
		{"id": int64(3), "order_id": int64(2), "product": "Widget", "amount": 75.0},
		{"id": int64(4), "order_id": int64(2), "product": "Doohickey", "amount": 125.0},
		// Order 3 has no line items
	}
	iing := db.NewIngester("line_items", itemSchema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := iing.Ingest(ctx, items); err != nil {
		t.Fatal(err)
	}
	if err := iing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestLateralJoin_InnerJoin(t *testing.T) {
	db := setupOrdersDB(t)
	ctx := context.Background()

	// LATERAL subquery: get line items for each order
	r, err := db.Query(ctx, `
		SELECT o.customer, li.product, li.amount
		FROM orders o
		JOIN LATERAL (
			SELECT * FROM line_items WHERE order_id = o.id
		) li ON true
		ORDER BY o.customer, li.amount
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Order 3 (Carol) has no items → excluded by INNER JOIN
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows (Alice:2 + Bob:2), got %d: %v", len(r.Rows), r.Rows)
	}

	// Verify ordering: Alice first (50, 100), then Bob (75, 125)
	expected := []struct {
		customer string
		amount   float64
	}{
		{"Alice", 50.0},
		{"Alice", 100.0},
		{"Bob", 75.0},
		{"Bob", 125.0},
	}
	for i, exp := range expected {
		row := r.Rows[i]
		if row["customer"] != exp.customer {
			t.Errorf("row %d: customer=%v, want %s", i, row["customer"], exp.customer)
		}
		if amt, ok := row["amount"].(float64); !ok || amt != exp.amount {
			t.Errorf("row %d: amount=%v, want %f", i, row["amount"], exp.amount)
		}
	}
}

func TestLateralJoin_LeftJoin(t *testing.T) {
	db := setupOrdersDB(t)
	ctx := context.Background()

	// LEFT JOIN LATERAL: orders without items should still appear
	r, err := db.Query(ctx, `
		SELECT o.customer, li.product
		FROM orders o
		LEFT JOIN LATERAL (
			SELECT * FROM line_items WHERE order_id = o.id
		) li ON true
		ORDER BY o.customer, li.product
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Alice:2 + Bob:2 + Carol:1(null) = 5 rows
	if len(r.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	// Carol should have NULL product (LEFT JOIN preserves unmatched)
	var carolFound bool
	for _, row := range r.Rows {
		if row["customer"] == "Carol" {
			carolFound = true
			if row["product"] != nil {
				t.Errorf("Carol should have NULL product, got %v", row["product"])
			}
		}
	}
	if !carolFound {
		t.Error("Carol not found in results")
	}
}

func TestLateralJoin_WithLocalFilter(t *testing.T) {
	db := setupOrdersDB(t)
	ctx := context.Background()

	// LATERAL with both correlated and local predicates
	r, err := db.Query(ctx, `
		SELECT o.customer, li.product, li.amount
		FROM orders o
		JOIN LATERAL (
			SELECT * FROM line_items
			WHERE order_id = o.id AND amount > 60
		) li ON true
		ORDER BY o.customer, li.amount
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Alice: only Gadget (100 > 60), Bob: Widget(75 > 60) + Doohickey(125 > 60)
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}
}

func TestLateralJoin_CrossJoin(t *testing.T) {
	db := setupOrdersDB(t)
	ctx := context.Background()

	// LATERAL via comma syntax (cross join)
	r, err := db.Query(ctx, `
		SELECT o.customer, li.product
		FROM orders o, LATERAL (
			SELECT * FROM line_items WHERE order_id = o.id
		) li
		ORDER BY o.customer, li.product
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(r.Rows), r.Rows)
	}
}

func TestLateralJoin_WithAggregation(t *testing.T) {
	db := setupOrdersDB(t)
	ctx := context.Background()

	// LATERAL subquery with aggregation
	r, err := db.Query(ctx, `
		SELECT o.customer, s.item_count, s.total_amount
		FROM orders o
		JOIN LATERAL (
			SELECT COUNT(*) as item_count, SUM(amount) as total_amount
			FROM line_items
			WHERE order_id = o.id
		) s ON true
		ORDER BY o.customer
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Alice and Bob have line items; Carol (no items) excluded by INNER JOIN
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	// Verify aggregation results
	for _, row := range r.Rows {
		cnt, _ := row["item_count"].(int64)
		if cnt != 2 {
			t.Errorf("expected item_count=2 for %v, got %v", row["customer"], row["item_count"])
		}
	}
}
