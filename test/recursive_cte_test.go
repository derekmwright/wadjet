package test

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

func setupHierarchyDB(t *testing.T) *wadjet.DB {
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
			{Name: "parent_id", Type: parquet.TypeInt64, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "employees", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Org chart:
	//   1 CEO (no parent)
	//   ├── 2 VP Engineering
	//   │   ├── 4 Sr Engineer
	//   │   └── 5 Jr Engineer
	//   └── 3 VP Sales
	//       └── 6 Sales Rep
	rows := []map[string]any{
		{"id": int64(1), "name": "CEO", "parent_id": nil},
		{"id": int64(2), "name": "VP Engineering", "parent_id": int64(1)},
		{"id": int64(3), "name": "VP Sales", "parent_id": int64(1)},
		{"id": int64(4), "name": "Sr Engineer", "parent_id": int64(2)},
		{"id": int64(5), "name": "Jr Engineer", "parent_id": int64(2)},
		{"id": int64(6), "name": "Sales Rep", "parent_id": int64(3)},
	}

	ing := db.NewIngester("employees", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRecursiveCTE_Hierarchy(t *testing.T) {
	db := setupHierarchyDB(t)
	ctx := context.Background()

	// Walk the org chart starting from CEO
	sql := `
		WITH RECURSIVE org AS (
			SELECT id, name, parent_id, 0 AS depth
			FROM employees
			WHERE parent_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.parent_id, o.depth + 1
			FROM employees e
			JOIN org o ON e.parent_id = o.id
		)
		SELECT id, name, depth FROM org ORDER BY id
	`
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows (all employees), got %d: %v", len(r.Rows), r.Rows)
	}

	// CEO should have depth 0
	ceo := r.Rows[0]
	if ceo["name"] != "CEO" {
		t.Errorf("first row expected CEO, got %v", ceo["name"])
	}

	// Check that we got all 6 employees
	names := make(map[string]bool)
	for _, row := range r.Rows {
		if name, ok := row["name"].(string); ok {
			names[name] = true
		}
	}
	for _, expected := range []string{"CEO", "VP Engineering", "VP Sales", "Sr Engineer", "Jr Engineer", "Sales Rep"} {
		if !names[expected] {
			t.Errorf("missing employee: %s", expected)
		}
	}
}

func TestRecursiveCTE_GenerateSeries(t *testing.T) {
	// Use recursive CTE to generate a series (synthetic, no table needed)
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	sql := `
		WITH RECURSIVE cnt(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM cnt WHERE n < 10
		)
		SELECT n FROM cnt ORDER BY n
	`
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	// Verify sequence 1..10
	for i, row := range r.Rows {
		var n int64
		switch v := row["n"].(type) {
		case int64:
			n = v
		case float64:
			n = int64(v)
		default:
			t.Fatalf("row %d: unexpected type %T", i, row["n"])
		}
		if n != int64(i+1) {
			t.Errorf("row %d: expected %d, got %d", i, i+1, n)
		}
	}
}

func TestRecursiveCTE_Fibonacci(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Generate first 10 Fibonacci numbers
	sql := `
		WITH RECURSIVE fib(n, a, b) AS (
			SELECT 1, 0, 1
			UNION ALL
			SELECT n + 1, b, a + b FROM fib WHERE n < 10
		)
		SELECT n, a FROM fib ORDER BY n
	`
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d: %v", len(r.Rows), r.Rows)
	}

	// Expected: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
	expected := []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}
	for i, row := range r.Rows {
		var a int64
		switch v := row["a"].(type) {
		case int64:
			a = v
		case float64:
			a = int64(v)
		default:
			t.Fatalf("row %d: unexpected type %T for 'a'", i, row["a"])
		}
		if a != expected[i] {
			t.Errorf("fib(%d) = %d, want %d", i+1, a, expected[i])
		}
	}
}

func TestRecursiveCTE_EmptyAnchor(t *testing.T) {
	db := setupHierarchyDB(t)
	ctx := context.Background()

	// Anchor returns no rows — result should be empty
	sql := `
		WITH RECURSIVE org AS (
			SELECT id, name, parent_id
			FROM employees
			WHERE id = 999
			UNION ALL
			SELECT e.id, e.name, e.parent_id
			FROM employees e
			JOIN org o ON e.parent_id = o.id
		)
		SELECT * FROM org
	`
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Errorf("expected 0 rows for empty anchor, got %d", len(r.Rows))
	}
}

func TestRecursiveCTE_SubtreeWalk(t *testing.T) {
	db := setupHierarchyDB(t)
	ctx := context.Background()

	// Walk subtree starting from VP Engineering (id=2)
	sql := `
		WITH RECURSIVE team AS (
			SELECT id, name FROM employees WHERE id = 2
			UNION ALL
			SELECT e.id, e.name FROM employees e JOIN team t ON e.parent_id = t.id
		)
		SELECT name FROM team ORDER BY name
	`
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	// VP Engineering + Sr Engineer + Jr Engineer = 3
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}
}
