package test

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/caelum"
	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func setupDMLTestDB(t *testing.T) *caelum.DB {
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
			{Name: "name", Type: parquet.TypeString},
			{Name: "age", Type: parquet.TypeInt64},
		},
	}

	if err := db.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Seed with data
	rows := []map[string]any{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
		{"id": int64(3), "name": "Charlie", "age": int64(35)},
		{"id": int64(4), "name": "Diana", "age": int64(28)},
		{"id": int64(5), "name": "Eve", "age": int64(22)},
	}

	ing := db.NewIngester("users", schema, nil, ingest.Config{
		MaxBufferRows: 100,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func queryCount(t *testing.T, db *caelum.DB, sql string) int {
	t.Helper()
	ctx := context.Background()
	result, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("Query(%q) error: %v", sql, err)
	}
	return len(result.Rows)
}

func queryRows(t *testing.T, db *caelum.DB, sql string) []map[string]any {
	t.Helper()
	ctx := context.Background()
	result, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("Query(%q) error: %v", sql, err)
	}
	return result.Rows
}

func TestInsert_Basic(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	before := queryCount(t, db, "SELECT * FROM users")

	result, err := db.Execute(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'Frank', 40)")
	if err != nil {
		t.Fatalf("Execute INSERT: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", result.RowsAffected)
	}
	if result.Command != "INSERT" {
		t.Fatalf("expected command INSERT, got %q", result.Command)
	}

	after := queryCount(t, db, "SELECT * FROM users")
	if after != before+1 {
		t.Fatalf("expected %d rows after insert, got %d", before+1, after)
	}
}

func TestInsert_MultipleRows(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'Frank', 40), (7, 'Grace', 33)")
	if err != nil {
		t.Fatalf("Execute INSERT: %v", err)
	}
	if result.RowsAffected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", result.RowsAffected)
	}

	after := queryCount(t, db, "SELECT * FROM users")
	if after != 7 { // 5 original + 2 new
		t.Fatalf("expected 7 rows, got %d", after)
	}
}

func TestDelete_WithWhere(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "DELETE FROM users WHERE id = 3")
	if err != nil {
		t.Fatalf("Execute DELETE: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expected 1 row deleted, got %d", result.RowsAffected)
	}
	if result.Command != "DELETE" {
		t.Fatalf("expected command DELETE, got %q", result.Command)
	}

	// Charlie (id=3) should be gone
	after := queryCount(t, db, "SELECT * FROM users")
	if after != 4 {
		t.Fatalf("expected 4 rows after delete, got %d", after)
	}
}

func TestDelete_AllRows(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "DELETE FROM users")
	if err != nil {
		t.Fatalf("Execute DELETE: %v", err)
	}
	if result.RowsAffected != 5 {
		t.Fatalf("expected 5 rows deleted, got %d", result.RowsAffected)
	}

	after := queryCount(t, db, "SELECT * FROM users")
	if after != 0 {
		t.Fatalf("expected 0 rows after full delete, got %d", after)
	}
}

func TestDelete_NoMatch(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "DELETE FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Execute DELETE: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Fatalf("expected 0 rows deleted, got %d", result.RowsAffected)
	}

	after := queryCount(t, db, "SELECT * FROM users")
	if after != 5 {
		t.Fatalf("expected 5 rows unchanged, got %d", after)
	}
}

func TestUpdate_Basic(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "UPDATE users SET name = 'Robert' WHERE id = 2")
	if err != nil {
		t.Fatalf("Execute UPDATE: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expected 1 row updated, got %d", result.RowsAffected)
	}
	if result.Command != "UPDATE" {
		t.Fatalf("expected command UPDATE, got %q", result.Command)
	}

	// Total count should be unchanged
	after := queryCount(t, db, "SELECT * FROM users")
	if after != 5 {
		t.Fatalf("expected 5 rows (count unchanged), got %d", after)
	}

	// The updated row should have the new name
	rows := queryRows(t, db, "SELECT * FROM users WHERE name = 'Robert'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row with name 'Robert', got %d", len(rows))
	}
}

func TestUpdate_MultipleColumns(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "UPDATE users SET name = 'Robert', age = 50 WHERE id = 2")
	if err != nil {
		t.Fatalf("Execute UPDATE: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expected 1 row updated, got %d", result.RowsAffected)
	}

	rows := queryRows(t, db, "SELECT * FROM users WHERE name = 'Robert'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row with name 'Robert', got %d", len(rows))
	}
}

func TestUpdate_NoMatch(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	result, err := db.Execute(ctx, "UPDATE users SET name = 'Nobody' WHERE id = 999")
	if err != nil {
		t.Fatalf("Execute UPDATE: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Fatalf("expected 0 rows updated, got %d", result.RowsAffected)
	}
}

func TestDML_ViaQueryDispatch(t *testing.T) {
	// Test that DML works through the Query() method (used by pgwire)
	db := setupDMLTestDB(t)
	ctx := context.Background()

	// INSERT via Query
	result, err := db.Query(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'Frank', 40)")
	if err != nil {
		t.Fatalf("Query INSERT: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["result"] != "INSERT 1" {
		t.Fatalf("expected result 'INSERT 1', got %v", result.Rows)
	}

	// DELETE via Query
	result, err = db.Query(ctx, "DELETE FROM users WHERE id = 6")
	if err != nil {
		t.Fatalf("Query DELETE: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["result"] != "DELETE 1" {
		t.Fatalf("expected result 'DELETE 1', got %v", result.Rows)
	}

	// UPDATE via Query
	result, err = db.Query(ctx, "UPDATE users SET age = 99 WHERE id = 1")
	if err != nil {
		t.Fatalf("Query UPDATE: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["result"] != "UPDATE 1" {
		t.Fatalf("expected result 'UPDATE 1', got %v", result.Rows)
	}
}

func TestDelete_ThenInsert(t *testing.T) {
	// Verify that delete + insert works correctly (row count is consistent)
	db := setupDMLTestDB(t)
	ctx := context.Background()

	// Delete 2 rows
	_, err := db.Execute(ctx, "DELETE FROM users WHERE age < 26")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	count := queryCount(t, db, "SELECT * FROM users")
	if count != 3 { // Eve(22) and Bob(25) deleted
		t.Fatalf("expected 3 rows after delete, got %d", count)
	}

	// Insert 1 new row
	_, err = db.Execute(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'Zara', 45)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	count = queryCount(t, db, "SELECT * FROM users")
	if count != 4 {
		t.Fatalf("expected 4 rows after delete+insert, got %d", count)
	}
}

func TestInsert_WrongColumnCount(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	_, err := db.Execute(ctx, "INSERT INTO users (id, name) VALUES (6, 'Frank', 40)")
	if err == nil {
		t.Fatal("expected error for column count mismatch")
	}
}

func TestDelete_NonexistentTable(t *testing.T) {
	db := setupDMLTestDB(t)
	ctx := context.Background()

	_, err := db.Execute(ctx, "DELETE FROM nonexistent WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}
