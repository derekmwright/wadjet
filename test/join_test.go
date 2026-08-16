package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupJoinTables(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create events table
	eventsSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
			{Name: "year", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "events", eventsSchema, []string{"year"}); err != nil {
		t.Fatal(err)
	}

	eventRows := []map[string]any{
		{"user_id": "alice", "amount": 100.0, "year": "2026"},
		{"user_id": "bob", "amount": 200.0, "year": "2026"},
		{"user_id": "alice", "amount": 150.0, "year": "2026"},
		{"user_id": "carol", "amount": 300.0, "year": "2026"},
		{"user_id": "unknown", "amount": 50.0, "year": "2026"},
	}

	ing := db.NewIngester("events", eventsSchema, []string{"year"}, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, eventRows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Create users table
	usersSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "name", Type: parquet.TypeString},
			{Name: "year", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "users", usersSchema, []string{"year"}); err != nil {
		t.Fatal(err)
	}

	userRows := []map[string]any{
		{"user_id": "alice", "name": "Alice Smith", "year": "2026"},
		{"user_id": "bob", "name": "Bob Jones", "year": "2026"},
		{"user_id": "carol", "name": "Carol White", "year": "2026"},
	}

	ing2 := db.NewIngester("users", usersSchema, []string{"year"}, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing2.Ingest(ctx, userRows); err != nil {
		t.Fatal(err)
	}
	if err := ing2.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestJoinQuery(t *testing.T) {
	db := setupJoinTables(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT e.user_id, e.amount, u.name FROM events e JOIN users u ON e.user_id = u.user_id")
	if err != nil {
		t.Fatal(err)
	}

	// Inner join: "unknown" has no match in users, so 4 rows expected
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows from inner join, got %d", len(result.Rows))
	}

	t.Logf("Inner JOIN results (%d rows):", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  user_id=%v amount=%v name=%v", row["user_id"], row["amount"], row["name"])
	}

	// All rows should have a name
	for _, row := range result.Rows {
		if row["name"] == nil {
			t.Fatalf("expected non-nil name in inner join result: %v", row)
		}
	}
}

func TestLeftJoinQuery(t *testing.T) {
	db := setupJoinTables(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT e.user_id, e.amount, u.name FROM events e LEFT JOIN users u ON e.user_id = u.user_id")
	if err != nil {
		t.Fatal(err)
	}

	// Left join: all 5 left rows preserved, "unknown" gets null name
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows from left join, got %d", len(result.Rows))
	}

	t.Logf("Left JOIN results (%d rows):", len(result.Rows))
	hasNull := false
	for _, row := range result.Rows {
		t.Logf("  user_id=%v amount=%v name=%v", row["user_id"], row["amount"], row["name"])
		if row["user_id"] == "unknown" && row["name"] == nil {
			hasNull = true
		}
	}

	if !hasNull {
		t.Fatal("expected null name for 'unknown' user in left join")
	}
}

func TestJoinWithAggregate(t *testing.T) {
	db := setupJoinTables(t)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT e.user_id, SUM(e.amount) as total FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY e.user_id ORDER BY total DESC")
	if err != nil {
		t.Fatal(err)
	}

	// 3 users matched (alice, bob, carol), "unknown" filtered by inner join
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(result.Rows))
	}

	t.Logf("JOIN + GROUP BY results:")
	for _, row := range result.Rows {
		t.Logf("  user_id=%v total=%v", row["user_id"], row["total"])
	}

	// carol should be first (300), then alice (250), then bob (200)
	if result.Rows[0]["user_id"] != "carol" {
		t.Fatalf("expected carol first, got %v", result.Rows[0]["user_id"])
	}
}
