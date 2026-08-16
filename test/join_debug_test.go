package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestJoinSelectStar(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, _ := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})

	eventsSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
		},
	}
	db.CreateTable(ctx, "events", eventsSchema, nil)

	usersSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	db.CreateTable(ctx, "users", usersSchema, nil)

	eIng := db.NewIngester("events", eventsSchema, nil, ingest.Config{MaxBufferRows: 100})
	eIng.Ingest(ctx, []map[string]any{
		{"user_id": "alice", "amount": 100.0},
		{"user_id": "bob", "amount": 200.0},
	})
	eIng.FlushAll(ctx)

	uIng := db.NewIngester("users", usersSchema, nil, ingest.Config{MaxBufferRows: 100})
	uIng.Ingest(ctx, []map[string]any{
		{"user_id": "alice", "name": "Alice"},
		{"user_id": "bob", "name": "Bob"},
	})
	uIng.FlushAll(ctx)

	// SELECT * from join should include all columns from both tables
	r, err := db.Query(ctx, "SELECT * FROM events e JOIN users u ON e.user_id = u.user_id ORDER BY e.amount")
	if err != nil {
		t.Fatal(err)
	}

	// Expect: user_id, amount from events + name from users (user_id deduped or qualified)
	t.Logf("schema: %v", r.Columns)
	for _, row := range r.Rows {
		t.Logf("  row: %v", row)
	}

	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}

	// amount and name must be present (not dropped by OutputFilter)
	row := r.Rows[0]
	if row["amount"] == nil {
		t.Error("amount column missing from SELECT * join output")
	}
	if row["name"] == nil {
		t.Error("name column missing from SELECT * join output")
	}
}
