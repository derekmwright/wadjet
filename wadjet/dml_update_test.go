package wadjet

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression test for sweep finding #19: executeUpdate boxed every row of
// every file (even at zero WHERE selectivity) and accumulated all updated
// rows table-wide before one Ingest — a broad UPDATE held the whole table
// as boxed maps. The rewrite streams per file: matched-only boxing,
// per-file markers + ingest. This test pins the end-to-end semantics of
// that rewrite across multiple files.
func TestExecuteUpdate_PerFileStreaming(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "status", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "jobs", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Two separate ingest flushes → two files, so the per-file loop runs
	// more than once.
	ing := db.NewIngester("jobs", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "status": "new"},
		{"id": int64(2), "status": "new"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(3), "status": "new"},
		{"id": int64(4), "status": "done"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Execute(ctx, "UPDATE jobs SET status = 'archived' WHERE status = 'new'")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", res.RowsAffected)
	}

	q, err := db.Query(ctx, "SELECT id, status FROM jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Rows) != 4 {
		t.Fatalf("got %d rows after UPDATE, want 4: %v", len(q.Rows), q.Rows)
	}
	statusByID := map[int64]string{}
	for _, r := range q.Rows {
		statusByID[r["id"].(int64)] = r["status"].(string)
	}
	for _, id := range []int64{1, 2, 3} {
		if statusByID[id] != "archived" {
			t.Errorf("id %d status = %q, want archived", id, statusByID[id])
		}
	}
	if statusByID[4] != "done" {
		t.Errorf("id 4 status = %q, want done (untouched)", statusByID[4])
	}

	// Zero-selectivity UPDATE must be a clean no-op.
	res, err = db.Execute(ctx, "UPDATE jobs SET status = 'x' WHERE status = 'nope'")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("zero-selectivity RowsAffected = %d, want 0", res.RowsAffected)
	}
}
