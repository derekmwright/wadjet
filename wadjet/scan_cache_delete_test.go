package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestScanCacheReplayHonorsDeleteMarkers: a table scanned twice in one
// query goes through the duplicate-scan cache (mergeDuplicateScans).
// Delete markers reach the scan as batch selection vectors — and the
// cache's replay path used to drop Sel from its shallow clones,
// resurrecting deleted rows for whichever consumer replayed. The
// self-join is built so a resurrected row on EITHER side changes the
// count (which side populates vs replays is a race), making the
// assertion deterministic.
//
// Rows: id 0..99, id2 = id+10. Join a.id = b.id2 pairs a(k+10) with
// b(k) for k = 0..89. DELETE 40 <= id < 60 kills a-side ids [40,60)
// (k in [30,50)) and b-side ids [40,60) (k in [40,60)) — correct count
// is 90 - |[30,60)| = 60. Resurrecting either side adds its 10
// non-overlapping k values back (a-side: [30,40); b-side: [50,60)),
// so the buggy result is 70 regardless of which side replayed.
func TestScanCacheReplayHonorsDeleteMarkers(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "id2", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "pairs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "id2": int64(i + 10)}
	}
	ing := db.NewIngester("pairs", schema, nil, ingest.Config{MaxBufferRows: 10000})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Sanity pre-delete: 90 pairs.
	res, err := db.Query(ctx, "SELECT COUNT(*) AS n FROM pairs a JOIN pairs b ON a.id = b.id2")
	if err != nil {
		t.Fatalf("pre-delete join: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(90) {
		t.Fatalf("pre-delete count = %v, want 90", n)
	}

	if _, err := db.Query(ctx, "DELETE FROM pairs WHERE id >= 40 AND id < 60"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err = db.Query(ctx, "SELECT COUNT(*) AS n FROM pairs a JOIN pairs b ON a.id = b.id2")
	if err != nil {
		t.Fatalf("post-delete join: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(60) {
		t.Fatalf("post-delete count = %v, want 60 (70 = a deleted-row side was resurrected from the scan cache)", n)
	}
}
