package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Bare COUNT(*) answers from the catalog manifest (metadata_count.go) —
// no scan. This pins the value against real data, the engagement
// counter, and the bail conditions: any filter, COUNT(col), COUNT
// alongside other aggregates, and merge-on-read delete markers must all
// fall back to the scan pipeline and stay correct.
func TestMetadataCountStar(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	const n = 500
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"id": int64(i)}
		if i%5 != 0 {
			r["v"] = int64(i)
		}
		rows[i] = r
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	count := func(sql string) int64 {
		t.Helper()
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: %d rows", sql, len(res.Rows))
		}
		for _, v := range res.Rows[0] {
			if c, ok := v.(int64); ok {
				return c
			}
		}
		t.Fatalf("%s: no int64 in %v", sql, res.Rows[0])
		return -1
	}

	before := physical.MetadataCountsPlanned.Load()
	if got := count("SELECT COUNT(*) FROM t"); got != n {
		t.Fatalf("COUNT(*) = %d, want %d", got, n)
	}
	if physical.MetadataCountsPlanned.Load() != before+1 {
		t.Fatal("metadata COUNT(*) did not engage")
	}

	// Bail conditions: correct values via the scan pipeline, no engagement.
	before = physical.MetadataCountsPlanned.Load()
	if got := count("SELECT COUNT(*) FROM t WHERE id < 100"); got != 100 {
		t.Fatalf("filtered COUNT(*) = %d", got)
	}
	if got := count("SELECT COUNT(v) FROM t"); got != n-n/5 {
		t.Fatalf("COUNT(v) = %d, want %d", got, n-n/5)
	}
	res, err := db.Query(ctx, "SELECT COUNT(*) AS c, SUM(id) AS s FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0]["c"] != int64(n) {
		t.Fatalf("multi-agg COUNT = %v", res.Rows[0])
	}
	if physical.MetadataCountsPlanned.Load() != before {
		t.Fatal("a bail-condition query engaged the metadata path")
	}

	// Merge-on-read deletes: manifest NumRows overcounts, so the fast
	// path must bail and the scan must respect the markers.
	if _, err := db.Execute(ctx, "DELETE FROM t WHERE id >= 400"); err != nil {
		t.Fatal(err)
	}
	before = physical.MetadataCountsPlanned.Load()
	if got := count("SELECT COUNT(*) FROM t"); got != 400 {
		t.Fatalf("post-delete COUNT(*) = %d, want 400", got)
	}
	if physical.MetadataCountsPlanned.Load() != before {
		t.Fatal("metadata COUNT(*) engaged despite delete markers")
	}
}
