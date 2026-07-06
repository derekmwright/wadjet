package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestAnalyzeTableSQL exercises the ANALYZE TABLE SQL command end-to-end: it
// must parse, dispatch through DB.Query, run catalog.AnalyzeTable over the
// table's parquet files, and leave the planner's column statistics (HLL NDV)
// populated and accurate.
func TestAnalyzeTableSQL(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "grp", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "metrics", schema, nil); err != nil {
		t.Fatal(err)
	}

	const nRows = 200
	grps := []string{"a", "b", "c", "d"} // NDV 4
	rows := make([]map[string]any, nRows)
	for i := 0; i < nRows; i++ {
		rows[i] = map[string]any{"id": int64(i), "grp": grps[i%len(grps)]} // id NDV 200
	}
	ing := db.NewIngester("metrics", schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// The command itself.
	res, err := db.Query(ctx, "ANALYZE TABLE metrics")
	if err != nil {
		t.Fatalf("ANALYZE TABLE: %v", err)
	}
	if len(res.Rows) != 1 || !strings.Contains(strings.ToLower(res.Rows[0]["result"].(string)), "analyzed") {
		t.Fatalf("unexpected ANALYZE result: %+v", res.Rows)
	}

	// Planner-facing stats must now carry accurate NDV.
	stats, err := db.Catalog().AggregateColumnStats(ctx, "metrics")
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}
	if cs, ok := stats["grp"]; !ok || cs.NDV != 4 {
		t.Errorf("grp NDV = %d (ok=%v), want 4", stats["grp"].NDV, ok)
	}
	if cs, ok := stats["id"]; !ok || cs.NDV < 190 || cs.NDV > 210 {
		t.Errorf("id NDV = %d (ok=%v), want ~200 (HLL error bound)", stats["id"].NDV, ok)
	}

	// ANALYZE also persists the RG-metadata blob; scans now plan from it
	// instead of reading parquet footers. Queries — including one whose
	// predicate exercises row-group pruning against blob stats — must
	// return identical results through the fast path.
	if m, err := db.Catalog().TableRGMeta(ctx, "metrics"); err != nil || len(m) == 0 {
		t.Fatalf("TableRGMeta after ANALYZE: %d files (err %v), want >0", len(m), err)
	}
	res, err = db.Query(ctx, "SELECT count(*) AS n FROM metrics WHERE id >= 150")
	if err != nil {
		t.Fatalf("post-ANALYZE pruning query: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(50) {
		t.Errorf("post-ANALYZE count = %v, want 50", n)
	}

	// Bareword form (no TABLE keyword) also works.
	if _, err := db.Query(ctx, "ANALYZE metrics"); err != nil {
		t.Fatalf("ANALYZE metrics (bareword): %v", err)
	}

	// Unknown table errors, naming the table.
	_, err = db.Query(ctx, "ANALYZE TABLE nope")
	if err == nil {
		t.Fatal("ANALYZE of a nonexistent table must error")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error should name the table: %v", err)
	}
}
