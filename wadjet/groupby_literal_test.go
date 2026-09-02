package wadjet

import (
	"context"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Literal GROUP BY keys are elided from the aggregate key set (they can't
// affect grouping) and re-attached as constant output columns — ClickBench
// Q35's `GROUP BY 1, URL` must group exactly like `GROUP BY URL`. The
// elision must NOT change semantics on the edges: an all-literal GROUP BY
// still groups (one row on data, zero rows on empty input).
func TestGroupByLiteralKeyElision(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "empty", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"k": "a", "v": int64(1)},
		{"k": "b", "v": int64(2)},
		{"k": "a", "v": int64(3)},
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Literal + column key: groups by k alone, constant column carried.
	res, err := db.Query(ctx, "SELECT 1, k, SUM(v) AS s FROM t GROUP BY 1, k ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows: %v", len(res.Rows), res.Rows)
	}
	wantS := map[string]int64{"a": 4, "b": 2}
	for _, r := range res.Rows {
		k := r["k"].(string)
		var s int64
		switch v := r["s"].(type) {
		case int64:
			s = v
		case float64:
			s = int64(v)
		case string:
			// SUM over an INT64 column is PostgreSQL's `numeric` since #784,
			// so it boxes as DECIMAL text. This test is about the literal key
			// elision, not the carrier.
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatalf("k=%v: SUM did not render as an integer: %v", r["k"], err)
			}
			s = n
		}
		if s != wantS[k] {
			t.Fatalf("k=%s sum=%d want %d (row %v)", k, s, wantS[k], r)
		}
		if v, ok := r["1"]; !ok || v == nil {
			t.Fatalf("constant column missing/null in %v", r)
		}
	}

	// All-literal GROUP BY over data: one group.
	res, err = db.Query(ctx, "SELECT COUNT(*) AS c FROM t GROUP BY 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("all-literal group over data: %d rows (%v)", len(res.Rows), res.Rows)
	}

	// All-literal GROUP BY over empty input: ZERO rows (not a scalar-agg
	// zero row) — the elision must keep at least one key to preserve this.
	res, err = db.Query(ctx, "SELECT COUNT(*) AS c FROM empty GROUP BY 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("all-literal group over empty input: %d rows (%v), want 0", len(res.Rows), res.Rows)
	}
}
