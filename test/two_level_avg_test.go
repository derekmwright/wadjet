package test

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// AVG alongside COUNT(DISTINCT x): the grouped form stays on the
// distinct-set path (extra-aggregate gate, Q10 metal lesson) and the
// global form rides the two-level rewrite via SUM+COUNT partials and a
// division projection — both must agree with hand-computed values. NULL
// behavior: AVG skips NULL inputs; an all-NULL group yields NULL
// (division by zero count reads NULL).
func TestTwoLevelDistinctAvg(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ev", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"k": int64(1), "x": int64(10), "v": int64(10)},
		{"k": int64(1), "x": int64(10), "v": int64(20)},
		{"k": int64(1), "x": int64(11), "v": nil},
		{"k": int64(2), "x": int64(20), "v": nil},
		{"k": int64(2), "x": int64(21), "v": nil},
	}
	ing := db.NewIngester("ev", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := db.Query(ctx, "SELECT k, COUNT(DISTINCT x) AS d, AVG(v) AS a, COUNT(*) AS c FROM ev GROUP BY k ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("got %d groups, want 2: %v", len(r.Rows), r.Rows)
	}
	g1, g2 := r.Rows[0], r.Rows[1]
	if d, _ := g1["d"].(int64); d != 2 {
		t.Fatalf("k=1 distinct = %v, want 2", g1["d"])
	}
	if a := toFloat64(g1["a"]); math.Abs(a-15.0) > 1e-9 {
		t.Fatalf("k=1 avg = %v, want 15", g1["a"])
	}
	// COUNT recombines as SUM of partials, which boxes numerically (#296)
	// — compare by value, matching the pinned distributed-distinct tests.
	if c := toFloat64(g1["c"]); c != 3 {
		t.Fatalf("k=1 count = %v, want 3", g1["c"])
	}
	if d, _ := g2["d"].(int64); d != 2 {
		t.Fatalf("k=2 distinct = %v, want 2", g2["d"])
	}
	if g2["a"] != nil {
		t.Fatalf("k=2 avg = %v (%T), want NULL", g2["a"], g2["a"])
	}

	// Global form: the AVG decomposition + finalization projection fire.
	r, err = db.Query(ctx, "SELECT COUNT(DISTINCT x) AS d, AVG(v) AS a, COUNT(*) AS c FROM ev")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("global: %d rows, want 1", len(r.Rows))
	}
	gr := r.Rows[0]
	if d := toFloat64(gr["d"]); d != 4 {
		t.Fatalf("global distinct = %v, want 4", gr["d"])
	}
	if a := toFloat64(gr["a"]); math.Abs(a-15.0) > 1e-9 {
		t.Fatalf("global avg = %v, want 15", gr["a"])
	}
	if c := toFloat64(gr["c"]); c != 5 {
		t.Fatalf("global count = %v, want 5", gr["c"])
	}
}
