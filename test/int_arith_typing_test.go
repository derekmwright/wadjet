package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Integer-preserving arithmetic must survive PLANNING, not just runtime
// (#297): inferProjectionType declared every BinaryOp Float64, so even
// though expr.BinOpNumeric computed int64, derived GROUP BY keys landed in
// Float64 vectors and fell off the typed-int aggregation paths — and
// results surfaced as floats. With schema-aware typing, `ip - 1` over an
// int column is an Int64 output end to end (SQL integer arithmetic;
// DuckDB agrees), while division and float operands keep the float path.
func TestIntArithProjectionTyping(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "ip", Type: parquet.TypeInt64},
		{Name: "w", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64},
	}}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 40)
	for i := 0; i < 40; i++ {
		rows = append(rows, map[string]any{
			"ip": int64(1000 + i%4), "w": int32(i % 3), "f": float64(i) + 0.5,
		})
	}
	ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 20})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Derived int GROUP BY key: values must be int64 and groups correct.
	r, err := db.Query(ctx, "SELECT ip - 1 AS k, COUNT(*) AS cnt FROM events GROUP BY ip - 1 ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 4 {
		t.Fatalf("got %d groups, want 4: %v", len(r.Rows), r.Rows)
	}
	for i, row := range r.Rows {
		k, ok := row["k"].(int64)
		if !ok {
			t.Fatalf("group key is %T (%v), want int64 — int arithmetic demoted to float", row["k"], row["k"])
		}
		if k != int64(999+i) {
			t.Fatalf("group %d key = %d, want %d", i, k, 999+i)
		}
		if cnt, _ := row["cnt"].(int64); cnt != 10 {
			t.Fatalf("group %d cnt = %v, want 10", i, row["cnt"])
		}
	}

	// Nested int arithmetic with an Int32 operand and literals stays int.
	r, err = db.Query(ctx, "SELECT (w + 1) * 2 AS k, COUNT(*) AS cnt FROM events GROUP BY (w + 1) * 2 ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("got %d groups, want 3: %v", len(r.Rows), r.Rows)
	}
	for i, row := range r.Rows {
		k, ok := row["k"].(int64)
		if !ok {
			t.Fatalf("nested key is %T (%v), want int64", row["k"], row["k"])
		}
		if want := int64((i + 1) * 2); k != want {
			t.Fatalf("nested group %d key = %d, want %d", i, k, want)
		}
	}

	// Division stays float (DuckDB semantics), and float operands demote.
	r, err = db.Query(ctx, "SELECT ip / 2 AS d, f + 1 AS g FROM events LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Rows[0]["d"].(float64); !ok {
		t.Fatalf("division result is %T (%v), want float64", r.Rows[0]["d"], r.Rows[0]["d"])
	}
	if _, ok := r.Rows[0]["g"].(float64); !ok {
		t.Fatalf("float-operand result is %T (%v), want float64", r.Rows[0]["g"], r.Rows[0]["g"])
	}
}
