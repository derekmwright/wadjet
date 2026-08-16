package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ORDER BY ... LIMIT n OFFSET m now routes through the Top-K sort with
// n+m kept rows (previously OFFSET disabled Top-K and the sort fully
// materialized its input — ClickBench Q40-43 shape). Verify the offset
// window is exact.
func TestOrderByLimitOffsetTopN(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "v", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"v": int64((i * 37) % 100)} // permutation of 0..99
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 1000})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, "SELECT v FROM t ORDER BY v LIMIT 4 OFFSET 10")
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{10, 11, 12, 13}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows: %v", len(res.Rows), res.Rows)
	}
	for i, r := range res.Rows {
		if r["v"] != want[i] {
			t.Fatalf("row %d: got %v, want %d (all: %v)", i, r["v"], want[i], res.Rows)
		}
	}

	// DESC + offset past most of the data.
	res, err = db.Query(ctx, "SELECT v FROM t ORDER BY v DESC LIMIT 3 OFFSET 97")
	if err != nil {
		t.Fatal(err)
	}
	want = []int64{2, 1, 0}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows: %v", len(res.Rows), res.Rows)
	}
	for i, r := range res.Rows {
		if r["v"] != want[i] {
			t.Fatalf("row %d: got %v, want %d", i, r["v"], want[i])
		}
	}
}
