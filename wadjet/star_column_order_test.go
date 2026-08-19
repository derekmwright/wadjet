package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestStarPreservesSchemaColumnOrder pins #342: `SELECT *` expands to the
// table's columns in DECLARATION order, not alphabetically.
//
// The column names were always right; only their POSITIONS were wrong, which
// is invisible to any comparison that matches on name — and silently
// transposing to anything that reads results positionally: a CSV export, an
// INSERT ... SELECT *, a driver binding by index, a golden file.
//
// The schema below is deliberately in reverse alphabetical order, so the two
// orderings cannot agree by accident.
func TestStarPreservesSchemaColumnOrder(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "zebra", Type: parquet.TypeInt64},
			{Name: "mango", Type: parquet.TypeString},
			{Name: "apple", Type: parquet.TypeInt64},
		},
	}
	if err := db.CreateTable(ctx, "fruit", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("fruit", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"zebra": int64(1), "mango": "m", "apple": int64(3)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	want := []string{"zebra", "mango", "apple"}
	for _, sql := range []string{
		"SELECT * FROM fruit",
		"SELECT f.* FROM fruit f",
	} {
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Columns) != len(want) {
			t.Fatalf("%s: got %d columns %v, want %d", sql, len(res.Columns), res.Columns, len(want))
		}
		for i := range want {
			if res.Columns[i] != want[i] {
				t.Errorf("%s: columns = %v, want %v — schema order, not alphabetical",
					sql, res.Columns, want)
				break
			}
		}
	}

	// A star sharing its select list with another item keeps the same order,
	// with the extra column after it.
	res, err := db.Query(ctx, "SELECT *, 1 AS extra FROM fruit")
	if err != nil {
		t.Fatalf("star with extra item: %v", err)
	}
	if len(res.Columns) != 4 {
		t.Fatalf("got %d columns %v, want 4", len(res.Columns), res.Columns)
	}
	for i := range want {
		if res.Columns[i] != want[i] {
			t.Fatalf("columns = %v, want the schema order %v first", res.Columns, want)
		}
	}

	// An explicit list is ordered by the list, which already worked and must
	// keep working.
	res, err = db.Query(ctx, "SELECT mango, apple FROM fruit")
	if err != nil {
		t.Fatalf("explicit list: %v", err)
	}
	if len(res.Columns) != 2 || res.Columns[0] != "mango" || res.Columns[1] != "apple" {
		t.Errorf("explicit list columns = %v, want [mango apple]", res.Columns)
	}
}
