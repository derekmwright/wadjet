package server

import (
	"context"
	"fmt"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The HTTP twin of wadjet.TestReUpdateDoesNotMultiplyRows and
// TestDeleteAfterReUpdateRemovesExactlyTheLiveRow.
//
// These executors are a second copy of the embedded ones and carried the same
// defect: neither applied the delete-marker filter to its match scan, so an
// UPDATE re-matched the copies its own earlier UPDATEs had superseded and
// re-emitted them — 1 row, then 2, then 4 (#674). The row set is read back
// through the same merge-on-read filter a SELECT applies, so what is asserted
// is what a client sees.
func TestHTTPReUpdateDoesNotMultiplyRows(t *testing.T) {
	ctx := context.Background()
	cat := visibilityCatalog(t)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "u", schema, nil); err != nil {
		t.Fatal(err)
	}
	httpDML(t, cat, "INSERT INTO u VALUES (1, 1)")
	httpDML(t, cat, "INSERT INTO u VALUES (2, 100)")

	for i := 2; i <= 6; i++ {
		httpDML(t, cat, fmt.Sprintf("UPDATE u SET n = %d WHERE id = 1", i))
		live := liveRows(t, cat, "u", schema)
		if got := countWhereID(live, 1); got != 1 {
			t.Fatalf("after %d UPDATEs id=1 is %d rows, want 1: %v", i-1, got, live)
		}
		if got := countWhereID(live, 2); got != 1 {
			t.Fatalf("the untouched row is %d copies after %d UPDATEs, want 1: %v", got, i-1, live)
		}
		for _, r := range live {
			if r["id"] == int64(1) && fmt.Sprint(r["n"]) != fmt.Sprint(i) {
				t.Errorf("after UPDATE %d, n = %v, want %d", i-1, r["n"], i)
			}
		}
	}
}

// The DELETE half: it removes the LIVE copy, reports one row, and a repeat
// reports none.
func TestHTTPDeleteAfterReUpdateRemovesExactlyTheLiveRow(t *testing.T) {
	ctx := context.Background()
	cat := visibilityCatalog(t)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "dv", schema, nil); err != nil {
		t.Fatal(err)
	}
	httpDML(t, cat, "INSERT INTO dv VALUES (1, 1)")
	httpDML(t, cat, "INSERT INTO dv VALUES (2, 2)")
	for i := 2; i <= 4; i++ {
		httpDML(t, cat, fmt.Sprintf("UPDATE dv SET n = %d WHERE id = 1", i))
	}

	if got := httpDML(t, cat, "DELETE FROM dv WHERE id = 1"); got != 1 {
		t.Errorf("DELETE reported %d rows affected, want 1 — a superseded copy is not a row", got)
	}
	live := liveRows(t, cat, "dv", schema)
	if len(live) != 1 || live[0]["id"] != int64(2) {
		t.Fatalf("after the DELETE the table holds %v, want only id=2", live)
	}
	if got := httpDML(t, cat, "DELETE FROM dv WHERE id = 1"); got != 0 {
		t.Errorf("a second DELETE of the same row reported %d rows affected, want 0", got)
	}
}

func visibilityCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return cat
}

// httpDML runs one statement through the HTTP DML dispatcher and returns the
// row count it reported.
func httpDML(t *testing.T, cat *catalog.Catalog, sql string) int64 {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parsing %q: %v", sql, err)
	}
	res, err := runHTTPDML(context.Background(), cat, parsed)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res.rowsAffected
}

// liveRows is the table's rows minus its delete markers — the merge-on-read
// view a SELECT returns, read here through the same per-file reader the DML
// scans use.
func liveRows(t *testing.T, cat *catalog.Catalog, table string, schema parquet.Schema) []map[string]any {
	t.Helper()
	ctx := context.Background()
	manifest, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	gone := catalog.DeletedRowsByFile(manifest.DeleteMarkers)
	var out []map[string]any
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			b, err := readDMLFile(ctx, cat, f.Path, schema.Columns)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				continue
			}
			removed := gone[f.Path]
			for i := 0; i < b.Len; i++ {
				if removed[int64(i)] {
					continue
				}
				out = append(out, b.RowAt(i))
			}
		}
	}
	return out
}

func countWhereID(rows []map[string]any, id int64) int {
	n := 0
	for _, r := range rows {
		if r["id"] == id {
			n++
		}
	}
	return n
}
