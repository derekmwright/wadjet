package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Statement shapes PostgreSQL clients send that this engine used to reject or,
// worse, answer differently depending on which execution path ran. All three
// come from one statement DataGrip issues when a table is double-clicked:
//
//	SELECT t.*, CTID FROM public.customer t LIMIT 501
func newClientShapeDB(t *testing.T) (context.Context, *DB) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 10)
	for i := 0; i < 10; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "name": "row"})
	}
	ing := db.NewIngester("items", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

// A schema-qualified name resolves to the table. The parser read only the
// first identifier, so `public.items` scanned a table named "public" — which
// resolved to nothing and produced an empty-scan error.
func TestQualifiedTableNames(t *testing.T) {
	ctx, db := newClientShapeDB(t)

	for _, sql := range []string{
		"SELECT id FROM items ORDER BY id",
		"SELECT id FROM public.items ORDER BY id",
		"SELECT id FROM wadjet.public.items ORDER BY id",
		"SELECT t.id FROM public.items t ORDER BY t.id",
	} {
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 10 {
			t.Fatalf("%s: got %d rows, want 10", sql, len(res.Rows))
		}
	}

	// A qualifier naming something this server does not have is an error, not
	// a silently ignored prefix.
	if _, err := db.Query(ctx, "SELECT id FROM otherschema.items"); err == nil {
		t.Fatal("expected an error for an unknown schema")
	} else if !strings.Contains(err.Error(), "unknown schema") {
		t.Fatalf("error should name the unknown schema, got: %v", err)
	}
}

// A star sharing its SELECT list with another item must expand. It used to
// reach execution as a projection of the literal column "*": the local
// pipeline failed, the coordinator treated that deterministic query error as
// an infrastructure failure and re-ran on the DAG, and the DAG answered
// without the projection AND without the LIMIT — every row for a LIMIT 3.
func TestStarWithAdditionalSelectItems(t *testing.T) {
	ctx, db := newClientShapeDB(t)

	for _, sql := range []string{
		"SELECT t.*, name FROM items t LIMIT 3",
		"SELECT t.*, CTID FROM public.items t LIMIT 3",
		"SELECT *, id FROM items LIMIT 3",
	} {
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 3 {
			t.Fatalf("%s: got %d rows, want 3 (LIMIT must survive star expansion)", sql, len(res.Rows))
		}
		// The star's own columns are present, not just the extra item.
		if _, ok := res.Rows[0]["id"]; !ok {
			t.Fatalf("%s: star did not expand, row = %v", sql, res.Rows[0])
		}
		if _, ok := res.Rows[0]["name"]; !ok {
			t.Fatalf("%s: star did not expand, row = %v", sql, res.Rows[0])
		}
	}
}

// PostgreSQL's system columns resolve to NULL. This engine has no physical row
// identity — rows live in immutable Parquet files that compaction rewrites —
// so ctid cannot be invented: UPDATE and DELETE are supported, and a
// fabricated row address would let a client address a row other than the one
// it read. NULL keeps the read working and makes such a write match nothing.
func TestSystemColumnsAreNull(t *testing.T) {
	ctx, db := newClientShapeDB(t)

	res, err := db.Query(ctx, "SELECT id, CTID FROM items ORDER BY id LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(res.Rows))
	}
	for _, row := range res.Rows {
		if v, ok := row["CTID"]; ok && v != nil {
			t.Fatalf("ctid should be NULL, got %v", v)
		}
	}

	// A write targeting the invented identity matches nothing.
	if _, err := db.Query(ctx, "DELETE FROM items WHERE ctid = '(0,1)'"); err != nil {
		t.Fatalf("delete by ctid should run and match nothing: %v", err)
	}
	after, err := db.Query(ctx, "SELECT COUNT(*) AS n FROM items")
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	switch v := after.Rows[0]["n"].(type) {
	case int64:
		n = v
	case float64:
		n = int64(v)
	}
	if n != 10 {
		t.Fatalf("delete by ctid removed rows: %d remain, want 10", n)
	}
}
