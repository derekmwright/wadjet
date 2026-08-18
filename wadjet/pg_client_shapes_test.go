package wadjet

import (
	"context"
	"fmt"
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
	// Each name is distinct so a test can assert the value that came back
	// belongs to the row that came back, and every one is 3 characters so the
	// LENGTH assertions below stay a fixed number.
	rows := make([]map[string]any, 0, 10)
	for i := 0; i < 10; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "name": itemName(int64(i))})
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

// itemName is the name paired with an id in the items fixture.
func itemName(id int64) string { return fmt.Sprintf("r%02d", id) }

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
		"SELECT items.*, id FROM items LIMIT 3",
		"SELECT *, id FROM items LIMIT 3",
		"SELECT *, name FROM items",
	} {
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		want := 3
		if !strings.Contains(sql, "LIMIT") {
			want = 10
		}
		if len(res.Rows) != want {
			t.Fatalf("%s: got %d rows, want %d (LIMIT must survive star expansion)", sql, len(res.Rows), want)
		}
		for _, row := range res.Rows {
			// The star's own columns carry VALUES, not just names. Star
			// expansion used to run after column pruning, so the scan below
			// read only the columns the OTHER select items named and every
			// column the star contributed came back NULL (#315).
			id, ok := row["id"].(int64)
			if !ok {
				t.Fatalf("%s: id = %v (%T), want an int64 — the star's columns must carry values, row = %v",
					sql, row["id"], row["id"], row)
			}
			name, ok := row["name"].(string)
			if !ok {
				t.Fatalf("%s: name = %v (%T), want a string — the star's columns must carry values, row = %v",
					sql, row["name"], row["name"], row)
			}
			// Values belong to the row they came back on: a star resolved to
			// the wrong source column would still be non-NULL.
			if id < 0 || id > 9 || name != itemName(id) {
				t.Fatalf("%s: row %v pairs id %d with name %q, want %q", sql, row, id, name, itemName(id))
			}
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

// The length family returns a count, but the planner typed it String, so the
// projection allocated a Bytes output vector and the Float64Data-writing vec
// kernel indexed off the end of a zero-length slice: `SELECT LENGTH(c) FROM t`
// panicked the whole server process, taking every connection with it. Third
// instance of that mismatch; #310 tracks removing the class.
func TestLengthFamilyOverStringColumn(t *testing.T) {
	ctx, db := newClientShapeDB(t)

	res, err := db.Query(ctx, "SELECT LENGTH(name) AS l, OCTET_LENGTH(name) AS o, "+
		"BIT_LENGTH(name) AS b, CHAR_LENGTH(name) AS c FROM items LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	row := res.Rows[0]
	// "row" is 3 characters.
	for col, want := range map[string]int64{"l": 3, "o": 3, "b": 24, "c": 3} {
		var got int64
		switch v := row[col].(type) {
		case int64:
			got = v
		case float64:
			got = int64(v)
		default:
			t.Fatalf("%s = %v (%T), want a number", col, row[col], row[col])
		}
		if got != want {
			t.Errorf("%s = %d, want %d", col, got, want)
		}
	}
}
