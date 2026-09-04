package pgwire

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Reaching a mixed-case table through its FOLDED spelling changes nothing the
// WIRE carries.
//
// The case concession for a RELATION name (ADR-0012) resolves the reference to
// the catalog's own spelling before planning. A RELATION name is not a column
// name, so nothing in RowDescription should move — but that is the assertion a
// value-only gate cannot make, and this arc's own review found a right value
// under a wrong declaration twice. The two spellings are compared FIELD FOR
// FIELD: names, type OIDs, and the rows themselves, on both the simple and the
// extended door.
func TestAFoldedTableSpellingCarriesTheSameRowDescription(t *testing.T) {
	ctx := context.Background()
	db := tncWireDB(t, ctx)
	srv := startTestServer(t, db)

	spellings := []struct{ label, sql string }{
		{"stored", `SELECT id, label FROM "TncWire" ORDER BY id`},
		{"as written", "SELECT id, label FROM TncWire ORDER BY id"},
		{"folded", "SELECT id, label FROM tncwire ORDER BY id"},
		{"upper", "SELECT id, label FROM TNCWIRE ORDER BY id"},
	}

	type shot struct {
		names []string
		oids  []int
		rows  [][]string
		tag   string
	}
	var ref shot
	for i, sp := range spellings {
		c := newPGClient(t, srv.Addr())
		c.startup("wadjet", "wadjet")
		names, oids, rows, tag := c.extendedQuery(sp.sql)
		got := shot{names, oids, rows, tag}
		if i == 0 {
			ref = got
			if len(ref.names) != 2 {
				t.Fatalf("the stored spelling described %d fields, want 2", len(ref.names))
			}
			continue
		}
		if fmt.Sprint(got.names) != fmt.Sprint(ref.names) {
			t.Errorf("%s spelling: RowDescription names %v, the stored spelling's are %v",
				sp.label, got.names, ref.names)
		}
		if fmt.Sprint(got.oids) != fmt.Sprint(ref.oids) {
			t.Errorf("%s spelling: RowDescription type OIDs %v, the stored spelling's are %v",
				sp.label, got.oids, ref.oids)
		}
		if fmt.Sprint(got.rows) != fmt.Sprint(ref.rows) {
			t.Errorf("%s spelling: rows %v, the stored spelling's are %v",
				sp.label, got.rows, ref.rows)
		}
		if got.tag != ref.tag {
			t.Errorf("%s spelling: command tag %q, the stored spelling's is %q",
				sp.label, got.tag, ref.tag)
		}
	}

	// …and the SIMPLE door, which describes rows through a different path.
	var refCols []string
	var refRows [][]string
	for i, sp := range spellings {
		c := newPGClient(t, srv.Addr())
		c.startup("wadjet", "wadjet")
		cols, rows, _ := c.simpleQuery(sp.sql)
		if i == 0 {
			refCols, refRows = cols, rows
			continue
		}
		if fmt.Sprint(cols) != fmt.Sprint(refCols) {
			t.Errorf("simple door, %s spelling: columns %v, the stored spelling's are %v",
				sp.label, cols, refCols)
		}
		if fmt.Sprint(rows) != fmt.Sprint(refRows) {
			t.Errorf("simple door, %s spelling: rows %v, the stored spelling's are %v",
				sp.label, rows, refRows)
		}
	}
}

func tncWireDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "label", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "TncWire", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("TncWire", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "label": "a"},
		{"id": int32(2), "label": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
