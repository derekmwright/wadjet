package pgwire

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// nfcDB registers a table with a ROW column whose CATALOG name is CamelCase,
// and whose FIELDS are declared in an order that is not alphabetical — so a
// rendering that lost the declaration is visible in the bytes rather than
// merely unproven.
func nfcDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "Attrs", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "zeta", Type: parquet.TypeInt64},
			{Name: "alpha", Type: parquet.TypeString},
		}},
	}}
	if err := db.Catalog().CreateTable(ctx, "nst", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("nst", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "Attrs": map[string]any{"zeta": int64(9), "alpha": "A"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestPGWireNestedColumnResolvesFoldedReference is the regression gate for a
// ROW / ARRAY / MAP column losing its DECLARATION on the wire because the
// reference that named it was folded.
//
// `nestedColumnSchemas` builds its map from the catalog, so it is keyed by the
// catalog's spelling; `nestedColumnFor` looks it up by the OUTPUT column name,
// which for an unquoted reference is the folded spelling (#731). Over a
// CamelCase schema the lookup missed, and the positional fallback is a no-op
// on this path (`ordered` is deliberately nil for the catalog-lookup schema),
// so `formatPgValueTyped` was called with no declaration: a ROW then renders
// in SORTED-KEY order instead of declared field order, and an ARRAY and a MAP
// stop being distinguishable.
//
// That is a wrong VALUE on the wire, not a wrong name — the #471/#769 failure
// mode re-entered through the case door — and it is silent: the DataRow is
// well formed and the field count is right.
func TestPGWireNestedColumnResolvesFoldedReference(t *testing.T) {
	for _, tt := range []struct{ name, sql string }{
		{"unquoted reference to a CamelCase ROW column", "SELECT Attrs FROM nst"},
		{"delimited reference in the schema's own case", `SELECT "Attrs" FROM nst`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := nfcDB(t)
			srv := startTestServer(t, db)
			client := newPGClient(t, srv.Addr())
			client.startup("test", "wadjet")
			client.writeMsg('Q', append([]byte(tt.sql), 0))

			var row []byte
			for {
				typ, payload, err := client.readMsg()
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				switch typ {
				case 'D':
					row = append([]byte(nil), payload...)
				case 'E':
					t.Fatalf("%s was refused: %s", tt.sql, client.parseError(payload))
				}
				if typ == 'Z' {
					break
				}
			}
			client.terminate()

			// The declared field order is (zeta, alpha) = (9, A). Sorted-key
			// order — what a declaration-less render produces — is (A, 9).
			if !bytes.Contains(row, []byte("(9,A)")) {
				t.Fatalf("DataRow %q does not carry the ROW in DECLARED field order (9,A): "+
					"the column's declaration did not resolve, so it rendered with sorted keys",
					row)
			}
		})
	}
}
