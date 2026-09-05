package pgwire

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// setupCamelCopyDB registers a table whose catalog schema carries the
// CamelCase column names a parquet dataset gives it. Every COPY fixture in
// this package spells its columns lower case, which is exactly why none of
// them could see the defect this gate exists for: with a lower-case schema
// the folded spelling a client sends and the schema's spelling are the SAME
// STRING.
func setupCamelCopyDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "UserAgent", Type: parquet.TypeString, Nullable: true},
	}}
	// Through the CATALOG: the DDL door folds a name it MINTS, so a CamelCase
	// schema is one a parquet dataset or ingest brought in.
	if err := db.Catalog().CreateTable(ctx, "hits", schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestPGWireCopyResolvesFoldedColumnNames is the regression gate for a COPY
// whose column list is spelled the way an unquoted identifier reaches the
// server.
//
// COPY's TABLE name already takes the read concession — a mixed-case table is
// reachable unquoted via `catalog.ResolveTableName` — but its COLUMN list was
// taken verbatim from the statement text and used two ways that are both
// byte-exact against the catalog schema: as the key into the per-column TYPE
// map, and as the key of the row map the ingester reads back with
// `row[col.Name]`.
//
// So `COPY hits (watchid, useragent)` missed the type map, and a miss there
// yields the ZERO `parquet.Column` — whose Type is `TypeBool`, the zero of the
// TypeID iota — so every field was parsed with `strconv.ParseBool`. The rows
// that survived that were then keyed `watchid`/`useragent`, which the
// ingester's byte-exact read does not find, so every named column was written
// NULL.
//
// A fold-aware client sends exactly this spelling. The gate asserts the
// stored VALUES, because the failure is silent wherever the columns are
// nullable: COPY reports its count and the table fills with NULLs.
func TestPGWireCopyResolvesFoldedColumnNames(t *testing.T) {
	for _, tt := range []struct {
		name string
		list string
	}{
		{"folded column list", "(watchid, useragent)"},
		{"the schema's own spelling", "(WatchID, UserAgent)"},
		{"delimited in the schema's own spelling", `("WatchID", "UserAgent")`},
		{"no column list at all", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupCamelCopyDB(t)
			srv := startTestServer(t, db)
			client := newPGClient(t, srv.Addr())
			client.startup("test", "wadjet")

			sql := "COPY hits " + tt.list + " FROM STDIN"
			client.writeMsg('Q', append([]byte(sql), 0))
			typ, payload, err := client.readMsg()
			if err != nil {
				t.Fatalf("reading CopyInResponse: %v", err)
			}
			if typ != 'G' {
				t.Fatalf("expected CopyInResponse ('G'), got '%c': %s", typ, client.parseError(payload))
			}
			client.sendCopyData("7\tagent-7\n")
			client.sendCopyDone()
			if typ, payload, err = client.readMsg(); err != nil {
				t.Fatalf("reading the reply: %v", err)
			}
			if typ == 'E' {
				t.Fatalf("%s was refused: %s", sql, client.parseError(payload))
			}
			client.terminate()

			res, err := db.Query(ctx, `SELECT WatchID, UserAgent FROM hits`)
			if err != nil {
				t.Fatalf("reading hits back: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows after COPY, want 1", len(res.Rows))
			}
			cells := res.Cells(0)
			if cells[0] != int64(7) || cells[1] != "agent-7" {
				t.Fatalf("COPY %s stored %v, want [7 agent-7] — the column list did not "+
					"resolve against the catalog schema, so the values were converted "+
					"against the zero parquet.Column and written under keys the "+
					"ingester does not read", tt.list, cells)
			}
		})
	}
}
