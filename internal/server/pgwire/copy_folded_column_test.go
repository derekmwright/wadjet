package pgwire

import (
	"context"
	"fmt"
	"strings"
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

// setupLowerCopyDB is the same table with an all-LOWER-case schema. It is the
// control that shows P6 was never a CamelCase-only problem: an UNQUOTED
// upper-case name in the column list has to fold before it can resolve, and
// over a lower-case schema there is nothing else for it to match.
func setupLowerCopyDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "watchid", Type: parquet.TypeInt64},
		{Name: "useragent", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.Catalog().CreateTable(ctx, "lhits", schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// copyOne runs one COPY and reports either the stored row or the refusal.
func copyOne(t *testing.T, db *wadjet.DB, table, list, readBack string) (string, string) {
	t.Helper()
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("test", "wadjet")
	client.writeMsg('Q', append([]byte("COPY "+table+" "+list+" FROM STDIN"), 0))
	typ, payload, err := client.readMsg()
	if err != nil {
		t.Fatalf("reading CopyInResponse: %v", err)
	}
	if typ == 'E' {
		client.terminate()
		return "", client.parseError(payload)
	}
	if typ != 'G' {
		t.Fatalf("expected CopyInResponse ('G'), got '%c'", typ)
	}
	client.sendCopyData("7\tagent-7\n")
	client.sendCopyDone()
	typ, payload, err = client.readMsg()
	if err != nil {
		t.Fatalf("reading the reply: %v", err)
	}
	if typ == 'E' {
		client.terminate()
		return "", client.parseError(payload)
	}
	client.terminate()
	res, err := db.Query(context.Background(), readBack)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if len(res.Rows) != 1 {
		return fmt.Sprintf("%d rows", len(res.Rows)), ""
	}
	return fmt.Sprintf("%v", res.Cells(0)), ""
}

// TestPGWireCopyColumnListFoldsLikeEveryOtherIdentifier is P6's gate: the COPY
// column list is READ the way the lexer reads an identifier, so the resolver
// downstream can apply the rule that distinguishes the two spellings.
//
// COPY is hand-parsed, and the parser used to strip quotes and fold nothing.
// By the time `batch.ResolveSchemaIndex` saw a name, an unquoted `WatchID` and
// a delimited `"WatchID"` were the same string, and the resolver's rule —
// byte-exact, then a unique case-insensitive match FOR A REFERENCE THAT IS
// ITSELF FOLDED — had nothing left to key on. It went wrong in both
// directions at once, which is why both halves are cells here.
func TestPGWireCopyColumnListFoldsLikeEveryOtherIdentifier(t *testing.T) {
	t.Run("an unquoted upper-case list folds onto a lower-case schema", func(t *testing.T) {
		// PostgreSQL accepts all three of these; before the fix wadjet
		// refused the last two with a `strconv.ParseBool` type error naming
		// the wrong problem.
		for _, list := range []string{
			`(watchid, useragent)`,
			`(WatchID, UserAgent)`,
			`(WATCHID, USERAGENT)`,
		} {
			got, refusal := copyOne(t, setupLowerCopyDB(t), "lhits", list,
				`SELECT watchid, useragent FROM lhits`)
			if refusal != "" {
				t.Errorf("COPY lhits %s was REFUSED: %s", list, refusal)
				continue
			}
			if got != "[7 agent-7]" {
				t.Errorf("COPY lhits %s stored %s, want [7 agent-7]", list, got)
			}
		}
	})

	t.Run("a delimited UPPER-case name does not resolve", func(t *testing.T) {
		// A reference carrying an upper-case letter can only have been
		// written delimited — the lexer folds an unquoted one — so it
		// resolves byte-exact ONLY (batch/schema.go item 4, ADR-0012). COPY
		// now reads its list the same way, so `"USERAGENT"` misses a column
		// spelled `UserAgent` here exactly as `SELECT "USERAGENT"` does.
		_, refusal := copyOne(t, setupCamelCopyDB(t), "hits", `("USERAGENT", "WATCHID")`,
			`SELECT WatchID, UserAgent FROM hits`)
		if refusal == "" {
			t.Fatal(`COPY hits ("USERAGENT", "WATCHID") was ACCEPTED against a schema ` +
				`spelling them UserAgent/WatchID — a delimited name is byte-exact`)
		}
		if !strings.Contains(refusal, "USERAGENT") {
			t.Errorf("the refusal does not name the column: %s", refusal)
		}
	})

	t.Run("COPY resolves a name exactly as the read door does", func(t *testing.T) {
		// The one spelling where the engine diverges from PostgreSQL by
		// design: a DELIMITED name that carries no upper-case letter is
		// indistinguishable from an unquoted one once the quotes are off, so
		// `"useragent"` takes the folded concession and resolves to
		// `UserAgent`, where PostgreSQL raises 42703. That is ADR-0012's
		// recorded boundary and it is not COPY's to decide — what IS COPY's
		// is that it answers the same as the read door for the same spelling.
		// Before the fix the two doors disagreed in both directions.
		db := setupCamelCopyDB(t)
		for _, spelling := range []string{`useragent`, `"useragent"`, `UserAgent`, `"UserAgent"`} {
			_, readErr := db.Query(context.Background(),
				`SELECT `+spelling+` FROM hits`)
			_, copyRefusal := copyOne(t, setupCamelCopyDB(t), "hits", `(watchid, `+spelling+`)`,
				`SELECT WatchID, UserAgent FROM hits`)
			readOK, copyOK := readErr == nil, copyRefusal == ""
			if readOK != copyOK {
				t.Errorf("%s: the read door %s it and COPY %s it",
					spelling,
					map[bool]string{true: "resolves", false: "refuses"}[readOK],
					map[bool]string{true: "resolves", false: "refuses"}[copyOK])
			}
		}
	})

	t.Run("a column that does not exist is 42703, not a type error", func(t *testing.T) {
		_, refusal := copyOne(t, setupLowerCopyDB(t), "lhits", `(nosuchcol, useragent)`,
			`SELECT watchid FROM lhits`)
		if refusal == "" {
			t.Fatal("COPY into a column that does not exist was accepted")
		}
		if strings.Contains(refusal, "ParseBool") {
			t.Errorf("the refusal is a TYPE error naming the wrong problem: %s", refusal)
		}
		if !strings.Contains(refusal, "nosuchcol") {
			t.Errorf("the refusal does not name the column: %s", refusal)
		}
	})
}
