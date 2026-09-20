// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// writeZeekConnLog writes a small Zeek-shaped JSON log. Zeek emits the
// connection 4-tuple as flat keys that contain literal dots ("id.orig_h"),
// which read_json's schema inference keeps verbatim as column names — so the
// natural spelling for referencing them is a double-quoted identifier.
func writeZeekConnLog(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conn.json")
	lines := `{"ts":"2026-08-16T00:00:01Z","uid":"CsRT1","id.orig_h":"10.0.0.1","id.orig_p":51000,"id.resp_h":"93.184.216.34","id.resp_p":443,"proto":"tcp","orig_bytes":1200}
{"ts":"2026-08-16T00:00:02Z","uid":"CsRT2","id.orig_h":"10.0.0.1","id.orig_p":51001,"id.resp_h":"93.184.216.34","id.resp_p":80,"proto":"tcp","orig_bytes":800}
{"ts":"2026-08-16T00:00:03Z","uid":"CsRT3","id.orig_h":"10.0.0.9","id.orig_p":53000,"id.resp_h":"8.8.8.8","id.resp_p":53,"proto":"udp","orig_bytes":90}
{"ts":"2026-08-16T00:00:04Z","uid":"CsRT4","id.orig_h":"10.0.0.9","id.orig_p":53001,"id.resp_h":"8.8.8.8","id.resp_p":53,"proto":"udp","orig_bytes":110}
{"ts":"2026-08-16T00:00:05Z","uid":"CsRT5","id.orig_h":"10.0.0.5","id.orig_p":44000,"id.resp_h":"93.184.216.34","id.resp_p":443,"proto":"tcp","orig_bytes":4000}
`
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func openTestDB(t *testing.T) (context.Context, *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

// queryRows runs a query and returns its rows keyed by the column names the
// result reports, so a mismatch between the reported schema and the row keys
// surfaces as a missing value rather than passing unnoticed.
func checkedRows(t *testing.T, ctx context.Context, db *wadjet.DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	out := make([]map[string]any, 0, len(res.Rows))
	for i, row := range res.Rows {
		keyed := make(map[string]any, len(res.Columns))
		for _, col := range res.Columns {
			v, ok := row[col]
			if !ok {
				t.Fatalf("query %q: reported column %q is absent from row %d (row keys: %v)",
					sql, col, i, keysOf(row))
			}
			keyed[col] = v
		}
		out = append(out, keyed)
	}
	return out
}

func keysOf(row map[string]any) []string {
	ks := make([]string, 0, len(row))
	for k := range row {
		ks = append(ks, k)
	}
	return ks
}

func valuesInOrder(rows []map[string]any, col string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprint(r[col]))
	}
	return out
}

// TestQuotedIdentifierZeekDottedColumns covers the motivating case for
// delimited identifiers: Zeek's dotted column names, referenced both ways.
func TestQuotedIdentifierZeekDottedColumns(t *testing.T) {
	ctx, db := openTestDB(t)
	path := writeZeekConnLog(t)
	src := func(sql string) string { return fmt.Sprintf(sql, path) }

	t.Run("projection and filter", func(t *testing.T) {
		rows := checkedRows(t, ctx, db, src(`SELECT "id.orig_h", "id.resp_p" FROM read_json('%s') WHERE "id.resp_p" = 443 ORDER BY "id.orig_h"`))
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
		}
		if got := valuesInOrder(rows, "id.orig_h"); got[0] != "10.0.0.1" || got[1] != "10.0.0.5" {
			t.Errorf("id.orig_h values: got %v, want [10.0.0.1 10.0.0.5]", got)
		}
	})

	// A QUALIFIED REFERENCE IS A QUALIFIED REFERENCE, on every relation.
	//
	// `id.orig_h` unquoted is `id` . `orig_h` — PostgreSQL's reading, and this
	// engine's — so it needs a relation called `id` in scope and is 42P01
	// without one. A flat column whose NAME contains a dot is spelled
	// `"id.orig_h"`, which is the whole reason delimited identifiers exist
	// here and the motivating Zeek case.
	//
	// This subtest used to pin the opposite for a file reader: the unquoted
	// spelling resolved to the flat column. That was never a rule of the
	// engine — a CATALOG table with the same literal column has always
	// refused it (the `catalog` cells below, identical at 0c0d33b6) — it was
	// an artefact of a relation with NO plan-time schema. The binder could
	// prove nothing absent over an open scope, so it declined; the reference
	// reached the executor, and `columnIndexFallback`'s qualifier-stripping
	// suffix match (`"." + bare`) found `id.orig_h` from `orig_h`. A reader
	// was the only relation that ever got there. Now that a reader publishes
	// its columns at plan time the binder answers first, and both relations
	// give PostgreSQL's answer.
	t.Run("a dotted column is quoted; unquoted is a qualifier", func(t *testing.T) {
		// The QUOTED spelling — unchanged, and the one users write.
		quoted := checkedRows(t, ctx, db, src(`SELECT "id.orig_h", COUNT(*) AS n, SUM("orig_bytes") AS bytes FROM read_json('%s') GROUP BY "id.orig_h" ORDER BY n DESC, "id.orig_h"`))
		if len(quoted) != 3 {
			t.Fatalf("expected 3 groups, got %d", len(quoted))
		}
		wantHosts := []string{"10.0.0.1", "10.0.0.9", "10.0.0.5"}
		wantCounts := []string{"2", "2", "1"}
		wantBytes := []string{"2000", "200", "4000"}
		qHosts := valuesInOrder(quoted, "id.orig_h")
		for i := range wantHosts {
			if qHosts[i] != wantHosts[i] {
				t.Errorf("quoted host %d: got %q, want %q", i, qHosts[i], wantHosts[i])
			}
			if got := valuesInOrder(quoted, "n")[i]; got != wantCounts[i] {
				t.Errorf("quoted count %d: got %q, want %q", i, got, wantCounts[i])
			}
			if got := valuesInOrder(quoted, "bytes")[i]; got != wantBytes[i] {
				t.Errorf("quoted bytes %d: got %q, want %q", i, got, wantBytes[i])
			}
		}

		// A CATALOG table holding the same literal columns, so each refusal
		// below is asserted on BOTH relations and neither has a rule of its
		// own. The `catalog` half passes at 0c0d33b6 too; the `reader` half
		// is what this arc changed.
		for _, ddl := range []string{
			`CREATE TABLE zeekcat ("id.orig_h" VARCHAR, "id.resp_p" BIGINT, orig_bytes BIGINT)`,
			`INSERT INTO zeekcat VALUES ('10.0.0.1',443,1200),('10.0.0.9',53,90),('10.0.0.5',443,4000)`,
			`CREATE TABLE id (orig_h VARCHAR)`,
			`INSERT INTO id VALUES ('from-the-relation-called-id')`,
		} {
			if _, err := db.Query(ctx, ddl); err != nil {
				t.Fatalf("%s: %v", ddl, err)
			}
		}

		reader := fmt.Sprintf(`read_json('%s')`, path)
		for _, rel := range []struct{ name, from string }{
			{"catalog", "zeekcat"},
			{"reader", reader},
		} {
			for _, c := range []struct{ clause, sql string }{
				{"select list", `SELECT id.orig_h FROM ` + rel.from},
				{"group by", `SELECT id.orig_h, COUNT(*) AS n FROM ` + rel.from + ` GROUP BY id.orig_h`},
				{"where", `SELECT orig_bytes FROM ` + rel.from + ` WHERE id.resp_p = 443`},
				{"order by", `SELECT orig_bytes FROM ` + rel.from + ` ORDER BY id.orig_h`},
				{"join on", `SELECT COUNT(*) AS n FROM ` + rel.from +
					` x JOIN zeekcat y ON id.orig_h = y."id.orig_h"`},
			} {
				t.Run(rel.name+"/"+c.clause, func(t *testing.T) {
					res, err := db.Query(ctx, c.sql)
					if err == nil {
						t.Fatalf("answered %v rows; `id.orig_h` unquoted names a relation "+
							"`id` that is not in scope, which PostgreSQL refuses\n  SQL: %s",
							len(res.Rows), c.sql)
					}
					if st := sqlerr.StateOf(err); st != "42P01" {
						t.Errorf("SQLSTATE %q, want 42P01: %v\n  SQL: %s", st, err, c.sql)
					}
					if !strings.Contains(err.Error(), `missing FROM-clause entry for table "id"`) {
						t.Errorf("the refusal does not name the missing relation: %v", err)
					}
				})
			}

			// THE CONTROL that makes the refusals above a rule about SCOPE
			// and not about the spelling: with a relation actually called
			// `id` in the FROM clause the reference is legal and answers.
			//
			// WHICH column it answers with is a PIN, not the rule. Beside a
			// relation that carries a FLAT column literally named
			// `id.orig_h`, the executor's exact-name match finds that column
			// first and the qualified reading loses: this answers the flat
			// column's value where PostgreSQL 17.11 answers the relation
			// `id`'s `orig_h`. Measured identically at 0c0d33b6 on BOTH
			// relations, so it is pre-existing and no part of this arc; it is
			// recorded as a filing candidate. If it starts answering
			// `from-the-relation-called-id`, that is the fix and this pin is
			// deleted as its proof.
			t.Run(rel.name+"/a real relation named id makes it legal", func(t *testing.T) {
				rows := checkedRows(t, ctx, db,
					`SELECT id.orig_h FROM `+rel.from+`, id`)
				if len(rows) == 0 {
					t.Fatal("the reference is legal with `id` in scope and must answer")
				}
				const fromTheRelation = "from-the-relation-called-id"
				for i, r := range rows {
					got := fmt.Sprint(r["orig_h"])
					if got == fromTheRelation {
						t.Errorf("row %d answered the relation `id`'s column, which is "+
							"PostgreSQL 17.11's reading — the pin below has been fixed, "+
							"delete it and this branch", i)
						continue
					}
					if !strings.HasPrefix(got, "10.0.0.") {
						t.Errorf("row %d: got %q, which is neither the flat column's value "+
							"nor the relation `id`'s", i, got)
					}
				}
			})
		}
	})

	t.Run("quoted alias keeps the source type", func(t *testing.T) {
		rows := checkedRows(t, ctx, db, src(`SELECT "id.orig_h" AS "src host", COUNT(*) AS n FROM read_json('%s') GROUP BY "id.orig_h" ORDER BY n DESC, "src host"`))
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
		}
		if got := valuesInOrder(rows, "src host"); got[0] != "10.0.0.1" {
			t.Errorf("aliased host values: got %v, want 10.0.0.1 first", got)
		}
	})

	t.Run("distinct and order by", func(t *testing.T) {
		rows := checkedRows(t, ctx, db, src(`SELECT DISTINCT "id.resp_h" FROM read_json('%s')`))
		if len(rows) != 2 {
			t.Fatalf("expected 2 distinct responders, got %d: %v", len(rows), rows)
		}
	})

	t.Run("mixed quoted and unquoted", func(t *testing.T) {
		rows := checkedRows(t, ctx, db, src(`SELECT uid, "id.orig_p", proto FROM read_json('%s') WHERE proto = 'udp' AND "id.resp_p" = 53 ORDER BY "id.orig_p"`))
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
		}
		if got := valuesInOrder(rows, "uid"); got[0] != "CsRT3" || got[1] != "CsRT4" {
			t.Errorf("uids: got %v, want [CsRT3 CsRT4]", got)
		}
	})
}

// TestQuotedIdentifierToolGeneratedShape covers the query shape BI tools and
// JDBC metadata builders emit against a catalog table: every identifier
// quoted, including the table name, with positional GROUP BY / ORDER BY.
func TestQuotedIdentifierToolGeneratedShape(t *testing.T) {
	ctx, db := openTestDB(t)

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "host", Type: parquet.TypeString},
			{Name: "proto", Type: parquet.TypeString},
			{Name: "bytes", Type: parquet.TypeInt64},
		},
	}
	if err := db.CreateTable(ctx, "conn_events", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"host": "a", "proto": "tcp", "bytes": int64(10)},
		{"host": "a", "proto": "tcp", "bytes": int64(20)},
		{"host": "b", "proto": "tcp", "bytes": int64(5)},
		{"host": "c", "proto": "udp", "bytes": int64(99)},
	}
	ing := db.NewIngester("conn_events", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("quoted table and columns", func(t *testing.T) {
		got := checkedRows(t, ctx, db,
			`SELECT "host", COUNT(*) AS "cnt" FROM "conn_events" WHERE "proto" = 'tcp' GROUP BY 1 ORDER BY 2 DESC LIMIT 10`)
		if len(got) != 2 {
			t.Fatalf("expected 2 rows, got %d: %v", len(got), got)
		}
		if h := valuesInOrder(got, "host"); h[0] != "a" || h[1] != "b" {
			t.Errorf("hosts: got %v, want [a b]", h)
		}
		if c := valuesInOrder(got, "cnt"); c[0] != "2" || c[1] != "1" {
			t.Errorf("counts: got %v, want [2 1]", c)
		}
	})

	t.Run("quoted table alias and qualified refs", func(t *testing.T) {
		got := checkedRows(t, ctx, db,
			`SELECT c."host", SUM(c."bytes") AS "total" FROM "conn_events" AS "c" GROUP BY c."host" ORDER BY "total" DESC`)
		if len(got) != 3 {
			t.Fatalf("expected 3 rows, got %d: %v", len(got), got)
		}
		if h := valuesInOrder(got, "host"); h[0] != "c" {
			t.Errorf("hosts: got %v, want c first", h)
		}
		if tot := valuesInOrder(got, "total"); tot[0] != "99" || tot[1] != "30" || tot[2] != "5" {
			t.Errorf("totals: got %v, want [99 30 5]", tot)
		}
	})

	t.Run("unquoted spelling unchanged", func(t *testing.T) {
		got := checkedRows(t, ctx, db,
			`SELECT host, COUNT(*) AS cnt FROM conn_events WHERE proto = 'tcp' GROUP BY 1 ORDER BY 2 DESC`)
		if len(got) != 2 {
			t.Fatalf("expected 2 rows, got %d: %v", len(got), got)
		}
		if h := valuesInOrder(got, "host"); h[0] != "a" || h[1] != "b" {
			t.Errorf("hosts: got %v, want [a b]", h)
		}
	})
}

// TestQuotedIdentifierErrors checks the input-handling messages a client sees.
func TestQuotedIdentifierErrors(t *testing.T) {
	ctx, db := openTestDB(t)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"unterminated", `SELECT "host FROM conn_events`, "unterminated quoted identifier"},
		{"zero length", `SELECT "" FROM conn_events`, "zero-length quoted identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Query(ctx, tt.sql)
			if err == nil {
				t.Fatalf("expected an error for %q", tt.sql)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err.Error(), tt.want)
			}
		})
	}
}
