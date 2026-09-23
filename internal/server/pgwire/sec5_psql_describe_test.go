// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// `\d` is the first thing an operator types, and it found nothing (#944).
//
// psql does not look a relation up by equality. It sends
//
//	WHERE c.relname OPERATOR(pg_catalog.~) '^(sec5_t)$' COLLATE pg_catalog.default
//
// and the catalog emulation matched pg_class predicates with
// `extractParamValue`, which reads `relname = '<literal>'` and nothing else.
// The regex spelling matched no branch, the statement fell through to the
// generic empty answer, and psql printed "Did not find any relation named
// …" — for a table that exists, for every identity, with and without
// authentication.
//
// The statements below are the ones psql 17.5 sends, captured with `psql -E`
// against a real PostgreSQL 17 server. Both of them have to answer: psql
// reports "Did not find any relation with OID …" when the SECOND one — which
// names no `relname` at all — comes back empty.

const (
	// psqlRelationLookup is `\d <name>`'s first statement, verbatim.
	psqlRelationLookup = "SELECT c.oid,\n  n.nspname,\n  c.relname\n" +
		"FROM pg_catalog.pg_class c\n" +
		"     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
		"WHERE c.relname OPERATOR(pg_catalog.~) '^(%s)$' COLLATE pg_catalog.default\n" +
		"  AND pg_catalog.pg_table_is_visible(c.oid)\nORDER BY 2, 3;"
	// psqlRelationDetail is the second, by the OID the first returned.
	psqlRelationDetail = "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, " +
		"c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, " +
		"c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE " +
		"c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, " +
		"c.relreplident, am.amname\nFROM pg_catalog.pg_class c\n" +
		" LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)\n" +
		"LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)\nWHERE c.oid = '%s';"
	// psqlColumnLookup is the third, the column list.
	psqlColumnLookup = "SELECT a.attname,\n  pg_catalog.format_type(a.atttypid, a.atttypmod),\n" +
		"  a.attnotnull\nFROM pg_catalog.pg_attribute a\n" +
		"WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped\nORDER BY a.attnum;"
	// psqlListRelations is `\dt`, which carries no relname pattern.
	psqlListRelations = "SELECT n.nspname as \"Schema\",\n  c.relname as \"Name\",\n" +
		"  c.relkind\nFROM pg_catalog.pg_class c\n" +
		"     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
		"WHERE c.relkind IN ('r','p','')\n      AND n.nspname <> 'pg_catalog'\n" +
		"  AND pg_catalog.pg_table_is_visible(c.oid)\nORDER BY 1,2;"
	// psqlListRelationsPattern is `\dt <pattern>` — the same listing with a
	// WILDCARD pattern.
	psqlListRelationsPattern = "SELECT n.nspname as \"Schema\",\n  c.relname as \"Name\",\n" +
		"  c.relkind\nFROM pg_catalog.pg_class c\n" +
		"     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
		"WHERE c.relkind IN ('r','p','')\n" +
		"  AND c.relname OPERATOR(pg_catalog.~) '^(sec5.*)$' COLLATE pg_catalog.default\n" +
		"ORDER BY 1,2;"
)

// sec5DescribeDB has an ordinary relation and a CamelCase one.
func sec5DescribeDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, name := range []string{"sec5_t", "Sec5Mixed"} {
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "label", Type: parquet.TypeString, Nullable: true},
		}}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	return db
}

// sec5Rows runs one statement and returns its rows as joined text.
func sec5Rows(t *testing.T, conn *pgconn.PgConn, sql string) []string {
	t.Helper()
	res := conn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("%s\n  -> %v", sql, res.Err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		fields := make([]string, 0, len(row))
		for _, v := range row {
			fields = append(fields, string(v))
		}
		out = append(out, strings.Join(fields, "|"))
	}
	return out
}

// TestPsqlDescribeResolvesItsRelation runs the three statements `\d` sends, in
// order, feeding each one the OID the previous answered — the whole exchange,
// through pgx, over the real protocol.
func TestPsqlDescribeResolvesItsRelation(t *testing.T) {
	db := sec5DescribeDB(t)
	srv := startTestServer(t, db)
	conn := sec5Pgconn(t, srv.Addr())

	for _, name := range []string{"sec5_t", "Sec5Mixed"} {
		t.Run(name, func(t *testing.T) {
			rows := sec5Rows(t, conn, fmt.Sprintf(psqlRelationLookup, name))
			if len(rows) != 1 {
				t.Fatalf("the relation lookup answered %d rows, want 1 — psql prints "+
					"\"Did not find any relation\" on an empty answer\n  %v", len(rows), rows)
			}
			parts := strings.Split(rows[0], "|")
			if len(parts) != 3 {
				t.Fatalf("lookup row %q does not carry oid|nspname|relname", rows[0])
			}
			oid, nsp, rel := parts[0], parts[1], parts[2]
			if rel != name {
				t.Errorf("relname = %q, want %q", rel, name)
			}
			if nsp != "public" {
				t.Errorf("nspname = %q, want \"public\" — psql prints the qualified name "+
					"in the header and an empty schema reads as `Table \".%s\"`", nsp, name)
			}

			detail := sec5Rows(t, conn, fmt.Sprintf(psqlRelationDetail, oid))
			if len(detail) != 1 {
				t.Fatalf("the detail lookup by OID answered %d rows, want 1 — psql prints "+
					"\"Did not find any relation with OID\" on an empty answer", len(detail))
			}
			d := strings.Split(detail[0], "|")
			if len(d) < 2 || d[1] != "r" {
				t.Errorf("relkind = %q, want \"r\" (psql reads it positionally to decide "+
					"what it is describing)", detail[0])
			}
			// The reloftype column is psql's `CASE WHEN c.reloftype = 0 THEN
			// '' ELSE …::regtype END`. This server does not evaluate the CASE,
			// so the honest answer is EMPTY: a `0` there is read as a type
			// NAME and printed as "Typed table of type: 0".
			if len(d) >= 12 && d[11] != "" {
				t.Errorf("the reloftype expression answered %q, want empty", d[11])
			}

			cols := sec5Rows(t, conn, fmt.Sprintf(psqlColumnLookup, oid))
			if len(cols) != 2 {
				t.Fatalf("the column lookup answered %d rows, want 2 (id, label): %v",
					len(cols), cols)
			}
			if !strings.HasPrefix(cols[0], "id|") || !strings.HasPrefix(cols[1], "label|") {
				t.Errorf("columns = %v, want id then label", cols)
			}
		})
	}

	// `\d` on a relation that does not exist still answers nothing, which is
	// what psql renders as "Did not find any relation named".
	if rows := sec5Rows(t, conn, fmt.Sprintf(psqlRelationLookup, "sec5_nosuch")); len(rows) != 0 {
		t.Errorf("the lookup of a missing relation answered %v, want nothing", rows)
	}

	// `\dt` carries no pattern and still lists both relations.
	list := sec5Rows(t, conn, psqlListRelations)
	joined := strings.Join(list, ",")
	if !strings.Contains(joined, "sec5_t") || !strings.Contains(joined, "Sec5Mixed") {
		t.Errorf("\\dt listed %v, want both relations", list)
	}
	if !strings.Contains(joined, "public") {
		t.Errorf("\\dt listed no schema: %v", list)
	}
}

// TestRelnameRegexAnswersAsPostgreSQL — the pattern operators are the
// engine's (textregexeq and its siblings), so every spelling psql and a client
// write is a predicate over pg_class that answers PostgreSQL's rows.
//
// These four were REFUSED while the catalog was a canned responder that could
// only recognise psql's one anchored whole-name form; the refusal was the
// honest disposition then, and a wrong listing (every relation, or none) was
// what it replaced.
func TestRelnameRegexAnswersAsPostgreSQL(t *testing.T) {
	db := sec5DescribeDB(t)
	srv := startTestServer(t, db)
	conn := sec5Pgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql   string
		want        []string // exact rows, when set
		has, hasNot string   // membership, for a listing over every relation
	}{
		{name: "a wildcard pattern", sql: psqlListRelationsPattern,
			want: []string{"public|sec5_t|r"}},
		{name: "an unanchored pattern",
			sql:  "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) 'sec5'",
			want: []string{"sec5_t"}},
		{name: "a negated match",
			sql: "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname !~ '^(sec5_t)$'",
			has: "Sec5Mixed", hasNot: "sec5_t"},
		{name: "a case-insensitive match",
			sql:  "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname ~* '^(SEC5_T)$'",
			want: []string{"sec5_t"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := sec5Rows(t, conn, tc.sql)
			if tc.want != nil {
				if strings.Join(rows, ",") != strings.Join(tc.want, ",") {
					t.Fatalf("rows = %v, want %v", rows, tc.want)
				}
				return
			}
			joined := "," + strings.Join(rows, ",") + ","
			if !strings.Contains(joined, ","+tc.has+",") || strings.Contains(joined, ","+tc.hasNot+",") {
				t.Fatalf("rows %v: want %q in and %q out", rows, tc.has, tc.hasNot)
			}
		})
	}
}

// TestPsqlDescribeFollowsTheTableDecision — the visibility filter SEC3
// installed applies on this path exactly as it does on the equality one.
//
// A denied relation answers ZERO rows to `\d`, which psql renders as "Did not
// find any relation named" — the product's anti-enumeration answer on this
// door, and NOT 42501: naming the relation in a refusal is what the metadata
// decision exists to avoid here, and the data door already refuses by name for
// anyone who reaches it.
func TestPsqlDescribeFollowsTheTableDecision(t *testing.T) {
	db := sec3CatalogDB(t)
	provider := sec3CatalogProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	for _, tc := range []struct {
		name, user, key string
		wantRows        int
	}{
		{"the denied identity finds no relation", "analyst-user", "analyst-key", 0},
		{"an identity that may read it does", "admin-user", "admin-key", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := sec3Pgconn(t, srv.Addr(), tc.user, tc.key)
			rows := sec5Rows(t, conn, fmt.Sprintf(psqlRelationLookup, "secret"))
			if len(rows) != tc.wantRows {
				t.Errorf("the psql-shaped lookup of a denied relation answered %d rows, want %d: %v",
					len(rows), tc.wantRows, rows)
			}
			// The permitted relation answers for both identities, so the
			// filter is the DECISION and not a blanket emptying of the branch.
			if got := sec5Rows(t, conn, fmt.Sprintf(psqlRelationLookup, "public_t")); len(got) != 1 {
				t.Errorf("the permitted relation answered %d rows, want 1", len(got))
			}
		})
	}
}

// sec5Pgconn connects with no credential, for the no-auth arms.
func sec5Pgconn(t *testing.T, addr string) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(context.Background(),
		fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", addr))
	if err != nil {
		t.Fatalf("pgconn.Connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}
