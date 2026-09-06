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
	// WILDCARD pattern, which this server does not model.
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

// TestAnUnsupportedRelnameRegexRefuses — the general regex operator is NOT
// implemented, and a pattern this server cannot answer says so.
//
// The alternative shipped for a year: the predicate was ignored, so a
// `\dt <pattern>` listing answered EVERY visible relation, and the anchored
// form answered NONE. A filter that is silently dropped is a wrong answer, and
// a silently empty one is #944 itself.
func TestAnUnsupportedRelnameRegexRefuses(t *testing.T) {
	db := sec5DescribeDB(t)
	srv := startTestServer(t, db)
	conn := sec5Pgconn(t, srv.Addr())

	for _, tc := range []struct{ name, sql string }{
		{"a wildcard pattern", psqlListRelationsPattern},
		{"an unanchored pattern",
			"SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) 'sec5'"},
		{"a negated match",
			"SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname !~ '^(sec5_t)$'"},
		{"a case-insensitive match",
			"SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname ~* '^(sec5_t)$'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, nil).Read()
			if res.Err == nil {
				t.Fatalf("an unsupported pattern answered %d rows instead of refusing",
					len(res.Rows))
			}
			pgErr, ok := res.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("refusal is not a PostgreSQL error: %v", res.Err)
			}
			if pgErr.Code != "0A000" {
				t.Errorf("SQLSTATE %s, want 0A000 (feature_not_supported)", pgErr.Code)
			}
			if !strings.Contains(pgErr.Message, "^(name)$") {
				t.Errorf("the refusal does not name the form that works: %s", pgErr.Message)
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

// TestAnchoredRelnameRegexReadsWhatPsqlSends is the unit half: every pattern
// psql actually emits, and every one it does not.
func TestAnchoredRelnameRegexReadsWhatPsqlSends(t *testing.T) {
	for _, tc := range []struct {
		name, sql        string
		found, supported bool
		want             string
	}{
		{name: "the plain anchored form psql sends",
			sql:   `WHERE c.relname OPERATOR(pg_catalog.~) '^(sec5_t)$' COLLATE pg_catalog.default`,
			found: true, supported: true, want: "sec5_t"},
		{name: "a quoted identifier keeps its case",
			sql:   `WHERE c.relname OPERATOR(pg_catalog.~) '^(Sec5Mixed)$'`,
			found: true, supported: true, want: "Sec5Mixed"},
		{name: "the bare operator spelling",
			sql:   `WHERE relname ~ '^(sec5_t)$'`,
			found: true, supported: true, want: "sec5_t"},
		{name: "an E-string with an escaped metacharacter",
			sql:   `WHERE c.relname OPERATOR(pg_catalog.~) E'^(sec5\\.dot)$'`,
			found: true, supported: true, want: "sec5.dot"},
		{name: "a wildcard pattern is not this form",
			sql:   `WHERE c.relname OPERATOR(pg_catalog.~) '^(sec5.*)$'`,
			found: true, supported: false},
		{name: "an unanchored pattern is not this form",
			sql:   `WHERE c.relname ~ 'sec5'`,
			found: true, supported: false},
		{name: "a negated match is a different operator",
			sql:   `WHERE c.relname !~ '^(sec5_t)$'`,
			found: true, supported: false},
		{name: "a case-insensitive match is a different operator",
			sql:   `WHERE c.relname ~* '^(sec5_t)$'`,
			found: true, supported: false},
		{name: "an equality lookup is not a regex at all",
			sql:   `WHERE c.relname = 'sec5_t'`,
			found: false},
		{name: "a regex over another column is not this predicate",
			sql:   `WHERE n.nspname !~ '^pg_toast' AND c.relname = 'sec5_t'`,
			found: false},
		{name: "relnamespace is not relname",
			sql:   `LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`,
			found: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, found, supported := anchoredRelnameRegex(tc.sql)
			if found != tc.found || supported != tc.supported {
				t.Fatalf("found=%v supported=%v, want found=%v supported=%v",
					found, supported, tc.found, tc.supported)
			}
			if tc.supported && got != tc.want {
				t.Errorf("name = %q, want %q", got, tc.want)
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
