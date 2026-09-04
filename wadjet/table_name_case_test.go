package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A TABLE name follows the same rule a column name does: byte-exact first,
// then a UNIQUE ASCII-case-insensitive match, and a DELIMITED reference gets
// no concession (ADR-0012).
//
// It is the same divergence from PostgreSQL and it exists for the same reason.
// PostgreSQL folds an unquoted reference and then matches the CATALOG exactly,
// so a relation created as `"MyTab"` is unreachable as `MyTab` there. Wadjet's
// tables come from parquet and ingest, where a user-chosen mixed-case name is
// ordinary and nothing quoted it at creation — so folding at the lexer (#731)
// without this concession would make every such table unreachable unquoted,
// which is a breaking change for a catalog written before the fold rather than
// a semantic improvement.
//
// The boundary is what keeps it a superset: a reference carrying an UPPER-CASE
// letter can only have been written between double quotes, so it resolves
// byte-exact only, and two tables differing only by case resolve to NOTHING
// with a refusal that names both. Picking one would be a silent wrong table,
// which is the whole failure class this arc exists to remove.
func TestATableNameResolvesLikeAColumnName(t *testing.T) {
	ctx := context.Background()
	db := tableCaseDB(t, ctx)

	cases := []struct {
		name  string
		sql   string
		want  []string
		state string // non-empty: the query must be refused with this SQLSTATE
		msg   string // substring the refusal must carry
	}{
		// The concession, both directions.
		{name: "the stored spelling reaches it",
			sql: `SELECT k FROM "TcMixed" ORDER BY k`, want: []string{"1", "2"}},
		{name: "the unquoted spelling as written reaches it",
			sql: "SELECT k FROM TcMixed ORDER BY k", want: []string{"1", "2"}},
		{name: "the folded spelling reaches it",
			sql: "SELECT k FROM tcmixed ORDER BY k", want: []string{"1", "2"}},
		{name: "an upper-case unquoted spelling reaches it",
			sql: "SELECT k FROM TCMIXED ORDER BY k", want: []string{"1", "2"}},
		{name: "a qualified reference reaches it",
			sql: "SELECT TcMixed.k FROM TcMixed ORDER BY k", want: []string{"1", "2"}},
		{name: "an alias over it reaches it",
			sql: "SELECT x.k FROM TcMixed x ORDER BY k", want: []string{"1", "2"}},
		{name: "a join reaches it",
			sql:  "SELECT TcMixed.k FROM TcMixed, tcplain WHERE TcMixed.k = tcplain.k ORDER BY k",
			want: []string{"1", "2"}},
		{name: "a derived table reaches it",
			sql: "SELECT v FROM (SELECT k AS v FROM TcMixed) s ORDER BY v", want: []string{"1", "2"}},
		{name: "a CTE reaches it",
			sql: "WITH c AS (SELECT k FROM TcMixed) SELECT k FROM c ORDER BY k", want: []string{"1", "2"}},

		// The BOUNDARY: a delimited reference carrying an upper-case letter
		// takes no concession, exactly as a delimited COLUMN reference does.
		{name: "a delimited upper-case reference takes no concession",
			sql: `SELECT k FROM "TCMIXED" ORDER BY k`, state: "42P01"},
		{name: "a delimited mixed-case reference is byte-exact and hits",
			sql: `SELECT k FROM "TcMixed" ORDER BY k`, want: []string{"1", "2"}},

		// TWO tables differing only by case resolve to NOTHING, and the
		// refusal names both. Each stays reachable by its own exact spelling.
		{name: "two tables differing only by case refuse",
			sql: "SELECT k FROM TcTwo ORDER BY k", state: "42P01",
			msg: "case-insensitively"},
		{name: "the first of the pair is reachable quoted",
			sql: `SELECT k FROM "TcTwo" ORDER BY k`, want: []string{"71"}},
		{name: "the second of the pair is reachable quoted",
			sql: `SELECT k FROM "TCTWO" ORDER BY k`, want: []string{"72"}},

		// A BYTE-EXACT match always wins, so a folded reference beside a
		// lower-case twin keeps naming the twin — which is also what
		// PostgreSQL does, since that IS the folded name.
		{name: "a byte-exact match wins over the case-insensitive one",
			sql: "SELECT k FROM TcAmb ORDER BY k", want: []string{"20"}},
		{name: "and the mixed-case twin is still reachable quoted",
			sql: `SELECT k FROM "TcAmb" ORDER BY k`, want: []string{"10"}},

		// A real miss is still a real miss.
		{name: "a table that does not exist is 42P01",
			sql: "SELECT k FROM tcnosuch ORDER BY k", state: "42P01"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if c.state != "" {
				if err == nil {
					t.Fatalf("answered %d rows; want %s\n  SQL: %s", len(res.Rows), c.state, c.sql)
				}
				if got := sqlerr.StateOf(err); got != c.state {
					t.Fatalf("SQLSTATE %s, want %s: %v\n  SQL: %s", got, c.state, err, c.sql)
				}
				if c.msg != "" && !strings.Contains(err.Error(), c.msg) {
					t.Fatalf("refusal %q does not carry %q\n  SQL: %s", err.Error(), c.msg, c.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v\n  SQL: %s", err, c.sql)
			}
			var got []string
			for i := range res.Rows {
				got = append(got, fmt.Sprint(res.Rows[i][res.Columns[0]]))
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Fatalf("answered %v, want %v\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}

// A DML statement REFERENCES an existing table, so it takes the same
// concession — and the WRITE has to land on the table the read resolved. A
// door that conceded on the lookup and then keyed the manifest byte-exact
// would write somewhere else, silently.
func TestADMLStatementWritesToTheTableItResolved(t *testing.T) {
	ctx := context.Background()
	db := tableCaseDB(t, ctx)

	count := func() int64 {
		t.Helper()
		res, err := db.Query(ctx, `SELECT COUNT(*) AS n FROM "TcMixed"`)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		n, ok := res.Rows[0]["n"].(int64)
		if !ok {
			t.Fatalf("count is %T, not int64", res.Rows[0]["n"])
		}
		return n
	}

	before := count()
	if _, err := db.Query(ctx, "INSERT INTO TcMixed (k) VALUES (99)"); err != nil {
		t.Fatalf("INSERT through the folded spelling: %v", err)
	}
	if got := count(); got != before+1 {
		t.Fatalf("after INSERT the stored table has %d rows, want %d — the write did not "+
			"land on the table the read resolved", got, before+1)
	}
	if _, err := db.Query(ctx, "DELETE FROM tcmixed WHERE k = 99"); err != nil {
		t.Fatalf("DELETE through the folded spelling: %v", err)
	}
	if got := count(); got != before {
		t.Fatalf("after DELETE the stored table has %d rows, want %d", got, before)
	}
	if _, err := db.Query(ctx, "UPDATE TCMIXED SET k = 3 WHERE k = 2"); err != nil {
		t.Fatalf("UPDATE through the folded spelling: %v", err)
	}
	res, err := db.Query(ctx, `SELECT k FROM "TcMixed" ORDER BY k`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for i := range res.Rows {
		got = append(got, fmt.Sprint(res.Rows[i]["k"]))
	}
	if strings.Join(got, ",") != "1,3" {
		t.Fatalf("after UPDATE the stored table holds %v, want [1 3]", got)
	}

	// CREATE does NOT concede: minting a name is not referencing one, so
	// creating the folded spelling beside the mixed-case table is a NEW table
	// and must not write into the existing one.
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "tcmixed", schema, nil); err != nil {
		t.Fatalf("CREATE of the folded spelling beside the mixed-case table: %v", err)
	}
	if got := count(); got != before {
		t.Fatalf("creating `tcmixed` changed `TcMixed` to %d rows, want %d", got, before)
	}
}

func tableCaseDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mk := func(name string, ks ...int64) {
		t.Helper()
		schema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
		var rows []map[string]any
		for _, k := range ks {
			rows = append(rows, map[string]any{"k": k})
		}
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// TcMixed is the shape the concession exists for: a mixed-case name that
	// no DDL quoted, the way parquet and ingest produce one.
	mk("TcMixed", 1, 2)
	mk("tcplain", 1, 2)
	// TcAmb has a lower-case twin, so a folded reference matches the twin
	// BYTE-EXACT and never reaches the concession.
	mk("TcAmb", 10)
	mk("tcamb", 20)
	// TcTwo and TCTWO differ ONLY by case with no folded twin, which is the
	// one shape the concession declines to answer.
	mk("TcTwo", 71)
	mk("TCTWO", 72)
	return db
}
