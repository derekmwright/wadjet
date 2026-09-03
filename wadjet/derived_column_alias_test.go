package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A derived table's COLUMN-ALIAS LIST renames its columns positionally (#613).
//
// `(SELECT s, n FROM t) AS b(kk, nn)` publishes kk and nn. Only the CTE arm
// honoured the list, so on a derived table the aliased names resolved to
// nothing: an EXISTS or IN over one answered 0 rows in SILENCE, and — once the
// unparseable re-run was made loud — 42601 at the parser, because the list was
// never accepted on that path at all.
//
// PostgreSQL 17, measured live over a two-column derived table:
//
//	AS b(kk, nn)        → kk, nn
//	AS b(kk)            → kk, n     (fewer aliases rename a PREFIX)
//	AS b(kk, nn, extra) → 42P10 `table "b" has 2 columns available but
//	                     3 columns specified`
func TestDerivedTableColumnAliasList(t *testing.T) {
	ctx := context.Background()
	db := dcaDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		cols  []string
		rows  []string
		state string
	}{
		{name: "renames positionally",
			sql:  "SELECT * FROM (SELECT s, n FROM dca) AS b(kk, nn) ORDER BY kk, nn",
			cols: []string{"kk", "nn"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"}},
		{name: "the aliased names are what a reference resolves",
			sql:  "SELECT b.kk, b.nn FROM (SELECT s, n FROM dca) AS b ORDER BY 1, 2",
			cols: []string{"s", "n"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"},
			state: "42703"},
		{name: "a correlated EXISTS over one",
			sql: "SELECT COUNT(*) AS c FROM dca a WHERE EXISTS (SELECT 1 FROM " +
				"(SELECT s, n FROM dca) AS b(kk, nn) WHERE b.kk = a.s AND b.nn = a.n)",
			cols: []string{"c"}, rows: []string{"[3]"}},
		{name: "an IN over one",
			sql: "SELECT COUNT(*) AS c FROM dca a WHERE a.s IN (SELECT b.kk FROM " +
				"(SELECT s FROM dca) AS b(kk))",
			cols: []string{"c"}, rows: []string{"[3]"}},
		{name: "a single-column list",
			sql:  "SELECT kk FROM (SELECT s FROM dca) AS b(kk) ORDER BY 1",
			cols: []string{"kk"}, rows: []string{"[a]", "[a]", "[b]"}},
		{name: "over an expression",
			sql:  "SELECT kk FROM (SELECT n + 1 FROM dca) AS b(kk) ORDER BY 1",
			cols: []string{"kk"}, rows: []string{"[11]", "[21]", "[31]"}},
		{name: "a CTE column list, which always worked",
			sql:  "WITH c(kk, nn) AS (SELECT s, n FROM dca) SELECT * FROM c ORDER BY 1, 2",
			cols: []string{"kk", "nn"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"}},
		// The BOUNDARY, both sides (rule 11).
		{name: "FEWER aliases rename a prefix",
			sql:  "SELECT * FROM (SELECT s, n FROM dca) AS b(kk) ORDER BY 1, 2",
			cols: []string{"kk", "n"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"}},
		{name: "MORE aliases than columns is 42P10",
			sql:   "SELECT * FROM (SELECT s, n FROM dca) AS b(kk, nn, extra)",
			state: "42P10"},
		{name: "MORE aliases on a CTE is 42P10 too",
			sql:   "WITH c(kk, nn, extra) AS (SELECT s, n FROM dca) SELECT * FROM c",
			state: "42P10"},
		{name: "FEWER aliases on a CTE rename a prefix",
			sql:  "WITH c(kk) AS (SELECT s, n FROM dca) SELECT * FROM c ORDER BY 1, 2",
			cols: []string{"kk", "n"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"}},
		{name: "no list at all keeps the inner names",
			sql:  "SELECT * FROM (SELECT s, n FROM dca) AS b ORDER BY 1, 2",
			cols: []string{"s", "n"}, rows: []string{"[a 10]", "[a 30]", "[b 20]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s: answered %d rows; want SQLSTATE %s", tc.sql, len(res.Rows), tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Fatalf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.Columns) != len(tc.cols) {
				t.Fatalf("published %v, want %v\n  SQL: %s", res.Columns, tc.cols, tc.sql)
			}
			for i, c := range tc.cols {
				if res.Columns[i] != c {
					t.Errorf("column %d = %q, want %q\n  SQL: %s", i, res.Columns[i], c, tc.sql)
				}
			}
			if len(res.Rows) != len(tc.rows) {
				t.Fatalf("%d rows, want %d\n  SQL: %s", len(res.Rows), len(tc.rows), tc.sql)
			}
			for i, w := range tc.rows {
				if got := fmt.Sprint(res.Cells(i)); got != w {
					t.Errorf("row %d = %s, want %s\n  SQL: %s", i, got, w, tc.sql)
				}
			}
		})
	}
}

func dcaDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE dca (id INT64, s STRING, n INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO dca VALUES (1,'a',10),(2,'b',20),(3,'a',30)"); err != nil {
		t.Fatal(err)
	}
	return db
}
