package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A non-recursive CTE's own name is NOT in scope inside its own body (#771).
//
// PostgreSQL's rule and the SQL standard's: a WITH item is visible to SIBLING
// and LATER items and to the main query, never inside itself. So
// `WITH t AS (SELECT … FROM t)` reads the BASE table t, and a WITH item that
// is not yet defined is 42P01 with a DETAIL naming it. Measured live on
// postgres:17-alpine.
//
// Wadjet registered the name BEFORE building or validating the body, so a CTE
// that shadows a base table could not read the table it shadows — and when
// nothing else answered to the name, the builder re-entered itself without
// bound and the PROCESS DIED with a stack overflow, which Go cannot recover
// from and any pgwire client can reach.
func TestCTEBodyDoesNotSeeItsOwnName(t *testing.T) {
	ctx := context.Background()
	db := cteScopeDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		cols  []string
		rows  []string
		state string
	}{
		{name: "a CTE shadowing a base table reads the table",
			sql: "WITH cs AS (SELECT id, a * 2 AS dv FROM cs) SELECT d.id, d.dv FROM cs d " +
				"JOIN cs e ON d.id = e.id ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 10]", "[2 12]"}},
		{name: "the same shadow with no join",
			sql:  "WITH cs AS (SELECT id, a * 2 AS dv FROM cs) SELECT id, dv FROM cs ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 10]", "[2 12]"}},
		{name: "a shadow that only renames",
			sql:  "WITH cs AS (SELECT id, a AS dv FROM cs) SELECT id, dv FROM cs ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 5]", "[2 6]"}},
		{name: "a shadow read through a star",
			sql:  "WITH cs AS (SELECT id, a * 2 AS dv FROM cs) SELECT * FROM cs ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 10]", "[2 12]"}},
		{name: "a shadow filtered inside and out",
			sql: "WITH cs AS (SELECT id, a * 2 AS dv FROM cs WHERE a > 5) SELECT id, dv FROM cs " +
				"WHERE dv > 0 ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[2 12]"}},
		// The name resolving to NOTHING inside the body used to be a FATAL
		// stack overflow: resolveTableOrCTE → Parse → build → itself.
		{name: "a self-reference with no base table is 42P01",
			sql: "WITH nosuch AS (SELECT * FROM nosuch) SELECT * FROM nosuch", state: "42P01"},
		{name: "a FORWARD reference to a later CTE is 42P01",
			sql:   "WITH c1 AS (SELECT x FROM c2), c2 AS (SELECT 1 AS x) SELECT * FROM c1",
			state: "42P01"},
		// The controls: everything else about CTE scope is unchanged.
		{name: "a LATER CTE sees an EARLIER one",
			sql:  "WITH c1 AS (SELECT 1 AS x), c2 AS (SELECT x + 1 AS y FROM c1) SELECT * FROM c2",
			cols: []string{"y"}, rows: []string{"[2]"}},
		{name: "a CTE with no shadow",
			sql:  "WITH c AS (SELECT id, a * 2 AS dv FROM cs) SELECT id, dv FROM c ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 10]", "[2 12]"}},
		{name: "a CTE referenced twice with no shadow",
			sql: "WITH c AS (SELECT id, a * 2 AS dv FROM cs) SELECT d.id, d.dv FROM c d " +
				"JOIN c e ON d.id = e.id ORDER BY 1",
			cols: []string{"id", "dv"}, rows: []string{"[1 10]", "[2 12]"}},
		{name: "the main query still sees the CTE, not the base table",
			sql:  "WITH cs AS (SELECT id, a * 2 AS dv FROM cs) SELECT COUNT(dv) AS n FROM cs",
			cols: []string{"n"}, rows: []string{"[2]"}},
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

func cteScopeDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE cs (id INT64, a INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO cs VALUES (1,5),(2,6)"); err != nil {
		t.Fatal(err)
	}
	return db
}
