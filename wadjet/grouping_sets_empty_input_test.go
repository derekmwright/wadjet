// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// AN EMPTY GROUPING SET OVER ZERO INPUT ROWS (#1335). A grouping set that is
// EMPTY (the grand total of ROLLUP and CUBE, an explicit `()`) is one group of
// every row, so over no input it answers ONE row, as an ungrouped aggregate
// does; a non-empty set has no group and answers none. PostgreSQL lists one
// row per empty set in the query, duplicates included. The engine answered
// zero rows for every one of these, because the grand-total group was only
// ever minted by a row; and `GROUPING SETS ((), ())` answered ONE row over
// any input, because with no named key the aggregate took its scalar fast path
// and never looked at the sets.
//
// Every want is PostgreSQL 17.11's answer, on the plain engine and under a
// memory budget small enough that the aggregate spills on the large table.
// Cells are rows joined by " | ", sorted, NULL spelled out.
func TestGroupingSetsEmptyInput(t *testing.T) {
	ctx := context.Background()
	arms := map[string]Config{
		"single":  {Store: objstore.NewMemStore(), Bucket: "test"},
		"spilled": {Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16},
	}
	// The three inputs of a cell: filtered to nothing, an empty table, and a
	// non-empty control.
	type cell struct{ sql, zero, full string }
	cells := []cell{
		{"SELECT g, MAX(x) AS m FROM %s GROUP BY ROLLUP(g)", "NULL,NULL", "1,20 | 2,30 | NULL,30"},
		{"SELECT g, COUNT(*), COUNT(x), COUNT(DISTINCT x), SUM(x), MAX(x), GROUPING(g) FROM %s GROUP BY ROLLUP(g)",
			"NULL,0,0,0,NULL,NULL,1", "1,2,2,2,30,20,0 | 2,1,1,1,30,30,0 | NULL,3,3,3,60,30,1"},
		{"SELECT g, h, COUNT(*), GROUPING(g, h) FROM %s GROUP BY ROLLUP(g, h)", "NULL,NULL,0,3",
			"1,1,1,0 | 1,2,1,0 | 1,NULL,2,1 | 2,1,1,0 | 2,NULL,1,1 | NULL,NULL,3,3"},
		{"SELECT g, h, COUNT(*), GROUPING(g, h) FROM %s GROUP BY CUBE(g, h)", "NULL,NULL,0,3",
			"1,1,1,0 | 1,2,1,0 | 1,NULL,2,1 | 2,1,1,0 | 2,NULL,1,1 | NULL,1,2,2 | NULL,2,1,2 | NULL,NULL,3,3"},
		{"SELECT g, SUM(x) FROM %s GROUP BY GROUPING SETS ((g), ())", "NULL,NULL", "1,30 | 2,30 | NULL,60"},
		{"SELECT g, SUM(x) FROM %s GROUP BY GROUPING SETS ((g))", "", "1,30 | 2,30"},
		{"SELECT COUNT(*) FROM %s GROUP BY GROUPING SETS ((), ())", "0 | 0", "3 | 3"},
		{"SELECT COUNT(*) FROM %s GROUP BY GROUPING SETS (())", "0", "3"},
		{"SELECT g, h, COUNT(*) FROM %s GROUP BY GROUPING SETS ((g, h), (g), ())", "NULL,NULL,0",
			"1,1,1 | 1,2,1 | 1,NULL,2 | 2,1,1 | 2,NULL,1 | NULL,NULL,3"},
		{"SELECT g + 1, COUNT(*) FROM %s GROUP BY ROLLUP(g + 1)", "NULL,0", "2,2 | 3,1 | NULL,3"},
		// HAVING sees the grand-total row and may drop it.
		{"SELECT g, COUNT(*) FROM %s GROUP BY ROLLUP(g) HAVING COUNT(*) = 0", "NULL,0", ""},
		{"SELECT g, COUNT(*) FROM %s GROUP BY ROLLUP(g) HAVING GROUPING(g) = 1", "NULL,0", "NULL,3"},
		{"SELECT g, COUNT(*) FROM %s GROUP BY ROLLUP(g) HAVING COUNT(*) > 0", "", "1,2 | 2,1 | NULL,3"},
		{"SELECT g, COUNT(*) AS c FROM %s GROUP BY ROLLUP(g) ORDER BY c DESC, g LIMIT 1", "NULL,0", "NULL,3"},
		// The aggregate read through a derived table, a CTE and a UNION ALL arm.
		{"SELECT COUNT(*) FROM (SELECT g, MAX(x) FROM %s GROUP BY ROLLUP(g)) d", "1", "3"},
		{"WITH c AS (SELECT g, MAX(x) AS m FROM %s GROUP BY ROLLUP(g)) SELECT * FROM c", "NULL,NULL", "1,20 | 2,30 | NULL,30"},
		{"SELECT g, MAX(x) FROM %s GROUP BY ROLLUP(g) UNION ALL SELECT 100, 100", "100,100 | NULL,NULL", "1,20 | 100,100 | 2,30 | NULL,30"},
		{"SELECT g, SUM(x) + 1, GROUPING(g) + 1 FROM %s GROUP BY ROLLUP(g)", "NULL,NULL,2", "1,31,1 | 2,31,1 | NULL,61,2"},
		// Controls: the shapes the empty-set rule must not touch.
		{"SELECT g, COUNT(*) FROM %s GROUP BY g", "", "1,2 | 2,1"},
		{"SELECT COUNT(*), MAX(x) FROM %s", "0,NULL", "3,30"},
		{"SELECT COUNT(*), COUNT(DISTINCT x), SUM(x) FROM %s", "0,0,NULL", "3,3,60"},
		{"SELECT COUNT(*), COUNT(DISTINCT x) FROM (SELECT g, x FROM %s) q GROUP BY g", "", "1,1 | 2,2"},
	}
	for name, cfg := range arms {
		t.Run(name, func(t *testing.T) {
			cfg.SpillDir = t.TempDir()
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE gse_t (id INT, g INT, h INT, x INT)",
				"INSERT INTO gse_t VALUES (1,1,1,10),(2,1,2,20),(3,2,1,30)",
				"CREATE TABLE gse_e (id INT, g INT, h INT, x INT)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			for _, c := range cells {
				for _, in := range []struct{ from, want, what string }{
					{"gse_t WHERE id < 0", c.zero, "filtered to nothing"},
					{"gse_e", c.zero, "empty table"},
					{"gse_t", c.full, "non-empty"},
				} {
					q := fmt.Sprintf(c.sql, in.from)
					if got := gseRows(db, q); got != in.want {
						t.Errorf("%s (%s)\n got  %q\n want %q (PostgreSQL 17.11)", q, in.what, got, in.want)
					}
				}
			}
			// The result types of the synthesized row are the ones the rows of a
			// non-empty input carry.
			q := "SELECT g, h, COUNT(*), SUM(x), MAX(x), GROUPING(g, h) FROM %s GROUP BY ROLLUP(g, h)"
			zt, ft := gseTypes(db, fmt.Sprintf(q, "gse_e")), gseTypes(db, fmt.Sprintf(q, "gse_t"))
			if zt != ft {
				t.Errorf("types over zero rows %s, over rows %s", zt, ft)
			}
			// And for keys of every family a key can be, derived ones included.
			for _, ddl := range []string{
				"CREATE TABLE gse_y (id INT, g BIGINT, s VARCHAR, d DATE, n NUMERIC(10,2), f DOUBLE, b BOOLEAN, x INT)",
				"INSERT INTO gse_y VALUES (1, 1, 'a', DATE '2024-01-02', 1.25, 0.5, true, 10)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			q = "SELECT g, s, d, n, f, b, g + 1, COUNT(*) FROM gse_y %s GROUP BY ROLLUP(g, s, d, n, f, b, g + 1)"
			zt, ft = gseTypes(db, fmt.Sprintf(q, "WHERE id < 0")), gseTypes(db, fmt.Sprintf(q, ""))
			if zt != ft {
				t.Errorf("key types over zero rows %s, over rows %s", zt, ft)
			}
		})
	}
}

// TestGroupingSetsEmptyInputBig is the same rule over a table big enough that
// the aggregate spills under the small budget: the non-empty control proves
// the spill path ran and still answers, the filtered arm that the identity row
// does not depend on it.
func TestGroupingSetsEmptyInputBig(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE gse_b (id INT, g INT, x INT)"); err != nil {
		t.Fatal(err)
	}
	var vals []string
	for i := 1; i <= 20000; i++ {
		vals = append(vals, fmt.Sprintf("(%d,%d,%d)", i, i, i))
	}
	if _, err := db.Query(ctx, "INSERT INTO gse_b VALUES "+strings.Join(vals, ",")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT g, COUNT(*), MAX(x) FROM gse_b WHERE id < 0 GROUP BY ROLLUP(g)", "NULL,0,NULL"},
		{"SELECT COUNT(*) FROM gse_b WHERE id < 0 GROUP BY GROUPING SETS ((), ())", "0 | 0"},
		{"SELECT COUNT(*) FROM (SELECT g, COUNT(*), MAX(x) FROM gse_b GROUP BY ROLLUP(g)) d", "20001"},
		{"SELECT g, COUNT(*), MAX(x) FROM gse_b WHERE g = 7 GROUP BY ROLLUP(g)", "7,1,7 | NULL,1,7"},
	} {
		if got := gseRows(db, c.sql); got != c.want {
			t.Errorf("%s\n got  %q\n want %q (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
}

// TestGroupingSetsEmptyInputFilter pins the COUNT(*) FILTER cells over zero
// rows at PostgreSQL 17.11's answer. They were a known residual of #1690: the
// FILTER clause was rewritten to SUM(CASE WHEN p THEN 1 ELSE 0 END), and the
// one row an empty grouping set answers over no input carried that SUM's NULL
// where a COUNT answers 0. The rewrite is COUNT(CASE WHEN p THEN 1 END) now.
func TestGroupingSetsEmptyInputFilter(t *testing.T) {
	ctx := context.Background()
	for name, cfg := range map[string]Config{
		"single":  {Store: objstore.NewMemStore(), Bucket: "test"},
		"spilled": {Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.SpillDir = t.TempDir()
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE st (id INT, g INT, x INT)",
				"INSERT INTO st VALUES (1,1,10),(2,1,20),(3,2,30)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			for _, c := range []struct{ sql, want string }{
				{"SELECT g, COUNT(*) FILTER (WHERE x > 10) FROM st WHERE id < 0 GROUP BY ROLLUP(g)", "NULL,0"},
				{"SELECT COUNT(*) FILTER (WHERE x > 10) FROM st WHERE id < 0 GROUP BY GROUPING SETS ((), ())", "0 | 0"},
			} {
				if got := gseRows(db, c.sql); got != c.want {
					t.Errorf("%s\n got  %q\n want %q (PostgreSQL 17.11)", c.sql, got, c.want)
				}
			}
		})
	}
}

// TestGroupingSetsEmptyInputOrdinalResidual pins GROUP BY ROLLUP (1), an
// ordinal inside ROLLUP, over zero rows at what the engine answers today. It
// is a known residual of #1480, NOT a statement of the right answer: the
// engine expands ROLLUP (1) into the grand total twice (over non-empty input
// it answers the grand-total row twice, which is #1480), so with the empty
// grouping set now answering its row over zero input (#1335) the statement
// moved from zero rows to two. PostgreSQL 17.11 answers one row. The day
// #1480 is fixed this pin must change to it.
func TestGroupingSetsEmptyInputOrdinalResidual(t *testing.T) {
	ctx := context.Background()
	for name, cfg := range map[string]Config{
		"single":  {Store: objstore.NewMemStore(), Bucket: "test"},
		"spilled": {Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.SpillDir = t.TempDir()
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE st (id INT, g INT, x INT)",
				"INSERT INTO st VALUES (1,1,10),(2,1,20),(3,2,30)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			const sql, tip, pg = "SELECT g, MAX(x) FROM st WHERE id < 0 GROUP BY ROLLUP (1)", "NULL,NULL | NULL,NULL", "NULL,NULL"
			if got := gseRows(db, sql); got != tip {
				t.Errorf("%s\n got  %q\n pinned %q (known residual of #1480; PostgreSQL 17.11 answers %q)", sql, got, tip, pg)
			}
		})
	}
}

// gseRows renders a statement's rows sorted and joined by " | ", NULL spelled
// out, or "ERR <message>".
func gseRows(db *DB, sql string) string {
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	rows := make([]string, len(res.Rows))
	for i := range res.Rows {
		var c []string
		for _, v := range res.Cells(i) {
			if v == nil {
				c = append(c, "NULL")
				continue
			}
			c = append(c, fmt.Sprint(v))
		}
		rows[i] = strings.Join(c, ",")
	}
	sort.Strings(rows)
	return strings.Join(rows, " | ")
}

func gseTypes(db *DB, sql string) string {
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	var types []string
	for _, m := range res.ColumnMetas {
		types = append(types, m.TypeID.String())
	}
	return strings.Join(types, ",")
}
