// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// AN AGGREGATE FILTER KEEPS ITS AGGREGATE (#1690). `AGG(a) FILTER (WHERE p)`
// is `AGG(CASE WHEN p THEN a END)`: a row the predicate rejects hands the
// aggregate a NULL value argument and every aggregate skips that. The one
// exception was COUNT(*), which has no argument and was rewritten to
// SUM(CASE WHEN p THEN 1 ELSE 0 END); a SUM over no rows is NULL where a COUNT
// is 0, so `COUNT(*) FILTER (WHERE p)` over zero rows answered NULL, in every
// grouping shape, and declared SUM's name. Two defects the same table found
// beside it are pinned here too: the arguments after the first were dropped
// (`string_agg(x, '-') FILTER` joined with the default separator, and
// `corr(x, y) FILTER` was refused for having one argument), and a COUNT(DISTINCT
// <expression>) answered NULL over zero input rows, which a FILTER hands it
// (`COUNT(DISTINCT x) FILTER (WHERE p)` counts a CASE).
//
// testdata/agg_filter_empty_pg17.tsv is the table: aggregate x input (zero rows,
// a filter matching none / some / all, a NULL value) x shape (ungrouped, GROUP
// BY, ROLLUP, GROUPING SETS ((), ()), two filtered aggregates, a filtered one
// beside an unfiltered one, a predicate on another column, a window
// aggregate), each cell's answer taken from PostgreSQL 17.11 and its declared
// type from the same oracle where the engine's type vocabulary agrees with it.
// Columns: cell key, SQL, PostgreSQL's answer (rows sorted, " | " between
// rows, floats to five significant digits), declared type ("-" when the
// engine's declaration is a known divergence unrelated to FILTER, such as
// STDDEV answering a float where PostgreSQL answers numeric).
func TestAggregateFilterEmptyInput(t *testing.T) {
	raw, err := os.ReadFile("testdata/agg_filter_empty_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
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
				"INSERT INTO st VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,NULL)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			cells := 0
			for _, line := range strings.Split(string(raw), "\n") {
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				f := strings.Split(line, "\t")
				if len(f) != 4 {
					t.Fatalf("malformed cell %q", line)
				}
				cells++
				key, sql, want, wantType := f[0], f[1], f[2], f[3]
				got := aggFilterNorm(gseRows(db, sql))
				if got != want {
					t.Errorf("%s\n  %s\n  got  %q\n  want %q (PostgreSQL 17.11)", key, sql, got, want)
				}
				if wantType != "-" {
					if gt := gseTypes(db, sql); gt != wantType {
						t.Errorf("%s\n  %s\n  declared %q, want %q (PostgreSQL 17.11)", key, sql, gt, wantType)
					}
				}
			}
			if cells < 500 {
				t.Fatalf("only %d cells read: the table is not the one the test pins", cells)
			}
		})
	}
}

// TestAggregateFilterEmptyInputBig is the same rule over a table big enough
// that the aggregate spills under the small budget. The non-empty cells prove
// the spill path ran and still answers; the empty ones that the identity row
// does not depend on it. Every want is PostgreSQL 17.11's answer.
func TestAggregateFilterEmptyInputBig(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 1 << 16, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE af_b (id INT, g INT, x INT)"); err != nil {
		t.Fatal(err)
	}
	var vals []string
	for i := 1; i <= 20000; i++ {
		vals = append(vals, fmt.Sprintf("(%d,%d,%d)", i, i, i))
	}
	if _, err := db.Query(ctx, "INSERT INTO af_b VALUES "+strings.Join(vals, ",")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT COUNT(*) FILTER (WHERE x > 10) FROM af_b WHERE id < 0", "0"},
		{"SELECT g, COUNT(*) FILTER (WHERE x > 10) FROM af_b WHERE id < 0 GROUP BY ROLLUP(g)", "NULL,0"},
		{"SELECT COUNT(*) FILTER (WHERE x > 10) FROM af_b WHERE id < 0 GROUP BY GROUPING SETS ((), ())", "0 | 0"},
		{"SELECT COUNT(DISTINCT x) FILTER (WHERE x > 10), SUM(x) FILTER (WHERE x > 10) FROM af_b WHERE id < 0", "0,NULL"},
		{"SELECT COUNT(*) FILTER (WHERE x > 10), COUNT(DISTINCT x) FILTER (WHERE x > 10), SUM(x) FILTER (WHERE x > 10) FROM af_b", "19990,19990,200009945"},
		{"SELECT COUNT(*) FILTER (WHERE x > 100000) FROM af_b", "0"},
		{"SELECT g % 3, COUNT(*) FILTER (WHERE x > 19990) FROM af_b GROUP BY g % 3", "0,3 | 1,3 | 2,4"},
	} {
		if got := gseRows(db, c.sql); got != c.want {
			t.Errorf("%s\n got  %q\n want %q (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
}

// TestAggregateFilterNotASum pins what the rewrite must not take from a user:
// a SUM written by hand over the same CASE is a SUM and answers NULL over zero
// rows, as PostgreSQL's does, while the FILTER spelling beside it answers 0.
func TestAggregateFilterNotASum(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE st (id INT, g INT, x INT)",
		"INSERT INTO st VALUES (1,1,10),(2,1,20),(3,2,30)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT SUM(CASE WHEN x > 10 THEN 1 ELSE 0 END) FROM st WHERE id < 0", "NULL"},
		{"SELECT SUM(CASE WHEN x > 10 THEN 1 ELSE 0 END), COUNT(*) FILTER (WHERE x > 10) FROM st WHERE id < 0", "NULL,0"},
		{"SELECT COUNT(*) FILTER (WHERE x > 10) FROM st", "2"},
		{"SELECT COUNT(*) FILTER (WHERE x > 10) AS c FROM st WHERE id < 0", "0"},
		// A COUNT beside a COUNT(DISTINCT) over zero rows (#1584) is unmoved.
		{"SELECT COUNT(*), COUNT(DISTINCT x) FROM st WHERE id < 0", "0,0"},
		{"SELECT COUNT(*) FILTER (WHERE x > 10), COUNT(DISTINCT x) FROM st WHERE id < 0", "0,0"},
		// The same COUNT(DISTINCT <expression>) with no FILTER in sight.
		{"SELECT COUNT(DISTINCT x + 1) FROM st WHERE id < 0", "0"},
		{"SELECT COUNT(DISTINCT x + 1) FROM st WHERE id < 0 GROUP BY g", ""},
		// Two COUNT(DISTINCT)s are not eligible for the two-level rewrite and
		// take the same path.
		{"SELECT COUNT(DISTINCT x), COUNT(DISTINCT id) FROM st WHERE id < 0", "0,0"},
	} {
		if got := gseRows(db, c.sql); got != c.want {
			t.Errorf("%s\n got  %q\n want %q (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
	// The label PostgreSQL publishes for the unaliased call is the aggregate's
	// own, not the SUM it used to be rewritten into.
	res, err := db.Query(ctx, "SELECT COUNT(*) FILTER (WHERE x > 10) FROM st")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ColumnMetas) != 1 || res.ColumnMetas[0].Name != "count" {
		t.Errorf("column metas %+v, want one column named count", res.ColumnMetas)
	}
}

// TestAggregateFilterWindowEmptyFrame pins the window form of the same rule: a
// FILTER aggregate over a frame holding no row (the first row's `1 PRECEDING
// AND 1 PRECEDING`) answers what the aggregate answers over no rows. COUNT(*)
// FILTER answered NULL there at the #1690 base (the SUM it was rewritten to)
// and answers 0, as PostgreSQL 17.11 does; COUNT(x) FILTER, SUM FILTER and the
// unfiltered controls did not move. Table st (1,1,10),(2,1,20),(3,2,30),
// (4,2,NULL), rows rendered id,value. Every want is PostgreSQL 17.11's.
func TestAggregateFilterWindowEmptyFrame(t *testing.T) {
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
				"INSERT INTO st VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,NULL)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			const prev = "OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)"
			for _, c := range []struct{ agg, over, want string }{
				{"COUNT(*) FILTER (WHERE x > 10)", prev, "1,0 | 2,0 | 3,1 | 4,1"},
				{"COUNT(*) FILTER (WHERE id > 100)", prev, "1,0 | 2,0 | 3,0 | 4,0"},
				{"COUNT(x) FILTER (WHERE x > 10)", prev, "1,0 | 2,0 | 3,1 | 4,1"},
				{"SUM(x) FILTER (WHERE x > 10)", prev, "1,NULL | 2,NULL | 3,20 | 4,30"},
				{"SUM(x) FILTER (WHERE x > 100)", prev, "1,NULL | 2,NULL | 3,NULL | 4,NULL"},
				// Controls: the unfiltered aggregates over the same frame.
				{"COUNT(*)", prev, "1,0 | 2,1 | 3,1 | 4,1"},
				{"COUNT(x)", prev, "1,0 | 2,1 | 3,1 | 4,1"},
				{"SUM(x)", prev, "1,NULL | 2,10 | 3,20 | 4,30"},
				// The empty frame is the LAST row's when the frame looks ahead.
				{"COUNT(*) FILTER (WHERE x > 10)", "OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)", "1,1 | 2,1 | 3,0 | 4,0"},
				{"COUNT(*) FILTER (WHERE x > 10)", "OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)", "1,0 | 2,0 | 3,0 | 4,1"},
			} {
				sql := "SELECT id, " + c.agg + " " + c.over + " FROM st"
				if got := gseRows(db, sql); got != c.want {
					t.Errorf("%s\n  got  %q\n  want %q (PostgreSQL 17.11)", sql, got, c.want)
				}
			}
			// PostgreSQL refuses DISTINCT in a window aggregate, and so does
			// the engine.
			sql := "SELECT id, COUNT(DISTINCT x) FILTER (WHERE x > 10) " + prev + " FROM st"
			if got := gseRows(db, sql); !strings.HasPrefix(got, "ERR ") {
				t.Errorf("%s\n  answered %q, PostgreSQL 17.11 refuses it", sql, got)
			}
		})
	}
}

// aggFilterNorm renders a gseRows answer the way the table spells it: a cell
// that is a float prints to five significant digits.
func aggFilterNorm(s string) string {
	if s == "" {
		return s
	}
	rows := strings.Split(s, " | ")
	for i, r := range rows {
		cells := strings.Split(r, ",")
		for j, c := range cells {
			if strings.ContainsAny(c, ".eE") {
				if v, err := strconv.ParseFloat(c, 64); err == nil {
					cells[j] = strconv.FormatFloat(v, 'g', 5, 64)
				}
			}
		}
		rows[i] = strings.Join(cells, ",")
	}
	return strings.Join(rows, " | ")
}
