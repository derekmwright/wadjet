// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A DISTINCT COUNT, AND A FILTER, OVER ZERO ROWS ON ALL FIVE ARMS (#1690,
// #1691).
//
// Two things changed in the embedded engine and both reach the DAG through
// the same parser and the same executor:
//
//   - COUNT(*) FILTER (WHERE p) is COUNT(CASE WHEN p THEN 1 END), not the
//     SUM(CASE ... ELSE 0 END) it used to be rewritten to (a SUM over zero rows
//     is NULL, a COUNT is 0).
//   - The ungrouped identity row (the one row an aggregate-only SELECT answers
//     over zero input rows) answers 0, not NULL, for COUNT(DISTINCT expr) and
//     APPROX_DISTINCT, as it already did for COUNT.
//
// The distributed arms plan a DISTINCT COUNT on the one-level RawInputAggregate
// shape (dagplan.aggNeedsWholeInput, #1288), whose merge runs the same
// HashAggregate, so a cell that moves on the embedded arms must move on the DAG
// arms too; this table pins that they move to the TRUE count and no arm lags.
//
// Every want is the true distinct count, which is also PostgreSQL 17.11's
// answer (the lat_ord cells and the window cells were taken from it live; the
// typemx cells follow from typematrix's generator: a column nulls where
// i%stride == stride-1, so c_i32 is NULL at id 28, 57, ...; 4828 of the 5000
// rows are non-NULL and distinct). A distinct count over zero ungrouped rows
// is 0, over zero grouped rows no rows.
//
// The table: {APPROX_DISTINCT, COUNT(DISTINCT)} x {zero rows, one row, every
// row, a column with NULLs, one all-NULL row} x {ungrouped, GROUP BY} x {plain,
// FILTER keeping everything, FILTER keeping nothing/one} x {alone, beside
// COUNT(*)}, a plain column and an expression, plus the statements the round-1
// review listed and the window empty-frame cell.
func TestDistinctCountFilterZeroRowsFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the typemx and lat_ord tables")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := brArms(t, ctx)

	type cell struct{ name, sql, want string }
	var cells []cell

	type input struct {
		name  string
		arg   string // the aggregate's argument
		where string
		n     int // rows the WHERE keeps
		d     int // distinct non-NULL arg values among them
		// the grouped form: GROUP BY id over where2, each group one row
		where2 string
		groups []string // "id,distinct" per group, the answer's rows
	}
	inputs := []input{
		{"zero", "c_i32", "WHERE id < 0", 0, 0, "WHERE id < 0", nil},
		{"one", "c_i32", "WHERE id = 5", 1, 1, "WHERE id = 5", []string{"5,1"}},
		{"all", "id", "", 5000, 5000, "WHERE id < 3", []string{"0,1", "1,1", "2,1"}},
		{"nullsCol", "c_i32", "", 5000, 4828, "WHERE id IN (27, 28, 29)", []string{"27,1", "28,0", "29,1"}},
		{"allNull", "c_i32", "WHERE id = 28", 1, 0, "WHERE id = 28", []string{"28,0"}},
	}
	for _, fn := range []string{"approx_distinct", "count(distinct"} {
		call := func(arg string) string {
			if fn == "approx_distinct" {
				return "approx_distinct(" + arg + ")"
			}
			return "COUNT(DISTINCT " + arg + ")"
		}
		short := strings.TrimSuffix(strings.TrimSuffix(fn, "("), " ")
		short = strings.ReplaceAll(short, "count(distinct", "countDistinct")
		for _, in := range inputs {
			for _, fl := range []struct{ name, clause string }{{"plain", ""}, {"filterAll", " FILTER (WHERE id >= 0)"}} {
				key := short + "/" + in.name + "/" + fl.name
				f := call(in.arg) + fl.clause
				// ungrouped: alone and beside COUNT(*) (plain and filtered)
				cells = append(cells,
					cell{key + "/ungrouped", fmt.Sprintf("SELECT %s AS v FROM typemx %s", f, in.where),
						fmt.Sprintf("rows=1 %d", in.d)},
					cell{key + "/ungroupedBesideCount", fmt.Sprintf("SELECT COUNT(*)%s AS n, %s AS v FROM typemx %s", fl.clause, f, in.where),
						fmt.Sprintf("rows=1 %d,%d", in.n, in.d)})
				// grouped: no rows over zero rows; one row per group otherwise
				rows := fmt.Sprintf("rows=%d %s", len(in.groups), strings.Join(in.groups, " | "))
				cells = append(cells,
					cell{key + "/grouped", fmt.Sprintf("SELECT id, %s AS v FROM typemx %s GROUP BY id", f, in.where2), rows})
				var withCount []string
				for _, g := range in.groups {
					id, d, _ := strings.Cut(g, ",")
					withCount = append(withCount, id+",1,"+d)
				}
				cells = append(cells,
					cell{key + "/groupedBesideCount", fmt.Sprintf("SELECT id, COUNT(*)%s AS n, %s AS v FROM typemx %s GROUP BY id", fl.clause, f, in.where2),
						fmt.Sprintf("rows=%d %s", len(withCount), strings.Join(withCount, " | "))})
			}
		}
		cells = append(cells,
			// A FILTER that rejects every row, and one that keeps one.
			cell{short + "/filterNone/ungrouped", "SELECT " + call("c_i32") + " FILTER (WHERE id < 0) AS v FROM typemx", "rows=1 0"},
			cell{short + "/filterOne/ungrouped", "SELECT " + call("c_i32") + " FILTER (WHERE id = 5) AS v FROM typemx", "rows=1 1"},
			cell{short + "/filterNone/grouped", "SELECT id, " + call("c_i32") + " FILTER (WHERE id < 0) AS v FROM typemx WHERE id < 3 GROUP BY id", "rows=3 0,0 | 1,0 | 2,0"},
			cell{short + "/filterOne/grouped", "SELECT id, " + call("c_i32") + " FILTER (WHERE id = 1) AS v FROM typemx WHERE id < 3 GROUP BY id", "rows=3 0,0 | 1,1 | 2,0"},
			cell{short + "/filterNone/besideCount", "SELECT COUNT(*) AS n, " + call("c_i32") + " FILTER (WHERE id < 0) AS v FROM typemx", "rows=1 5000,0"},
			// An expression argument (not the one-level shape's plain column).
			cell{short + "/expr/zero", "SELECT " + call("c_i32 + 1") + " AS v FROM typemx WHERE id < 0", "rows=1 0"},
			cell{short + "/expr/zeroFilter", "SELECT " + call("c_i32 + 1") + " FILTER (WHERE id >= 0) AS v FROM typemx WHERE id < 0", "rows=1 0"},
			cell{short + "/expr/zeroBesideCount", "SELECT COUNT(*) AS n, " + call("id % 37") + " AS v FROM typemx WHERE id < 0", "rows=1 0,0"},
			cell{short + "/expr/one", "SELECT " + call("id % 37") + " AS v FROM typemx WHERE id = 5", "rows=1 1"},
			cell{short + "/expr/all", "SELECT " + call("id % 37") + " AS v FROM typemx", "rows=1 37"},
			cell{short + "/expr/zeroGrouped", "SELECT id, " + call("id % 37") + " AS v FROM typemx WHERE id < 0 GROUP BY id", "rows=0 "},
			// The round-1 review's statements, on lat_ord (ids 1, 2, 3).
			cell{short + "/lat/one", "SELECT " + call("id") + " FROM lat_ord WHERE id = 1", "rows=1 1"},
			cell{short + "/lat/zero", "SELECT " + call("id") + " FROM lat_ord WHERE id < 0", "rows=1 0"},
			cell{short + "/lat/zeroFilter", "SELECT " + call("id") + " FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0", "rows=1 0"},
			cell{short + "/lat/all", "SELECT " + call("id") + " FROM lat_ord", "rows=1 3"},
		)
	}

	cells = append(cells,
		// #1690 itself, and its neighbours, on five arms. Each want is
		// PostgreSQL 17.11's.
		cell{"1690/countStarFilterZero", "SELECT COUNT(*) FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0", "rows=1 0"},
		cell{"1690/countStarFilterSome", "SELECT COUNT(*) FILTER (WHERE id > 1) FROM lat_ord", "rows=1 2"},
		cell{"1690/countStarFilterNone", "SELECT COUNT(*) FILTER (WHERE id > 100) FROM lat_ord", "rows=1 0"},
		cell{"1690/rollupZero", "SELECT id, COUNT(*) FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0 GROUP BY ROLLUP(id)", "rows=1 NULL,0"},
		cell{"1690/groupingSetsZero", "SELECT COUNT(*) FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0 GROUP BY GROUPING SETS ((), ())", "rows=2 0 | 0"},
		cell{"1690/groupedZero", "SELECT id, COUNT(*) FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0 GROUP BY id", "rows=0 "},
		cell{"1690/countXFilterZero", "SELECT COUNT(id) FILTER (WHERE id > 1), SUM(id) FILTER (WHERE id > 1) FROM lat_ord WHERE id < 0", "rows=1 0,NULL"},
		// Must not move: a user-written SUM is a SUM (NULL over zero rows),
		// and a COUNT beside a COUNT(DISTINCT) answers 0.
		cell{"keep/userSumCase", "SELECT SUM(CASE WHEN id > 1 THEN 1 ELSE 0 END) FROM lat_ord WHERE id < 0", "rows=1 NULL"},
		// PostgreSQL 17.11 answers 0,0 on all five arms; the DAG arms answered
		// NULL,0 until #1692 (the stage planner dropped the two-level
		// rewrite's coalesce over the re-summed COUNT).
		cell{"keep/countBesideDistinct", "SELECT COUNT(*), COUNT(DISTINCT id) FROM lat_ord WHERE id < 0", "rows=1 0,0"},
		cell{"keep/countBesideDistinctTypemx", "SELECT COUNT(*) AS n, COUNT(DISTINCT c_i32) FROM typemx WHERE id < 0", "rows=1 0,0"},
		cell{"keep/unfilteredZero", "SELECT COUNT(id), SUM(id), MIN(id), MAX(id), AVG(id) FROM lat_ord WHERE id < 0", "rows=1 0,NULL,NULL,NULL,NULL"},
		// The window empty frame (PostgreSQL 17.11, lat_ord copy): the first
		// row's frame holds no row; COUNT(*) FILTER answers 0 there.
		cell{"window/countStarFilter", "SELECT id, COUNT(*) FILTER (WHERE total > 100) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM lat_ord", "rows=3 1,0 | 2,1 | 3,1"},
		cell{"window/countColFilter", "SELECT id, COUNT(total) FILTER (WHERE total > 100) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM lat_ord", "rows=3 1,0 | 2,1 | 3,1"},
		cell{"window/sumFilter", "SELECT id, SUM(total) FILTER (WHERE total > 100) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM lat_ord", "rows=3 1,NULL | 2,150 | 3,200"},
		cell{"window/countStar", "SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM lat_ord", "rows=3 1,0 | 2,1 | 3,1"},
	)

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v", tc.sql, arm.name, err)
					continue
				}
				got := brRender(res)
				t.Logf("%-14s %s", arm.name, got)
				if got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
