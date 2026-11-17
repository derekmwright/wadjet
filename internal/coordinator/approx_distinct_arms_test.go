// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// APPROX_DISTINCT ANSWERS THE SAME COUNT ON ALL FIVE ARMS (#1288).
//
// The DAG used to split approx_distinct into a per-task partial and a merge.
// The partial is a COUNT — the engine's approx_distinct state is an exact
// distinct set (exec's distinctSet), and only its count() is written out —
// and the merge re-ran approx_distinct over those counts, answering how many
// DISTINCT per-task counts there were: 2 for `WHERE id < 20` over four files,
// 1 over the whole table, on dag, dag-shuffled and dag-morsel4 alike. The
// planner now gates it onto the one-level RawInputAggregate shape
// (dagplan.aggNeedsWholeInput), the route COUNT(DISTINCT) already takes.
//
// "The same" means the SAME value, not one within a sketch's error bound:
// there is no sketch. Every arm runs the identical exact set over the
// identical rows, so any difference at all is a wrong answer. `want`, where
// set, is the fixture's true distinct count, computed from typematrix's
// generator (a column nulls where i%stride == stride-1).
func TestApproxDistinctFiveArmsAgree(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the approx_distinct table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := brArms(t, ctx)

	type cell struct{ name, sql, want string }
	var cells []cell
	flat := []string{"c_bool", "c_i32", "c_i64", "c_f32", "c_f64", "c_str", "c_bytes", "c_ts",
		"c_ipv4", "c_ipv6", "c_cidr", "c_mac", "c_port", "c_proto", "c_dur", "c_uuid", "c_date", "c_dec"}
	nested := []string{"c_arr", "c_row", "c_rownest", "c_map", "c_vec"}
	for _, c := range flat {
		cells = append(cells,
			cell{"type20/" + c, "SELECT approx_distinct(" + c + ") AS v FROM typemx WHERE id < 20", ""},
			cell{"typeAll/" + c, "SELECT approx_distinct(" + c + ") AS v FROM typemx", ""})
	}
	for _, c := range nested {
		cells = append(cells,
			cell{"type20/" + c, "SELECT approx_distinct(" + c + ") AS v FROM typemx_nested WHERE id < 20", ""},
			cell{"typeAll/" + c, "SELECT approx_distinct(" + c + ") AS v FROM typemx_nested", ""})
	}
	cells = append(cells,
		// The issue's statement, and its truth: ids 0..19, no NULL below 28.
		cell{"truth/i32_20", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id < 20", "rows=1 20"},
		// 5000 rows less the 172 that NULL on stride 29.
		cell{"truth/i32_all", "SELECT approx_distinct(c_i32) AS v FROM typemx", "rows=1 4828"},
		cell{"truth/proto_all", "SELECT approx_distinct(c_proto) AS v FROM typemx", "rows=1 256"},
		cell{"truth/one", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id = 5", "rows=1 1"},
		// id 28 is c_i32's first NULL: one row, nothing to count.
		cell{"truth/onlyNull", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id = 28", "rows=1 0"},
		// 100 rows, 3 of them NULL (ids 28, 57, 86).
		cell{"truth/nulls100", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id < 100", "rows=1 97"},

		cell{"grp/few", "SELECT g, approx_distinct(c_i32) AS v FROM typemx GROUP BY g", ""},
		cell{"grp/fewStr", "SELECT g, approx_distinct(c_str) AS v FROM typemx GROUP BY g", ""},
		cell{"grp/many", "SELECT id % 500 AS k, approx_distinct(c_proto) AS v FROM typemx GROUP BY id % 500", ""},
		cell{"grp/manyCol", "SELECT c_proto, approx_distinct(c_i64) AS v FROM typemx GROUP BY c_proto", ""},
		// Zero input rows: the single arm answers NULL here (one all-NULL
		// row answers 0); the DAG must answer what it answers.
		cell{"filter/zero", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id < 0", ""},
		cell{"filter/zeroGrouped", "SELECT g, approx_distinct(c_i32) AS v FROM typemx WHERE id < 0 GROUP BY g", "rows=0 "},
		cell{"filter/all", "SELECT approx_distinct(c_i32) AS v FROM typemx WHERE id >= 0", "rows=1 4828"},
		cell{"beside/countStar", "SELECT COUNT(*) AS n, approx_distinct(c_i32) AS v FROM typemx WHERE id < 300", ""},
		cell{"beside/sum", "SELECT SUM(id) AS s, approx_distinct(c_i32) AS v FROM typemx WHERE id < 300", ""},
		cell{"beside/countDistinct", "SELECT COUNT(DISTINCT c_i32) AS d, approx_distinct(c_i32) AS v FROM typemx WHERE id < 300", ""},
		cell{"beside/twoApprox", "SELECT approx_distinct(c_str) AS a, approx_distinct(c_proto) AS b FROM typemx", "rows=1 4884,256"},
		cell{"beside/grouped", "SELECT g, COUNT(*) AS n, SUM(id) AS s, approx_distinct(c_i32) AS v FROM typemx GROUP BY g", ""},
		cell{"distinctArg", "SELECT approx_distinct(DISTINCT c_proto) AS v FROM typemx", "rows=1 256"},
		cell{"having", "SELECT g, approx_distinct(c_proto) AS v FROM typemx GROUP BY g HAVING approx_distinct(c_proto) > 200", ""},
		cell{"expr", "SELECT approx_distinct(id % 37) AS v FROM typemx", "rows=1 37"},
		cell{"join", "SELECT approx_distinct(b.c_proto) AS v FROM typemx a JOIN typemx b ON a.id = b.id WHERE a.id < 1000", ""},
		cell{"joinGrouped", "SELECT a.g, approx_distinct(b.c_i32) AS v FROM typemx a JOIN typemx b ON a.id = b.id GROUP BY a.g", ""},
		cell{"derived/outerSum", "SELECT SUM(v) AS s FROM (SELECT g, approx_distinct(c_i32) AS v FROM typemx GROUP BY g) x", "rows=1 4828"},
		cell{"derived/inner", "SELECT approx_distinct(x.c) AS v FROM (SELECT c_proto AS c FROM typemx WHERE id < 3000) x", "rows=1 256"},
		cell{"cte", "WITH w AS (SELECT c_proto AS c FROM typemx) SELECT approx_distinct(c) AS v FROM w", "rows=1 256"},
	)

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			first := ""
			for i, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v", tc.sql, arm.name, err)
					continue
				}
				got := brRender(res)
				if i == 0 {
					first = got
				} else if got != first {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  single answered %s", tc.sql, arm.name, got, first)
				}
				if tc.want != "" && got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (the true distinct count)", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
