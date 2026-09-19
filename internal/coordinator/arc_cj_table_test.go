// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// THE BUILD'S ROW SET SURVIVES THE BATCH BOUNDARY (#1189), on five arms.
//
// A predicate over one side of a cross join is PUSHED DOWN onto that side's
// scan — `EXPLAIN` plans `Join: cross join` over `Filter` over `Scan` — so the
// question is not whether a filter above a join runs, but whether the rows it
// rejected are still in the build relation when the probe walks it. At
// 1c2b4d25 they were, as soon as the build arrived in more than one batch.
//
// Four dimensions vary against each other: the build's BATCH LAYOUT (one row,
// three files, three row groups in one file, and 2047/2048/2049/4097 across
// batch.DefaultBatchSize — the dimension the defect turns on); the PREDICATE
// (bare boolean, comparison, UNKNOWN over a NULL, IN, BETWEEN, IS NULL, OR);
// the SHAPE above the join (comma join, an INNER join whose ON is an
// EXPRESSION — the same cross join with the condition lifted, ADR-0006 — a
// LEFT join with a non-equi ON, which is arc JR's residual path, and a breaker
// above the join: aggregate, GROUP BY, DISTINCT, ORDER BY LIMIT, window); and
// which side the planner BUILDS, forced by the FROM order and measured with
// EXPLAIN (cj_author/replay/sides.sql — a cross join's children are left as
// written and the RIGHT one is built, so the other assignment is driven at the
// operator, in exec.TestAJoinsBuildOwnsTheRowSetItStores).

// Thirteen cells pass at 1c2b4d25 and each is a CONTROL, not a gap: a build of
// ONE batch (nothing to merge), a filter over the PROBE side (consumed on
// arrival, never stored), JR's residual path (which must be byte-identical to
// base), the cells whose filtered relation is the join's probe, and two whose
// predicate prunes the build back to one row group — widened in
// e_between_wide and e_inlist4097, which both fail at base.
//
// Every `want` is PostgreSQL 17.11's answer over the same rows
// (cj_author/pg/pg.tsv). No cell is pinned: the three DAG arms answer those
// rows at 1c2b4d25 as well, which is why #1189 is labelled `engine` and not
// `distributed`.
func TestCJTheBuildRowSetSurvivesTheBatchBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over seven build layouts")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := cjArms(t, ctx)

	cells := cjTableCells()
	names := make([]string, 0, len(cells))
	for n := range cells {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		c := cells[name]
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(c.sql)
				if err != nil {
					got = "ERR " + err.Error()
				} else {
					got = jrStripCols(got)
				}
				want := c.want
				if p, ok := c.pin[arm.name]; ok {
					want = p
				}
				ok := got == want
				if !ok && strings.HasPrefix(want, "ERR ") {
					ok = strings.Contains(got, strings.TrimPrefix(want, "ERR "))
				}
				if !ok {
					why := ""
					if c.why != "" {
						why = "\n  pinned: " + c.why
					}
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s", c.sql, arm.name, got, want, why)
				}
			}
		})
	}
}

type cjCell struct {
	name string
	sql  string
	want string
	pin  map[string]string
	why  string
}

func cjTableCells() map[string]cjCell {
	list := []cjCell{
		{name: "a_b1", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b1 b WHERE b.f", want: "rows=1 | 3"},
		{name: "a_b2047", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b2047 b WHERE b.f", want: "rows=1 | 4095"},
		{name: "a_b2048", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b2048 b WHERE b.f", want: "rows=1 | 4095"},
		{name: "a_b2049", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b2049 b WHERE b.f", want: "rows=1 | 4098"},
		{name: "a_b3f", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f", want: "rows=1 | 18"},
		{name: "a_b4097", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.f", want: "rows=1 | 8193"},
		{name: "a_brg", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_brg b WHERE b.f", want: "rows=1 | 18"},
		{name: "b_between", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.n BETWEEN 2 AND 5", want: "rows=1 | 9"},
		{name: "b_both", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f AND p.pid > 1", want: "rows=1 | 12"},
		{name: "b_buildcmp", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.n > 2", want: "rows=1 | 15"},
		{name: "b_inlist", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.s IN ('alpha', 'zeta')", want: "rows=1 | 6"},
		{name: "b_isnull", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.n IS NULL", want: "rows=1 | 6"},
		{name: "b_notbool", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE NOT b.f", want: "rows=1 | 9"},
		{name: "b_nullyield", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.n > 4", want: "rows=1 | 12"},
		{name: "b_or", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f OR b.n IS NULL", want: "rows=1 | 21"},
		{name: "b_probecmp", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE p.pid > 1", want: "rows=1 | 18"},
		{name: "b_rows", sql: "SELECT p.pid, b.bid, b.f FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f ORDER BY p.pid, b.bid", want: "rows=18 | 1,1,true | 1,3,true | 1,4,true | 1,6,true | 1,7,true | 1,9,true | 2,1,true | 2,3,true | 2,4,true | 2,6,true | 2,7,true | 2,9,true | 3,1,true | 3,3,true | 3,4,true | 3,6,true | 3,7,true | 3,9,true"},
		{name: "c_comma", sql: "SELECT COUNT(*) FROM cj_p p, cj_b3f b WHERE b.f", want: "rows=1 | 18"},
		{name: "c_distinct", sql: "SELECT DISTINCT b.s FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f ORDER BY 1", want: "rows=6 | alpha | delta | eta | gamma | iota | zeta"},
		{name: "c_group", sql: "SELECT b.f, COUNT(*) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f GROUP BY b.f ORDER BY 1", want: "rows=1 | true,18"},
		{name: "c_left", sql: "SELECT COUNT(*) FROM cj_p p LEFT JOIN cj_b3f b ON b.n > p.pid + 90", want: "rows=1 | 3"},
		{name: "c_leftresid", sql: "SELECT p.pid, COUNT(b.bid) FROM cj_p p LEFT JOIN cj_b3f b ON LOWER(b.s) = 'alpha' GROUP BY p.pid ORDER BY p.pid", want: "rows=3 | 1,1 | 2,1 | 3,1"},
		{name: "c_onexpr", sql: "SELECT COUNT(*) FROM cj_p p JOIN cj_b3f b ON b.s IN ('alpha', 'zeta')", want: "rows=1 | 6"},
		{name: "c_onexpr2", sql: "SELECT COUNT(*) FROM cj_p p JOIN cj_b3f b ON LOWER(b.s) <> 'beta'", want: "rows=1 | 24"},
		{name: "c_orderlim", sql: "SELECT p.pid, b.bid FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f ORDER BY p.pid, b.bid LIMIT 5", want: "rows=5 | 1,1 | 1,3 | 1,4 | 1,6 | 1,7"},
		{name: "c_sum", sql: "SELECT SUM(b.bid) FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f", want: "rows=1 | 90"},
		{name: "c_window", sql: "SELECT b.bid, ROW_NUMBER() OVER (ORDER BY b.bid, p.pid) AS rn FROM cj_p p CROSS JOIN cj_b3f b WHERE b.f ORDER BY rn LIMIT 6", want: "rows=6 | 1,1 | 1,2 | 1,3 | 3,4 | 3,5 | 3,6"},
		{name: "d_big_first", sql: "SELECT COUNT(*) FROM cj_b4097 b CROSS JOIN cj_p p WHERE b.f", want: "rows=1 | 8193"},
		{name: "d_big_probe", sql: "SELECT COUNT(*) FROM cj_b4097 c CROSS JOIN cj_b3f b WHERE b.f", want: "rows=1 | 24582"},
		{name: "d_big_probe_rows", sql: "SELECT b.bid, COUNT(*) FROM cj_b4097 c CROSS JOIN cj_b3f b WHERE b.f GROUP BY b.bid ORDER BY b.bid", want: "rows=6 | 1,4097 | 3,4097 | 4,4097 | 6,4097 | 7,4097 | 9,4097"},
		{name: "d_both_filtered", sql: "SELECT COUNT(*) FROM cj_b3f b CROSS JOIN cj_brg c WHERE b.f AND c.f", want: "rows=1 | 36"},
		{name: "d_both_multibatch", sql: "SELECT COUNT(*) FROM cj_b2049 c CROSS JOIN cj_brg b WHERE b.f AND c.f", want: "rows=1 | 8196"},
		{name: "d_both_sum", sql: "SELECT SUM(b.bid + c.bid) FROM cj_b3f b CROSS JOIN cj_brg c WHERE b.f AND c.f", want: "rows=1 | 360"},
		{name: "d_filtered_first", sql: "SELECT COUNT(*) FROM cj_b3f b CROSS JOIN cj_p p WHERE b.f", want: "rows=1 | 18"},
		{name: "d_left_is_build", sql: "SELECT COUNT(*) FROM cj_b3f b CROSS JOIN cj_b4097 c WHERE b.f", want: "rows=1 | 24582"},
		{name: "d_left_is_build_sum", sql: "SELECT SUM(b.bid) FROM cj_b3f b CROSS JOIN cj_b4097 c WHERE b.f", want: "rows=1 | 122910"},
		{name: "d_probe_filtered", sql: "SELECT COUNT(*) FROM cj_b3f b CROSS JOIN cj_p p WHERE p.pid > 1", want: "rows=1 | 18"},
		{name: "e_between4097", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.n BETWEEN 100 AND 200", want: "rows=1 | 225"},
		{name: "e_between_wide", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.n BETWEEN 100 AND 3000", want: "rows=1 | 6525"},
		{name: "e_distinct4097", sql: "SELECT COUNT(DISTINCT b.bid) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.f", want: "rows=1 | 2731"},
		{name: "e_group2049", sql: "SELECT b.f, COUNT(*) FROM cj_p p CROSS JOIN cj_b2049 b WHERE b.f GROUP BY b.f ORDER BY 1", want: "rows=1 | true,4098"},
		{name: "e_inlist2049", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b2049 b WHERE b.s IN ('alpha')", want: "rows=1 | 1227"},
		{name: "e_inlist4097", sql: "SELECT COUNT(*) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.s IN ('alpha')", want: "rows=1 | 2457"},
		{name: "e_order2049", sql: "SELECT b.bid FROM cj_p p CROSS JOIN cj_b2049 b WHERE b.f ORDER BY b.bid LIMIT 4", want: "rows=4 | 1 | 1 | 1 | 3"},
		{name: "e_sum4097", sql: "SELECT SUM(b.bid) FROM cj_p p CROSS JOIN cj_b4097 b WHERE b.f", want: "rows=1 | 16785408"},
	}
	out := make(map[string]cjCell, len(list))
	for _, c := range list {
		if _, dup := out[c.name]; dup {
			panic("duplicate cell name " + c.name)
		}
		out[c.name] = c
	}
	return out
}
