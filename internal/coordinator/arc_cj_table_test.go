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
// scan — `EXPLAIN SELECT … FROM c2 CROSS JOIN c1 WHERE c1.f` plans
// `Join: cross join` over `Filter: [c1.f]` over `Scan: c1` — so the question
// this table asks is not whether a filter above a join runs, but whether the
// rows it rejected are still in the build relation when the probe walks it.
// At 1c2b4d25 they were, as soon as the build arrived in more than one batch.
//
// The table varies four things against each other, and what each one is for:
//
//	A  the build's BATCH LAYOUT — one row, three files, three row groups in
//	   one file, and the 2047/2048/2049/4097 crossing of
//	   batch.DefaultBatchSize. This is the dimension the defect turns on, and
//	   cj_b1/cj_b2047/cj_b2048 are the controls that were already right.
//	B  the PREDICATE — a bare boolean, a comparison, a comparison that yields
//	   UNKNOWN over a NULL, an IN list, a BETWEEN, IS NULL, an OR, a predicate
//	   over the PROBE side instead, and one over both. A predicate over the
//	   probe is the other control: a probe batch is consumed as it arrives and
//	   never stored, so its row set was never at risk.
//	C  the SHAPE above the join — a comma join, an INNER join whose ON is an
//	   EXPRESSION (which the planner spells as this same cross join with the
//	   condition lifted into a filter, ADR-0006's routed-probe amendment), a
//	   LEFT join whose ON is non-equi (arc JR's residual path, which must be
//	   byte-identical to base), and a cross join under an aggregate, a GROUP
//	   BY, a DISTINCT, an ORDER BY LIMIT and a window.
//	D  which relation the planner BUILDS, forced by writing the big or the
//	   filtered relation first, plus the cell where BOTH sides are filtered
//	   and both span batches.
//	E  the shape crossed with the 2048-row boundary, because a shape that
//	   reads the join's output through a breaker could mask a wrong row set
//	   that a bare COUNT shows.
//
// THIRTEEN CELLS PASS AT 1c2b4d25, and each is a CONTROL rather than a gap.
// They are what makes the other thirty-two evidence:
//
//	a_b1, a_b2047, a_b2048          a build of ONE batch: nothing to merge
//	b_probecmp, d_probe_filtered    the filter is over the PROBE side, which
//	                                is consumed as it arrives and never stored
//	c_left, c_leftresid             arc JR's residual path, which must be —
//	                                and is — byte-identical to base
//	d_filtered_first, d_big_first,  the filtered relation is the join's LEFT
//	d_left_is_build(_sum)           input. Measured with EXPLAIN
//	                                (cj_author/replay/sides.sql): the planner
//	                                leaves a cross join's children as written
//	                                and builds the RIGHT one, so a filtered
//	                                LEFT input is the probe. The other
//	                                assignment — the filtered side as the
//	                                build with the sides swapped — is driven
//	                                at the operator in
//	                                exec.TestAJoinsBuildOwnsTheRowSetItStores.
//	e_between4097, e_inlist2049     the predicate PRUNES row groups down to
//	                                one, so the build arrives in one batch
//	                                after all. e_between_wide and e_inlist4097
//	                                are the same two predicates widened to
//	                                span row groups, and both fail at base.
//
// Every `want` is PostgreSQL 17.11's answer over the same rows
// (cj_author/pg/pg.tsv). No cell is pinned: the three DAG arms answer
// PostgreSQL's rows at 1c2b4d25 as well, which is why #1189 is labelled
// `engine` and not `distributed`.
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
