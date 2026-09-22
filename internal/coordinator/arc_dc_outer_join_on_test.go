// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// AN OUTER JOIN'S ON BESIDE A WHERE THAT REJECTS ITS PADDING — the body shape
// the 776-cell table does not write (arc DC round-1 review, P1 and B2).
//
// Every `on*Left*` / `onRight*` site in dcSites() writes a body with NO WHERE
// clause, so the outer-join guard in liftBodyOuterConditions was never reached
// at the row set, and neither was what happens after it fires: the subquery
// declines the rewrite and is re-run per outer row with the outer values
// substituted, so `o.total > 100` in the ON becomes `100 > 100`.
//
// That rerun used to REFUSE ("100 > 100 cannot be represented as an equi-join
// key … legal for an inner join only") whenever the body's WHERE rejects the
// outer join's padding. The site is not the rerun: pushFilterThroughJoin
// DEMOTES such a join to inner (#335) after liftInnerJoinOnResiduals (#336) has
// run, and routeOuterJoinOnResiduals (#358) skips a join that is no longer
// outer, so a non-key ON conjunct sat in an inner join's ON with nothing left to
// place it. The PLAIN/* cells are the same demotion with no subquery at all —
// they refused at base too — and `PLAIN/rightNoWhereCtl` is the control the
// route has always placed.
//
// Cells:
//   - P1/*: an outer join's ON beside a correlated WHERE whose padding
//     survives; with the `!isInnerOrCrossJoin` guard deleted each is wrong.
//   - B2/*: the review's family, named by its cell id there. The first four
//     were PINNED refusals in the review's patch; they answer now, so the pins
//     are deleted and PostgreSQL's row set is asserted instead.
//   - PLAIN/*: the demotion itself.
//
// Wants are live PostgreSQL 17.11 over the dc_out / dc_in / dc_side fixture
// this package writes.
func TestArcDCAnOuterJoinsOnBesideACorrelatedWhere(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := dcArms(t, ctx)

	cases := []struct{ name, sql, want string }{
		{name: "P1/EXISTS/leftOnOuterOnly",
			sql: "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b " +
				"LEFT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "P1/IN/leftOnOuterOnly",
			sql: "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b " +
				"LEFT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "P1/EXISTS/rightOnOuterOnly",
			sql: "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b " +
				"RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE c.j = o.id) ORDER BY a",
			want: "rows=3 1 | 2 | 9"},
		{name: "P1/EXISTS/leftOnOuterEq",
			sql: "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b " +
				"LEFT JOIN dc_side c ON c.j = b.k AND o.grp = 20 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2/G02-rightOnOuter/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=0 "},
		{name: "B2/J09-rightOnOuterTrue/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT c.j FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 0 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=1 1"},
		{name: "B2/J11-rightOnOuterTrue/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 0 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=4 1 | 2 | 3 | NULL"},
		{name: "B2/H07-rightOnOuter/NOTIN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id NOT IN (SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=4 1 | 2 | 3 | 9"},
		{name: "B2/H05-rightOnCorr/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > c.amt2 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=1 1"},
		{name: "B2/H08-rightOnOuter/SCALAR",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id = (SELECT MAX(b.k) FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=0 "},
		{name: "B2/H09-rightOnOuter/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=2 2 | NULL"},
		{name: "B2/H13-rightOnOuter-where2/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE b.amt > 0) ORDER BY a",
			want: "rows=1 2"},
		{name: "B2/H14-rightOnOuterFn/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND ABS(o.total) > 100 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=0 "},
		{name: "B2/J02-rightOnOuter-inner/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT c.id FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 100 WHERE c.amt2 > 0 AND b.tag = o.grp) ORDER BY a",
			want: "rows=0 "},
		{name: "B2/J10-rightOnOuterFnTrue/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT c.j FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND ABS(o.total) > 0 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=1 1"},
		{name: "B2/J12-rightOnOuterTrue/SCALAR",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id = (SELECT MAX(c.j) FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 0 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=1 1"},
		{name: "B2/J13-rightOnOuterTrue/NOTIN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id NOT IN (SELECT c.j FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND o.total > 0 WHERE b.tag = o.grp) ORDER BY a",
			want: "rows=3 2 | 3 | 9"},
		{name: "PLAIN/rightConstFalse",
			sql:  "SELECT b.k AS a FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND 100 > 100 WHERE b.tag = 10 ORDER BY a",
			want: "rows=0 "},
		{name: "PLAIN/rightConstTrue",
			sql:  "SELECT b.k AS a FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND 100 > 0 WHERE b.tag = 10 ORDER BY a",
			want: "rows=2 1 | 1"},
		{name: "PLAIN/rightCrossResidual",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND b.amt < c.amt2 WHERE b.tag >= 10 ORDER BY a, c",
			want: "rows=1 1,7"},
		{name: "PLAIN/rightBuildOnly",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND c.amt2 > 100 WHERE b.amt > 0 ORDER BY a, c",
			want: "rows=1 2,8"},
		{name: "PLAIN/leftCrossResidual",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND b.amt < c.amt2 WHERE c.amt2 > 0 ORDER BY a, c",
			want: "rows=1 1,7"},
		{name: "PLAIN/leftConstFalse",
			sql:  "SELECT b.k AS a FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND 1 = 0 WHERE c.id > 0 ORDER BY a",
			want: "rows=0 "},
		{name: "PLAIN/leftProbeOnly",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b LEFT JOIN dc_side c ON c.j = b.k AND b.amt > 100 WHERE c.id IS NOT NULL ORDER BY a, c",
			want: "rows=2 1,7 | 2,8"},
		{name: "PLAIN/fullBothReject",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b FULL JOIN dc_side c ON c.j = b.k AND b.amt < c.amt2 WHERE b.tag > 0 AND c.id > 0 ORDER BY a, c",
			want: "rows=1 1,7"},
		{name: "PLAIN/fullOneReject",
			sql:  "SELECT b.k AS a, c.id AS c FROM dc_in b FULL JOIN dc_side c ON c.j = b.k AND b.amt < c.amt2 WHERE b.tag > 0 ORDER BY a, c",
			want: "rows=4 1,7 | 1,NULL | 2,NULL | 5,NULL"},
		{name: "PLAIN/rightNoWhereCtl",
			sql:  "SELECT b.k AS a FROM dc_in b RIGHT JOIN dc_side c ON c.j = b.k AND 100 > 100 ORDER BY a",
			want: "rows=3 NULL | NULL | NULL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				if got := arm.run(tc.sql); got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
