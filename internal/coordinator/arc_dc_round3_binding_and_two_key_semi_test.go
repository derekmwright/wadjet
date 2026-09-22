// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// Arc DC round 3: the two findings of the round-2 closure review, as row sets
// against live PostgreSQL 17.11 on five arms (cell names are the review's).

type dcRound3Case struct{ name, sql, want string }

func dcRunRound3(t *testing.T, cases []dcRound3Case) {
	t.Helper()
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := dcArms(t, ctx)
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

// A NAME THE BODY'S OWN FROM SUPPLIES IS THE BODY'S (review B2 / N7). The
// namespace rule was applied in one direction only: a name the body cannot
// supply became outer, but a name the body DOES supply and the enclosing query
// also has was still read as the enclosing row's, because the classifier kept
// the whole enclosing column map. `EXISTS (SELECT 1 FROM dc_side c WHERE id =
// c.j)` planned `semi ON id = j` — o.id = c.j — and answered 3 rows for
// PostgreSQL's 5. With the namespace known the classifier reads the enclosing
// map restricted to what the body cannot supply. 58 cells fail at 9f7901e6.
func TestArcDCANameTheBodySuppliesIsTheBodysOnEveryArm(t *testing.T) {
	dcRunRound3(t, []dcRound3Case{
		{name: "H-bodyIdMixed/ON/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k AND id < total WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "H-bodyIdMixedEq/ON/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k AND id - 7 = total - 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=1 1"},
		{name: "H-bodyIdMixed/ON/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND id < total WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "H-bodyIdMixed/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k WHERE b.k = o.id AND id < total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "H-bodyIdVsInner/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k WHERE b.k = o.id AND id > b.k) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "D-mixed150/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k WHERE b.k = o.id AND id < total - 150) ORDER BY a",
			want: "rows=1 2"},
		{name: "D-mixedEq/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k WHERE b.k = o.id AND id - 7 = total - 100) ORDER BY a",
			want: "rows=1 1"},
		{name: "H2-eqkey/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_side c WHERE id = c.j) ORDER BY a",
			want: "rows=5 1 | 2 | 3 | 9 | NULL"},
		{name: "H2-eqkey/W/SCAL",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE (SELECT COUNT(*) FROM dc_side c WHERE id = c.j) > 0 ORDER BY a",
			want: "rows=5 1 | 2 | 3 | 9 | NULL"},
		{name: "H2-vsInner/W/NOTEXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k WHERE b.k = o.id AND id > b.k) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "H2-grp/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, tag AS grp FROM dc_in) b WHERE b.k = o.id AND grp = o.grp + 0) ORDER BY a",
			want: "rows=1 1"},
		{name: "H2-grp2/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, tag AS grp FROM dc_in) b WHERE b.k = o.id AND grp > b.k) ORDER BY a",
			want: "rows=2 1 | 2"},
	})
}

// A CORRELATED IN OVER A SELF-JOIN BODY (review B1 / N6). The IN key and the
// correlation key are two integer pairs (`semi ON j = id AND j = id`), and when
// the planner builds the enclosing side (RIGHT SEMI) the probe arm that marks
// matched build rows had no case for a two-integer key: it marked nothing, and
// the single-process arms answered EMPTY. Round 2's hoist took shapes base
// refused (`o.amt2 > 100` in the body) onto that defect — ten cells from a
// refusal to a wrong row set. The K-ctl-* cells are the same body with no
// outer-only condition (wrong at base too) and the siblings the review found
// unaffected. 14 cells fail at 9f7901e6.
func TestArcDCACorrelatedInOverASelfJoinAnswersOnEveryArm(t *testing.T) {
	dcRunRound3(t, []dcRound3Case{
		{name: "K-sameKey/ON-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, id * 10 AS t2 FROM dc_side) o WHERE o.id IN (SELECT b.id FROM dc_side b JOIN dc_side c ON c.id = b.id AND o.t2 > 75 WHERE b.id = o.id) ORDER BY a",
			want: "rows=2 8 | 9"},
		{name: "K-sameKey2/ON-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, id * 10 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.id FROM dc_out b JOIN dc_side c ON c.id = b.id AND o.t2 > 15 WHERE b.id = o.id) ORDER BY a",
			want: "rows=1 9"},
		{name: "K-sameKey3/ON-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT j AS id, j * 10 AS t2 FROM dc_side) o WHERE o.id IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.t2 > 15 WHERE b.id = o.id) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-sameKey3/ON-q/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT j AS id, j * 10 AS t2 FROM dc_side) o WHERE EXISTS (SELECT 1 FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.t2 > 15 WHERE b.id = o.id) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-sameKey3/W-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT j AS id, j * 10 AS t2 FROM dc_side) o WHERE o.id IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.id AND o.t2 > 15) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-sameKey3base/ON-q/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.amt2 > 100 WHERE b.id = o.j) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-sameKey3base2/ON-q/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.id FROM dc_out b JOIN dc_side c ON c.id = b.id AND o.total > 5 WHERE b.id = o.id) ORDER BY a",
			want: "rows=1 9"},
		{name: "K-sameKey4/ON-q/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.total > 100 WHERE b.id = o.id) ORDER BY a",
			want: "rows=1 2"},
		{name: "K-ctl-noOuterOnly/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.j) ORDER BY a",
			want: "rows=3 1 | 2 | 9"},
		{name: "K-ctl-noOuterOnly/EXISTS",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE EXISTS (SELECT 1 FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.j) ORDER BY a",
			want: "rows=3 1 | 2 | 9"},
		{name: "K-ctl-uncorr/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id) ORDER BY a",
			want: "rows=3 1 | 2 | 9"},
		{name: "K-ctl-plainOuterFilter/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.amt2 > 100 AND o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.j) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-W/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.j AND o.amt2 > 100) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-Wuncorr/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE o.amt2 > 100) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-bare/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_out c ON c.id = b.id WHERE b.id = o.j AND amt2 > 100) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-ON/EXISTS",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE EXISTS (SELECT 1 FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.amt2 > 100 WHERE b.id = o.j) ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-ON/SCAL",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE (SELECT MAX(b.total) FROM dc_out b JOIN dc_out c ON c.id = b.id AND o.amt2 > 100 WHERE b.id = o.j) > 0 ORDER BY a",
			want: "rows=2 2 | 9"},
		{name: "K-hoist-ONnoKey/IN",
			sql:  "SELECT o.j AS a FROM dc_side o WHERE o.j IN (SELECT b.id FROM dc_out b JOIN dc_in c ON c.k = b.id AND o.amt2 > 100 WHERE b.id = o.j) ORDER BY a",
			want: "rows=1 2"},
	})
}
