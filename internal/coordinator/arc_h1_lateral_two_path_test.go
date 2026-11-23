// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"regexp"
	"testing"
	"time"
)

// A LATERAL JOIN KEYS ON THE NAME THE SUBQUERY PUBLISHES — #767, four arms.
//
//	SELECT d.k, s.gg, s.c FROM typemx_dim d JOIN LATERAL (
//	  SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true
//
// answered ZERO ROWS on the single-process path where PostgreSQL 17 and both
// DAG arms answer seven — silently, and invisibly to every existing gate,
// because it is a TWO-PATH divergence in a family whose whole corpus lives on
// one fixture.
//
// The decorrelation promotes the correlated equality into the join condition,
// where it names the INNER column `t.g`. `lateralSelectsColumn` sees that the
// SELECT list publishes that value and declines to inject a key — correctly,
// the value IS published. It is published as `gg`. So the join's build key
// named a column the subquery's output does not carry, `exec.HashJoin`
// resolved it to index -1 — the degenerate all-rows-equal key — and nothing
// matched.
//
// `lateralPublishedKeyName` records the published spelling and the promoted
// equality is rewritten to it. The MIRROR case is deliberately untouched: an
// item whose ALIAS matches the key's name while its SOURCE is something else
// (`SELECT amount AS order_id`) publishes a different value under that name,
// and keying on it would answer a plausible wrong number where the engine
// answers an obvious zero — protocol item 8. That one is still pinned in the
// D5 census as `boundary_inner_alias_shadowing_the_key_answers_nothing`.
//
// Every Want is live PostgreSQL 17 over the same rows.
func TestArcH1ALateralKeysOnThePublishedName(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct {
		name, sql, want string
		// budgeted, when true, tolerates on the SPILLED arm exactly one
		// outcome besides PostgreSQL's rows: budgetedClonePresizeRefusal
		// (see there for the mechanism and why it is not pinned either way).
		budgeted bool
	}{
		// THE DEFECT: the group key is SELECTED under an alias.
		{name: "grouped-lateral-selecting-its-key-under-an-alias", budgeted: true,
			sql: `SELECT d.k AS k, s.gg AS gg, s.c AS c FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			want: `k,gg,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | 6,6,660`},
		{name: "counted", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 7`},
		// The LEFT spelling's COUNT. It is kept as a CONTROL and marked as
		// one: `COUNT(*)` over a LEFT JOIN is 8 whether or not the lateral
		// matches a single row, so it cannot fail on revert and cannot see
		// the DAG defect either. The shape that can is
		// TestArcH1ALeftLateralOverAGroupedSubqueryStillFailsOnTheDAG below.
		{name: "ctl-left-spelling-count-keeps-every-outer-row", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 8`},
		// A NON-AGGREGATED lateral whose projection publishes the key under an
		// alias reaches the same site — the injection is declined there too.
		{name: "non-aggregated-lateral-selecting-its-key-under-an-alias",
			sql: `SELECT o.customer AS c, li.oid AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT order_id AS oid FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.oid`,
			want: `c,a | Alice,1 | Alice,1 | Bob,2 | Bob,2`},

		// THE CONTROLS. The key published under its OWN name, the key not
		// published at all (the injection fires), and the two shapes the D5
		// census already covers — none of them may move.
		{name: "ctl-key-published-under-its-own-name", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 7`},
		{name: "ctl-key-not-published-at-all", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d JOIN LATERAL (` +
				`SELECT COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 7`},
		{name: "ctl-ungrouped-aggregate-keeps-the-unmatched-row",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s ON true ` +
				`ORDER BY o.customer`,
			want: `c,n | Alice,2 | Bob,2 | Carol,0`},
		{name: "ctl-non-aggregated-lateral",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.amount`,
			want: `c,a | Alice,50 | Alice,100 | Bob,75 | Bob,125`},
		{name: "ctl-grouped-lateral-over-the-small-fixture",
			sql: `SELECT o.customer AS c, s.n2 AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n2 FROM lat_item WHERE order_id = o.id GROUP BY order_id) s ` +
				`ON true ORDER BY o.customer`,
			want: `c,n | Alice,2 | Bob,2`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					if tc.budgeted && arm.name == spilledArm && budgetedClonePresizeRefusal(err) {
						continue
					}
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// budgetedClonePresizeRefusal is the ONE refusal the budgeted cells of the arc
// H1 / J1 lateral gates (and the CW2 / CW3 container cells that aggregate the
// same table) may give on the 512 KiB arm instead of PostgreSQL's answer,
// matched whole: the join build above the decorrelated aggregate refuses
// its first arrival with nothing built, against a floor that is entirely
// forced `spill tracking`.
//
// The mechanism, traced at #1706 line 1706e (it is not the CROSS-join build
// these gates' comments used to name): the lateral body aggregates the
// 5,000-row typemx, so its input estimate is 5,000 and the presize and
// group-state pool bounds of ADR-0006 (2026-11-22, 2026-11-23) leave the
// 4096-slot capacity in place. The aggregate runs partitioned, and each of the
// seven clones that receives one of the seven `g` groups force-charges about
// 107 KB (98,304 of it the pool's first chunk) for ONE group (about 238 KB in
// the CW cells, whose string-keyed index keeps its 4096 slots too). They
// release in HashAggregate.Close as the drain emits, concurrently with the
// join's first arrival reservation: every refusal measured had five to seven
// still outstanding (535,465 to 751,332 bytes); with four or fewer the
// reservation fits the budget.
// Free, over 30 fresh runs at each of GOMAXPROCS 1, 2, 4 and the default, the
// 21 budgeted cells of H1A, J1A and J1B refused 2,510 of 2,520 times and
// answered PostgreSQL's rows 10 times (at 2, 4 and the default, never at 1);
// with the join's first reservation delayed 100 ms half or more answer. The
// verdict is the release order, so neither side is pinned;
// sizing a partitioned clone below its parent's estimate, or ordering the
// join's reservation after the drain's releases, is a decision ADR-0006 has
// not taken (#1706).
func budgetedClonePresizeRefusal(err error) bool {
	return err != nil && budgetedClonePresizeRE.MatchString(err.Error())
}

var budgetedClonePresizeRE = regexp.MustCompile(`^building physical plan: building hash table: hash join build: ` +
	`query: memory budget exceeded \(used=\d+, requested=\d+, budget=524288, of which forced=\d+ ` +
	`by "spill tracking"\) \(build_rows=0, batches=0\)$`)
