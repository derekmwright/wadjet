package coordinator

import (
	"context"
	"strings"
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
		// budgeted, when true, tolerates the SPILLED arm's loud memory
		// refusal: a lateral over the 5000-row fixture builds a hash join
		// whose build side does not fit a 512 KiB budget, and a CROSS-shaped
		// build refuses rather than spilling (ADR-0006's routed-probe
		// amendment). Loud, and a condition rather than a shape.
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
					if tc.budgeted && arm.name == spilledArm &&
						strings.Contains(err.Error(), "memory budget exceeded") {
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

// A LEFT JOIN LATERAL OVER A QUERY-GROUPED SUBQUERY STILL FAILS ON BOTH DAG
// ARMS — #767's own named symptom, OPEN, pinned here with what the tree
// actually does.
//
// The filing says: "LEFT JOIN LATERAL (agg) fails on the stage DAG with a
// .wshf column-naming error (`"s.id"` vs `"order_id"`, ADR-0010)". This arc's
// first cut claimed that shape was unreachable — refused before a shuffle was
// emitted and answered on the coordinator-local route. IT IS NOT. Measured on
// the tip, with the lateral's columns PROJECTED (the COUNT(*) spelling above
// never builds the join output that breaks):
//
//	single   PostgreSQL's 8 rows, including the unmatched `7,NULL,NULL`
//	spilled  a loud memory refusal at the 512 KiB budget (ADR-0006)
//	dag      with ORDER BY:    stage join-5: column "s.c" does not exist in
//	                           the input schema
//	         without ORDER BY: gather: …/join-5/….wshf declares 2 columns
//	                           where an earlier file of the same stage input
//	                           declared 3 (ADR-0010)
//	         UnreachableOutputLocalRoutes +0 — NOT refused, NOT routed
//
// The two DAG messages are one defect seen from two sides: the join stage's
// output does not carry the lateral's alias-qualified columns on the
// NULL-padded path, so a projection above it cannot name them and two of the
// stage's files describe two different relations.
//
// It is PRE-EXISTING (base 28dcf346 fails identically on the DAG) and it is
// NOT the aliased-key defect commit 364fb735 fixed: the spelling that
// publishes the key under its OWN name fails the same way.
//
// WHY IT IS PINNED RATHER THAN ROUTED. The check that routes the UNGROUPED
// lateral — `assertGatherOutputIsReachable` — declines to model a JOIN's
// output at all, and ADR-0025 says why: "a join's output is the qualified
// union of two sides, and asserting over it produces false refusals". Making
// it route would mean either that model or a new refusal keyed on a shape
// this commit has measured and not proved the boundary of, which is the
// bounded model rule 11 forbids. So the DAG half of #767 stays OPEN with the
// mechanism written down, and this gate FAILS the day it is fixed.
func TestArcH1ALeftLateralOverAGroupedSubqueryStillFailsOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct{ name, sql, want string }{
		{"aliased-key-with-order-by",
			`SELECT d.k AS k, s.gg AS gg, s.c AS c FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			`k,gg,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | ` +
				`6,6,660 | 7,NULL,NULL`},
		// The key under its OWN name, published under its own name too. The
		// ALIAS matters and that is measured, not assumed: spelled
		// `s.g AS gg` this same query ANSWERS on both DAG arms, so the defect
		// needs the outer output name to be the inner column's — which is
		// the naming half of the same ADR-0010 seam.
		{"key-under-its-own-name-with-order-by",
			`SELECT d.k AS k, s.g AS g, s.c AS c FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			`k,g,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | ` +
				`6,6,660 | 7,NULL,NULL`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.UnreachableOutputLocalRoutes()
				}
				cols, rows, err := arm.run(tc.sql)
				switch {
				case arm.coord != nil:
					// BOTH DAG arms: the shape is neither answered nor routed.
					if err == nil {
						t.Errorf("%s arm ANSWERED %s — #767's DAG half is fixed, so this pin "+
							"is spent: delete it, close the issue's DAG half and say so in "+
							"the close text\n  SQL: %s", arm.name, e3Render(cols, rows), tc.sql)
						break
					}
					if moved := arm.coord.UnreachableOutputLocalRoutes() - before; moved != 0 {
						t.Errorf("%s arm ROUTED (UnreachableOutputLocalRoutes +%d) — the shape "+
							"is now refused and answered locally, which is the filing's other "+
							"acceptable disposition: delete this pin and record it\n  SQL: %s",
							arm.name, moved, tc.sql)
					}
				case arm.name == spilledArm:
					// The 512 KiB arm refuses on the join build (ADR-0006's
					// routed-probe amendment): a condition, and loud.
					if err != nil && strings.Contains(err.Error(), "memory budget exceeded") {
						break
					}
					fallthrough
				default:
					if err != nil {
						t.Errorf("%s arm refused a shape it answers: %v\n  SQL: %s",
							arm.name, err, tc.sql)
						break
					}
					if got := e3Render(cols, rows); got != tc.want {
						t.Errorf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm.name, got, tc.want, tc.sql)
					}
				}
			}
		})
	}
}
