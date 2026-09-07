package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/wadjet"
)

// SQL SCOPES INNERMOST-FIRST, AND A CTE REFERENCE OR A DERIVED TABLE IS A
// RELATION WITH A SCHEMA — #955, on FOUR arms with the routing counters.
//
// An unqualified column inside a subquery binds to the subquery's OWN FROM
// when that FROM — base table, CTE reference, derived table, set-operation arm
// — has a column of that name. Only a name NO inner relation carries is a
// reference to the enclosing query.
//
// The classifier could only ask a CATALOG about a NAME, so a CTE reference and
// a derived table resolved to nothing, an unqualified name they supply fell
// through to the outer scope, and the subquery was called CORRELATED. Two
// things followed, both silent:
//
//   - the outer row's value was substituted into the subquery's WHERE, so
//     `WHERE id < 4000` became `WHERE 1 < 4000` — constant TRUE — and the
//     predicate was gone. `WITH c AS (SELECT id, c_i64 AS v FROM typemx)
//     SELECT (SELECT MAX(v) FROM c WHERE id < 4000) FROM decpair WHERE id < 2`
//     answered 4999014997 for PostgreSQL's 3999011997 on all four arms.
//   - the subquery was RE-RUN ONCE PER OUTER ROW. Over two rows that is
//     invisible; over 5000 at a 512 KiB budget it never finished, which is why
//     every cell here carries a deadline and the spilled arm is an arm.
//
// The DAG arms assert `CorrelatedLocalRoutes` beside the rows because rows
// alone cannot tell "the DAG executed this" from "the DAG refused the plan and
// the coordinator-local pipeline answered". Every uncorrelated cell claims
// ZERO, and the three genuinely correlated controls claim ONE — that pairing
// is the boundary, not the row values, because the row values of cells 12, 13
// and 19 were right before this fix as well.
//
// Every want is live PostgreSQL 17 over the same rows (the `i1_` fixtures in
// this arc's ROUND0).

// i1Deadline bounds ONE query on ONE arm. The healthy time for every cell here
// is under two seconds on every arm; the shape this gate exists for did not
// finish in ten minutes, because a mis-correlated subquery is re-run per outer
// row. Three minutes is two orders of magnitude of headroom for a loaded
// machine and still a decisive verdict on an unbounded re-run.
const i1Deadline = 3 * time.Minute

type i1Arm struct {
	name  string
	run   func(ctx context.Context, sql string) ([]string, [][]any, error)
	coord *Coordinator
}

// i1Arms is e3Arms with the context per QUERY rather than per test, so a cell
// that hangs fails its own cell instead of the whole binary.
func i1Arms(t *testing.T, ctx context.Context) []i1Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
	runSingle := func(db *wadjet.DB) func(context.Context, string) ([]string, [][]any, error) {
		return func(c context.Context, sql string) ([]string, [][]any, error) {
			return e3PosSingle(c, db, sql)
		}
	}
	runDAG := func(co *Coordinator) func(context.Context, string) ([]string, [][]any, error) {
		return func(c context.Context, sql string) ([]string, [][]any, error) {
			return e3PosDAG(c, co, sql)
		}
	}
	return []i1Arm{
		{"single", runSingle(single), nil},
		{spilledArm, runSingle(spilled), nil},
		{"dag", runDAG(dag), dag},
		{"dagshuf", runDAG(shuf), shuf},
	}
}

type i1Cell struct {
	name string
	sql  string
	// want is what PostgreSQL 17 answers over these rows, rendered by
	// e3Render: "cols | r0c0,r0c1 | …".
	want string
	// wantErr, when set, says PostgreSQL 17 RAISES on this shape and names a
	// substring of the sentence every unpinned arm must raise too. A right
	// answer where PostgreSQL refuses is as much a divergence as a wrong one.
	wantErr string
	// routes is the routing delta each DAG arm must produce for this one
	// query. The zero value says the DAG EXECUTED the shape as stages.
	routes a2Routes
	// pin, when set, says this cell DIVERGES from PostgreSQL on EVERY arm and
	// records what this engine answers instead. The gate then asserts the
	// divergence: a cell that starts agreeing FAILS, which is how the pin gets
	// deleted.
	pin string
	// pinArms is the same claim per ARM, for a shape the arms answer
	// DIFFERENTLY — the value where the arm diverges, keyed by arm name; an
	// arm absent from it is held to want. A substring is enough, so a pinned
	// error can be named by its sentence rather than by a task id.
	pinArms map[string]string
	pinWhy  string
}

func i1Cells() []i1Cell {
	return []i1Cell{
		// --- the filing shape and its discriminators ----------------------
		{name: "01_the_filing_shape_unqualified_id_over_a_cte",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT MAX(v) FROM c WHERE id < 4000) AS mx FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "02_ctl_the_qualified_inner_name",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT MAX(v) FROM c WHERE c.id < 4000) AS mx FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "03_ctl_the_aliased_cte_reference",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT MAX(v) FROM c x WHERE x.id < 4000) AS mx FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "04_ctl_the_same_spelling_over_a_base_table",
			sql:  `SELECT (SELECT MAX(c_i64) FROM typemx WHERE id < 4000) AS mx FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "05_ctl_the_same_subquery_at_top_level",
			sql:  `WITH c AS (SELECT id, c_i64 AS v FROM typemx) SELECT MAX(v) AS mx FROM c WHERE id < 4000`,
			want: `mx | 3999011997`},

		// --- the same rule at every shape of inner relation ---------------
		//
		// The `ScalarProjection` deltas below are not this arc's subject: a
		// SELECT-list scalar subquery whose producer the DAG cannot prove is
		// one row is answered on the coordinator-local route, which is what
		// that counter says. What matters here is that `Correlated` is ZERO —
		// these shapes are not correlated and must not be classified so.
		{name: "06_nested_two_deep",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT MAX(v) FROM c WHERE id < (SELECT MAX(id) FROM c WHERE id < 4000)) AS mx ` +
				`FROM decpair WHERE id < 2`,
			want: `mx | 3997011991`, routes: a2Routes{ScalarProjection: 1}},
		{name: "07_the_cte_column_is_an_explicit_alias",
			sql: `WITH c AS (SELECT id AS id, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 4000) AS n FROM decpair WHERE id < 2`,
			want: `n | 4000`},
		{name: "08_the_inner_from_is_a_derived_table",
			sql: `SELECT (SELECT MAX(v) FROM (SELECT id, c_i64 AS v FROM typemx) t WHERE id < 4000) AS mx ` +
				`FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "09_ctl_a_derived_table_with_a_column_alias_list",
			sql: `SELECT (SELECT MAX(vv) FROM (SELECT id, c_i64 FROM typemx) t(idd, vv) WHERE idd < 4000) AS mx ` +
				`FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "10_the_inner_from_is_a_set_operation_arm",
			sql: `SELECT (SELECT MAX(v) FROM (SELECT id, c_i64 AS v FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT id, c_i64 FROM typemx WHERE id >= 2000) t WHERE id < 4000) AS mx ` +
				`FROM decpair WHERE id < 2`,
			want: `mx | 3999011997`},
		{name: "11_the_same_name_in_both_scopes_binds_the_inner_one",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT id, (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 3 ORDER BY id`,
			want: `id,n | 1,10 | 2,10`},
		{name: "17_the_unqualified_name_is_in_the_subquerys_HAVING",
			sql: `WITH c AS (SELECT id, g, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT MAX(v) FROM c GROUP BY g HAVING MIN(id) < 1) AS mx FROM decpair WHERE id < 2`,
			want: `mx | 4998014994`, routes: a2Routes{ScalarProjection: 1}},
		{name: "18_the_derived_tables_select_list_is_a_star",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx) t WHERE id < 4000) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: `n | 4000`},
		{name: "20_the_cte_reference_is_under_a_join_inside_the_subquery",
			sql: `WITH c AS (SELECT id, g, c_i64 AS v FROM typemx) ` +
				`SELECT (SELECT COUNT(*) FROM c JOIN typemx_dim ON c.g = typemx_dim.k WHERE id < 10) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: `n | 10`},
		{name: "25_the_cte_bodys_select_list_is_a_star",
			sql: `WITH c AS (SELECT * FROM typemx) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 4000) AS n FROM decpair WHERE id < 2`,
			want: `n | 4000`},

		// --- the second symptom: the per-outer-row re-run ------------------
		//
		// 5000 outer rows. At base this did not finish on the spilled arm in
		// ten minutes and answered 3871 for PostgreSQL's 3870 on the other
		// three — 4000 ids minus the 129 whose c_i64 is NULL, which is the
		// arithmetic of a dropped predicate, not of a rounding.
		{name: "14_the_hang_shape_5000_outer_rows",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 < (SELECT MAX(v) FROM c WHERE id < 4000)`,
			want: `n | 3870`},

		// --- the boundary, attempted from the other side -------------------
		//
		// A name the inner relation does NOT carry is still an outer
		// reference, and these three still route. Their VALUES were right
		// before this fix too: it is the counter that separates "we stopped
		// mis-correlating" from "we stopped correlating".
		{name: "12_ctl_genuinely_correlated_on_a_qualified_outer_name",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE c.id < d.id) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,1 | 2,2 | 3,3`, routes: a2Routes{Correlated: 1}},
		{name: "13_ctl_genuinely_correlated_on_a_column_only_the_outer_has",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE c.id < d.b) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,13 | 2,13 | 3,13`, routes: a2Routes{Correlated: 1}},
		{name: "19_ctl_an_unqualified_name_only_the_outer_has_is_correlated",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE c.id < 10 AND s = '1.50') AS n ` +
				`FROM decpair d WHERE d.id < 3 ORDER BY d.id`,
			want: `id,n | 1,10 | 2,0`, routes: a2Routes{Correlated: 1}},

		// --- the other two constructs over the same inner relation ---------
		{name: "15_exists_over_a_cte_on_an_unqualified_inner_name",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM decpair d WHERE EXISTS ` +
				`(SELECT 1 FROM c WHERE id < 4000 AND v > 3999011000)`,
			want: `n | 9`},
		{name: "16_ctl_in_subquery_over_a_cte_on_an_unqualified_inner_name",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN (SELECT id FROM c WHERE id < 4)`,
			want: `n | 3`},

		// --- what cell 15 landed in when it stopped being correlated -------
		//
		// An UNCORRELATED `EXISTS` in a filter predicate failed on both DAG
		// arms with `EXISTS subquery requires a SubqueryRunner` — #524's
		// family with the EXISTS arm never written. It is not a CTE defect
		// and these four cells say so: a base table, a NOT EXISTS, a
		// QUALIFIED CTE reference and one beside another predicate all failed
		// the same way at base, none of them through the scope classifier.
		// Cell 15 reached it only because it stopped being mis-correlated,
		// which is why they are gated here.
		{name: "21_uncorrelated_exists_over_a_base_table",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE EXISTS ` +
				`(SELECT 1 FROM typemx WHERE id < 4000)`,
			want: `n | 9`},
		{name: "22_uncorrelated_not_exists_over_a_base_table",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE NOT EXISTS ` +
				`(SELECT 1 FROM typemx WHERE id < 4000)`,
			want: `n | 0`},
		{name: "23_uncorrelated_exists_over_a_cte_on_a_qualified_inner_name",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM decpair d WHERE EXISTS ` +
				`(SELECT 1 FROM c WHERE c.id < 4000 AND v > 3999011000)`,
			want: `n | 9`},
		{name: "24_uncorrelated_exists_that_is_false_beside_another_predicate",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 3 AND EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0)`,
			want: `n | 0`},
		// And the boundary of THAT: an EXISTS that reads an outer row is not
		// a query-wide constant, is not evaluated at plan time, and keeps its
		// route. An inequality correlation is the spelling that does not
		// decorrelate into a semi join, so it reaches the same site.
		{name: "26_ctl_a_correlated_exists_is_not_a_plan_time_constant",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < d.id)`,
			want: `n | 9`, routes: a2Routes{Correlated: 1}},

		// --- a column-alias list interacting with the scope rule -----------
		//
		// These two are right for the RIGHT reason and would break if the
		// alias overlay were dropped: a list HIDES the names it replaces, so
		// `id` is not an inner name here and IS a reference to the enclosing
		// query — which is exactly what PostgreSQL 17 answers, 5000 rather
		// than 10, on both spellings.
		{name: "27_ctl_a_derived_star_under_an_alias_list_hides_the_renamed_name",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx) t(idd) WHERE id < 10) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: `n | 5000`, routes: a2Routes{Correlated: 1}},
		{name: "28_ctl_a_cte_column_list_hides_the_renamed_name",
			sql: `WITH c(kk) AS (SELECT * FROM typemx) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 5000`, routes: a2Routes{Correlated: 1}},

		// --- the divergence this arc does NOT close ------------------------
		//
		// PostgreSQL's column-alias list renames the LEADING columns and the
		// rest keep their names: `WITH c(kk) AS (SELECT id, s FROM decpair)`
		// publishes `kk` AND `s`, so the `s` below is c's and the subquery is
		// not correlated — one row per outer row.
		//
		// This engine treats the list as the WHOLE namespace, in the binder
		// (`registerCTE` stores `cte.Columns` outright) and in the logical
		// builder, so `s` is not an inner name, binds the enclosing query, and
		// the subquery is re-run per outer row with the value substituted:
		// 9 for the row whose `s` is '1.50' and 0 for the one whose is '1.5'.
		//
		// It is the same FAMILY as this arc's subject and a different root
		// cause, and fixing it at the classifier alone would be a bandaid: the
		// classifier would call the reference inner while the binder still
		// refuses `s` as unknown, turning a wrong number into a refusal for a
		// legal query. Closing it means applying a column-alias list
		// POSITIONALLY everywhere it is read — the binder, the CTE
		// materialization and the derived-table path, which also owns
		// ADR-0012's recorded divergence that a list over a `SELECT *` body is
		// not applied at all. That is its own arc.
		{name: "29_pin_a_short_cte_column_list_hides_the_columns_it_did_not_rename",
			sql: `WITH c(kk) AS (SELECT id, s FROM decpair) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE s = '1.50') AS n FROM decpair d WHERE d.id < 3`,
			want: `n | 1 | 1`,
			pin:  `n | 9 | 0`,
			pinWhy: "a column-alias list is the WHOLE namespace here, not a positional rename, " +
				"so `s` is read as the enclosing query's and substituted per outer row",
			routes: a2Routes{Correlated: 1}},

		// --- A STAR PUBLISHES WHAT IT STANDS FOR (round-1 review B1/P3) ----
		//
		// `alias.*` is ONE source. Reading it as "every FROM item" published a
		// SUPERSET to the classifier, and a superset turns an OUTER reference
		// into an inner one — #955 with the sign flipped. The first two cells
		// are the shapes that broke: `c` publishes `k, label`, so `id` is
		// decpair's and PostgreSQL counts every row of c.
		{name: "30_a_qualified_star_publishes_one_side_of_a_join_cte",
			sql: `WITH c AS (SELECT dim.* FROM typemx_dim dim JOIN typemx tx ON tx.g = dim.k) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 4616`, routes: a2Routes{Correlated: 1}},
		{name: "31_a_qualified_star_publishes_one_side_of_a_join_derived",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT dim.* FROM typemx_dim dim JOIN typemx tx ` +
				`ON tx.g = dim.k) t WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 4616`, routes: a2Routes{Correlated: 1}},
		// CONTROLS, and what they attempt. The other side of the same join
		// DOES publish `id`, so the same spelling one alias over is INNER.
		// They do not gate the star commit — the over-claim it replaced also
		// called `id` inner here, so they pass with it reverted — they gate
		// the LAZY repair of it: a fix that answered cells 30/31 by making
		// every qualified star UNKNOWN would send `id` to the outer scope here
		// and answer 4616 for PostgreSQL's 10. Both were 4616 at the arc's
		// base and are gated as values by the arc's first commit.
		{name: "32_ctl_the_qualified_star_names_the_side_that_has_the_name",
			sql: `WITH c AS (SELECT tx.* FROM typemx_dim dim JOIN typemx tx ON tx.g = dim.k) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 10`},
		{name: "33_ctl_a_bare_star_is_every_from_item",
			sql: `WITH c AS (SELECT * FROM typemx_dim dim JOIN typemx tx ON tx.g = dim.k) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 10`},
		{name: "34_a_qualified_star_under_a_column_alias_list",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT dim.* FROM typemx_dim dim JOIN typemx tx ` +
				`ON tx.g = dim.k) t(kk, ll) WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 4616`, routes: a2Routes{Correlated: 1}},

		// --- THE PER-ROW RE-RUN SUBSTITUTES ONLY BARE NAMES (B3) -----------
		//
		// An inner bare name BESIDE an outer qualified one, which is the
		// commonest correlated shape there is. All four answered 0 for every
		// outer row at base — the predicate had become `1 < 1`, `2 < 2`,
		// `3 < 3` — over a base table as well as a CTE.
		{name: "35_an_inner_bare_name_beside_an_outer_qualified_one",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE id < d.id) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,1 | 2,2 | 3,3`, routes: a2Routes{Correlated: 1}},
		{name: "36_the_same_over_a_base_table",
			sql: `SELECT d.id, (SELECT COUNT(*) FROM typemx WHERE id < d.id) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,1 | 2,2 | 3,3`, routes: a2Routes{Correlated: 1}},
		{name: "37_two_inner_bare_names_and_one_outer",
			sql: `WITH c AS (SELECT id, g, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE id < d.id AND g < 3) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,1 | 2,2 | 3,3`, routes: a2Routes{Correlated: 1}},
		{name: "38_the_mixed_predicate_nested_two_deep",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE c.id < ` +
				`(SELECT MAX(id) FROM typemx WHERE id < d.id)) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,0 | 2,1 | 3,2`, routes: a2Routes{Correlated: 1}},
		// The control that was right at base too: no name collides, so the
		// substitution had nothing to over-reach into.
		{name: "39_ctl_a_correlated_subquery_with_no_name_collision",
			sql: `WITH c AS (SELECT id, g, c_i64 AS v FROM typemx) ` +
				`SELECT d.id, (SELECT COUNT(*) FROM c WHERE g = 1 AND c.id < d.id) AS n ` +
				`FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `id,n | 1,0 | 2,1 | 3,1`, routes: a2Routes{Correlated: 1}},

		// --- THE EXISTS CONSTANT REACHES EVERY POSITION (B2) ---------------
		//
		// `AND` was reachable only because the filter is split into conjuncts
		// before the walk. `OR` and `NOT` cannot be split, so these four
		// failed both DAG arms at base with the SubqueryRunner error.
		{name: "40_a_false_exists_under_OR",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 2 OR EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0)`,
			want: `n | 1`},
		{name: "41_a_true_exists_under_OR",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 2 OR EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 10)`,
			want: `n | 9`},
		{name: "42_a_not_exists_under_OR",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 2 OR NOT EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0)`,
			want: `n | 9`},
		{name: "43_a_parenthesized_NOT_over_an_exists",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE NOT (EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0))`,
			want: `n | 9`},
		{name: "44_an_exists_inside_an_AND_of_an_OR",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 5 AND (d.id > 3 OR EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0))`,
			want: `n | 1`},
		{name: "45_an_exists_in_a_CASE_condition",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE CASE WHEN EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0) THEN true ELSE d.id < 2 END`,
			want: `n | 1`},
		{name: "46_an_exists_in_a_HAVING",
			sql: `SELECT COUNT(*) AS n FROM (SELECT a, COUNT(*) AS c FROM decpair GROUP BY a ` +
				`HAVING COUNT(*) > 0 AND EXISTS (SELECT 1 FROM typemx t WHERE t.id < 0)) q`,
			want: `n | 0`},
		{name: "47_an_exists_in_a_JOIN_condition",
			sql: `SELECT COUNT(*) AS n FROM decpair d JOIN typemx_dim k ON d.id = k.k AND EXISTS ` +
				`(SELECT 1 FROM typemx t WHERE t.id < 0)`,
			want: `n | 0`},

		// --- THE CHAIN IS NOT TRUNCATED (B4) -------------------------------
		//
		// Nine links was one past the removed bound and answered 5000 for
		// PostgreSQL's 4000; twelve is well past it.
		{name: "48_a_nine_link_chain_of_star_bodied_ctes", sql: i1Chain(9), want: `n | 4000`},
		{name: "49_a_twelve_link_chain_of_star_bodied_ctes", sql: i1Chain(12), want: `n | 4000`},

		// --- WHAT THIS ARC UNMASKED AND DOES NOT CLOSE ---------------------
		//
		// Three shapes were WRONG on every arm at base because the subquery
		// was mis-correlated and its predicate dropped. With the scope fixed
		// the single-process arms answer PostgreSQL and the DAG arms reach
		// pre-existing lowering gaps that the wrong answer had been hiding.
		// wrong → (right on two arms, loud on two) is within doctrine, and
		// each is pinned with the sentence it fails by so the day the gap
		// closes the pin fails.
		//
		// MULTIPLE qualified stars in one SELECT list: the logical builder
		// emits a projection column literally named `dim.*` that no executor
		// schema carries. All four arms failed that way at base; the DAG arms
		// now plan the CTE as a stage and answer.
		{name: "50_pin_two_qualified_stars_in_one_select_list",
			sql: `WITH c AS (SELECT dim.*, tx.* FROM typemx_dim dim JOIN typemx tx ON tx.g = dim.k) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 10`,
			pinArms: map[string]string{
				"single":   `column "dim.*" does not exist in the input schema`,
				spilledArm: `column "dim.*" does not exist in the input schema`,
			},
			pinWhy: "the logical builder does not expand a SECOND qualified star; it emits a " +
				"projection column named `dim.*` and the single-process CTE materialization " +
				"then cannot resolve it (all four arms failed this way at base)"},
		// A RECURSIVE CTE named inside a subquery. It has no stage lowering
		// (ADR-0021 §1b), and the DAG reaches that as a build failure rather
		// than as the routed refusal §1c would give it.
		{name: "51_pin_a_recursive_cte_named_inside_a_subquery",
			sql: `WITH RECURSIVE r(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM r WHERE id < 5) ` +
				`SELECT (SELECT COUNT(*) FROM r WHERE id < 4) AS n FROM decpair WHERE id < 2`,
			want: `n | 3`,
			pinArms: map[string]string{
				"dag":     `has no dependencies and no ScanFiles`,
				"dagshuf": `has no dependencies and no ScanFiles`,
			},
			pinWhy: "a recursive CTE has no stage lowering, and the DAG reaches that as an " +
				"unbuildable stage instead of the routed refusal ADR-0021 §1c gives a " +
				"subquery it cannot run (all four arms answered 5 for PostgreSQL's 3 at base)"},
		// A UNION of two star arms as the subquery's FROM. The stage's column
		// pruning drops a column the union still declares.
		{name: "52_pin_a_union_of_two_star_arms_as_the_inner_from",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT * FROM typemx WHERE id >= 2000) t WHERE id < 10) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: `n | 10`,
			pinArms: map[string]string{
				"dag":     `column "g" does not exist in the input schema`,
				"dagshuf": `column "g" does not exist in the input schema`,
			},
			pinWhy: "the union stage's column pruning drops a column its own arms still " +
				"declare (all four arms answered 0 for PostgreSQL's 10 at base)"},

		// The fourth member of that family, and the DERIVED-TABLE spelling of
		// controls 32 and 33: a star over a JOIN inside a derived table, then
		// FILTERED. The builder does not give the derived plan the column the
		// filter names. All four arms answered 4616 for PostgreSQL's 10 at
		// base — silently wrong, because the mis-correlated subquery dropped
		// the predicate — and are loud now that the predicate is kept.
		{name: "53_pin_a_derived_qualified_star_over_a_join_is_filtered",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT tx.* FROM typemx_dim dim JOIN typemx tx ` +
				`ON tx.g = dim.k) t WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 10`,
			pinArms: map[string]string{
				"single": `filter column "id" does not exist in the input schema`, spilledArm: `filter column "id" does not exist in the input schema`,
				"dag": `filter column "id" does not exist in the input schema`, "dagshuf": `filter column "id" does not exist in the input schema`,
			},
			pinWhy: "a derived table whose body is a star over a JOIN does not publish that " +
				"star's columns to a filter above it (all four arms answered 4616 at base)",
			routes: a2Routes{UnreachableOutput: 1}},
		{name: "54_pin_a_derived_bare_star_over_a_join_is_filtered",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx_dim dim JOIN typemx tx ` +
				`ON tx.g = dim.k) t WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: `n | 10`,
			pinArms: map[string]string{
				"single": `filter column "id" does not exist in the input schema`, spilledArm: `filter column "id" does not exist in the input schema`,
				"dag": `filter column "id" does not exist in the input schema`, "dagshuf": `filter column "id" does not exist in the input schema`,
			},
			pinWhy: "the bare-star twin of 53, same site (all four arms answered 4616 at base)",
			routes: a2Routes{UnreachableOutput: 1}},

		// --- A BOOLEAN CONNECTIVE SHORT-CIRCUITS ---------------------------
		//
		// Hoisting is UNCONDITIONAL evaluation, so hoisting a SCALAR out of an
		// arm the query may never reach makes that arm's failure the query's
		// answer. Measured on PostgreSQL 17: the same five-row subquery is
		// never needed in the first cell and needed in the second, so the
		// first ANSWERS and only the second raises. Hoisting made both 21000
		// (round-1 review P2), which is a query PostgreSQL answers, refused.
		//
		// Only the EXISTS leaves are hoisted now, and a scalar leaf in a
		// boolean position keeps the disposition it had: right on the
		// single-process arms, and a loud task failure on the DAG, pinned.
		// Answering it there needs the DAG to evaluate a subquery lazily per
		// row, which is a lowering, not a scope repair.
		{name: "55_a_multirow_scalar_under_OR_that_is_never_needed",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 100 OR d.id > ` +
				`(SELECT id FROM typemx WHERE id < 5)`,
			want: `n | 9`,
			pinArms: map[string]string{
				"dag": "subqueries require a SubqueryRunner", "dagshuf": "subqueries require a SubqueryRunner",
			},
			pinWhy: "the DAG has no lazy per-row scalar evaluation, so a scalar leaf in a " +
				"short-circuitable position ships to a worker that cannot compile it"},
		{name: "56_ctl_a_multirow_scalar_under_OR_that_IS_needed_raises",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 0 OR d.id > ` +
				`(SELECT id FROM typemx WHERE id < 5)`,
			wantErr: "more than one row returned by a subquery used as an expression",
			pinArms: map[string]string{
				"dag": "subqueries require a SubqueryRunner", "dagshuf": "subqueries require a SubqueryRunner",
			},
			pinWhy: "same site as 55; the single-process arms raise PostgreSQL's own 21000 here, " +
				"which is what makes 55's answer a short circuit rather than a missing check"},
		{name: "57_ctl_a_one_row_scalar_under_OR_that_IS_needed",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id < 0 OR d.id > ` +
				`(SELECT MAX(id) FROM typemx WHERE id < 5)`,
			want: `n | 5`,
			pinArms: map[string]string{
				"dag": "subqueries require a SubqueryRunner", "dagshuf": "subqueries require a SubqueryRunner",
			},
			pinWhy: "same site as 55, with a subquery that could be hoisted safely — the rule is " +
				"about the POSITION, not about this subquery's row count"},
		{name: "58_ctl_a_one_row_scalar_under_NOT",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE NOT (d.id > ` +
				`(SELECT MAX(id) FROM typemx WHERE id < 5))`,
			want: `n | 4`,
			pinArms: map[string]string{
				"dag": "subqueries require a SubqueryRunner", "dagshuf": "subqueries require a SubqueryRunner",
			},
			pinWhy: "the NOT spelling of 57"},
	}
}

// i1Chain is n chained star-bodied CTEs with a subquery over the last one. The
// classifier has to follow every link to know that `id` is the chain's and not
// the enclosing query's, so the chain length is the assertion.
func i1Chain(n int) string {
	var b strings.Builder
	b.WriteString("WITH c1 AS (SELECT * FROM typemx)")
	for i := 2; i <= n; i++ {
		fmt.Fprintf(&b, ", c%d AS (SELECT * FROM c%d)", i, i-1)
	}
	fmt.Fprintf(&b, " SELECT (SELECT COUNT(*) FROM c%d WHERE id < 4000) AS n "+
		"FROM decpair WHERE id < 2", n)
	return b.String()
}

func TestArcI1AnUnqualifiedNameBindsTheInnerRelation(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := i1Arms(t, ctx)

	for _, tc := range i1Cells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				var before a2Routes
				if arm.coord != nil {
					before = a2ReadRoutes(arm.coord)
				}
				got, err := i1Run(ctx, arm, tc.sql)
				if arm.coord != nil {
					a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.coord), tc.routes, tc.sql)
				}
				if want, pinned := tc.pinArms[arm.name]; pinned {
					// A pinned arm claims a DIVERGENCE, and the divergence may
					// be a value or a refusal. Either way the day it agrees
					// with PostgreSQL this fails and the pin is deleted.
					switch {
					case err != nil && strings.Contains(err.Error(), want):
						// The recorded failure, unchanged.
					case err == nil && got == want:
						// The recorded wrong value, unchanged.
					case err == nil && got == tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this pin is FIXED: "+
							"delete it from i1Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					default:
						t.Errorf("%s arm answers %v%s, which is neither PostgreSQL's %s nor the "+
							"pinned %q\n  SQL: %s", arm.name, got, i1ErrText(err), tc.want, want, tc.sql)
					}
					continue
				}
				if tc.wantErr != "" {
					switch {
					case err == nil:
						t.Errorf("%s arm ANSWERED %s where PostgreSQL 17 raises %q\n  SQL: %s",
							arm.name, got, tc.wantErr, tc.sql)
					case !strings.Contains(err.Error(), tc.wantErr):
						t.Errorf("%s arm raises a different refusal\n  got  %v\n  want a sentence "+
							"containing %q (PostgreSQL 17)\n  SQL: %s", arm.name, err, tc.wantErr, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17 answers %s",
						arm.name, err, tc.sql, tc.want)
					continue
				}
				if tc.pin != "" {
					switch got {
					case tc.want:
						t.Errorf("%s arm now AGREES with PostgreSQL (%s), so this pin is FIXED: "+
							"delete it from i1Cells.\n  pinned reason: %s\n  SQL: %s",
							arm.name, tc.want, tc.pinWhy, tc.sql)
					case tc.pin:
						// The recorded divergence, unchanged.
					default:
						t.Errorf("%s arm answers %s, which is neither PostgreSQL's %s nor the "+
							"pinned %s\n  SQL: %s", arm.name, got, tc.want, tc.pin, tc.sql)
					}
					continue
				}
				if got != tc.want {
					t.Errorf("%s arm\n  got  %s\n  want %s (PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// i1Run runs one query on one arm under i1Deadline and renders its result. A
// deadline that expires is this gate's second assertion, not an infrastructure
// hiccup: the defect's own second symptom is a subquery re-run once per outer
// row, and rows that never arrive are the shape of it.
func i1Run(ctx context.Context, arm i1Arm, sql string) (string, error) {
	qctx, cancel := context.WithTimeout(ctx, i1Deadline)
	defer cancel()
	type result struct {
		cols []string
		rows [][]any
		err  error
	}
	done := make(chan result, 1)
	go func() {
		cols, rows, err := arm.run(qctx, sql)
		done <- result{cols, rows, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return "", r.err
		}
		return e3Render(r.cols, r.rows), nil
	case <-qctx.Done():
		return "", context.DeadlineExceeded
	}
}

// i1ErrText renders an error for a failure message, or nothing when there is
// none, so a pinned cell's diagnostic reads the same whether the arm diverged
// by VALUE or by REFUSAL.
func i1ErrText(err error) string {
	if err == nil {
		return ""
	}
	return " (error: " + err.Error() + ")"
}
