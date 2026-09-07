package coordinator

import (
	"context"
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
	// routes is the routing delta each DAG arm must produce for this one
	// query. The zero value says the DAG EXECUTED the shape as stages.
	routes a2Routes
	// pin, when set, says this cell DIVERGES from PostgreSQL and records what
	// this engine answers instead. The gate then asserts the divergence: a
	// cell that starts agreeing FAILS, which is how the pin gets deleted.
	pin    string
	pinWhy string
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
	}
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
