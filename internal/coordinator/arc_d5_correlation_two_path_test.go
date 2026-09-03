package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// The correlation census (ADR-0021 §1c): every shape of a correlated subquery
// this arc touches, on FOUR ARMS, each answer anchored to LIVE PostgreSQL 17
// over the same fixture rather than to another arm.
//
//	single         the embedded single-process engine
//	spilled        the same under a 512 KiB budget, run FIVE times — a single
//	               passing spilled run proves nothing (ADR-0027 §5)
//	dag            the three-worker stage DAG, LocalFastPathBytes = 0
//	dag-shuffled   the same with BroadcastBytesOverride = 1, so every build
//	               side goes through a hash join and an exchange
//
// Every DAG cell asserts the ROUTING COUNTERS beside the rows (protocol rule
// 11). Rows alone cannot tell "the DAG executed this" from "the DAG refused
// the plan and the coordinator-local pipeline answered", and the difference is
// this arc's whole subject: a shape that the decorrelator now expresses shows
// ZERO routes, and one it deliberately declines shows ONE — which is the cost
// of the decline, recorded rather than described.
type arcD5Cell struct {
	issue, name, sql string
	// want is the whole result, na2Run-rendered and sorted.
	want []string
	// wantErrLike, when set, is a substring every arm's error must carry.
	wantErrLike string
	// wantErrLikeDAG, when set, is what the two DAG arms' error must carry
	// instead — a stage carries its filter as TEXT and the worker re-parses
	// it, so a refusal the single-process compiler makes by name the DAG can
	// make somewhere else entirely.
	wantErrLikeDAG string
	// wantDAG, when set, is what the two DAG arms must answer INSTEAD of
	// `want`.
	wantDAG []string
	// The per-DAG-arm counter deltas. 0 = the DAG executed the shape.
	wantCorrRoutes        int64
	wantScalarProjRoutes  int64
	wantInSubqueryRoutes  int64
	wantUnreachableRoutes int64
	// pgSays records PostgreSQL 17's answer in prose where `want` cannot hold
	// it (a refusal, or a deliberate divergence).
	pgSays string
}

func arcD5Cells() []arcD5Cell {
	var out []arcD5Cell
	for _, group := range [][]arcD5Cell{
		arcD5CTEScopeCells(),
		arcD5TypedRerunCells(),
		arcD5NotInCells(),
		arcD5AggregatePlacementCells(),
		arcD5FailedSubquerySetCells(),
		arcD5AggregateArgumentCells(),
	} {
		out = append(out, group...)
	}
	return out
}

// ---------------------------------------------------------------------------
// #535 — a CTE reference is a SCOPE, and the correlation collectors can see it.
//
// A derived table's alias is stamped onto every scan below it; a CTE's name is
// not, and deliberately (logical.resolveTableOrCTE says why). The four
// outer-scope collectors read only NodeScan, so `u` was a name none of them
// had heard of and `WHERE EXISTS (… WHERE t.k = u.did)` over a CTE `u` was not
// recognized as CORRELATED at all: 0 rows in silence on the single-process
// pipeline before v0.18.16 made it loud, "EXISTS subquery requires a
// SubqueryRunner" on the DAG, and — for the IN spelling — an inner filter
// `k = did` over a column the build side does not carry.
//
// Every cell here now runs ON THE DAG (wantCorrRoutes 0): the shapes are
// decorrelated into ordinary semi/anti joins, so both distribution arms
// execute them rather than routing them local.
func arcD5CTEScopeCells() []arcD5Cell {
	const cte = `WITH u AS (SELECT g AS did FROM typemx WHERE id < 50) `
	return []arcD5Cell{
		{issue: "#535", name: "exists_over_a_cte",
			sql: cte + `SELECT COUNT(*) AS c FROM u WHERE EXISTS (` +
				`SELECT 1 FROM typemx_dim WHERE typemx_dim.k = u.did)`,
			want: []string{"c=int64:47"}},
		{issue: "#535", name: "not_exists_over_a_cte",
			sql: cte + `SELECT COUNT(*) AS c FROM u WHERE NOT EXISTS (` +
				`SELECT 1 FROM typemx_dim WHERE typemx_dim.k = u.did)`,
			want: []string{"c=int64:3"}},
		{issue: "#535", name: "scalar_subquery_over_a_cte",
			sql: cte + `SELECT COUNT(*) AS c FROM u WHERE (` +
				`SELECT COUNT(*) FROM typemx_dim WHERE typemx_dim.k = u.did) > 0`,
			want: []string{"c=int64:47"}},
		// The IN spelling was not merely un-decorrelated: it WAS rewritten,
		// and the correlation term — whose `u.` qualifier named nothing —
		// was classified as an INNER-only filter and stripped to `k = did`,
		// a comparison the build side has no `did` for. It reached the
		// client as `ColColFilter: could not resolve kernel for k 0 did`.
		{issue: "#535", name: "correlated_in_over_a_cte",
			sql: cte + `SELECT COUNT(*) AS c FROM u WHERE u.did IN (` +
				`SELECT d.k FROM typemx_dim d WHERE d.k = u.did)`,
			want: []string{"c=int64:47"}},
		// `FROM u AS z` — PostgreSQL makes the reference's own alias the only
		// spelling the enclosing query may use, and Node.CTERefAlias is where
		// that name lives.
		{issue: "#535", name: "exists_over_a_cte_under_a_reference_alias",
			sql: cte + `SELECT COUNT(*) AS c FROM u AS z WHERE EXISTS (` +
				`SELECT 1 FROM typemx_dim WHERE typemx_dim.k = z.did)`,
			want: []string{"c=int64:47"}},
		// The UNQUALIFIED spelling, which needs the second half of the fix:
		// the column map has to know that `did` — a name the CTE's Project
		// invents and no scan below emits — belongs to the CTE's scope.
		{issue: "#535", name: "exists_over_a_cte_correlated_on_a_bare_name",
			sql: cte + `SELECT COUNT(*) AS c FROM u WHERE EXISTS (` +
				`SELECT 1 FROM typemx_dim WHERE typemx_dim.k = did)`,
			want: []string{"c=int64:47"}},
		// The controls, which answered at this arc's base and must keep
		// answering: the DERIVED-table spelling of the same query, the
		// ALIASED base table, and a CTE on the INNER side (which was never
		// the defect — the collectors walk the OUTER subtree).
		{issue: "#535", name: "control_derived_table_spelling",
			sql: `SELECT COUNT(*) AS c FROM (SELECT g AS did FROM typemx WHERE id < 50) u ` +
				`WHERE EXISTS (SELECT 1 FROM typemx_dim WHERE typemx_dim.k = u.did)`,
			want: []string{"c=int64:47"}},
		{issue: "#535", name: "control_aliased_base_table_correlation",
			sql: `SELECT COUNT(*) AS c FROM typemx t0 WHERE t0.id < 50 AND EXISTS (` +
				`SELECT 1 FROM typemx sub WHERE sub.g = t0.g)`,
			want: []string{"c=int64:47"}},
		{issue: "#535", name: "control_cte_on_the_inner_side",
			sql: `WITH d AS (SELECT k FROM typemx_dim) SELECT COUNT(*) AS c FROM typemx a ` +
				`WHERE a.id < 50 AND EXISTS (SELECT 1 FROM d WHERE d.k = a.g)`,
			want: []string{"c=int64:47"}, wantCorrRoutes: 1},
		{issue: "#535", name: "control_uncorrelated_in_over_a_cte",
			sql: `WITH u AS (SELECT g AS did, id FROM typemx WHERE id < 50) ` +
				`SELECT COUNT(*) AS c FROM u WHERE u.did IN (SELECT d.k FROM typemx_dim d)`,
			want: []string{"c=int64:47"}},
		// THE BOUNDARY, unchanged by this arc and pinned rather than
		// described (rule 11). An outer table correlated BY ITS TABLE NAME
		// where the inner relation reads the SAME table under an alias is
		// invisible to plansql.DanglingTableRefs — `typemx.g` is not dangling,
		// because the subquery's own FROM is `typemx sub`. The CTE fix does
		// not reach it: there is no CTE here and no scope to record. It stays
		// SILENTLY WRONG at 50 for PostgreSQL's 47 on the two single-process
		// arms and LOUD on the DAG, and closing it is a classifier repair
		// (ADR-0021 §1c). The day it answers 47 this pin FAILS.
		{issue: "#535", name: "boundary_unaliased_base_table_correlation_stays_silent",
			sql: `SELECT COUNT(*) AS c FROM typemx WHERE id < 50 AND EXISTS (` +
				`SELECT 1 FROM typemx sub WHERE sub.g = typemx.g)`,
			want:           []string{"c=int64:50"},
			wantErrLikeDAG: "SubqueryRunner",
			pgSays:         "47"},
	}
}

// ---------------------------------------------------------------------------
// #679 — the re-run renders every outer value TYPED.
//
// A correlated subquery this engine cannot express as a join is re-executed
// per outer row with the outer values substituted as LITERAL TEXT, and the
// renderer read the Go BOX rather than the column's type. `batch.Vector.
// GetValue` hands a DECIMAL back as its rendered text, so `a.w_d2 = b.k`
// became `'2.00' = b.k` and raised 22P02 for a query PostgreSQL answers with
// 3 rows. DATE, TIMESTAMP, BYTES, the six network types and UUID reached the
// same `default:` arm.
//
// The per-type matrix below is the gate the fix earns: all 18 flat types as
// the OUTER value of a correlated EXISTS whose inner is a derived table (the
// decline that keeps the shape on the re-run), each against PostgreSQL's own
// answer over the same rows. A rendering that re-types a value answers a
// different number, and the counts differ per type — 29, 30, 38, 58, 60 — so
// no single wrong answer passes them all.
func arcD5TypedRerunCells() []arcD5Cell {
	// PostgreSQL 17 over the type-matrix fixture, measured live.
	want := map[string]int{
		"c_bool": 58, "c_i32": 29, "c_i64": 29, "c_f32": 29, "c_f64": 29,
		"c_str": 29, "c_bytes": 29, "c_ts": 29, "c_ipv4": 29, "c_ipv6": 30,
		"c_cidr": 38, "c_mac": 30, "c_port": 30, "c_proto": 60, "c_dur": 30,
		"c_uuid": 30, "c_date": 60, "c_dec": 30,
	}
	cols := make([]string, 0, len(want))
	for c := range want {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	out := make([]arcD5Cell, 0, len(cols)+5)
	for _, c := range cols {
		out = append(out, arcD5Cell{
			issue: "#679", name: "rerun_renders_" + c,
			sql: fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx a WHERE a.id < 60 AND EXISTS (`+
				`SELECT 1 FROM (SELECT %s AS k FROM typemx WHERE id >= 30 AND id < 5000 `+
				`GROUP BY %s) b WHERE a.%s = b.k)`, c, c, c),
			want:           []string{fmt.Sprintf("n=int64:%d", want[c])},
			wantCorrRoutes: 1,
		})
	}
	// The issue's own shape and its NOT EXISTS twin: a DECIMAL outer value
	// against a BIGINT inner, over a derived table.
	out = append(out,
		arcD5Cell{issue: "#679", name: "exists_over_a_derived_table_cross_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:3"}, wantCorrRoutes: 1},
		arcD5Cell{issue: "#679", name: "not_exists_over_a_derived_table_cross_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE NOT EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:7"}, wantCorrRoutes: 1},
		// The three controls that answered at base: three of the four width
		// pairs were already right, which is what would have let a wrong fix
		// pass. The trigger is a DECIMAL outer against an INTEGER inner over
		// a derived table, not "a correlated EXISTS".
		arcD5Cell{issue: "#679", name: "control_derived_table_same_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_i64 = b.k)`,
			want: []string{"n=int64:9"}, wantCorrRoutes: 1},
		arcD5Cell{issue: "#679", name: "control_derived_table_decimal_decimal",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_d4 AS k FROM numwidth GROUP BY w_d4) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:7"}, wantCorrRoutes: 1},
		arcD5Cell{issue: "#679", name: "control_base_table_cross_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM numwidth b WHERE a.w_d2 = b.w_i64)`,
			want: []string{"n=int64:3"}},
	)
	return out
}

// ---------------------------------------------------------------------------
// #538 / #578 — a correlated NOT IN keeps NOT IN's three-valued rule.
//
// It was lowered to a plain anti join, which answers the TWO-valued question
// "did nothing match" — its NOT EXISTS twin. Measured against live PostgreSQL
// 17 over the multikey fixture, three shapes answered 13 for 9, 6 and 9, on
// all four arms and in silence; 13 is exactly what the corresponding NOT
// EXISTS answers, which is the diagnosis.
//
// The lowering is now DECLINED (logical.correlatedNotInIsNotAnAntiJoin says
// why an anti join cannot carry the rule and what would be needed to make one
// that could), so the predicate stays a subquery and
// expr.CorrelatedInSubquery.EvalBoolNull answers it per outer row. That is the
// exact rule — and it costs a route on the DAG, which is what
// `wantCorrRoutes: 1` records here rather than leaving to prose.
func arcD5NotInCells() []arcD5Cell {
	return []arcD5Cell{
		{issue: "#578", name: "correlated_not_in_string_key",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s NOT IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n)`,
			want: []string{"n=int64:9"}, wantCorrRoutes: 1,
			pgSays: "9; the NOT EXISTS twin answers 13, which is what the anti join gave"},
		{issue: "#578", name: "correlated_not_in_integer_key",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n NOT IN (` +
				`SELECT b.n FROM mk_inner b WHERE b.s = a.s)`,
			want: []string{"n=int64:6"}, wantCorrRoutes: 1},
		// The build key guarded IS NOT NULL, so the poison here comes from
		// the PROBE's own NULL — #578's half that needs no per-group state.
		{issue: "#578", name: "correlated_not_in_null_free_build_key",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s NOT IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n AND b.s IS NOT NULL)`,
			want: []string{"n=int64:9"}, wantCorrRoutes: 1},
		{issue: "#538", name: "correlated_not_in_decimal_key",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.d NOT IN (` +
				`SELECT b.d FROM mk_inner b WHERE b.n = a.n)`,
			want: []string{"n=int64:5"}, wantCorrRoutes: 1},
		// EVERY group is empty here, and that is the edge both a flag on the
		// operator and a plain `x IS NOT NULL` conjunct get wrong: `x NOT IN
		// ()` is TRUE for every row INCLUDING a NULL-keyed one, because there
		// is nothing for the comparison to be UNKNOWN about. 40 of 40.
		{issue: "#538", name: "correlated_not_in_every_group_empty",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s NOT IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n AND b.id < 0)`,
			want: []string{"n=int64:40"}, wantCorrRoutes: 1,
			pgSays: "40 — an empty list makes NOT IN TRUE even for a NULL probe key"},
		{issue: "#538", name: "correlated_not_in_over_a_cte",
			sql: `WITH u AS (SELECT g AS did FROM typemx WHERE id < 50) ` +
				`SELECT COUNT(*) AS c FROM u WHERE u.did NOT IN (` +
				`SELECT d.k FROM typemx_dim d WHERE d.k = u.did)`,
			want: []string{"c=int64:3"}, wantCorrRoutes: 1,
			pgSays: "3 — and those 3 rows are exactly the NULL-keyed ones, whose group is empty"},
		{issue: "#538", name: "correlated_not_in_self_join_key",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key = a.w_key)`,
			want: []string{"n=int64:0"}, wantCorrRoutes: 1},
		// The controls, which must keep their PLAN as well as their answer.
		// The NOT EXISTS twin is what an anti join really means and still
		// decorrelates (0 routes); the UNCORRELATED NOT IN keeps #507's
		// null-aware anti join; and the correlated IN — which needs no
		// third value — still decorrelates too. A fix that declined all
		// three would pass a rows-only check and cost every one of them
		// its join.
		{issue: "#578", name: "control_correlated_not_exists_twin",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE NOT EXISTS (` +
				`SELECT 1 FROM mk_inner b WHERE b.n = a.n AND b.s = a.s)`,
			want: []string{"n=int64:13"}},
		{issue: "#578", name: "control_uncorrelated_not_in_stays_null_aware",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s NOT IN (` +
				`SELECT b.s FROM mk_inner b)`,
			want:   []string{"n=int64:0"},
			pgSays: "0 — mk_inner.s holds a NULL, which empties a NOT IN outright"},
		{issue: "#578", name: "control_correlated_in_still_decorrelates",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n)`,
			want: []string{"n=int64:27"}},
		{issue: "#578", name: "control_uncorrelated_not_in_over_a_filter",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 3)`,
			want: []string{"n=int64:5"}},
	}
}

// ---------------------------------------------------------------------------
// #809's DAG half, and the silent shape beside it — an aggregate in a
// SUBQUERY's own WHERE.
//
// The placement rule is level-local and deliberately does not descend into a
// subquery: a subquery is its own level, and `WHERE h > (SELECT AVG(h) FROM
// t)` is ordinary SQL. What that left uncovered is the subquery's OWN level,
// which nothing else reaches when the planner takes the subquery APART rather
// than running it. A decorrelated IN builds its inner plan straight from the
// parsed subquery, so `SUM(b.w_i32) > 0` reached a Filter and the whole
// predicate answered every row — 10 of 10, in silence, on all four arms,
// where PostgreSQL 17 raises 42803. The EXISTS spelling is the same.
//
// #809's own shape is the loud half of the same gap: the single-process arm
// raised PostgreSQL's 42803 (its Runner plans the subquery, and the rule
// fires there) while the DAG arm failed in the worker's filter compiler with
// "subqueries require a SubqueryRunner" and no SQLSTATE at all.
func arcD5AggregatePlacementCells() []arcD5Cell {
	const pgSays = "42803 `aggregate functions are not allowed in WHERE`"
	return []arcD5Cell{
		{issue: "#809", name: "aggregate_in_a_decorrelated_not_in_subquery_where",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE SUM(b.w_i32) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE",
			pgSays:      pgSays + " — this answered 10 of 10 rows in silence"},
		{issue: "#809", name: "aggregate_in_a_decorrelated_in_subquery_where",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE SUM(b.w_i32) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE", pgSays: pgSays},
		{issue: "#809", name: "aggregate_in_a_decorrelated_exists_subquery_where",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM numwidth b WHERE SUM(b.w_i32) > 0 AND b.w_key = a.w_key)`,
			wantErrLike: "aggregate functions are not allowed in WHERE", pgSays: pgSays},
		{issue: "#809", name: "aggregate_in_a_scalar_subquery_where_is_loud_on_the_dag_too",
			sql: `SELECT g FROM gcov WHERE h > (` +
				`SELECT AVG(x.h) FROM gcov x WHERE SUM(x.h) > 0) GROUP BY g`,
			wantErrLike: "aggregate functions are not allowed in WHERE",
			pgSays:      pgSays + " — the DAG arm used to fail with no SQLSTATE at all"},
		// The two spellings that carry no relation qualifier. They were
		// equally silent before and are refused now: the rule asks whether an
		// aggregate names a relation the subquery does NOT provide, not
		// whether every reference is one of its own — see
		// logical.checkSubqueryAggregatePlacement for why that way round.
		{issue: "#809", name: "count_star_in_a_subquery_where",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE COUNT(*) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE", pgSays: pgSays},
		{issue: "#809", name: "unqualified_aggregate_in_a_subquery_where",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE SUM(w_i32) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE", pgSays: pgSays},
		// THE BOUNDARY, pinned rather than described. An aggregate inside a
		// subquery may belong to the OUTER level, and PostgreSQL ACCEPTS it —
		// this query answers one row there. This engine refuses it on every
		// arm, at this arc's base and at its tip alike: the subquery is
		// re-run standalone and the level-local rule fires at ITS level, with
		// no outer scope to say the aggregate is not its own. That is a
		// LOWERING gap, not a semantic decision, and the plan-time rule is
		// deliberately written so it does not reach this shape — nothing that
		// could one day answer is refused earlier because of it. The day it
		// answers `g=int32:1|n=int64:7`, this pin FAILS.
		{issue: "#809", name: "boundary_outer_level_aggregate_inside_a_subquery_is_refused",
			sql: `SELECT g, COUNT(*) AS n FROM typemx WHERE id < 50 GROUP BY g ` +
				`HAVING (SELECT MAX(d.k) FROM typemx_dim d WHERE d.k = SUM(typemx.g)) > 0 ORDER BY g`,
			wantErrLike:    "could not be executed",
			wantCorrRoutes: 1,
			pgSays:         "one row, g=1 n=7 — an aggregate of the OUTER level is legal inside a subquery"},
		// The controls: a subquery whose aggregate is in the SELECT list (the
		// ordinary shape the level-local scope exists for) and an ordinary
		// inner-only predicate. Both must keep answering, or the new descent
		// has refused legal SQL.
		{issue: "#809", name: "control_aggregate_in_a_subquery_select_list",
			sql:  `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i64 > (SELECT AVG(b.w_i64) FROM numwidth b)`,
			want: []string{"n=int64:1"}},
		{issue: "#809", name: "control_ordinary_inner_predicate",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 3)`,
			want: []string{"n=int64:3"}},
	}
}

// ---------------------------------------------------------------------------
// #601 — a failed IN-subquery is not an EMPTY set.
//
// `InSubquery.resolveSlow` set `emptySet = true` on a subquery ERROR as well
// as on a genuinely empty result, and #550/#571 had made `emptySet` decide
// NULL-keyed probe rows (`x IN (empty)` = FALSE, `x NOT IN (empty)` = TRUE,
// which is correct for a REAL empty set) — so a subquery that FAILED silently
// decided those rows where it previously returned UNKNOWN.
//
// The mechanism was fixed by v0.18.16's consumer half (844b502b): the run's
// error now raises `SubqueryRunFailedError` before `emptySet` is ever
// computed. It had no regression test of its own, which the issue asks for,
// and these are it — an IN and a NOT IN whose subquery cannot be run, over a
// column that holds NULLs, so the rows the conflation used to decide are in
// the fixture.
func arcD5FailedSubquerySetCells() []arcD5Cell {
	return []arcD5Cell{
		{issue: "#601", name: "failed_in_subquery_is_not_an_empty_set",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE SUM(b.w_i32) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE",
			pgSays: "42803 — and the rows this decided are numwidth's NULL-keyed ones, " +
				"which `x IN (empty)` would have answered FALSE for"},
		{issue: "#601", name: "failed_not_in_subquery_is_not_an_empty_set",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE SUM(b.w_i32) > 0)`,
			wantErrLike: "aggregate functions are not allowed in WHERE",
			pgSays:      "42803 — `x NOT IN (empty)` would have answered TRUE for every one of them"},
		// The control: a REAL empty set still decides those rows, which is
		// the behaviour #550/#571 added and this must not undo.
		{issue: "#601", name: "control_a_genuinely_empty_set_still_decides_null_keys",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 0)`,
			want:   []string{"n=int64:10"},
			pgSays: "10 — every row, NULL-keyed ones included, because the list is empty"},
	}
}

// ---------------------------------------------------------------------------
// #734 — a correlated subquery inside an AGGREGATE ARGUMENT.
//
// Cells are added by the commit that lowers the shape; until then the
// aggregate-argument family is covered by the arc-A census, which records it
// as LOUD (v0.18.16's consumer half) with PostgreSQL's answers beside it.
func arcD5AggregateArgumentCells() []arcD5Cell { return nil }

func TestArcD5CorrelationMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range arcD5Cells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			wantOnDAG := want
			if tc.wantDAG != nil {
				wantOnDAG = append([]string(nil), tc.wantDAG...)
				sort.Strings(wantOnDAG)
			}
			check := func(arm string, got []string, err error) {
				t.Helper()
				want := want
				wantErr := tc.wantErrLike
				if strings.HasPrefix(arm, "dag") {
					want = wantOnDAG
					if tc.wantErrLikeDAG != "" {
						wantErr = tc.wantErrLikeDAG
					}
				}
				if wantErr != "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, but this shape must be LOUD\n"+
							"  want an error containing %q\n  PostgreSQL 17: %s\n  SQL: %s",
							arm, got, wantErr, tc.pgSays, tc.sql)
						return
					}
					if !strings.Contains(err.Error(), wantErr) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm, err, wantErr, tc.sql)
					}
					return
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17: %v", arm, err, tc.sql, want)
					return
				}
				if len(got) != len(want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm, len(got), len(want), got, want, tc.sql)
					return
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm, i, got[i], want[i], tc.sql)
						return
					}
				}
			}

			sgot, serr := na2Run(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)

			// FIVE runs on the budgeted arm: a spill is a condition, not a
			// query shape (ADR-0027 §5), so which batch crosses the budget
			// moves between runs and one passing run proves nothing.
			for i := 0; i < 5; i++ {
				got, err := na2Run(tmdRunSingle(ctx, spilled, tc.sql))
				check("spilled", got, err)
				if t.Failed() {
					break
				}
			}

			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := arcD5Routes(arm.c)
				got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				// Rule 11: the counters travel beside the rows, and ALL of
				// them are read on EVERY cell — a right-to-routed move is
				// exactly what a row assertion cannot see.
				for i, d := range arcD5RouteDelta(before, arcD5Routes(arm.c)) {
					wantRoute := [4]int64{
						tc.wantCorrRoutes, tc.wantScalarProjRoutes,
						tc.wantInSubqueryRoutes, tc.wantUnreachableRoutes,
					}[i]
					if d != wantRoute {
						t.Errorf("%s arm: %s moved by %d, want %d\n"+
							"  (0 = the DAG executed this shape; 1 = it refused the plan and "+
							"the coordinator-local pipeline answered)\n  SQL: %s",
							arm.name, arcD5RouteNames[i], d, wantRoute, tc.sql)
					}
				}
			}
		})
	}
}

var arcD5RouteNames = [4]string{
	"CorrelatedLocalRoutes", "ScalarProjectionLocalRoutes",
	"InSubqueryLocalRoutes", "UnreachableOutputLocalRoutes",
}

func arcD5Routes(c *Coordinator) [4]int64 {
	return [4]int64{
		c.CorrelatedLocalRoutes(), c.ScalarProjectionLocalRoutes(),
		c.InSubqueryLocalRoutes(), c.UnreachableOutputLocalRoutes(),
	}
}

func arcD5RouteDelta(before, after [4]int64) [4]int64 {
	var d [4]int64
	for i := range d {
		d[i] = after[i] - before[i]
	}
	return d
}
