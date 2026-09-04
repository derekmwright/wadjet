package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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
// Every cell runs all four. The 18 GENERATED per-type rendering cells are the
// one exception and they say so in the field that does it (skipBudgetedArm),
// with the measurement behind the choice: a per-row re-run under a budget
// spends seconds per row in forced-reservation backoff, and the rendering
// those cells gate is decided before any budget exists.
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
	// wantErrLikeSpilled, when set, is what the BUDGETED arm alone must fail
	// with. A spill is a CONDITION and not a shape (ADR-0027), so a defect
	// that only the spilled path reaches is a real state the census has to be
	// able to say — otherwise the shape has to be dropped, which is how one
	// goes unrecorded.
	wantErrLikeSpilled string
	// skipBudgetedArm drops the 512 KiB arm for this cell, and only the 18
	// generated per-type rendering cells set it. The reason is measured, not
	// assumed, and it is the same fact this arc is about:
	//
	// a correlated subquery the decorrelator cannot express is RE-RUN ONCE PER
	// OUTER ROW, and each re-run scans the inner relation again — measured,
	// 2N+1 reads of the inner file for N outer rows against a flat 3 for the
	// spelling that decorrelates. Under this arm's 512 KiB budget each of
	// those loads reserves more than the WHOLE budget, which no relief can
	// admit, and memory.ReserveOrForce waits out its full two seconds before
	// forcing anyway. So the arm costs two seconds per outer row: 295 forced
	// reservations in ten minutes here, one cell per five minutes, against
	// 0.71 s for the same cell with no budget. Both halves are pinned in
	// TestCorrelatedRerunReadsTheInnerOncePerOuterRow and its budgeted
	// sibling, with the numbers; neither is a leak — `used` is flat.
	//
	// What that arm would add here is nothing: the literal these cells gate is
	// built in expr.readOuterValues from the VECTOR's own TypeID, before any
	// operator or budget sees it (expr.TestOuterLiteralRendersEveryTypeAsItsOwn
	// Type covers all 22 types with no engine at all), and the two DAG arms
	// ROUTE this shape to the coordinator-local pipeline — asserted, corr+1 —
	// so they already ARE the single-process engine. The five hand-written
	// #679 cells below, over the ten-row numwidth fixture, keep all four arms.
	//
	// The degradation itself is a finding and is reported as such; it belongs
	// to the re-run's memory lifetime, not to the rendering.
	skipBudgetedArm bool
	// wantDAG, when set, is what the two DAG arms must answer INSTEAD of
	// `want`.
	wantDAG []string
	// The per-DAG-arm counter deltas. 0 = the DAG executed the shape.
	wantCorrRoutes        int64
	wantScalarProjRoutes  int64
	wantInSubqueryRoutes  int64
	wantUnreachableRoutes int64
	// wantSQLState, when set, is the SQLSTATE every arm's error must carry.
	// A documented refusal is a promise about the CODE as much as the text,
	// and the code is what a client branches on.
	wantSQLState string
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
		arcD5LateralCells(),
		arcD5AggregatePlacementCells(),
		arcD5FailedSubquerySetCells(),
		arcD5MeasuredCells(),
		arcD5DerivedInnerCells(),
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
		// A CTE on the INNER side is now BUILT into the semi join's build
		// side rather than declined, so both DAG arms execute it (#852).
		{issue: "#535", name: "control_cte_on_the_inner_side",
			sql: `WITH d AS (SELECT k FROM typemx_dim) SELECT COUNT(*) AS c FROM typemx a ` +
				`WHERE a.id < 50 AND EXISTS (SELECT 1 FROM d WHERE d.k = a.g)`,
			want: []string{"c=int64:47"}},
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
		// THE SAME BOUNDARY WITH A CTE ON BOTH SIDES, which is what says the
		// boundary is "the OUTER reference is UNALIASED" and not "there is no
		// CTE". The outer CTE reference here is bare (`FROM u`), so `u.did` in
		// the subquery names a scope the INNER `u` answers to as well and no
		// collector can tell them apart: silent 0 for PostgreSQL's 47. One
		// alias later — the control below — it answers 47 on every arm, which
		// is what makes this the boundary of a NAME and not of the CTE fix.
		{issue: "#535", name: "boundary_cte_on_both_sides_outer_unaliased_stays_silent",
			sql: `WITH u AS (SELECT g AS did, id FROM typemx WHERE id < 50) ` +
				`SELECT COUNT(*) AS n FROM u WHERE EXISTS (` +
				`SELECT 1 FROM u b WHERE b.did = u.did AND b.id <> u.id)`,
			want:           []string{"n=int64:0"},
			wantErrLikeDAG: "SubqueryRunner",
			pgSays:         "47"},
		{issue: "#535", name: "control_cte_on_both_sides_outer_aliased",
			sql: `WITH u AS (SELECT g AS did, id FROM typemx WHERE id < 50) ` +
				`SELECT COUNT(*) AS n FROM u a WHERE EXISTS (` +
				`SELECT 1 FROM u b WHERE b.did = a.did AND b.id <> a.id)`,
			want: []string{"n=int64:47"}},
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
// the OUTER value of a correlated EXISTS, each against PostgreSQL's own answer
// over the same rows. A rendering that re-types a value answers a different
// number, and the counts differ per type — 14, 15, 19, 29, 30, five distinct
// answers — so no single wrong answer passes them all.
//
// THE POSITION MOVED AND THE NUMBERS DID NOT (#852). These cells used to reach
// the re-run through a DERIVED-TABLE inner, which was the decline that kept
// the shape off the join path. A derived-table inner now LOWERS, so that
// spelling no longer re-runs anything and the gate would have been silently
// testing the join's comparison kernels instead of the literal renderer. They
// reach it through the position that is still not a decorrelation site — an
// aggregate ARGUMENT (deferral D1) — with the SAME predicate over the SAME
// rows, so every expected count is unchanged and each was re-measured on all
// four arms. `SUM(CASE WHEN <pred> THEN 1 ELSE 0 END)` over the rows a
// `COUNT(*) … WHERE <pred>` counted is the same number by construction.
func arcD5TypedRerunCells() []arcD5Cell {
	// PostgreSQL 17 over the type-matrix fixture, measured live.
	//
	// The ranges are BOUNDED on purpose. A re-run happens PER OUTER ROW, so
	// the work is (outer rows × inner rows) per execution and there are eight
	// executions per cell (one single, five spilled, two DAG). Over the whole
	// 5000-row table that is 43 million row reads across this group and the
	// census exceeded its own 30-minute context. 30 × 585 keeps the per-type
	// spread that makes a wrong rendering visible — 14, 15, 19, 29, 30, four
	// distinct answers and c_i32 alone at 14 — at a seventeenth of the cost.
	want := map[string]int{
		"c_bool": 29, "c_i32": 14, "c_i64": 15, "c_f32": 15, "c_f64": 15,
		"c_str": 15, "c_bytes": 15, "c_ts": 15, "c_ipv4": 15, "c_ipv6": 15,
		"c_cidr": 19, "c_mac": 15, "c_port": 15, "c_proto": 30, "c_dur": 15,
		"c_uuid": 15, "c_date": 30, "c_dec": 15,
	}
	cols := make([]string, 0, len(want))
	for c := range want {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	out := make([]arcD5Cell, 0, len(cols)+9)
	for _, c := range cols {
		out = append(out, arcD5Cell{
			issue: "#679", name: "rerun_renders_" + c,
			sql: fmt.Sprintf(`SELECT SUM(CASE WHEN EXISTS (`+
				`SELECT 1 FROM (SELECT %s AS k FROM typemx WHERE id >= 15 AND id < 600 `+
				`GROUP BY %s) b WHERE a.%s = b.k) THEN 1 ELSE 0 END) AS n `+
				`FROM typemx a WHERE a.id < 30`, c, c, c),
			want:            []string{fmt.Sprintf("n=int64:%d", want[c])},
			wantCorrRoutes:  1,
			skipBudgetedArm: true,
		})
	}
	// The issue's own shape and its NOT EXISTS twin: a DECIMAL outer value
	// against a BIGINT inner. In the aggregate-argument position, which is
	// where the RE-RUN — and so the literal rendering this issue is about —
	// still happens.
	out = append(out,
		arcD5Cell{issue: "#679", name: "rerun_renders_a_decimal_outer_cross_width",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM (` +
				`SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k) ` +
				`THEN 1 ELSE 0 END) AS n FROM numwidth a`,
			want: []string{"n=int64:3"}, wantCorrRoutes: 1},
		arcD5Cell{issue: "#679", name: "rerun_renders_a_decimal_outer_cross_width_negated",
			sql: `SELECT SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM (` +
				`SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k) ` +
				`THEN 1 ELSE 0 END) AS n FROM numwidth a`,
			want: []string{"n=int64:7"}, wantCorrRoutes: 1},

		// The same four shapes in a WHERE, which now LOWER (#852). They are
		// kept because they still gate the cross-width comparison — the same
		// DECIMAL-against-BIGINT question, asked of the JOIN's kernel instead
		// of the literal renderer — and because the ROUTE is the fix's proof:
		// zero, where every one of them cost one before.
		arcD5Cell{issue: "#679", name: "exists_over_a_derived_table_cross_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:3"}},
		arcD5Cell{issue: "#679", name: "not_exists_over_a_derived_table_cross_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE NOT EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:7"}},
		// The three controls that answered at base: three of the four width
		// pairs were already right, which is what would have let a wrong fix
		// pass. The trigger is a DECIMAL outer against an INTEGER inner over
		// a derived table, not "a correlated EXISTS".
		arcD5Cell{issue: "#679", name: "control_derived_table_same_width",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_i64 AS k FROM numwidth GROUP BY w_i64) b WHERE a.w_i64 = b.k)`,
			want: []string{"n=int64:9"}},
		arcD5Cell{issue: "#679", name: "control_derived_table_decimal_decimal",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT w_d4 AS k FROM numwidth GROUP BY w_d4) b WHERE a.w_d2 = b.k)`,
			want: []string{"n=int64:7"}},
		// THE DOCUMENTED REFUSAL, end to end. `docs/sql-reference.md` and
		// ADR-0021 §1e promise 0A000 for an outer value with no literal
		// spelling; a promise about a SQLSTATE that no query reaches is not a
		// promise. The container columns live in typemx_nested.
		//
		// These two used to be spelled as a WHERE-clause EXISTS over a derived
		// table. That spelling now LOWERS, so it never renders a literal and
		// never reaches the refusal — the pair below it records what it
		// answers instead. The promise is kept in the position that still
		// re-runs.
		arcD5Cell{issue: "#679", name: "container_outer_value_is_refused_with_0A000",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM typemx_nested b ` +
				`WHERE b.c_arr = a.c_arr) THEN 1 ELSE 0 END) AS n ` +
				`FROM typemx_nested a WHERE a.id < 5`,
			wantErrLike:    "has no literal spelling that reads back as the same value",
			wantSQLState:   "0A000",
			wantCorrRoutes: 1,
			pgSays:         "PostgreSQL compares arrays and answers; this engine has no literal for one"},
		arcD5Cell{issue: "#679", name: "vector_outer_value_is_refused_with_0A000",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM typemx_nested b ` +
				`WHERE b.c_vec = a.c_vec) THEN 1 ELSE 0 END) AS n ` +
				`FROM typemx_nested a WHERE a.id < 5`,
			wantErrLike:    "has no literal spelling that reads back as the same value",
			wantSQLState:   "0A000",
			wantCorrRoutes: 1,
			pgSays:         "the refusal is this engine's own; PostgreSQL has no VECTOR type"},
		// And what the LOWERED spelling of those two answers, which is the
		// other half of the same finding: a container correlation that never
		// renders a literal needs no literal, so it is not refused — it is
		// ANSWERED, and the answer is PostgreSQL's. The ARRAY cell was
		// measured against live PostgreSQL 17 over these same five rows (4);
		// VECTOR has no PostgreSQL type, so its 3 is this engine's own and is
		// derived from the fixture: c_vec is [i, i+0.5, -i, 0.25], distinct
		// per row, so ids 0, 1 and 2 match and ids 3 and 4 do not.
		arcD5Cell{issue: "#679", name: "container_correlation_lowers_and_answers",
			sql: `SELECT COUNT(*) AS n FROM typemx_nested a WHERE a.id < 5 AND EXISTS (` +
				`SELECT 1 FROM (SELECT c_arr AS k, id FROM typemx_nested WHERE id < 3) b ` +
				`WHERE b.k = a.c_arr)`,
			want:   []string{"n=int64:4"},
			pgSays: "4 — an empty array equals an empty array, so a.id 0 and 3 both match b.id 0"},
		arcD5Cell{issue: "#679", name: "vector_correlation_lowers_and_answers",
			sql: `SELECT COUNT(*) AS n FROM typemx_nested a WHERE a.id < 5 AND EXISTS (` +
				`SELECT 1 FROM (SELECT c_vec AS k, id FROM typemx_nested WHERE id < 3) b ` +
				`WHERE b.k = a.c_vec)`,
			want:   []string{"n=int64:3"},
			pgSays: "no VECTOR type; 3 is the fixture's own arithmetic"},
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
// The lowering is now DECLINED (logical.tryDecorrelateInSubquery's closing
// comment says why an anti join cannot carry the rule and what would be needed
// to make one that could), so the predicate stays a subquery and
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
// #767 part 1 — an aggregated LATERAL keeps the outer row it matches nothing
// for.
//
// PostgreSQL evaluates a LATERAL subquery ONCE PER OUTER ROW, and an UNGROUPED
// aggregate over an empty input still yields one row — so an outer row the
// lateral matches nothing for SURVIVES, with COUNT reading 0 and every other
// aggregate NULL. `buildLateralSubquery` decorrelates by injecting the
// correlated column into the subquery's GROUP BY, which turns "one row per
// outer row" into "one row per group that EXISTS": an INNER join then dropped
// that row (2 for PostgreSQL's 3, in silence) and the LEFT spelling kept it at
// COUNT = NULL, which is a different wrong answer to the same question.
//
// A subquery the QUERY grouped is untouched and is the control: `GROUP BY x`
// over an empty input yields no row in PostgreSQL either.
func arcD5LateralCells() []arcD5Cell {
	const dagCarrier = "does not exist in the input schema"
	const lat = `JOIN LATERAL (SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s `
	const left = `LEFT ` + lat
	return []arcD5Cell{
		// --- ON true / no ON: the lateral yields a row for every outer row,
		// and nothing filters it. Both spellings answer the same.
		{issue: "#767", name: "inner_lateral_ungrouped_count_keeps_the_unmatched_row",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "left_lateral_ungrouped_count_reads_zero_not_null",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + left +
				`ON true ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},

		// --- A LATER RIGHT OR FULL JOIN NULL-EXTENDS THE LATERAL'S COLUMNS,
		// and that is where BOTH halves of the repair stop being entitled to
		// speak. The COALESCE and the moved ON live in the ENCLOSING query,
		// so they see the whole FROM clause's result; what they are about is
		// the LATERAL's own output. A RIGHT or FULL join to the right of the
		// lateral is exactly what separates those two relations — it
		// MANUFACTURES rows whose `s.n` is NULL, and neither rewrite can tell
		// one of those from a row the lateral produced.
		//
		// Measured before the bound was added, against these same inputs:
		// the moved `ON s.n > 1` DELETED the manufactured row (2 rows for
		// PostgreSQL's 3), and `ON true` printed `n=0` in it where PostgreSQL
		// prints NULL. Both were RIGHT at fd679ae9 — the same right-to-wrong
		// class as the review's B1, found by asking where else the rewrite's
		// scope and its warrant come apart.
		//
		// So `lateralEmptyInputPlan` declines entirely when a later join can
		// null-extend, and these four cells are what says the decline holds.
		// The join is `c2.id = o.id AND c2.id < 3`, so `lat_ord`'s row 3 has
		// no partner and is the manufactured row in every one of them.
		//
		// READ THESE FOUR WITH THE THREE AFTER THEM. The `AND c2.id < 3` is
		// what makes PostgreSQL drop Carol too, so these cells show the
		// decline being SAFE and say nothing about what it COSTS. The plain
		// spelling — `ON c2.id = o.id`, every outer row matching — is where
		// the decline is visible as a divergence, and it is pinned below.
		{issue: "#767", name: "boundary_right_join_after_lateral_on_true_keeps_null_not_zero",
			sql: `SELECT o.customer AS c, s.n AS n, c2.id AS cid FROM lat_ord o ` + lat +
				`ON true RIGHT JOIN lat_ord c2 ON c2.id = o.id AND c2.id < 3 ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=int64:1", "c=Bob|n=int64:2|cid=int64:2",
				"c=NULL|n=NULL|cid=int64:3"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "boundary_right_join_after_lateral_with_on_keeps_the_row",
			sql: `SELECT o.customer AS c, s.n AS n, c2.id AS cid FROM lat_ord o ` + lat +
				`ON s.n > 1 RIGHT JOIN lat_ord c2 ON c2.id = o.id AND c2.id < 3 ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=int64:1", "c=Bob|n=int64:2|cid=int64:2",
				"c=NULL|n=NULL|cid=int64:3"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "boundary_full_join_after_lateral_with_on_agrees",
			sql: `SELECT o.customer AS c, s.n AS n, c2.id AS cid FROM lat_ord o ` + lat +
				`ON s.n > 1 FULL JOIN lat_ord c2 ON c2.id = o.id AND c2.id < 3 ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=int64:1", "c=Bob|n=int64:2|cid=int64:2",
				"c=NULL|n=NULL|cid=int64:3"},
			wantUnreachableRoutes: 1},
		// THE COST OF THE DECLINE, pinned rather than described: with a FULL
		// join after it, the ungrouped empty-input row is NOT restored, so
		// Carol's `[Carol, 0, NULL]` is missing and this answers 3 for
		// PostgreSQL's 4. That is what fd679ae9 answered too — the decline
		// keeps the old answer rather than trading it for a different wrong
		// one. Restoring it needs the default applied at the LATERAL's own
		// output, before the later join sees it, which is a plan-level change
		// and not a SelectInfo rewrite (report deferral D5).
		{issue: "#767", name: "boundary_full_join_after_lateral_loses_the_empty_input_row",
			sql: `SELECT o.customer AS c, s.n AS n, c2.id AS cid FROM lat_ord o ` + lat +
				`ON true FULL JOIN lat_ord c2 ON c2.id = o.id AND c2.id < 3 ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=int64:1", "c=Bob|n=int64:2|cid=int64:2",
				"c=NULL|n=NULL|cid=int64:3"},
			wantUnreachableRoutes: 1,
			pgSays:                "four rows — Carol's defaulted [Carol, 0, NULL] as well"},
		// WHAT THE DECLINE COSTS, in the spelling that shows it. With every
		// outer row matching, PostgreSQL's `Carol|0|Carol` is a row the
		// lateral's empty-input default produced and the null-extending join
		// then kept; declining the repair means Carol's lateral columns are
		// never defaulted, so the pair reads NULL and the row survives with
		// the wrong values rather than being dropped. Same cardinality, one
		// row wrong, NO error and NO route — a silent divergence, which is
		// why it is pinned as filed rather than described in a comment.
		//
		// The day either of these answers PostgreSQL's row, the cell FAILS
		// and the decline has been closed — which is D10's mechanism: the
		// default applied at the LATERAL's own output, before any later join
		// sees it.
		{issue: "#767", name: "boundary_right_join_after_lateral_plain_spelling_loses_the_default",
			sql: `SELECT o.customer AS c, s.n AS n, c2.customer AS cid FROM lat_ord o ` + lat +
				`ON true RIGHT JOIN lat_ord c2 ON c2.id = o.id ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=Alice", "c=Bob|n=int64:2|cid=Bob",
				"c=NULL|n=NULL|cid=Carol"},
			wantUnreachableRoutes: 1,
			pgSays:                "Alice|2|Alice, Bob|2|Bob, Carol|0|Carol — only Carol's pair diverges"},
		{issue: "#767", name: "boundary_full_join_after_lateral_plain_spelling_loses_the_default",
			sql: `SELECT o.customer AS c, s.n AS n, c2.customer AS cid FROM lat_ord o ` + lat +
				`ON true FULL JOIN lat_ord c2 ON c2.id = o.id ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=Alice", "c=Bob|n=int64:2|cid=Bob",
				"c=NULL|n=NULL|cid=Carol"},
			wantUnreachableRoutes: 1,
			pgSays:                "Alice|2|Alice, Bob|2|Bob, Carol|0|Carol — the FULL spelling diverges the same way"},
		// AND THE ONE THAT AGREES, which is what bounds the cost to the
		// DEFAULT row. Round 1's review reported this spelling as diverging
		// too; measured against live PostgreSQL 17 it does not. With
		// `ON s.n > 1` the lateral row Carol would have had is rejected by
		// the lateral join's own condition, so PostgreSQL null-extends the
		// pair exactly as the un-repaired plan does and both read
		// `NULL|NULL|Carol`. The decline costs the DEFAULT row and nothing
		// else — a claim this cell makes and the two above bound.
		{issue: "#767", name: "control_right_join_after_lateral_with_an_on_agrees",
			sql: `SELECT o.customer AS c, s.n AS n, c2.customer AS cid FROM lat_ord o ` + lat +
				`ON s.n > 1 RIGHT JOIN lat_ord c2 ON c2.id = o.id ORDER BY 3`,
			want: []string{"c=Alice|n=int64:2|cid=Alice", "c=Bob|n=int64:2|cid=Bob",
				"c=NULL|n=NULL|cid=Carol"},
			wantUnreachableRoutes: 1},

		// WHERE THE DEFAULT STOPS: inside a SUBQUERY or an EXISTS.
		//
		// The walk that applies the default covers every plansql node that can
		// hold a column reference, and `SubqueryNode` / `ExistsNode` are the
		// two it does not enter — they carry SQL TEXT rather than a tree.
		// ADR-0021 §1h used to justify that by saying a lateral output is not
		// in their scope. PostgreSQL refutes it: a subquery in the enclosing
		// query CAN name the lateral's output, PostgreSQL resolves it, and it
		// applies the empty-input default there like anywhere else.
		//
		// Here the outer row's `s.n` is substituted into the subquery's text
		// per row by the re-run (§1e), and on the padded row it substitutes
		// the LEFT join's NULL rather than 0 — so `amount > NULL` matches
		// nothing and Carol's answer collapses. Not a regression: fd679ae9
		// answers the same. Silent, on all four arms, which is why both are
		// pinned rather than described.
		//
		// Closing it is not a bigger walk. The reference lives in TEXT, so
		// reaching it means parsing the subquery, rewriting the tree and
		// rendering it back — and the value that needs defaulting is an OUTER
		// value substituted by the re-run, which is the correlation model's
		// next layer rather than this rewrite's (report deferral D11). The day
		// either cell answers PostgreSQL's row it FAILS, and that is the day
		// the layer landed.
		//
		// The counters say the same thing from the other side: both route
		// with CorrelatedLocalRoutes rather than UnreachableOutputLocalRoutes
		// — the DAG declines these for the CORRELATED SUBQUERY in them, not
		// for the lateral's projection, which is exactly the layer that owns
		// the defect.
		{issue: "#767", name: "boundary_scalar_subquery_reads_the_pad_not_the_default",
			sql: `SELECT o.customer AS c, ` +
				`(SELECT COUNT(*) FROM lat_item i WHERE i.amount > s.n * 40) AS k ` +
				`FROM lat_ord o ` + lat + `ON true ORDER BY 1`,
			want:           []string{"c=Alice|k=2", "c=Bob|k=2", "c=Carol|k=0"},
			wantCorrRoutes: 1,
			pgSays:         "Alice 2, Bob 2, Carol 4 — Carol's s.n is 0 there, not NULL"},
		{issue: "#767", name: "boundary_exists_reads_the_pad_and_drops_the_row",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true WHERE EXISTS (` +
				`SELECT 1 FROM lat_item i WHERE i.amount > s.n * 40) ORDER BY 1`,
			want:           []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2"},
			wantCorrRoutes: 1,
			pgSays:         "three rows — Carol survives at 0, because 0 * 40 admits every amount"},

		// A WINDOW over a defaulted COUNT: a third query shape reaching the
		// SAME carrier defect the `SELECT *` cell pins (report deferral D4).
		// The single-process arm answers PostgreSQL's running total over all
		// three rows — Carol's defaulted 0 contributing nothing to it, which
		// is the repair working — and both DAG arms fail in the shuffle with
		// ADR-0010's name-consistency check, because the join stage carries
		// `s.id` where an earlier file of the same stage input named the
		// column `order_id`.
		//
		// Three distinct shapes now reach one message, which is what says the
		// defect is the stage model's carried-column derivation and not any
		// of the three spellings. Wrong-to-loud is the allowed direction: at
		// fd679ae9 this answered two rows, dropping Carol.
		//
		// The VALUE is PostgreSQL's; the TYPE is not. `SUM` over a BIGINT is
		// `numeric` in PostgreSQL and float HERE — while the very same sum
		// over the very same column, spelled as a GROUP BY aggregate rather
		// than a window, now comes back DECIMAL and agrees (see
		// `lateral_count_default_reaches_an_aggregate_argument` above). One
		// number, two spellings, two boxes: that is ADR-0024's rung, not this
		// repair's doing, and it has no lateral in its own repro.
		{issue: "#767", name: "window_over_the_default_reaches_the_dag_carrier_defect",
			sql: `SELECT o.customer AS c, SUM(s.n) OVER (ORDER BY o.customer) AS running ` +
				`FROM lat_ord o ` + lat + `ON true ORDER BY 1`,
			want: []string{"c=Alice|running=float:2", "c=Bob|running=float:4",
				"c=Carol|running=float:4"},
			wantErrLikeDAG: `names column 2 "s.id"`,
			pgSays:         "2, 4, 4 as NUMERIC — the values agree, the box does not (ADR-0024)"},

		// The control that says the decline is CONDITIONAL on null-extension
		// and not on "any join after the lateral": a LEFT join cannot null-
		// extend what is to its left, so the repair still applies and Carol
		// still reads 0.
		{issue: "#767", name: "control_left_join_after_lateral_still_defaults",
			sql: `SELECT o.customer AS c, s.n AS n, c2.id AS cid FROM lat_ord o ` + lat +
				`ON true LEFT JOIN lat_ord c2 ON c2.id = o.id AND c2.id < 3 ORDER BY 1`,
			want: []string{"c=Alice|n=int64:2|cid=int64:1", "c=Bob|n=int64:2|cid=int64:2",
				"c=Carol|n=int64:0|cid=NULL"},
			wantUnreachableRoutes: 1},

		// --- THE `ON` MATRIX. The review found six PostgreSQL-correct answers
		// turned wrong by a repair that forced LEFT and defaulted the COUNT
		// without reading the join's own condition: `ON s.n > 5` answered
		// three rows for PostgreSQL's none, printing 0 where the counts were
		// 2. PostgreSQL evaluates the lateral per outer row and THEN applies
		// the ON, so the ON has to keep deciding — see
		// logical.lateralEmptyInputPlan for the three cases.
		{issue: "#767", name: "inner_on_over_the_lateral_aggregate",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON s.n > 1 ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "inner_on_no_row_can_satisfy",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON s.n > 5 ORDER BY o.customer`,
			want:                  []string{},
			wantUnreachableRoutes: 1,
			pgSays:                "no rows — and the repair printed three, with n=0 where the counts are 2"},
		// The one the DECLINE alone would still get wrong, and the reason
		// this is a pad-then-filter and not a decline: the DEFAULT row PASSES
		// this ON, so PostgreSQL keeps the unmatched outer row.
		{issue: "#767", name: "inner_on_the_default_row_satisfies",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON s.n = 0 ORDER BY o.customer`,
			want:                  []string{"c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1,
			pgSays:                "Carol at 0 — the lateral's own empty-input row, kept by an ON it satisfies"},
		{issue: "#767", name: "inner_on_names_only_the_outer_row",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON o.total > 100 ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "inner_on_constant_false",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON 1 = 0 ORDER BY o.customer`,
			want:                  []string{},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "inner_on_over_a_non_count_aggregate",
			sql: `SELECT o.customer AS c, s.mn AS mn FROM lat_ord o JOIN LATERAL (` +
				`SELECT MIN(amount) AS mn FROM lat_item WHERE order_id = o.id) s ` +
				`ON s.mn > 60 ORDER BY o.customer`,
			want:                  []string{"c=Bob|mn=float:75"},
			wantUnreachableRoutes: 1,
			pgSays:                "Bob 75 — MIN of nothing is NULL and NULL > 60 is UNKNOWN, so Carol is dropped"},
		{issue: "#767", name: "inner_on_over_two_outputs",
			sql: `SELECT o.customer AS c, s.n AS n, s.t AS t FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n, SUM(amount) AS t FROM lat_item WHERE order_id = o.id) s ` +
				`ON s.n >= 0 ORDER BY o.customer`,
			want: []string{
				"c=Alice|n=int64:2|t=float:150", "c=Bob|n=int64:2|t=float:200",
				"c=Carol|n=int64:0|t=NULL"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "left_on_over_the_lateral_aggregate",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + left +
				`ON s.n > 1 ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=NULL"},
			wantUnreachableRoutes: 1,
			pgSays:                "Carol NULL — an OUTER join pads the pair its ON rejects"},
		// THE BOUNDARY of the repair, pinned rather than described. An OUTER
		// join whose ON the DEFAULT row would satisfy needs the lateral's
		// columns NULLED per column for the pairs the ON rejects and KEPT for
		// the one it accepts — a CASE per output over a schema this pass does
		// not have — so the join is left exactly as written and Carol reads
		// NULL where PostgreSQL reads 0. Alice and Bob are right, which is
		// what says this is the one cell and not the class.
		{issue: "#767", name: "boundary_left_on_the_default_row_satisfies_reads_null",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + left +
				`ON s.n = 0 ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=NULL", "c=Bob|n=NULL", "c=Carol|n=NULL"},
			wantUnreachableRoutes: 1,
			pgSays:                "Alice NULL, Bob NULL, Carol 0 — only Carol's cell diverges"},
		// A subquery the QUERY grouped keeps the ordinary rule, ON and all.
		{issue: "#767", name: "control_user_grouped_lateral_with_an_on",
			sql: `SELECT o.customer AS c, s.n2 AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT order_id, COUNT(*) AS n2 FROM lat_item WHERE order_id = o.id ` +
				`GROUP BY order_id) s ON s.n2 > 1 ORDER BY o.customer`,
			want: []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2"}},

		// --- WHERE / HAVING / aggregate-argument / IN: every position the
		// default has to reach. The walker's missing `InExpr` arm dropped the
		// unmatched row from `WHERE s.n IN (0, 2)` in silence, and the
		// aggregate-argument fields were not rewritten at all, so `SUM(s.n)`
		// read the pad — NULL on the single-process arm and a hard
		// `column "s.n" does not exist in the input schema` on both DAG arms.
		{issue: "#767", name: "lateral_count_default_reaches_the_where_clause",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true WHERE s.n = 0 ORDER BY o.customer`,
			want:                  []string{"c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "lateral_count_default_reaches_an_in_list",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true WHERE s.n IN (0, 2) ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "lateral_count_default_reaches_a_between",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true WHERE s.n BETWEEN 0 AND 1 ORDER BY o.customer`,
			want:                  []string{"c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		// The BOX here is exact, and it changed under this branch at the
		// landing rebase: `SUM` over a BIGINT used to come back int64 and now
		// comes back a DECIMAL, which is what PostgreSQL declares
		// (`pg_typeof(SUM(s.n))` = `numeric`). That is main's numeric work
		// arriving, not this arc's, and it moves the cell TOWARD PostgreSQL —
		// so the expectation follows the engine rather than pinning the box
		// this arc happened to be written against.
		//
		// Worth reading beside `window_over_the_default_reaches_the_dag_
		// carrier_defect` below, which is the SAME sum over the SAME column in
		// a window frame and still comes back FLOAT. One spelling is now
		// PostgreSQL's numeric and the other is not: an asymmetry inside
		// ADR-0024's rung that these two cells now hold still, and neither of
		// them is a correlation defect.
		{issue: "#767", name: "lateral_count_default_reaches_an_aggregate_argument",
			sql: `SELECT o.customer AS c, SUM(s.n) AS t FROM lat_ord o ` + lat +
				`ON true GROUP BY o.customer HAVING SUM(s.n) >= 0 ORDER BY c`,
			want: []string{"c=Alice|t=2", "c=Bob|t=2", "c=Carol|t=0"},
			pgSays: "Alice 2, Bob 2, Carol 0 as NUMERIC — the values and now the box too. " +
				"Carol read NULL on the single-process arm and FAILED on both DAG arms " +
				"until the aggregate-argument fields were rewritten"},
		{issue: "#767", name: "lateral_count_default_reaches_an_order_by",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o ` + lat +
				`ON true ORDER BY s.n, o.customer`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		// ARITHMETIC over the default. The VALUES are PostgreSQL's; the BOX is
		// not, and the divergence is not this repair's — it is the
		// polymorphic-declaration rung, reproducible with no lateral in
		// sight: `SELECT COALESCE(o.id, 0) + 1 FROM lat_ord o` comes back
		// float64 here and bigint in PostgreSQL, while `COALESCE(o.id, 0)`
		// and `o.id + 1` each stay int64. The default this repair substitutes
		// is a COALESCE, so it puts that rung under any expression written
		// over a lateral COUNT. Pinned with the box it gives; the day the
		// rung is fixed this fails and becomes int64.
		{issue: "#767", name: "boundary_arithmetic_over_the_default_is_float_boxed",
			sql: `SELECT o.customer AS c, s.n + 1 AS n FROM lat_ord o ` + lat +
				`ON true ORDER BY o.customer`,
			want:                  []string{"c=Alice|n=float:3", "c=Bob|n=float:3", "c=Carol|n=float:1"},
			wantUnreachableRoutes: 1,
			pgSays:                "3, 3, 1 as BIGINT — the values agree, the declared type does not"},

		// THE `SELECT *` BOUNDARY, pinned for real this time. A star expands
		// in a later pass over the plan's own schema, so there is nothing in
		// the SelectInfo for the rewrite to reach and the padded COUNT reads
		// NULL where PostgreSQL reads 0 — and the decorrelation's injected
		// join key (`order_id`) is in the star's output, which PostgreSQL's
		// is not. On the DAG the shape is LOUD, and that is a shuffle
		// name-consistency refusal (ADR-0010) rather than a value.
		{issue: "#767", name: "boundary_select_star_over_an_aggregated_lateral",
			sql: `SELECT * FROM lat_ord o ` + lat + `ON true ORDER BY o.customer`,
			want: []string{
				"id=int64:1|customer=Alice|total=float:150|order_id=int64:1|n=int64:2",
				"id=int64:2|customer=Bob|total=float:200|order_id=int64:2|n=int64:2",
				"id=int64:3|customer=Carol|total=float:0|order_id=NULL|n=NULL"},
			wantErrLikeDAG: "where an earlier file of the same stage input named it",
			pgSays: "three rows with columns (id, customer, total, n) and Carol at n = 0 — " +
				"no order_id, and no refusal on any arm"},

		// The NON-aggregated lateral, which none of this may touch.
		// THE NESTED SHAPES the filing asks for, and the third of them is a
		// silent wrong answer this arc FOUND rather than fixed.
		//
		// A LATERAL whose own FROM is a DERIVED TABLE or a CTE REFERENCE is
		// right on every arm: the empty-input default still reaches Carol, and
		// the extra Project between the aggregate and the scan changes
		// nothing. These two are the controls.
		{issue: "#767", name: "control_lateral_over_a_derived_table",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n FROM (SELECT order_id FROM lat_item) d ` +
				`WHERE d.order_id = o.id) s ON true ORDER BY 1`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		{issue: "#767", name: "control_lateral_over_a_cte_reference",
			sql: `WITH d AS (SELECT order_id FROM lat_item) ` +
				`SELECT o.customer AS c, s.n AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n FROM d WHERE d.order_id = o.id) s ON true ORDER BY 1`,
			want:                  []string{"c=Alice|n=int64:2", "c=Bob|n=int64:2", "c=Carol|n=int64:0"},
			wantUnreachableRoutes: 1},
		// A SECOND LATERAL THAT READS THE FIRST ONE'S OUTPUT. PostgreSQL
		// resolves `s.n` inside the second lateral's WHERE — a lateral may
		// name any FROM item to its left — and answers `Alice 2 2, Bob 2 2,
		// Carol 0 0`. This engine answers `m = 0` for all three, on all four
		// arms, in silence.
		//
		// The plan says why: `buildLateralSubquery` promotes the second
		// lateral's correlated EQUALITY (`order_id = o.id`) into the join
		// condition and DROPS `amount > s.n * 10` entirely — the second
		// aggregate carries no filter at all — after which both LEFT joins key
		// on a column called `order_id` and the second matches nothing, so the
		// empty-input default fills `m` with 0.
		//
		// NOT this arc's doing and not a regression: the plan holds no
		// semi/anti join and no decorrelated inner, which are the only nodes
		// this arc's change can produce, and `buildLateralSubquery` is
		// untouched by it. Filed; pinned here with PostgreSQL's answer so the
		// day it is fixed this cell fails.
		{issue: "#767", name: "boundary_a_second_lateral_reading_the_first_ones_output",
			sql: `SELECT o.customer AS c, s.n AS n, t.m AS m FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s ON true ` +
				`JOIN LATERAL (SELECT COUNT(*) AS m FROM lat_item ` +
				`WHERE order_id = o.id AND amount > s.n * 10) t ON true ORDER BY 1`,
			want: []string{"c=Alice|n=int64:2|m=int64:0", "c=Bob|n=int64:2|m=int64:0",
				"c=Carol|n=int64:0|m=int64:0"},
			wantUnreachableRoutes: 1,
			pgSays: "Alice 2 2, Bob 2 2, Carol 0 0 — the second lateral's " +
				"`amount > s.n * 10` is dropped from the plan here"},
		{issue: "#767", name: "control_non_aggregated_lateral_inner",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.amount`,
			want: []string{
				"c=Alice|a=float:50", "c=Alice|a=float:100",
				"c=Bob|a=float:75", "c=Bob|a=float:125"}},
		{issue: "#767", name: "control_non_aggregated_lateral_left",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT amount FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.amount`,
			want: []string{
				"c=Alice|a=float:50", "c=Alice|a=float:100",
				"c=Bob|a=float:75", "c=Bob|a=float:125", "c=Carol|a=NULL"}},
		{issue: "#767", name: "control_non_aggregated_lateral_with_an_on",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount FROM lat_item WHERE order_id = o.id) li ON li.amount > 60 ` +
				`ORDER BY o.customer, li.amount`,
			want: []string{
				"c=Alice|a=float:100", "c=Bob|a=float:75", "c=Bob|a=float:125"}},
		// A GROUPED outer query over the lateral, so the row Carol
		// contributes is counted rather than only printed.
		{issue: "#767", name: "control_grouped_outer_over_the_lateral",
			sql: `SELECT o.customer AS c, COUNT(*) AS k FROM lat_ord o ` + lat +
				`ON true GROUP BY o.customer ORDER BY c`,
			want: []string{"c=Alice|k=int64:1", "c=Bob|k=int64:1", "c=Carol|k=int64:1"}},
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
// #616 / #614 / #714 — the three shapes this arc MEASURED rather than moved.
//
// Each was filed against a tree that has since changed under it, and each is
// pinned here with what it actually does now beside PostgreSQL's answer, so
// the record is a fixture and not a memory.
func arcD5MeasuredCells() []arcD5Cell {
	return []arcD5Cell{
		// #616 — a correlated scalar subquery whose own FROM is a COMMA JOIN.
		// It ANSWERS PostgreSQL's value on all four arms for every shape
		// tried, which the issue's "cannot be executed" no longer describes.
		//
		// It now LOWERS as well, on both DAG arms (zero routes, where each of
		// these cost one before). The comma FROM is built as a chain of
		// condition-less cross joins and liftWhereEquiPredsIntoJoins turns its
		// equalities into join conditions BEFORE the correlation terms are
		// classified — which is what stops innerOnlyPredicate declining a
		// condition that names two inner relations at once (#852 /
		// logical.decorrelatedInnerPlan).
		//
		// WHAT THAT COSTS, pinned rather than described: the 512 KiB arm no
		// longer answers these three. A re-run builds no hash table, and the
		// join it is replaced by materialises a 5 000-row build whose INDEX
		// entries a grace eviction cannot free (#823, "grace eviction frees
		// build columns, not index entries") — so the operator refuses past
		// the budget, which is ADR-0006's designed answer and not a wrong one.
		// Measured: these answer at a 1 MiB budget and refuse at 512 KiB. The
		// day the index becomes spillable, or the plan's floor drops, these
		// three stop erroring and the pin fails.
		{issue: "#616", name: "comma_joined_correlated_inner_two_relations",
			sql: `SELECT COUNT(*) AS n FROM typemx p WHERE p.id < 50 AND p.c_i32 = (` +
				`SELECT MIN(b.c_i32) FROM typemx b, typemx_dim d WHERE b.g = d.k AND b.id = p.id)`,
			want:               []string{"n=int64:46"},
			wantErrLikeSpilled: "memory budget exceeded",
			pgSays:             "46 on every unbudgeted arm; at 512 KiB the join's build does not fit"},
		{issue: "#616", name: "comma_joined_correlated_inner_group_key",
			sql: `SELECT COUNT(*) AS n FROM typemx a WHERE a.id < 50 AND a.g = (` +
				`SELECT MIN(b.g) FROM typemx b, typemx_dim d WHERE b.g = d.k AND b.id = a.id)`,
			want:               []string{"n=int64:47"},
			wantErrLikeSpilled: "memory budget exceeded",
			pgSays:             "47 on every unbudgeted arm; at 512 KiB the join's build does not fit"},
		// The 40-row fixture refuses at 512 KiB too, and its numbers say the
		// cost is the PLAN's shape rather than any one relation's size:
		// `build_rows=15` against `used=498058` of a 524288-byte budget — the
		// join's own build is fifteen rows, and what fills the budget is the
		// three scans, the aggregate and the second join a re-run never builds
		// at all.
		{issue: "#616", name: "comma_joined_correlated_inner_over_the_multikey_fixture",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n = (` +
				`SELECT MIN(b.n) FROM mk_inner b, mk_dim d WHERE b.g = d.k AND b.id = a.id)`,
			want:               []string{"n=int64:21"},
			wantErrLikeSpilled: "memory budget exceeded",
			pgSays:             "21 on every unbudgeted arm; at 512 KiB the plan does not fit"},
		// THE ONE THAT STILL FAILS, and it is not a correlation defect: the
		// same table on BOTH sides of the inner comma join, under a MEMORY
		// BUDGET, panics inside the hash join's dual-int-key probe —
		// `exec.HashJoinProbe.lookupBuild` reads `h.buildBatches[0]` before
		// walking the chain and a SPILLED build has no batch 0. Every other
		// arm answers PostgreSQL's 9. Recorded here because #616's shape is
		// how it was reached; it belongs to the join's spill path.
		{issue: "#616", name: "boundary_self_comma_joined_inner_panics_under_a_budget",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i64 = (` +
				`SELECT MIN(b.w_i64) FROM numwidth b, numwidth c ` +
				`WHERE b.w_key = c.w_key AND b.w_key = a.w_key)`,
			want:               []string{"n=int64:9"},
			wantErrLikeSpilled: "internal error in operator chain",
			pgSays:             "9 on every arm; the budgeted arm panics in exec/join.go's lookupBuild"},

		// #614 — a DERIVED TABLE inside a subquery's FROM that references the
		// ENCLOSING query. MEASURED against live PostgreSQL 17, because the
		// question was open: it is LEGAL WITHOUT LATERAL and PostgreSQL
		// ANSWERS it with 40. LATERAL governs references to same-level FROM
		// siblings; a reference to an OUTER-QUERY column from inside a
		// sub-SELECT's derived table needs none. #614's own text is right and
		// the "PostgreSQL rejects this" reading is wrong.
		//
		// This engine refuses it on all four arms with 42P01 `missing
		// FROM-clause entry for table "a"` — a message that asserts the SQL
		// is invalid, which it is not. The refusal comes from the PHYSICAL
		// column-scope validator, whose scope for a derived table inside a
		// subquery does not merge the enclosing query's aliases. Loud, not
		// wrong; pinned with PostgreSQL's answer so the day it changes this
		// fires.
		{issue: "#614", name: "boundary_derived_table_in_a_subquery_from_references_the_outer_query",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT s, n FROM mk_inner WHERE mk_inner.n = a.n) d)`,
			wantErrLike: `missing FROM-clause entry for table "a"`,
			pgSays: "40 — this is legal SQL and needs no LATERAL; the refusal's message " +
				"says the reference is invalid, and it is not"},

		// #714 — an aggregate argument containing a SCALAR SUBQUERY. The
		// issue's headline is "refused on the stage DAG (subqueries require a
		// SubqueryRunner)"; that tree is gone. It ANSWERS on all four arms,
		// with the DAG routing the plan to the coordinator-local pipeline for
		// its SELECT-list subquery (#659's route), and the VALUE is
		// PostgreSQL's.
		//
		// What is still divergent is the TYPE: `SUM(a + (SELECT 1))` over a
		// DECIMAL column comes back FLOAT8 where PostgreSQL says numeric, and
		// the control one line down shows the same SUM without the subquery
		// staying exact. That is a numeric-typing residual (ADR-0024's
		// literal/declaration rung), not a correlation one, and it is pinned
		// with the box each renders.
		{issue: "#714", name: "scalar_subquery_in_an_aggregate_argument_answers",
			sql:                  `SELECT SUM(a + (SELECT 1)) AS s FROM decpair`,
			want:                 []string{"s=float:59.99"},
			wantScalarProjRoutes: 1,
			pgSays:               "numeric 59.99 — the VALUE agrees, the TYPE does not"},
		{issue: "#714", name: "scalar_aggregate_subquery_in_an_aggregate_argument_answers",
			sql:                  `SELECT SUM(a + (SELECT MAX(id) FROM decpair)) AS s FROM decpair`,
			want:                 []string{"s=float:115.99"},
			wantScalarProjRoutes: 1,
			pgSays:               "numeric 115.99"},
		{issue: "#714", name: "control_the_same_sum_without_a_subquery_stays_exact",
			sql:  `SELECT SUM(a) AS s FROM decpair`,
			want: []string{"s=52.99"},
			pgSays: "numeric 52.99 — rendered as exact text here, which is what the two " +
				"cells above lose"},
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
		// The subquery has to PLAN and fail at RUN time, or `resolveSlow` —
		// the function this issue is about — is never entered at all. A
		// `LIMIT` is what keeps it a subquery: `tryDecorrelateInSubquery`
		// declines a bounded set (#482), so the predicate stays an
		// `expr.InSubquery` and the run's failure is raised exactly where
		// `emptySet = true` used to be assigned.
		//
		// The first cells written for this issue used an aggregate in the
		// subquery's WHERE, which commit 4 now refuses at PLAN time — so they
		// asserted a refusal that never reached the code they were closing.
		{issue: "#601", name: "failed_in_subquery_is_not_an_empty_set",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key / 0 > 0 LIMIT 5)`,
			wantErrLike:          "IN subquery could not be executed",
			wantInSubqueryRoutes: 1,
			pgSays: "division by zero — and the rows this used to decide are numwidth's " +
				"NULL-keyed ones, which `x IN (empty)` would have answered FALSE for"},
		{issue: "#601", name: "failed_not_in_subquery_is_not_an_empty_set",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key / 0 > 0 LIMIT 5)`,
			wantErrLike:          "IN subquery could not be executed",
			wantInSubqueryRoutes: 1,
			pgSays:               "division by zero — `x NOT IN (empty)` would have answered TRUE for every one"},
		// The CORRELATED spelling reaches the same rule in the other
		// evaluator, expr.CorrelatedInSubquery.EvalBoolNull, whose ordering
		// this arc also changed (error, then empty set, then the probe's NULL).
		{issue: "#601", name: "failed_correlated_not_in_subquery_is_not_an_empty_set",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s NOT IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n AND b.n / 0 > 0)`,
			wantErrLike:    "IN subquery could not be executed",
			wantCorrRoutes: 1,
			pgSays:         "division by zero"},
		// The controls. A REAL empty set still decides the NULL-keyed rows —
		// the behaviour #550/#571 added and this must not undo — in the
		// bounded spelling that reaches resolveSlow and in the decorrelated
		// one; and a bounded set that RUNS, so "it errored" cannot pass as
		// "the LIMIT spelling always errors".
		{issue: "#601", name: "control_a_genuinely_empty_bounded_set_still_decides_null_keys",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 0 LIMIT 5)`,
			want:   []string{"n=int64:10"},
			pgSays: "10 — every row, NULL-keyed ones included, because the list is empty"},
		{issue: "#601", name: "control_a_genuinely_empty_set_still_decides_null_keys",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 0)`,
			want: []string{"n=int64:10"}},
		{issue: "#601", name: "control_a_bounded_set_that_runs",
			sql: `SELECT COUNT(*) AS n FROM numwidth a WHERE a.w_i32 NOT IN (` +
				`SELECT b.w_i32 FROM numwidth b WHERE b.w_key < 3 LIMIT 5)`,
			want: []string{"n=int64:5"}},
	}
}

// ---------------------------------------------------------------------------
// #734 — a correlated subquery inside an AGGREGATE ARGUMENT.
//
// The aggregate's derived-input compile site asked for NO outer scope, so a
// correlated subquery there was compiled as UNCORRELATED and run ONCE against
// no outer row — a query-wide constant. `SUM(CASE WHEN EXISTS (SELECT 1 FROM
// decpair y WHERE y.id = x.id * 2) THEN 1 ELSE 0 END)` read FALSE and answered
// 0 for PostgreSQL's 4, in silence, until v0.18.16 made the dangling re-run
// loud. The IDENTICAL expression one level down — in a derived table's SELECT
// list — has always answered, because that site does ask.
//
// The DAG arms route these to the coordinator-local pipeline, and that is the
// RESIDUAL, asserted rather than described: an aggregate ARGUMENT is not a
// decorrelation site at all (decorrelateExists / decorrelateInSubqueries /
// decorrelateScalarSubqueries walk NodeFilter only), so even a plain
// COLUMN-keyed correlation stays a per-row subquery there while the same
// correlation in a WHERE becomes a semi join — which is the pair of cells
// below the headline group.
func arcD5AggregateArgumentCells() []arcD5Cell {
	return []arcD5Cell{
		{issue: "#734", name: "exists_in_an_aggregate_argument",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM decpair y WHERE y.id = x.id * 2) ` +
				`THEN 1 ELSE 0 END) AS v FROM decpair x`,
			want: []string{"v=int64:4"}, wantCorrRoutes: 1},
		{issue: "#734", name: "not_exists_in_an_aggregate_argument",
			sql: `SELECT SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM decpair y WHERE y.id = x.id * 2) ` +
				`THEN 1 ELSE 0 END) AS v FROM decpair x`,
			want: []string{"v=int64:5"}, wantCorrRoutes: 1},
		{issue: "#734", name: "exists_in_a_count_argument",
			sql: `SELECT COUNT(CASE WHEN EXISTS (SELECT 1 FROM decpair y WHERE y.id = x.id * 2) ` +
				`THEN 1 END) AS v FROM decpair x`,
			want: []string{"v=int64:4"}, wantCorrRoutes: 1},
		// The IN spelling of the same position. Its answer is 0 and that is
		// PostgreSQL's: `y.id = x.id * 2 AND x.id = y.id` forces x.id = 0,
		// which no row holds. It sits beside its non-zero twin below so a
		// "0 because the correlation was dropped" cannot pass as this.
		{issue: "#734", name: "in_subquery_in_an_aggregate_argument",
			sql: `SELECT SUM(CASE WHEN x.id IN (SELECT y.id FROM decpair y WHERE y.id = x.id * 2) ` +
				`THEN 1 ELSE 0 END) AS v FROM decpair x`,
			want: []string{"v=int64:0"}, wantCorrRoutes: 1,
			pgSays: "0 — and 9 for the same shape correlated on y.id = x.id"},
		{issue: "#734", name: "in_subquery_in_an_aggregate_argument_nonzero",
			sql: `SELECT SUM(CASE WHEN x.id IN (SELECT y.id FROM decpair y WHERE y.id = x.id) ` +
				`THEN 1 ELSE 0 END) AS v FROM decpair x`,
			want: []string{"v=int64:9"}, wantCorrRoutes: 1},
		// A GROUPED aggregate, so the correlated argument is evaluated on the
		// rows of every group and not once for the whole relation.
		{issue: "#734", name: "exists_in_a_grouped_aggregate_argument",
			sql: `SELECT x.id AS gk, SUM(CASE WHEN EXISTS (SELECT 1 FROM decpair y ` +
				`WHERE y.id = x.id * 2) THEN 1 ELSE 0 END) AS v FROM decpair x GROUP BY x.id ORDER BY gk`,
			want: []string{
				"gk=int64:1|v=int64:1", "gk=int64:2|v=int64:1", "gk=int64:3|v=int64:1",
				"gk=int64:4|v=int64:1", "gk=int64:5|v=int64:0", "gk=int64:6|v=int64:0",
				"gk=int64:7|v=int64:0", "gk=int64:8|v=int64:0", "gk=int64:9|v=int64:0"},
			wantCorrRoutes: 1},
		{issue: "#734", name: "not_exists_in_a_max_argument_over_a_column",
			sql: `SELECT MAX(CASE WHEN NOT EXISTS (SELECT 1 FROM typemx_dim d WHERE d.k = a.g) ` +
				`THEN a.id ELSE 0 END) AS v FROM typemx a WHERE a.id < 50`,
			want: []string{"v=int64:38"}, wantCorrRoutes: 1},
		{issue: "#734", name: "exists_in_a_grouped_aggregate_argument_over_a_column_key",
			sql: `SELECT a.g AS gk, SUM(CASE WHEN EXISTS (SELECT 1 FROM typemx_dim d WHERE d.k = a.g) ` +
				`THEN 1 ELSE 0 END) AS v FROM typemx a WHERE a.id < 50 GROUP BY a.g ORDER BY gk`,
			want: []string{
				"gk=NULL|v=int64:0", "gk=int32:0|v=int64:8", "gk=int32:1|v=int64:7",
				"gk=int32:2|v=int64:7", "gk=int32:3|v=int64:6", "gk=int32:4|v=int64:6",
				"gk=int32:5|v=int64:6", "gk=int32:6|v=int64:7"},
			wantCorrRoutes: 1},
		// THE RESIDUAL, as a PAIR. The same correlation, on a plain column,
		// in the two positions: a WHERE decorrelates into a semi join and
		// both DAG arms EXECUTE it (0 routes); an aggregate ARGUMENT does
		// not, and routes. Closing that needs the aggregate argument to
		// become a decorrelation site — a marker LEFT join publishing a
		// hidden slot the argument reads — which is #734's own next step and
		// not this commit's. The day the second cell shows 0 routes, this
		// pin FAILS and the residual is closed.
		{issue: "#734", name: "control_same_correlation_in_a_where_decorrelates",
			sql: `SELECT COUNT(*) AS v FROM typemx a WHERE a.id < 50 AND EXISTS (` +
				`SELECT 1 FROM typemx_dim d WHERE d.k = a.g)`,
			want: []string{"v=int64:47"}},
		{issue: "#734", name: "residual_same_correlation_in_an_aggregate_argument_routes",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM typemx_dim d WHERE d.k = a.g) ` +
				`THEN 1 ELSE 0 END) AS v FROM typemx a WHERE a.id < 50`,
			want: []string{"v=int64:47"}, wantCorrRoutes: 1,
			pgSays: "47 — the value is right on every arm; what is pinned here is the ROUTE"},
		// The control that answered at base and must keep answering: an
		// UNCORRELATED subquery in an aggregate argument was never this
		// defect, which is what says the CORRELATION and not the position was
		// the trigger. It takes the SELECT-list route, not the correlated one.
		// THE HAVING POSITION, pinned. The same expression in a HAVING is right
		// on both single-process arms and LOUD on both DAG arms, and it was loud
		// there at this arc's base too: the fix reaches buildAggregate's
		// derived-INPUT compile, and a HAVING term is materialized by a second
		// site that still compiles without the outer scope. Not a regression —
		// recorded so the DAG failure is not left unpinned.
		{issue: "#734", name: "boundary_exists_in_a_having_aggregate_is_loud_on_the_dag",
			sql: `SELECT o.customer AS c FROM lat_ord o GROUP BY o.customer ` +
				`HAVING SUM(CASE WHEN EXISTS (SELECT 1 FROM lat_item i WHERE i.order_id = o.id) ` +
				`THEN 1 ELSE 0 END) > 0 ORDER BY c`,
			want:           []string{"c=Alice", "c=Bob"},
			wantErrLikeDAG: "SubqueryRunner",
			pgSays:         "Alice and Bob on every arm; the DAG refuses at a second compile site"},
		{issue: "#734", name: "control_uncorrelated_exists_in_an_aggregate_argument",
			sql: `SELECT SUM(CASE WHEN EXISTS (SELECT 1 FROM decpair y WHERE y.id = 2) ` +
				`THEN 1 ELSE 0 END) AS v FROM decpair x`,
			want: []string{"v=int64:9"}, wantScalarProjRoutes: 1},
	}
}

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
				if arm == "spilled" && tc.wantErrLikeSpilled != "" {
					wantErr = tc.wantErrLikeSpilled
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
					if tc.wantSQLState != "" {
						if got := sqlerr.StateOf(err); got != tc.wantSQLState {
							t.Errorf("%s arm: SQLSTATE %q, want %q — a documented refusal is a "+
								"promise about the CODE as much as the text\n  %v\n  SQL: %s",
								arm, got, tc.wantSQLState, err, tc.sql)
						}
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
			// moves between runs and one passing run proves nothing. The 18
			// generated per-type cells opt out — see skipBudgetedArm for the
			// measurement that says why, and what covers them instead.
			if !tc.skipBudgetedArm {
				for i := 0; i < 5; i++ {
					got, err := na2Run(tmdRunSingle(ctx, spilled, tc.sql))
					check("spilled", got, err)
					if t.Failed() {
						break
					}
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

// ---------------------------------------------------------------------------
// #852 — a correlated subquery whose own FROM is a DERIVED TABLE, a CTE
// REFERENCE or a COMMA LIST is decorrelated like any other.
//
// The three decorrelations used to assemble the build side out of
// `NewScan(info.Tables[0].Name, …)`, which is a model of a FROM clause with
// three holes in it: a derived table has no name a Scan can hold, neither does
// a CTE reference, and a comma list past the first entry was dropped outright.
// Declining all three was right and SLOW — the subquery stayed a per-row
// predicate and the re-run read the whole inner relation once per outer row
// (2N+1 reads for N outer rows, `TestEveryCorrelatedInnerIsReadOnce`) while
// both DAG arms routed the plan to the coordinator-local pipeline.
//
// The build side is now the subquery's own plan (logical.decorrelatedInnerPlan
// over the builder's buildFromClause), so every cell here answers the SAME
// number it always did and costs ZERO routes. The route is the whole point of
// the group: the answers were never wrong, so only the counter can see the
// change — a cell that starts routing again means the rewrite has declined and
// the per-row re-run is back.
//
// The BASE-TABLE spelling of each shape sits beside it, because a pair is what
// says "the same question, differently spelled" rather than "some number".
func arcD5DerivedInnerCells() []arcD5Cell {
	return []arcD5Cell{
		{issue: "#852", name: "control_exists_over_a_base_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM mk_inner b WHERE b.n = a.n)`,
			want: []string{"n=int64:40"}},
		{issue: "#852", name: "exists_over_a_derived_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT s, n FROM mk_inner) b WHERE b.n = a.n)`,
			want: []string{"n=int64:40"}},
		{issue: "#852", name: "exists_over_a_cte_reference",
			sql: `WITH b AS (SELECT s, n FROM mk_inner) SELECT COUNT(*) AS n FROM mk_outer a ` +
				`WHERE EXISTS (SELECT 1 FROM b WHERE b.n = a.n)`,
			want: []string{"n=int64:40"}},
		{issue: "#852", name: "not_exists_over_a_derived_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE NOT EXISTS (` +
				`SELECT 1 FROM (SELECT s, n FROM mk_inner) b WHERE b.n = a.n)`,
			want:   []string{"n=int64:0"},
			pgSays: "0 — every mk_outer row has an mk_inner row of the same n"},
		{issue: "#852", name: "control_correlated_in_over_a_base_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s IN (` +
				`SELECT b.s FROM mk_inner b WHERE b.n = a.n)`,
			want: []string{"n=int64:27"}},
		{issue: "#852", name: "correlated_in_over_a_derived_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.s IN (` +
				`SELECT b.s FROM (SELECT s, n FROM mk_inner) b WHERE b.n = a.n)`,
			want: []string{"n=int64:27"}},
		{issue: "#852", name: "correlated_in_over_a_cte_reference",
			sql: `WITH b AS (SELECT s, n FROM mk_inner) SELECT COUNT(*) AS n FROM mk_outer a ` +
				`WHERE a.s IN (SELECT b.s FROM b WHERE b.n = a.n)`,
			want: []string{"n=int64:27"}},
		// The scalar spelling adds an Aggregate and a LEFT JOIN to the plan,
		// and at 512 KiB that sits ON the floor rather than under or over it:
		// the scans FORCE their row-group loads past the budget (ADR-0006's
		// forced producers) and the join, which honours it, then refuses with
		// `used` already past the limit before it has reserved anything —
		// `used=535822, requested=72, build_rows=0`. A re-run built neither
		// operator, so this is a cost the lowering added.
		//
		// The arm is DROPPED rather than pinned either way, and the reason is
		// the measurement: across census runs it both answered and refused.
		// Which batch crosses a budget is a CONDITION and not a shape
		// (ADR-0027), so pinning either outcome here would be pinning a coin
		// flip. It answers at 1 MiB, and the three #616 comma cells — which
		// refuse at 512 KiB on every replicate — carry the same finding
		// deterministically.
		{issue: "#852", name: "correlated_scalar_over_a_derived_table",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n = (` +
				`SELECT MAX(b.n) FROM (SELECT s, n FROM mk_inner) b WHERE b.n = a.n)`,
			want:            []string{"n=int64:40"},
			skipBudgetedArm: true},
		// THE KEY-ATTRIBUTION CELL. A derived table that RENAMES its column,
		// with the OUTER key sharing the source column's name — which is what
		// happens whenever both sides read the same table. The build root
		// emits `k`; the subtree READS `c_bool`; and the semi/anti narrowing
		// used to decide which side of `c_bool = k` is the build key by
		// walking what the subtree reads, so it projected `c_bool` over a root
		// that has only `k` and the query failed at build time with `column
		// "c_bool" does not exist in the input schema` on every arm.
		// Attribution reads the EMITTED set now (ADR-0021 §1j). Revert that
		// one hunk and this cell is the one that fails.
		{issue: "#852", name: "derived_inner_that_renames_its_key_column",
			sql: `SELECT COUNT(*) AS n FROM typemx a WHERE a.id < 30 AND EXISTS (` +
				`SELECT 1 FROM (SELECT c_bool AS k FROM typemx WHERE id >= 15 AND id < 600 ` +
				`GROUP BY c_bool) b WHERE a.c_bool = b.k)`,
			want:            []string{"n=int64:29"},
			skipBudgetedArm: true,
			pgSays:          "29 - the same number the re-run spelling of this answers"},
		{issue: "#852", name: "exists_over_a_comma_joined_inner",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM mk_inner b, mk_dim d WHERE b.g = d.k AND b.n = a.n)`,
			want: []string{"n=int64:40"}},
		{issue: "#852", name: "control_exists_over_an_explicit_join_inner",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM mk_inner b JOIN mk_dim d ON b.g = d.k WHERE b.n = a.n)`,
			want: []string{"n=int64:40"}},
		// THE BOUNDARY, measured and pinned rather than described: a derived
		// table or a CTE reference JOINED to another relation still declines,
		// and the route is what it costs.
		//
		// The build side would then carry TWO renamings — the join's own
		// (probe bare, build qualified where the bare name collides, decided
		// by reorderJoins) and the derived arm's Project, whose published name
		// is one no scan below it produces. The logical model tracks both and
		// the single-process arm answers correctly; the stage DAG's
		// carried-column derivation does not, and answers a DIFFERENT number
		// rather than failing. Measured over the TPC-H SF0.01 fixture, which
		// is where the two-path oracle caught it:
		//
		//	SELECT COUNT(*) FROM nation a WHERE a.n_nationkey IN (
		//	  SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk
		//	                     FROM nation c) s
		//	  JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)
		//	-- PostgreSQL 17 and single-process: 3.  Stage DAG: 10.
		//
		// The spelling that puts the derived arm on the PROBE happens to agree
		// today, and that is the reason both decline: which arm goes where is
		// reorderJoins' decision from row counts, so a cut drawn there would
		// move under the fixture. Closing it is the stage model's carried
		// columns (physical/join_carried_columns.go), not this rewrite's.
		{issue: "#852", name: "boundary_derived_inner_joined_to_a_relation_still_routes",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT c.n AS k, c.g AS rk FROM mk_inner c) s ` +
				`JOIN mk_dim b ON b.k = s.rk WHERE s.k = a.n)`,
			want:           []string{"n=int64:40"},
			wantCorrRoutes: 1,
			pgSays:         "40 on every arm; what is pinned here is the ROUTE the decline costs"},
		{issue: "#852", name: "boundary_cte_inner_joined_to_a_relation_still_routes",
			sql: `WITH s AS (SELECT c.n AS k, c.g AS rk FROM mk_inner c) ` +
				`SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM s JOIN mk_dim b ON b.k = s.rk WHERE s.k = a.n)`,
			want:           []string{"n=int64:40"},
			wantCorrRoutes: 1,
			pgSays:         "40 — the CTE spelling of the same boundary"},
		// The UNCORRELATED spelling of the divergence itself, over this
		// package's own fixture: both arms must agree, and they do only
		// because the rewrite declines. The day this is decorrelated and the
		// carried columns are still wrong, the two arms answer 24 and 40.
		{issue: "#852", name: "boundary_uncorrelated_in_over_a_joined_derived_inner_agrees",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n IN (` +
				`SELECT s.k FROM (SELECT c.n AS k, c.g AS rk FROM mk_inner c) s ` +
				`JOIN mk_inner b ON b.g = s.rk WHERE s.k < 3)`,
			want:   []string{"n=int64:24"},
			pgSays: "24 — mk_outer's n cycles 0..4 and the set is {0,1,2}"},

		// THE SECOND BOUNDARY: a derived table that COMPUTES a column it
		// publishes. This is innerSemiJoinKey's #516 rule one level down —
		// that guard refuses a COMPUTED select item as a semi-join key, and a
		// derived table hides the computation from it, because from the
		// subquery's side `SELECT b.m FROM (SELECT n + 1 AS m FROM t) b` is a
		// plain column reference.
		//
		// Measured: with the rewrite firing, the single-process arm evaluated
		// `n + 1` and answered PostgreSQL's 32 while the stage DAG carried `m`
		// as if it were a scan column, found none, built an EMPTY semi join
		// and answered 0 — silently, on both DAG arms.
		// `benchmarks/tpch.TestMultiKeyCorrelatedTwoPath/derived_in_computed`
		// is the entry that caught it.
		//
		// The RENAME control one line down is what says the trigger is the
		// EXPRESSION and not the published name: `n AS m` lowers and both arms
		// answer 40.
		{issue: "#852", name: "boundary_derived_inner_that_computes_its_key_still_routes",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE EXISTS (` +
				`SELECT 1 FROM (SELECT n + 1 AS m FROM mk_inner) b WHERE b.m = a.n)`,
			want:           []string{"n=int64:32"},
			wantCorrRoutes: 1,
			pgSays:         "32 - mk_inner's n cycles 0..4, so m is 1..5 and mk_outer's 0 misses"},
		{issue: "#852", name: "boundary_uncorrelated_in_over_a_computed_derived_inner_agrees",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n IN (` +
				`SELECT b.m FROM (SELECT n + 1 AS m FROM mk_inner) b)`,
			want:   []string{"n=int64:32"},
			pgSays: "32 - the uncorrelated spelling takes the materialized-set route, no counter"},
		{issue: "#852", name: "control_derived_inner_that_renames_its_key_lowers",
			sql: `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n IN (` +
				`SELECT b.m FROM (SELECT n AS m FROM mk_inner) b)`,
			want:   []string{"n=int64:40"},
			pgSays: "40 - a RENAME publishes the scan's own values, so every outer row matches"},

		// THE ONE RELATION THE BUILD SIDE STILL DECLINES, and the decline is
		// what keeps the materialized-IN route's own refusal reachable: a
		// RECURSIVE CTE reference is a tagged scan the physical planner
		// resolves by fixed-point iteration from a cache a semi-join build
		// side is not prepared through. It routes, and that route is the cost.
		{issue: "#852", name: "boundary_recursive_cte_inner_still_routes",
			sql: `WITH RECURSIVE r(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM r WHERE x < 4) ` +
				`SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n IN (SELECT r.x FROM r)`,
			want:                 []string{"n=int64:40"},
			wantInSubqueryRoutes: 1,
			pgSays:               "40 — every mk_outer row's n is in 0..4"},
	}
}
