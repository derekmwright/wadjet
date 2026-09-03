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
	return append(append(
		arcD5CTEScopeCells(),
		arcD5TypedRerunCells()...),
		arcD5AggregateArgumentCells()...)
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
