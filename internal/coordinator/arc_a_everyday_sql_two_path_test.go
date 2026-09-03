package coordinator

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// The "everyday SQL" census: shapes an ordinary reporting query produces,
// each answer anchored to LIVE PostgreSQL 17 rather than to another arm.
//
// Four arms, because an answer can differ between them (ADR-0018 §3,
// ADR-0027 §5):
//
//	single         the embedded single-process engine
//	spilled        the same under a 512 KiB budget, run FIVE times — a
//	               single passing spilled run proves nothing (ADR-0027 §5)
//	dag            the three-worker stage DAG, LocalFastPathBytes = 0
//	dag-shuffled   the same with BroadcastBytesOverride = 1, so every build
//	               side goes through a hash join and an exchange
//
// Every cell that names a DAG arm also asserts the ROUTING COUNTER beside
// the rows (protocol rule 11): rows alone cannot tell "the DAG executed
// this" from "the DAG refused it and the coordinator-local pipeline
// answered", so a right-to-routed regression is invisible to a row
// assertion. `wantCorrRoutes` is the per-DAG-arm delta of
// `Coordinator.CorrelatedLocalRoutes()`.
type arcACell struct {
	issue, name, sql string
	// want is the whole result, na2Run-rendered and sorted. Every shape
	// either carries an ORDER BY or returns one row.
	want []string
	// wantErrLike, when set, is a substring every arm's error must carry:
	// the shape is LOUD, and PostgreSQL refuses it too (or wadjet
	// deliberately does — the cell's comment says which).
	wantErrLike string
	// wantCorrRoutes is the CorrelatedLocalRoutes delta each DAG arm must
	// show for this shape. 0 = the DAG executed it.
	wantCorrRoutes int64
	// pgSays records PostgreSQL 17's answer in prose when `want` cannot
	// hold it (a refusal, or a deliberate divergence).
	pgSays string
}

func arcACells() []arcACell {
	return []arcACell{
		// ------------------------------------------------------------------
		// #783 — a GROUP BY above a derived table with a LIMIT answered ZERO
		// ROWS on the single-process path.
		//
		// `Pipeline.runParallel`'s "the warm-up batch already satisfied the
		// LIMIT, don't launch workers" early-out finalized the sink without
		// the warm-up batch: in PARTITIONED-AGGREGATION mode that batch is
		// parked in `pendingWarmup` for worker 0, and worker 0 is exactly
		// what the early-out returns before spawning. Every row was in the
		// parked slice, the sink was an empty HashAggregate, and it was
		// finalized. The DAG escapes because its stage planner puts the LIMIT
		// in its own stage, so the aggregate fragment holds no exec.Limit.
		//
		// The N in the LIMIT is not the trigger and the key's SHAPE is not
		// the trigger, so 1 / 100 / 5000 / OFFSET and the renamed and
		// computed keys are all here. DISTINCT is the same pipeline with a
		// different sink spelling.
		{issue: "#783", name: "group_over_derived_limit_count",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 100) x GROUP BY x.id) z`,
			want: []string{"ngroups=int64:100"}},
		{issue: "#783", name: "group_over_derived_limit_rows",
			sql: `SELECT x.id AS i, COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 100) x GROUP BY x.id ORDER BY i LIMIT 3`,
			want: []string{
				"i=int64:0|n=int64:1", "i=int64:1|n=int64:1", "i=int64:2|n=int64:1"}},
		{issue: "#783", name: "group_over_derived_limit_renamed",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.w AS w, COUNT(*) AS n FROM (SELECT g AS w FROM typemx LIMIT 100) x GROUP BY x.w) z`,
			want: []string{"ngroups=int64:8"}},
		{issue: "#783", name: "group_over_derived_limit_computed",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.w AS w, COUNT(*) AS n FROM (SELECT g * 3 AS w FROM typemx LIMIT 100) x GROUP BY x.w) z`,
			want: []string{"ngroups=int64:8"}},
		{issue: "#783", name: "group_over_derived_limit_1",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 1) x GROUP BY x.id) z`,
			want: []string{"ngroups=int64:1"}},
		{issue: "#783", name: "group_over_derived_limit_5000",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 5000) x GROUP BY x.id) z`,
			want: []string{"ngroups=int64:5000"}},
		{issue: "#783", name: "group_over_derived_limit_offset",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 100 OFFSET 10) x GROUP BY x.id) z`,
			want: []string{"ngroups=int64:100"}},
		{issue: "#783", name: "derived_limit_then_distinct",
			sql:  `SELECT COUNT(*) AS ndistinct FROM (SELECT DISTINCT x.id AS i FROM (SELECT id FROM typemx LIMIT 100) x) z`,
			want: []string{"ndistinct=int64:100"}},
		{issue: "#783", name: "distinct_directly_over_derived_limit",
			sql:  `SELECT COUNT(*) AS ndistinct FROM (SELECT DISTINCT id FROM (SELECT id FROM typemx LIMIT 100) x) z`,
			want: []string{"ndistinct=int64:100"}},
		// The controls that were RIGHT at base and must stay right. Each one
		// removes one of the four conditions the defect needs: no GROUP BY
		// (so `usePartitioned` is false and the warm-up batch went straight
		// to `Sink.Consume`), no LIMIT (so nothing is exhausted), a scalar
		// aggregate over the same derived LIMIT, and the CTE spelling (whose
		// LIMIT is consumed in the CTE's own materializing pipeline).
		{issue: "#783", name: "control_no_group",
			sql:  `SELECT COUNT(*) AS n FROM (SELECT id FROM typemx LIMIT 100) x`,
			want: []string{"n=int64:100"}},
		{issue: "#783", name: "control_no_limit",
			sql:  `SELECT COUNT(*) AS ngroups FROM (SELECT x.w AS w, COUNT(*) AS n FROM (SELECT g * 3 AS w FROM typemx) x GROUP BY x.w) z`,
			want: []string{"ngroups=int64:8"}},
		{issue: "#783", name: "control_cte_with_limit",
			sql:  `WITH x AS (SELECT id FROM typemx LIMIT 100) SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n FROM x GROUP BY x.id) z`,
			want: []string{"ngroups=int64:100"}},
		{issue: "#783", name: "control_scalar_agg_over_derived_limit",
			sql:  `SELECT MAX(x.id) AS m FROM (SELECT id FROM typemx LIMIT 100) x`,
			want: []string{"m=int64:99"}},
	}
}

func TestArcAEverydaySQLMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range arcACells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			// na2Run sorts the rendered rows, so a cell above may be written
			// in the query's own order and is sorted here to match. This gate
			// compares a MULTISET; row ORDER is gated by the ordered DuckDB
			// digest in benchmarks/tpch, not here.
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, got []string, err error) {
				t.Helper()
				if tc.wantErrLike != "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, but this shape must be LOUD\n"+
							"  want an error containing %q\n  PostgreSQL 17: %s\n  SQL: %s",
							arm, got, tc.wantErrLike, tc.pgSays, tc.sql)
						return
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm, err, tc.wantErrLike, tc.sql)
					}
					return
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17: %v", arm, err, tc.sql, tc.want)
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

			// The spilled arm runs FIVE times. A spill is a condition, not a
			// query shape (ADR-0027 §5): one passing run proves nothing,
			// because which batch crosses the budget moves between runs.
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
				before := arm.c.CorrelatedLocalRoutes()
				got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				// Rule 11: the routing counter travels beside the rows. A
				// shape that answers correctly because the DAG REFUSED it
				// and the local pipeline ran is not the DAG answering.
				if d := arm.c.CorrelatedLocalRoutes() - before; d != tc.wantCorrRoutes {
					t.Errorf("%s arm: CorrelatedLocalRoutes moved by %d, want %d\n"+
						"  (0 = the DAG executed this shape; 1 = it refused the plan and "+
						"the coordinator-local pipeline answered)\n  SQL: %s",
						arm.name, d, tc.wantCorrRoutes, tc.sql)
				}
			}
		})
	}
}

// #783's second defect at the same line: the early-out returned WITHOUT
// releasing the producer count, so the queue-closer goroutine spawned for
// partitioned aggregation blocked in Wait() forever — a leaked goroutine and
// p.Workers channels per query, on the ordinary "GROUP BY over a derived
// LIMIT" path.
//
// Asserted by counting goroutines around a batch of the shape rather than by
// reading the fix's own bookkeeping: a leak that the fix's own counter cannot
// see is the one this is for.
func TestLimitExhaustedPartitionedPipelineLeaksNoGoroutine(t *testing.T) {
	ctx := context.Background()
	db := tmdStandalone(t, ctx)

	const sql = `SELECT COUNT(*) AS ngroups FROM (SELECT x.id AS i, COUNT(*) AS n ` +
		`FROM (SELECT id FROM typemx LIMIT 100) x GROUP BY x.id) z`

	// Warm the path once so one-off goroutines (pools, background flushers)
	// are already running before the baseline is taken.
	if _, err := db.Query(ctx, sql); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	before := arcASettledGoroutines()
	const runs = 20
	for i := 0; i < runs; i++ {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	after := arcASettledGoroutines()
	// Each leaked query left one closer goroutine, so 20 runs leaked 20.
	// The slack absorbs unrelated churn without absorbing the defect.
	if after-before > runs/4 {
		t.Errorf("goroutines %d -> %d over %d runs of a partitioned pipeline whose "+
			"LIMIT is satisfied by the warm-up batch: the queue-closer goroutine is "+
			"still blocked in producersWG.Wait() (#783)", before, after, runs)
	}
	if exec.PartitionedAggRuns.Load() == 0 {
		t.Fatal("no pipeline ran in partitioned-aggregation mode, so this gate " +
			"exercised nothing (#783's condition 2)")
	}
}

// arcASettledGoroutines reads runtime.NumGoroutine once the count has stopped
// moving, so a query's own transient workers are not counted as a leak. A
// LEAKED goroutine never settles, which is the difference this measures.
func arcASettledGoroutines() int {
	last := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		runtime.GC()
		n := runtime.NumGoroutine()
		if n == last {
			return n
		}
		last = n
	}
	return last
}
