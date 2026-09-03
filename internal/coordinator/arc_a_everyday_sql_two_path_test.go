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

		// ------------------------------------------------------------------
		// #670 — `COUNT(col) OVER` counted the FRAME'S ROWS instead of the
		// non-NULL values in it. The input vector was never read: the answer
		// was `hi - lo`, which is `COUNT(*)`'s answer under COUNT(col)'s
		// spelling.
		//
		// Both spellings ride in ONE query wherever possible, because the
		// defect makes them equal and a fixture that asks only one cannot
		// see that. `m1` is NULL at id 16, 33 and 50 within the first 60
		// rows, which is what makes the two columns differ at all.
		//
		// The four sites are three code paths, and the shapes below reach
		// each: PARTITION BY is the partition-at-a-time columnar walker
		// (window.go), the row-oriented twin of it is the spilled arm, and a
		// spec with NO PARTITION BY streams through window_global.go — once
		// for the whole-input form and once through the ORDER-BY running
		// frame that backfills at each peer group's close.
		{issue: "#670", name: "count_col_over_partition",
			sql: `SELECT id AS i, COUNT(m1) OVER (PARTITION BY g) AS cm, ` +
				`COUNT(*) OVER (PARTITION BY g) AS cr FROM typemx WHERE id < 20 ORDER BY id`,
			want: []string{
				"i=int64:0|cm=int64:3|cr=int64:3", "i=int64:1|cm=int64:3|cr=int64:3",
				"i=int64:2|cm=int64:2|cr=int64:3", "i=int64:3|cm=int64:3|cr=int64:3",
				"i=int64:4|cm=int64:3|cr=int64:3", "i=int64:5|cm=int64:2|cr=int64:2",
				"i=int64:6|cm=int64:2|cr=int64:2", "i=int64:7|cm=int64:3|cr=int64:3",
				"i=int64:8|cm=int64:3|cr=int64:3", "i=int64:9|cm=int64:2|cr=int64:3",
				"i=int64:10|cm=int64:3|cr=int64:3", "i=int64:11|cm=int64:3|cr=int64:3",
				"i=int64:12|cm=int64:1|cr=int64:1", "i=int64:13|cm=int64:2|cr=int64:2",
				"i=int64:14|cm=int64:3|cr=int64:3", "i=int64:15|cm=int64:3|cr=int64:3",
				"i=int64:16|cm=int64:2|cr=int64:3", "i=int64:17|cm=int64:3|cr=int64:3",
				"i=int64:18|cm=int64:3|cr=int64:3", "i=int64:19|cm=int64:2|cr=int64:2"}},
		{issue: "#670", name: "count_col_over_whole_input",
			sql: `SELECT DISTINCT COUNT(m1) OVER () AS cm, COUNT(*) OVER () AS cr ` +
				`FROM typemx WHERE id < 20`,
			want: []string{"cm=int64:19|cr=int64:20"}},
		{issue: "#670", name: "count_col_over_ordered_running_frame",
			sql: `SELECT id AS i, COUNT(m1) OVER (ORDER BY id) AS cm FROM typemx ` +
				`WHERE id < 20 ORDER BY id`,
			want: []string{
				"i=int64:0|cm=int64:1", "i=int64:1|cm=int64:2", "i=int64:2|cm=int64:3",
				"i=int64:3|cm=int64:4", "i=int64:4|cm=int64:5", "i=int64:5|cm=int64:6",
				"i=int64:6|cm=int64:7", "i=int64:7|cm=int64:8", "i=int64:8|cm=int64:9",
				"i=int64:9|cm=int64:10", "i=int64:10|cm=int64:11", "i=int64:11|cm=int64:12",
				"i=int64:12|cm=int64:13", "i=int64:13|cm=int64:14", "i=int64:14|cm=int64:15",
				"i=int64:15|cm=int64:16", "i=int64:16|cm=int64:16", "i=int64:17|cm=int64:17",
				"i=int64:18|cm=int64:18", "i=int64:19|cm=int64:19"}},
		// An ALL-NULL frame. COUNT is the one aggregate that answers 0 rather
		// than NULL over nothing, and it must keep doing so — a fix that made
		// COUNT(col) NULL here would agree with SUM and disagree with
		// PostgreSQL.
		{issue: "#670", name: "count_col_over_an_all_null_frame",
			sql: `SELECT id AS i, COUNT(m1) OVER () AS cm, COUNT(*) OVER () AS cr ` +
				`FROM typemx WHERE id IN (16,33,50) ORDER BY id`,
			want: []string{
				"i=int64:16|cm=int64:0|cr=int64:3", "i=int64:33|cm=int64:0|cr=int64:3",
				"i=int64:50|cm=int64:0|cr=int64:3"}},
		{issue: "#670", name: "count_col_over_an_all_null_partition",
			sql: `SELECT id AS i, COUNT(m1) OVER (PARTITION BY g) AS cm, ` +
				`COUNT(*) OVER (PARTITION BY g) AS cr FROM typemx WHERE id IN (16,33,50) ORDER BY id`,
			want: []string{
				"i=int64:16|cm=int64:0|cr=int64:1", "i=int64:33|cm=int64:0|cr=int64:1",
				"i=int64:50|cm=int64:0|cr=int64:1"}},
		// An EMPTY frame is 0 for BOTH spellings — the row-count answer and
		// the non-NULL-count answer coincide there, which is why an empty
		// frame alone cannot gate this fix and is here as a control.
		{issue: "#670", name: "count_over_an_empty_frame_is_zero_for_both",
			sql: `SELECT id AS i, COUNT(m1) OVER (PARTITION BY g ORDER BY id ` +
				`ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cm, ` +
				`COUNT(*) OVER (PARTITION BY g ORDER BY id ` +
				`ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cr ` +
				`FROM typemx WHERE id < 8 ORDER BY id`,
			want: []string{
				"i=int64:0|cm=int64:0|cr=int64:0", "i=int64:1|cm=int64:0|cr=int64:0",
				"i=int64:2|cm=int64:0|cr=int64:0", "i=int64:3|cm=int64:0|cr=int64:0",
				"i=int64:4|cm=int64:0|cr=int64:0", "i=int64:5|cm=int64:0|cr=int64:0",
				"i=int64:6|cm=int64:0|cr=int64:0", "i=int64:7|cm=int64:1|cr=int64:1"}},
		// COUNT(*) OVER must not move. This partition holds exactly two rows,
		// one of them the NULL one, so the star form's answer (2) and the
		// column form's (1) differ by the whole of the defect: a fix that
		// made the star form count non-NULLs too fails here.
		{issue: "#670", name: "control_count_star_over_partition_stays_rows",
			sql: `SELECT id AS i, COUNT(m1) OVER (PARTITION BY g) AS cm, ` +
				`COUNT(*) OVER (PARTITION BY g) AS cr FROM typemx WHERE id IN (2,16) ORDER BY id`,
			want: []string{
				"i=int64:2|cm=int64:1|cr=int64:2", "i=int64:16|cm=int64:1|cr=int64:2"}},
		{issue: "#670", name: "control_grouped_count_unchanged",
			sql:  `SELECT COUNT(m1) AS cm, COUNT(*) AS cr FROM typemx WHERE id < 20`,
			want: []string{"cm=int64:19|cr=int64:20"}},
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
