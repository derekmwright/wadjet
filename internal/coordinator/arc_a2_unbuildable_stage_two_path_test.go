package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A stage the dispatcher cannot build task inputs for is REFUSED, not
// dispatched (#812).
//
//	WITH c AS (SELECT id, d92 AS v FROM zzp)
//	SELECT c.id FROM c WHERE c.id < (SELECT COUNT(*) FROM c)
//
// failed on both DAG arms with `stage sort-4 has no dependencies and no
// ScanFiles` — an internal message about the planner's own output, carrying no
// SQLSTATE — while the same query over a BASE TABLE and over a DERIVED TABLE
// planned and answered. `substituteScalarDependencies` rewrites the
// predicate's dependency and leaves a stage naming neither a dependency nor a
// table.
//
// #806 refused ONE producer of that shape, a table-less SELECT's `dual` stage,
// by node kind. This asks the same question of the FINISHED stage list, which
// is what a second producer needed and what a third will not need again: the
// condition is a property of the plan the dispatcher is handed, not of the SQL
// that produced it.
//
// The refusal is a HANDOFF and the gate asserts it as one. Every cell reads
// `UnbuildableStageLocalRoutes` beside its rows, because "the DAG answered
// this" and "the DAG refused it and the coordinator answered" are different
// states the same rows cannot tell apart (rule 11) — and the day
// `substituteScalarDependencies` is repaired, the counter going to 0 is what
// says so.
type a2StageCell struct {
	issue, name, sql string
	want             []string
	// wantRoutes is the UnbuildableStageLocalRoutes delta each DAG arm shows.
	wantRoutes int64
	pgSays     string
}

func a2StageCells() []a2StageCell {
	return []a2StageCell{
		{issue: "#812", name: "where_scalar_subquery_over_a_cte",
			sql: `WITH c AS (SELECT id, d92 AS v FROM zzp) ` +
				`SELECT c.id AS id FROM c WHERE c.id < (SELECT COUNT(*) FROM c) ORDER BY c.id`,
			want:       []string{"id=int64:1", "id=int64:2"},
			wantRoutes: 1,
			pgSays:     "2 rows: 1, 2"},
		{issue: "#812", name: "where_scalar_max_over_a_cte",
			sql: `WITH c AS (SELECT id, d92 AS v FROM zzp) ` +
				`SELECT c.id AS id FROM c WHERE c.id < (SELECT MAX(c2.id) FROM c c2) ORDER BY c.id`,
			want:       []string{"id=int64:1", "id=int64:2"},
			wantRoutes: 1,
			pgSays:     "2 rows: 1, 2"},
		// The two spellings that ALWAYS worked, and which localize the defect
		// to the CTE: the same predicate over a base table and over a derived
		// table plans and DISPATCHES, so neither routes.
		{issue: "#812", name: "ctl_where_scalar_subquery_over_a_base_table",
			sql:    `SELECT id FROM zzp WHERE id < (SELECT COUNT(*) FROM zzp) ORDER BY id`,
			want:   []string{"id=int64:1", "id=int64:2"},
			pgSays: "2 rows: 1, 2"},
		{issue: "#812", name: "ctl_where_scalar_subquery_over_a_derived_table",
			sql: `SELECT id FROM zzp WHERE id < (SELECT COUNT(*) FROM (SELECT id FROM zzp) d) ` +
				`ORDER BY id`,
			want:   []string{"id=int64:1", "id=int64:2"},
			pgSays: "2 rows: 1, 2"},
		// A SELECT-LIST scalar subquery over a CTE takes the OTHER refusal
		// (#659, ScalarProjectionLocalRoutes) and must keep taking it: this
		// cell is here so a change that moved it into the new refusal would
		// be visible as a route on the wrong counter.
		{issue: "#812", name: "ctl_select_list_scalar_subquery_over_a_cte_uses_the_other_route",
			sql: `WITH c AS (SELECT id, d92 AS v FROM zzp) ` +
				`SELECT c.id AS id, (SELECT MAX(c2.id) FROM c c2) AS m FROM c ORDER BY c.id`,
			want: []string{
				"id=int64:1|m=3", "id=int64:2|m=3", "id=int64:3|m=3"},
			pgSays: "3 rows, m = 3"},
	}
}

func TestAnUndispatchableStageRoutesInsteadOfFailing(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range a2StageCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, got []string, err error) {
				t.Helper()
				sort.Strings(got)
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", arm, err, tc.pgSays, tc.sql)
					return
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm, got, want, tc.sql)
				}
			}
			sgot, serr := na2Run(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := arm.c.UnbuildableStageLocalRoutes()
				got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				if d := arm.c.UnbuildableStageLocalRoutes() - before; d != tc.wantRoutes {
					t.Errorf("%s arm: UnbuildableStageLocalRoutes moved by %d, want %d\n"+
						"  (0 = the DAG dispatched this plan; 1 = it refused an undispatchable "+
						"stage and the coordinator-local pipeline answered)\n  SQL: %s",
						arm.name, d, tc.wantRoutes, tc.sql)
				}
			}
		})
	}
}
