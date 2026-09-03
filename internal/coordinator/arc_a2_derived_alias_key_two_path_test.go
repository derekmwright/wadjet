package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A SORT or WINDOW key that names a derived table's COMPUTED alias — #807 and
// #658, and the phantom scan column that was under both of them.
//
// `SELECT x.w FROM (SELECT g * 3 AS w FROM t ORDER BY w LIMIT 5) x` was right
// on the single-process pipeline and LOUD on both DAG arms:
// `sort: key column "w" does not exist in the input schema`. The window
// spelling was the same site one caller over:
// `window: PARTITION BY "gk" is not a column of its input (input has: id, g)`.
//
// # The mechanism, and why the loud half went first
//
// `physical.derivedAliasSourceColumn` declines a computed alias by design, so
// `SortKeySpec.AliasSource` stays empty and the stage keys on a name nothing
// emits. The repair — give the key the alias's DEFINITION and materialize it
// onto the producing fragment — was attempted, measured and stopped one layer
// below, because
//
//	the SCAN'S REQUESTED COLUMN LIST ALREADY CONTAINED THE ALIAS.
//
// A derived table's alias BECOMES the scan's `TableAlias`, and
// `logical.sanitizeScanNeeds` kept a qualified reference's bare column
// whenever the qualifier matched — schema or no schema — so `x.w` was written
// into the scan below as a column `typemx` does not have. Every model of what
// a stage emits reads that list (`physical.stageEmittedColumns`, and through
// it `emittedThroughPassThrough`, `gatherOutputSources`, `stageStreamColumns`),
// so the pass believed `w` existed and skipped the materialization; asking
// instead whether some fragment MATERIALIZES it built the projection from the
// same list and moved the failure down to the scan.
//
// That phantom is #776's own mechanism one consumer over, and it is closed:
// a scan requests only columns its table HAS. Every cell asserts
// `UnreachableOutputLocalRoutes` beside the rows, because a row check cannot
// tell "the DAG ran this" from "the DAG refused it and the coordinator-local
// pipeline answered", and a move in either direction is invisible without the
// counter (rule 11).
type a2AliasKeyCell struct {
	issue, name, sql string
	// want is the single-process answer, which is PostgreSQL's.
	want []string
	// wantUnreach is the UnreachableOutputLocalRoutes delta each DAG arm must
	// show. 1 = the DAG refused the plan and the coordinator-local pipeline
	// answered — which is why these cells assert the counter beside the rows:
	// a fix that makes the DAG EXECUTE a shape it currently routes is
	// invisible to a row check, and a regression from executed to routed is
	// equally invisible (rule 11).
	wantUnreach int64
	pgSays      string
}

func a2AliasKeyCells() []a2AliasKeyCell {
	return []a2AliasKeyCell{
		// ---- #807: a SORT key over a computed derived alias ---------------
		{issue: "#807", name: "grouped_over_derived_computed_alias",
			sql: `SELECT x.w, COUNT(*) AS n FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) x ` +
				`GROUP BY x.w ORDER BY x.w`,
			want:        []string{"w=int64:0|n=int64:100"},
			wantUnreach: 1,
			pgSays:      "one row, 0|100"},
		{issue: "#807", name: "derived_computed_alias_inner_order_and_limit",
			sql:         `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:        []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			wantUnreach: 1,
			pgSays:      "5 rows of 0"},
		{issue: "#807", name: "derived_computed_alias_no_inner_limit",
			sql:         `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w) x ORDER BY x.w LIMIT 5`,
			want:        []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			wantUnreach: 1,
			pgSays:      "5 rows of 0"},
		// ANY computed alias, not just arithmetic: the walk declines on
		// `proj.Column == ""`, which a function call has too.
		{issue: "#807", name: "derived_computed_alias_function_call",
			sql:         `SELECT x.w FROM (SELECT ABS(g) AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:        []string{"w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0"},
			wantUnreach: 1,
			pgSays:      "5 rows of 0"},
		// RIGHT, and right by ROUTING: the CTE spelling and the shape with no
		// sort above the derived table both refuse the DAG plan and answer on
		// the coordinator's local pipeline. A fix that makes them EXECUTE is
		// what the counter is here to notice.
		{issue: "#807", name: "routed_cte_spelling",
			sql: `WITH c AS (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) ` +
				`SELECT c.w, COUNT(*) AS n FROM c GROUP BY c.w ORDER BY c.w`,
			want:        []string{"w=int64:0|n=int64:100"},
			wantUnreach: 1,
			pgSays:      "one row, 0|100"},
		{issue: "#807", name: "routed_count_above_the_derived_limit",
			sql:         `SELECT COUNT(*) AS n FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) x`,
			want:        []string{"n=int64:100"},
			wantUnreach: 1,
			pgSays:      "100"},
		// The control that says the defect is the COMPUTED alias and not the
		// derived table: a plain RENAME has a source column, so
		// derivedAliasSourceColumn answers and the DAG executes it.
		{issue: "#807", name: "ctl_plain_rename_executes_on_the_dag",
			sql:    `SELECT x.w FROM (SELECT g AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:   []string{"w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0"},
			pgSays: "5 rows of 0"},

		// ---- #658: the same site, reached by a WINDOW key -----------------
		//
		// The FILED shape — a plain renamed alias — is FIXED and the census
		// confirms it; the residual is the computed one, which nothing in the
		// tree named until this file.
		{issue: "#658", name: "window_partition_by_computed_alias",
			sql: `SELECT z.id, z.gk, SUM(z.v) OVER (PARTITION BY z.gk) AS s ` +
				`FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:2", "id=int64:3|gk=int64:6|s=float:3",
				"id=int64:4|gk=int64:8|s=float:4", "id=int64:5|gk=int64:10|s=float:5"},
			wantUnreach: 1,
			pgSays:      "6 rows, each its own partition"},
		{issue: "#658", name: "window_order_by_computed_alias",
			sql: `SELECT z.id, z.gk, SUM(z.v) OVER (ORDER BY z.gk) AS s ` +
				`FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:3", "id=int64:3|gk=int64:6|s=float:6",
				"id=int64:4|gk=int64:8|s=float:10", "id=int64:5|gk=int64:10|s=float:15"},
			wantUnreach: 1,
			pgSays:      "6 rows, running total"},
		{issue: "#658", name: "routed_window_cte_spelling",
			sql: `WITH c AS (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) ` +
				`SELECT c.id, c.gk, SUM(c.v) OVER (PARTITION BY c.gk) AS s FROM c ORDER BY c.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:2", "id=int64:3|gk=int64:6|s=float:3",
				"id=int64:4|gk=int64:8|s=float:4", "id=int64:5|gk=int64:10|s=float:5"},
			wantUnreach: 1,
			pgSays:      "6 rows, each its own partition"},
		{issue: "#658", name: "ctl_window_partition_by_plain_rename",
			sql: `SELECT z.id, z.gk, SUM(z.v) OVER (PARTITION BY z.gk) AS s ` +
				`FROM (SELECT id, g AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`,
			want: []string{
				"id=int64:0|gk=int32:0|s=float:0", "id=int64:1|gk=int32:1|s=float:1",
				"id=int64:2|gk=int32:2|s=float:2", "id=int64:3|gk=int32:3|s=float:3",
				"id=int64:4|gk=int32:4|s=float:4", "id=int64:5|gk=int32:5|s=float:5"},
			pgSays: "6 rows — the FILED #658 shape, fixed and gated here"},
	}
}

func TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG(t *testing.T) {
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

	for _, tc := range a2AliasKeyCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)

			got, err := na2Run(tmdRunSingle(ctx, single, tc.sql))
			if err != nil {
				t.Fatalf("single arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", err, tc.pgSays, tc.sql)
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("single arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
					got, want, tc.pgSays, tc.sql)
			}

			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := arm.c.UnreachableOutputLocalRoutes()
				dgot, derr := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				if derr != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s",
						arm.name, derr, tc.pgSays, tc.sql)
				} else if strings.Join(dgot, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v\n  SQL: %s",
						arm.name, dgot, want, tc.sql)
				}
				if d := arm.c.UnreachableOutputLocalRoutes() - before; d != tc.wantUnreach {
					t.Errorf("%s arm: UnreachableOutputLocalRoutes moved by %d, want %d\n"+
						"  (0 = the DAG planned this shape; 1 = it refused the plan and the "+
						"coordinator-local pipeline answered)\n  SQL: %s",
						arm.name, d, tc.wantUnreach, tc.sql)
				}
			}
		})
	}
}
