package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A SORT or WINDOW key that names a derived table's COMPUTED alias — #807 and
// #658, and the two things that were under them.
//
// `SELECT x.w FROM (SELECT g * 3 AS w FROM t ORDER BY w LIMIT 5) x` was right
// on the single-process pipeline and LOUD on both DAG arms:
// `sort: key column "w" does not exist in the input schema`. The window
// spelling was the same site one caller over:
// `window: PARTITION BY "gk" is not a column of its input (input has: id, g)`.
//
// # Two names, and the phantom that hid them
//
// `physical.derivedAliasSourceColumn` declines a computed alias BY DESIGN —
// there is no source column to point at — so `SortKeySpec.AliasSource` stayed
// empty and the stage keyed on a name nothing emits. The repair is ADR-0026
// §2's two names at a third caller: `Column` is what the key is CALLED, the
// alias the query wrote, and `AliasExpr` is where the value comes FROM,
// `g * 3` spelled in the producer's own scope. The definition is MATERIALIZED
// onto the producing fragment under the alias's own name, so every consumer
// above — the sort's own key, an outer sort's key, the gather's rename, the
// window's PARTITION BY and its distribution — keeps reading one name.
//
// Answering with the DEFINITION instead of the alias would be wrong wherever
// some fragment DOES materialize it, which is the mistake ADR-0025 records for
// an aggregate's argument below a join. `derivedAliasDefinition` therefore
// looks through Project, Filter, Sort and Limit and stops at everything else:
// below a JOIN, an AGGREGATE, a DISTINCT or a set operation the alias is real,
// and the plain-rename and CTE controls here are what hold that boundary.
//
// The attempt before this one was blocked one layer down, and the record is
// worth keeping because it says where the next one starts:
//
//	the SCAN'S REQUESTED COLUMN LIST ALREADY CONTAINED THE ALIAS.
//
// A derived table's alias BECOMES the scan's `TableAlias`, and
// `logical.sanitizeScanNeeds` kept a qualified reference's bare column
// whenever the qualifier matched, so `x.w` was written into the scan below as
// a column `typemx` does not have. Every model of what a stage emits reads
// that list, so the pass believed `w` existed and skipped the
// materialization; asking instead whether some fragment MATERIALIZES it built
// the projection from the same list and moved the failure down to the scan.
// That phantom is #776's own mechanism and it is closed first.
//
// Every cell asserts `UnreachableOutputLocalRoutes` beside the rows. A row
// check cannot tell "the DAG ran this" from "the DAG refused it and the
// coordinator-local pipeline answered" — the CTE and count-above spellings
// were RIGHT before this change, by routing — so the counter is the only thing
// that sees the move in either direction (rule 11).
type a2AliasKeyCell struct {
	issue, name, sql string
	// want is the single-process answer, which is PostgreSQL's.
	want []string
	// wantUnreach is the UnreachableOutputLocalRoutes delta each DAG arm must
	// show. It is 0 for every cell: the DAG plans and RUNS all of them. A 1
	// here would mean the plan was refused and the coordinator-local pipeline
	// answered — the same rows, which is exactly why the counter is asserted.
	wantUnreach int64
	pgSays      string
}

func a2AliasKeyCells() []a2AliasKeyCell {
	return []a2AliasKeyCell{
		// ---- #807: a SORT key over a computed derived alias ---------------
		{issue: "#807", name: "grouped_over_derived_computed_alias",
			sql: `SELECT x.w, COUNT(*) AS n FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) x ` +
				`GROUP BY x.w ORDER BY x.w`,
			want:   []string{"w=int64:0|n=int64:100"},
			pgSays: "one row, 0|100"},
		{issue: "#807", name: "derived_computed_alias_inner_order_and_limit",
			sql:    `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:   []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			pgSays: "5 rows of 0"},
		{issue: "#807", name: "derived_computed_alias_no_inner_limit",
			sql:    `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w) x ORDER BY x.w LIMIT 5`,
			want:   []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			pgSays: "5 rows of 0"},
		// ANY computed alias, not just arithmetic: the walk declines on
		// `proj.Column == ""`, which a function call has too.
		{issue: "#807", name: "derived_computed_alias_function_call",
			sql:    `SELECT x.w FROM (SELECT ABS(g) AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:   []string{"w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0"},
			pgSays: "5 rows of 0"},
		// The CTE spelling and the shape with no sort above the derived table.
		// Both were right BY ROUTING before this change and both execute now,
		// which no row assertion can see.
		{issue: "#807", name: "cte_spelling",
			sql: `WITH c AS (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) ` +
				`SELECT c.w, COUNT(*) AS n FROM c GROUP BY c.w ORDER BY c.w`,
			want:   []string{"w=int64:0|n=int64:100"},
			pgSays: "one row, 0|100"},
		{issue: "#807", name: "count_above_the_derived_limit",
			sql:    `SELECT COUNT(*) AS n FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) x`,
			want:   []string{"n=int64:100"},
			pgSays: "100"},
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
			pgSays: "6 rows, each its own partition"},
		{issue: "#658", name: "window_order_by_computed_alias",
			sql: `SELECT z.id, z.gk, SUM(z.v) OVER (ORDER BY z.gk) AS s ` +
				`FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:3", "id=int64:3|gk=int64:6|s=float:6",
				"id=int64:4|gk=int64:8|s=float:10", "id=int64:5|gk=int64:10|s=float:15"},
			pgSays: "6 rows, running total"},
		{issue: "#658", name: "window_cte_spelling",
			sql: `WITH c AS (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) ` +
				`SELECT c.id, c.gk, SUM(c.v) OVER (PARTITION BY c.gk) AS s FROM c ORDER BY c.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:2", "id=int64:3|gk=int64:6|s=float:3",
				"id=int64:4|gk=int64:8|s=float:4", "id=int64:5|gk=int64:10|s=float:5"},
			pgSays: "6 rows, each its own partition"},
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
