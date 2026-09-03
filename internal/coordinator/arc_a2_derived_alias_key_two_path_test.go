package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SORT or WINDOW key that names a derived table's COMPUTED alias — the
// DEFERRAL of #807 and #658, pinned with the mechanism that stopped it.
//
// `SELECT x.w FROM (SELECT g * 3 AS w FROM t ORDER BY w LIMIT 5) x` is right on
// the single-process pipeline and LOUD on both DAG arms:
// `sort: key column "w" does not exist in the input schema`. The window
// spelling is the same site one caller over:
// `window: PARTITION BY "gk" is not a column of its input (input has: id, g)`.
// `physical.derivedAliasSourceColumn` declines a computed alias by design —
// its own doc says it returns "" for one — so `key.AliasSource` stays empty,
// `resolveDerivedAliasSortKeys` skips the key, and the stage keys on a name
// nothing emits.
//
// # Why this is a pin and not a fix
//
// The repair ADR-0026 points at was attempted and measured, and it is blocked
// one layer down. Giving the key the alias's DEFINITION as a synthetic key's
// SourceExpr and letting `resolveHiddenSortKeys` materialize it onto the
// producing fragment is the right shape — that machinery already exists for
// __sortkey_N and `materializeSortKey` names the column after the key — and it
// does not work, for a reason neither issue names:
//
//	the SCAN'S REQUESTED COLUMN LIST ALREADY CONTAINS THE ALIAS.
//
// Column pruning records `w` as a column the scan needs, because the Project
// that publishes it sits above that scan. Every model of "what does this stage
// emit" in the planner — `stageEmittedColumns`, and through it
// `emittedThroughPassThrough` and `gatherOutputSources` — reads a scan's
// emitted set off that list. So the pass believes `w` exists and skips the
// materialization; and when the test is changed to ask whether some fragment
// MATERIALIZES the name instead (which is the right question, and what
// `resolveDerivedAliasSortKeys` already asks), the projection is built from the
// same list and carries `w` as a PASS-THROUGH column — so the failure moves
// from the sort stage to the scan stage,
// `operator execute: column "w" does not exist in the input schema`, and the
// query is no better off.
//
// That phantom column is #776's own mechanism, one consumer over: a scan
// REQUESTS a column its table does not have, the parquet reader narrows it away
// silently, and every reachability model above it believes the scan produces
// it. Repairing #807 needs that fixed FIRST — the pruner must not put a
// Project's output name into the scan below it — and that is a change to
// column pruning, which every query goes through.
//
// So, protocol rule 11: the fix is bounded by a model this commit knows to be
// incomplete, and it is DEFERRED with the mechanism rather than shipped
// bounded. What lands is this pin, which the tree did not have — nothing
// anywhere named #807, and the shapes that are RIGHT today are right by
// ROUTING, which no row assertion can see.
type a2AliasKeyCell struct {
	issue, name, sql string
	// want is the single-process answer, which is PostgreSQL's.
	want []string
	// wantErrLikeDAG, when set, is the substring both DAG arms must fail
	// with. Deleting it is the fix's proof.
	wantErrLikeDAG string
	// wantStateDAG is the SQLSTATE that failure must carry. It is 0A000 for
	// every cell here — PostgreSQL ANSWERS these queries, so the class a
	// client is owed is "this engine does not implement it" — and it is
	// asserted because these two shapes are exactly the ones that reached a
	// client CLASSLESS while #649's own census cells carried 22003 and 22012.
	// A pin that records a refusal without its class records half of it.
	wantStateDAG string
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
			want:           []string{"w=int64:0|n=int64:100"},
			wantErrLikeDAG: `sort: key column "w" does not exist in the input schema`,
			wantStateDAG:   "0A000",
			pgSays:         "one row, 0|100"},
		{issue: "#807", name: "derived_computed_alias_inner_order_and_limit",
			sql:            `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:           []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			wantErrLikeDAG: `sort: key column "w" does not exist in the input schema`,
			wantStateDAG:   "0A000",
			pgSays:         "5 rows of 0"},
		{issue: "#807", name: "derived_computed_alias_no_inner_limit",
			sql:            `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w) x ORDER BY x.w LIMIT 5`,
			want:           []string{"w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0", "w=int64:0"},
			wantErrLikeDAG: `sort: key column "w" does not exist in the input schema`,
			wantStateDAG:   "0A000",
			pgSays:         "5 rows of 0"},
		// ANY computed alias, not just arithmetic: the walk declines on
		// `proj.Column == ""`, which a function call has too.
		{issue: "#807", name: "derived_computed_alias_function_call",
			sql:            `SELECT x.w FROM (SELECT ABS(g) AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`,
			want:           []string{"w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0", "w=int32:0"},
			wantErrLikeDAG: `sort: key column "w" does not exist in the input schema`,
			wantStateDAG:   "0A000",
			pgSays:         "5 rows of 0"},
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
			wantErrLikeDAG: `window: PARTITION BY "gk" is not a column of its input`,
			wantStateDAG:   "0A000",
			pgSays:         "6 rows, each its own partition"},
		{issue: "#658", name: "window_order_by_computed_alias",
			sql: `SELECT z.id, z.gk, SUM(z.v) OVER (ORDER BY z.gk) AS s ` +
				`FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`,
			want: []string{
				"id=int64:0|gk=int64:0|s=float:0", "id=int64:1|gk=int64:2|s=float:1",
				"id=int64:2|gk=int64:4|s=float:3", "id=int64:3|gk=int64:6|s=float:6",
				"id=int64:4|gk=int64:8|s=float:10", "id=int64:5|gk=int64:10|s=float:15"},
			wantErrLikeDAG: `window: ORDER BY "gk" is not a column of its input`,
			wantStateDAG:   "0A000",
			pgSays:         "6 rows, running total"},
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
				if tc.wantErrLikeDAG != "" {
					if derr == nil {
						t.Errorf("%s arm: ANSWERED %v — #807/#658's deferral is LIFTED for this "+
							"shape. Delete this pin's wantErrLikeDAG and assert the rows.\n  SQL: %s",
							arm.name, dgot, tc.sql)
					} else {
						if !strings.Contains(derr.Error(), tc.wantErrLikeDAG) {
							t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
								arm.name, derr, tc.wantErrLikeDAG, tc.sql)
						}
						if st := sqlerr.StateOf(derr); st != tc.wantStateDAG {
							t.Errorf("%s arm: SQLSTATE %q, want %q — a refusal a client "+
								"cannot classify is half a refusal, and #649's invariant "+
								"covers this shape too\n  SQL: %s",
								arm.name, st, tc.wantStateDAG, tc.sql)
						}
					}
				} else {
					if derr != nil {
						t.Errorf("%s arm: %v\n  SQL: %s", arm.name, derr, tc.sql)
					} else if strings.Join(dgot, "\n") != strings.Join(want, "\n") {
						t.Errorf("%s arm\n  got  %v\n  want %v\n  SQL: %s",
							arm.name, dgot, want, tc.sql)
					}
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
