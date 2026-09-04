package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// TWO ARMS may read ONE producer (#715, #660).
//
// Identical subplans are deduped and a CTE referenced twice is deduped by
// construction, so a set operation whose arms are the same query lowers to a
// union stage that lists one stage twice in Dependencies. That is a legal
// shape: the producer materializes once and both arms consume it.
//
// The DAG refused it. `UnionArm` carried a `DepStage` copy of
// `Dependencies[i]`, written when the arm was built, and six later passes had
// to keep the copy in step; `collapseRedundantFinalMergeSort` — which drops a
// trivial merge_sort above a Singleton sort and repoints its consumers — moved
// Dependencies, LeftDepStage and RightDepStage and not the copy.
// ValidateNativeDAGShape then refused the plan (`union stage union-6 arm 0
// names producer "merge_sort-2" but Dependencies[0] is "sort-1"`), correctly,
// since two disagreeing records cannot say which stream an arm's projection
// applies to — and the refusal is a plain error, so nothing routed it to the
// local engine and it reached the client for a query the single-process path
// answers.
//
// The copy is gone. `Stage.UnionArmDep(i)` reads `Dependencies[i]`, so there is
// one record and nothing to keep in step.
//
// The routing counters matter as much as the rows here: this is a refusal
// class, and "the DAG answered" and "the DAG refused and the coordinator-local
// pipeline answered" are the two states identical rows cannot tell apart.
type setOpSharedProdCell struct {
	issue, name, sql string
	want             []string
	wantRoutes       a2Routes
}

func setOpSharedProdCells() []setOpSharedProdCell {
	ids := func(vals ...string) []string { return vals }
	return []setOpSharedProdCell{
		{issue: "#715", name: "two_identical_sorted_producers",
			sql: `SELECT k FROM (SELECT id AS k FROM typemx WHERE id < 5 ORDER BY id) a ` +
				`UNION ALL SELECT k FROM (SELECT id AS k FROM typemx WHERE id < 5 ORDER BY id) b`,
			want: ids("0", "0", "1", "1", "2", "2", "3", "3", "4", "4")},
		{issue: "#715", name: "two_identical_sorted_and_limited_producers",
			sql: `SELECT id FROM (SELECT id FROM typemx WHERE id < 5 ORDER BY id LIMIT 3) a ` +
				`UNION ALL SELECT id FROM (SELECT id FROM typemx WHERE id < 5 ORDER BY id LIMIT 3) b`,
			want: ids("0", "0", "1", "1", "2", "2")},
		{issue: "#715", name: "a_twice_referenced_sorted_cte",
			sql: `WITH c AS (SELECT id FROM typemx WHERE id < 5 ORDER BY id) ` +
				`SELECT id FROM c UNION ALL SELECT id FROM c`,
			want: ids("0", "0", "1", "1", "2", "2", "3", "3", "4", "4")},
		// #660's own query. It already answered at 2d4220c9 — the CTE-alias
		// flattening pass had been taught to move the copy — and it is here
		// because it is the same shape and the same class, and because a
		// regression in the producer record would take it out again.
		{issue: "#660", name: "a_twice_referenced_cte_counted",
			sql: `WITH c AS (SELECT id, c_i64 AS v FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM (SELECT id FROM c UNION ALL SELECT id FROM c) u`,
			want: ids("10000")},
		// The dedup / counting siblings over one producer: the union stage is
		// followed by a GroupByAll aggregate (UNION) or a tagged counting
		// aggregate (INTERSECT / EXCEPT), so the shared producer travels one
		// stage further.
		{issue: "#715", name: "union_distinct_over_one_producer",
			sql: `WITH c AS (SELECT id FROM typemx WHERE id < 5 ORDER BY id) ` +
				`SELECT id FROM c UNION SELECT id FROM c`,
			want: ids("0", "1", "2", "3", "4")},
		{issue: "#715", name: "intersect_over_one_producer",
			sql: `WITH c AS (SELECT id FROM typemx WHERE id < 5 ORDER BY id) ` +
				`SELECT id FROM c INTERSECT SELECT id FROM c`,
			want: ids("0", "1", "2", "3", "4")},
		{issue: "#715", name: "except_over_one_producer",
			sql: `WITH c AS (SELECT id FROM typemx WHERE id < 5 ORDER BY id) ` +
				`SELECT id FROM c EXCEPT SELECT id FROM c`,
			want: nil},
		// The control just outside the shape: two arms whose producers are
		// DIFFERENT, so nothing is deduped and each arm has a stage of its
		// own. It answered before and must keep answering.
		{issue: "#715", name: "ctl_two_different_sorted_producers",
			sql: `SELECT k FROM (SELECT id AS k FROM typemx WHERE id < 5 ORDER BY id) a ` +
				`UNION ALL SELECT k FROM (SELECT id AS k FROM typemx WHERE id < 3 ORDER BY id) b`,
			want: ids("0", "0", "1", "1", "2", "2", "3", "4")},
	}
}

func TestTwoSetOperationArmsMayReadOneProducer(t *testing.T) {
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

	for _, tc := range setOpSharedProdCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, res *oracle.Result, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17 answers %v", arm, err, tc.sql, want)
				}
				if got := strings.Join(setOpCanonRows(res), " "); got != strings.Join(want, " ") {
					t.Errorf("%s arm rows\n  got  %v\n  want %v (PostgreSQL 17)\n  SQL: %s",
						arm, got, want, tc.sql)
				}
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				check(arm.name, dres, derr)
			}
		})
	}
}
