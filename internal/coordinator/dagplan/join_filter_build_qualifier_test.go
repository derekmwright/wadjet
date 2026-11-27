// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestJoinStageFilterQualifiesBuildArmDefinitions pins WHERE the #1678 line j
// condition was lost: at plan time, on the join stage's FilterExprs. An ON
// comparing an expression with a build-arm derived column stays a filter above
// a keyless join, and the filter is re-spelled to the column's definition. In
// a self-join that definition's bare `n_name` named the PROBE's column on the
// join's stream, so the stage carried `lower(n1.n_name) = (lower(n_name))` —
// the probe compared with itself. The build arm's references must reach the
// stage qualified by its build alias; the probe's stay as written.
func TestJoinStageFilterQualifiesBuildArmDefinitions(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	for _, c := range []struct {
		name, sql, want string
	}{
		{"expression", `SELECT n1.n_nationkey FROM nation n1
			JOIN (SELECT n_nationkey, lower(n_name) AS ln FROM nation) n2 ON lower(n1.n_name) = n2.ln`,
			"lower(n1.n_name) = (lower(n2.n_name))"},
		{"renameUnderOr", `SELECT n1.n_nationkey FROM nation n1
			JOIN (SELECT n_nationkey, n_regionkey AS rk FROM nation) n2
			ON n1.n_regionkey + 1 = n2.rk OR n1.n_nationkey = n2.n_nationkey`,
			"n1.n_regionkey + 1 = n2.n_regionkey or n1.n_nationkey = n2.n_nationkey"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, bt := range []int64{0, 1} { // broadcast, then shuffled
				parsed, err := plansql.Parse(c.sql)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				info, err := plansql.ExtractSelect(parsed)
				if err != nil {
					t.Fatalf("extract: %v", err)
				}
				node, err := logical.BuildFromSelect(info)
				if err != nil {
					t.Fatalf("logical: %v", err)
				}
				annotate := func(n *logical.Node) { physical.NewPlanner(cat).AnnotateScanColumns(ctx, n) }
				annotate(node)
				node = logical.Optimize(node, annotate)
				p := NewStagePlanner(physical.NewPlanner(cat))
				p.WorkerCount = 3
				p.BroadcastBytesThreshold = bt
				stages, err := p.PlanDistributed(ctx, node)
				if err != nil {
					t.Fatalf("PlanDistributed: %v", err)
				}
				var got []string
				for _, s := range stages {
					switch s.Type {
					case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
						got = append(got, s.FilterExprs...)
					}
				}
				found := false
				for _, f := range got {
					if strings.EqualFold(f, c.want) {
						found = true
					}
				}
				if !found {
					t.Errorf("broadcast threshold %d: the join stage filters %q, want %q\n%s",
						bt, got, c.want, renderStagesForPlacement(stages))
				}
			}
		})
	}
}

// THE OUTER-JOIN ROUTE NEVER TAKES A TPC-H QUERY OFF THE STAGE DAG (#1678
// line j, round 3: the TPC-H routing stop). The route is decided here, at
// stage emission, so planning is where it is proven absent: every one of the
// 22 queries plans to stages at both broadcast settings and none comes back
// as ErrOuterJoinDerivedFilterDistributed (Q13, the one outer join, filters
// inside its derived table, not above the join).
func TestTPCHNeverTakesTheOuterJoinDerivedFilterRoute(t *testing.T) {
	for qn := 1; qn <= 22; qn++ {
		q, ok := tpch.TPCHQueries[qn]
		if !ok {
			t.Fatalf("Q%02d missing", qn)
		}
		for _, bt := range []int64{0, 1} {
			if _, err := planNationStages(t, q.SQL, bt); errors.Is(err, ErrOuterJoinDerivedFilterDistributed) {
				t.Errorf("Q%02d at broadcast threshold %d took the outer-join route: %v", qn, bt, err)
			}
		}
	}
}

// planNationStages plans sql over the TPC-H catalog at one broadcast
// threshold, returning the stages or PlanDistributed's error.
func planNationStages(t *testing.T, sql string, bt int64) ([]Stage, error) {
	t.Helper()
	cat, ctx := setupTPCHCatalog(t)
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	node, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical: %v", err)
	}
	annotate := func(n *logical.Node) { physical.NewPlanner(cat).AnnotateScanColumns(ctx, n) }
	annotate(node)
	node = logical.Optimize(node, annotate)
	p := NewStagePlanner(physical.NewPlanner(cat))
	p.WorkerCount = 3
	p.BroadcastBytesThreshold = bt
	return p.PlanDistributed(ctx, node)
}

// THE BUILD ARM IS TOLD APART BY WHERE A REFERENCE CAME FROM, NOT BY ITS NODE
// (#1678 line j, round 2). A CTE read on both sides of the join hands both
// arms the same definition nodes; round 1 recognised the probe arm's
// references by node and so left the build arm's `lower(n_name)` bare too —
// `(lower(n_name)) < (lower(n_name))`, the probe compared with itself. And a
// definition qualified by its TABLE's name names a qualifier the join's
// stream does not carry; the worker read it as the bare, probe-first column.
// A qualifier the stream does carry (the inner alias the stage builds under)
// is kept.
func TestJoinStageFilterQualifiesSharedAndInnerQualifiedDefinitions(t *testing.T) {
	for _, c := range []struct {
		name, sql, want string
	}{
		{"cteBothSides", `WITH c AS (SELECT n_nationkey, lower(n_name) AS ln FROM nation)
			SELECT a.n_nationkey FROM c a JOIN c b ON a.ln < b.ln`,
			"(lower(n_name)) < (lower(b.n_name))"},
		{"tableQualified", `SELECT n1.n_nationkey FROM nation n1
			JOIN (SELECT nation.n_nationkey, lower(nation.n_name) AS ln FROM nation) n2 ON lower(n1.n_name) = n2.ln`,
			"lower(n1.n_name) = (lower(n2.n_name))"},
		{"innerAliasKept", `SELECT n1.n_nationkey FROM nation n1
			JOIN (SELECT x.n_nationkey, lower(x.n_name) AS ln FROM nation x) n2 ON lower(n1.n_name) = n2.ln`,
			"lower(n1.n_name) = (lower(x.n_name))"},
		// Round 3: the derived table is a LOWER join's build and the filter
		// rides the upper join (the OR reads w); round 2 left the definition
		// bare there, `upper(a.n_name) != (upper(n_name)) or …`.
		{"lowerJoinBuild", `SELECT a.n_nationkey FROM nation a
			JOIN (SELECT n_nationkey, upper(n_name) AS k FROM nation) b ON a.n_nationkey = b.n_nationkey
			JOIN region w ON a.n_regionkey = w.r_regionkey WHERE upper(a.n_name) <> b.k OR lower(w.r_name) = lower(b.k)`,
			"upper(a.n_name) != (upper(b.n_name)) or lower(w.r_name) = lower((upper(b.n_name)))"},
		// …and a join inside the upper join's BUILD arm: its own qualifier
		// names y's column; the upper join's, which knows its build's scans
		// by bare name only, answered x's.
		{"joinInsideBuildArm", `SELECT a.n_nationkey FROM nation a
			JOIN (SELECT x.n_nationkey AS id, x.n_name AS xs, y.k FROM nation x
			      JOIN (SELECT n_nationkey, upper(n_name) AS k FROM nation) y ON x.n_nationkey = y.n_nationkey) b
			ON a.n_nationkey = b.id WHERE upper(a.n_name) = b.k OR upper(b.xs) = 'C'`,
			"upper(a.n_name) = (upper(y.n_name)) or upper(x.n_name) = 'C'"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, bt := range []int64{0, 1} {
				stages, err := planNationStages(t, c.sql, bt)
				if err != nil {
					t.Fatalf("PlanDistributed: %v", err)
				}
				var got []string
				for _, s := range stages {
					switch s.Type {
					case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
						got = append(got, s.FilterExprs...)
					}
				}
				found := false
				for _, f := range got {
					if strings.EqualFold(f, c.want) {
						found = true
					}
				}
				if !found {
					t.Errorf("broadcast threshold %d: the join stage filters %q, want %q\n%s",
						bt, got, c.want, renderStagesForPlacement(stages))
				}
			}
		})
	}
}

// AN OUTER JOIN'S FILTER OVER A DERIVED TABLE'S COMPUTED COLUMN ROUTES, AND
// NOTHING ELSE DOES (#1678 line j, round 2). Re-spelled, the filter reads the
// right columns, but a task whose padded side's partition is empty shapes
// that side from a declared schema that disagrees with the stream, and the
// gather refused the statement (ADR-0010). The refusal fires when the derived
// table is a side the join PADS — the build of a LEFT join, the probe of a
// RIGHT join, either side of a FULL join — and not for the derived table on
// the preserved side (round 3), the same filter over an INNER join, an outer
// join with no filter, an outer join whose filter names only base columns, or
// one over a renamed column.
func TestOuterJoinDerivedFilterRoutes(t *testing.T) {
	const d = `(SELECT n_nationkey, lower(n_name) AS ln FROM nation)`
	for _, c := range []struct {
		name, sql string
		routes    bool
	}{
		{"left", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n2.ln IS NULL`, true},
		// Round 3: the derived table is the PRESERVED side of this RIGHT join
		// (its build); a task whose build partition is empty emits nothing, so
		// the declared side schema never stands in for it, and the stage DAG
		// answers it (#1678 line j's ow/right and ow/rightOrNull, right on
		// every DAG arm with the qualification alone).
		{"right", `SELECT n1.n_nationkey FROM nation n1 RIGHT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n1.n_name IS NULL`, false},
		{"derivedFirstLeft", `SELECT n1.n_nationkey FROM ` + d + ` n2 LEFT JOIN nation n1 ON n2.n_nationkey = n1.n_nationkey
			WHERE n2.ln <> lower(n1.n_name) OR n1.n_name IS NULL`, false},
		// …and a padded derived table of a LOWER outer join, read by the
		// filter an upper INNER join carries: over a padded row the
		// definition is computed from NULL inputs (R0's join2_outer_coalesce).
		{"lowerLeftUnderInner", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN (SELECT n_nationkey, 1 AS one FROM nation) n2
			ON n1.n_nationkey = n2.n_nationkey JOIN region r ON n1.n_regionkey = r.r_regionkey
			WHERE n2.one IS NULL OR r.r_name = 'ASIA'`, true},
		{"lowerLeftPreservedUnderInner", `SELECT n1.n_nationkey FROM (SELECT n_nationkey, n_regionkey, 1 AS one FROM nation) n1
			LEFT JOIN nation n2 ON n1.n_nationkey = n2.n_nationkey JOIN region r ON n1.n_regionkey = r.r_regionkey
			WHERE n1.one IS NULL OR r.r_name = 'ASIA'`, false},
		{"full", `SELECT n1.n_nationkey FROM nation n1 FULL JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n2.ln IS NULL OR n1.n_name IS NULL`, true},
		{"derivedFirstRight", `SELECT n1.n_nationkey FROM ` + d + ` n2 RIGHT JOIN nation n1 ON n2.n_nationkey = n1.n_nationkey
			WHERE n2.ln <> lower(n1.n_name) OR n2.n_nationkey IS NULL`, true},
		{"inner", `SELECT n1.n_nationkey FROM nation n1 JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln`, false},
		{"leftNoFilter", `SELECT n1.n_nationkey, n2.ln FROM nation n1 LEFT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey`, false},
		{"leftBaseFilter", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN nation n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE n1.n_name <> n2.n_name OR n2.n_name IS NULL`, false},
		// A RENAME is declared by its source name, as the stream carries it:
		// the DAG answers it (R0's S3, S4, pd_left_isnull).
		{"leftRenameFilter", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN (SELECT n_nationkey, n_name AS nm FROM nation) n2
			ON n1.n_nationkey = n2.n_nationkey WHERE n1.n_name <> n2.nm OR n2.nm IS NULL`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, bt := range []int64{0, 1} {
				_, err := planNationStages(t, c.sql, bt)
				routed := errors.Is(err, ErrOuterJoinDerivedFilterDistributed)
				if err != nil && !routed {
					t.Fatalf("broadcast threshold %d: PlanDistributed: %v", bt, err)
				}
				if routed != c.routes {
					t.Errorf("broadcast threshold %d: routed=%v, want %v (err %v)", bt, routed, c.routes, err)
				}
			}
		})
	}
}
