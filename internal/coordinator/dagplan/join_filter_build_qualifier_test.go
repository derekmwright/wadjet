// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"strings"
	"testing"

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

// AN OUTER JOIN'S FILTER OVER A DERIVED TABLE'S COLUMN ROUTES, AND NOTHING
// ELSE DOES (#1678 line j, round 2). Re-spelled, the filter reads the right
// columns, but a task whose padded side's partition is empty shapes that side
// from a declared schema that disagrees with the stream, and the gather
// refused the statement (ADR-0010). The refusal fires for LEFT, RIGHT and
// FULL with the derived table on either side, and not for the same filter
// over an INNER join, an outer join with no filter, or an outer join whose
// filter names only base columns.
func TestOuterJoinDerivedFilterRoutes(t *testing.T) {
	const d = `(SELECT n_nationkey, lower(n_name) AS ln FROM nation)`
	for _, c := range []struct {
		name, sql string
		routes    bool
	}{
		{"left", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n2.ln IS NULL`, true},
		{"right", `SELECT n1.n_nationkey FROM nation n1 RIGHT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n1.n_name IS NULL`, true},
		{"full", `SELECT n1.n_nationkey FROM nation n1 FULL JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln OR n2.ln IS NULL OR n1.n_name IS NULL`, true},
		{"derivedFirstRight", `SELECT n1.n_nationkey FROM ` + d + ` n2 RIGHT JOIN nation n1 ON n2.n_nationkey = n1.n_nationkey
			WHERE n2.ln <> lower(n1.n_name) OR n2.n_nationkey IS NULL`, true},
		{"inner", `SELECT n1.n_nationkey FROM nation n1 JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE lower(n1.n_name) <> n2.ln`, false},
		{"leftNoFilter", `SELECT n1.n_nationkey, n2.ln FROM nation n1 LEFT JOIN ` + d + ` n2 ON n1.n_nationkey = n2.n_nationkey`, false},
		{"leftBaseFilter", `SELECT n1.n_nationkey FROM nation n1 LEFT JOIN nation n2 ON n1.n_nationkey = n2.n_nationkey
			WHERE n1.n_name <> n2.n_name OR n2.n_name IS NULL`, false},
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
