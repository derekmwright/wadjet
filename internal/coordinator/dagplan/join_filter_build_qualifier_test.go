// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
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
