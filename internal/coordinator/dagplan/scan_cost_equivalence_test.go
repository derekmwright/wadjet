// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The cost guard's two possible sources must answer the same thing.
//
// `enforceQueryLimits` used to read the DISTRIBUTED stage list —
// EstimateCost(plan.Stages, node) — which meant the embedded engine emitted a
// stage DAG on every query purely to decide whether to refuse it. The guard
// now reads the logical plan instead (Planner.EstimatePlanScanCost), and that
// is a user-visible refusal boundary (SQLSTATE 53400): if the two sources
// disagree by one byte, some query is refused on one path and answered on the
// other.
//
// They apply the same rule to the same manifest — for every partition the
// filter admits, sum SizeBytes and NumRows and count the files — so they
// agree EXACTLY on 18 of the 22 TPC-H queries. The four that differ differ
// because the two paths COUNT A SCAN A DIFFERENT NUMBER OF TIMES, and each
// one is pinned below with the mechanism:
//
//   - Q17, Q18, Q21: the logical tree names the same table twice (the main
//     query and a decorrelated subquery over it) and the stage DAG shares one
//     subplan between them (shared_subplan_dedup zeroes the duplicate's
//     estimate). The logical walk counts both, so it is the HIGHER number —
//     the conservative direction for a guard.
//   - Q22: the reverse. The correlated `c_acctbal` subquery becomes its own
//     scan STAGE, which the logical walk does not see because the scan lives
//     inside a predicate expression rather than in the tree. The logical walk
//     is the LOWER number here by one `customer` scan.
//
// A pin that starts agreeing FAILS, because that means one of the two walks
// changed and nobody said so.
func TestTheLogicalScanCostMatchesTheStageCost(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	build := func(t *testing.T, sql string) *logical.Node {
		t.Helper()
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		lp, err := logical.BuildFromSelect(info)
		if err != nil {
			t.Fatalf("logical plan: %v", err)
		}
		ann := func(p *logical.Node) { physical.NewPlanner(cat).AnnotateScanColumns(ctx, p) }
		ann(lp)
		return logical.Optimize(lp, ann)
	}

	// The measured divergences, per query: logical bytes/rows/files vs stage
	// bytes/rows/files. Everything not listed must agree exactly.
	type pair struct{ logical, stages physical.QueryCost }
	pinned := map[int]pair{
		17: {physical.QueryCost{TotalBytes: 13002342400, TotalRows: 124000000, TotalFiles: 1240},
			physical.QueryCost{TotalBytes: 6501171200, TotalRows: 62000000, TotalFiles: 620}},
		18: {physical.QueryCost{TotalBytes: 14313062400, TotalRows: 136500000, TotalFiles: 1365},
			physical.QueryCost{TotalBytes: 8021606400, TotalRows: 76500000, TotalFiles: 765}},
		21: {physical.QueryCost{TotalBytes: 20468203520, TotalRows: 195200000, TotalFiles: 1952},
			physical.QueryCost{TotalBytes: 14176747520, TotalRows: 135200000, TotalFiles: 1352}},
		22: {physical.QueryCost{TotalBytes: 1730150400, TotalRows: 16500000, TotalFiles: 165},
			physical.QueryCost{TotalBytes: 1887436800, TotalRows: 18000000, TotalFiles: 180}},
	}

	compared := 0
	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			continue
		}
		t.Run(fmt.Sprintf("Q%02d", qNum), func(t *testing.T) {
			node := build(t, sql)

			logicalPlanner := physical.NewPlanner(cat)
			fromLogical := logicalPlanner.EstimatePlanScanCost(ctx, node)

			stagePlanner := NewStagePlanner(physical.NewPlanner(cat))
			stagePlanner.WorkerCount = 4
			stages, err := stagePlanner.PlanDistributed(ctx, node)
			if err != nil {
				t.Skipf("refused on the distributed path, no stage cost to compare: %v", err)
			}
			fromStages := EstimateCost(stages, node)

			compared++
			if want, ok := pinned[qNum]; ok {
				got := pair{
					physical.QueryCost{TotalBytes: fromLogical.TotalBytes, TotalRows: fromLogical.TotalRows, TotalFiles: fromLogical.TotalFiles},
					physical.QueryCost{TotalBytes: fromStages.TotalBytes, TotalRows: fromStages.TotalRows, TotalFiles: fromStages.TotalFiles},
				}
				if got != want {
					t.Fatalf("the pinned divergence moved:\n  logical want %+v got %+v\n  stages  want %+v got %+v\n"+
						"Either a walk changed or the plan did. Re-measure, and say which in the commit.",
						want.logical, got.logical, want.stages, got.stages)
				}
				return
			}
			if fromLogical.TotalBytes != fromStages.TotalBytes {
				t.Errorf("TotalBytes: logical %d, stages %d — the cost guard would refuse this query on one path and answer it on the other",
					fromLogical.TotalBytes, fromStages.TotalBytes)
			}
			if fromLogical.TotalRows != fromStages.TotalRows {
				t.Errorf("TotalRows: logical %d, stages %d", fromLogical.TotalRows, fromStages.TotalRows)
			}
			if fromLogical.TotalFiles != fromStages.TotalFiles {
				t.Errorf("TotalFiles: logical %d, stages %d", fromLogical.TotalFiles, fromStages.TotalFiles)
			}
			if fromLogical.HasFilter != fromStages.HasFilter || fromLogical.HasLimit != fromStages.HasLimit {
				t.Errorf("filter/limit flags: logical %v/%v, stages %v/%v",
					fromLogical.HasFilter, fromLogical.HasLimit, fromStages.HasFilter, fromStages.HasLimit)
			}
		})
	}
	if compared < 15 {
		t.Fatalf("only %d queries were compared; the corpus or the planner changed and this gate is measuring almost nothing", compared)
	}
}
