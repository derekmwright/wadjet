package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// TestQ18NativeDAGShape_Regression asserts Q18's native-DAG plan shape stays
// flat — the merge_aggregate (38 intermediates) and merge_sort (95 intermediates)
// trees that the legacy planner emits for SF10 lineitem must be collapsed by
// collapseMergeTreesForNativeDAG / fuseSortIntoPredecessor before EnsureDistribution
// hands the stages to the coordinator. A regression here means a stage explosion
// at SF10 (151 stages → ~17), reproducing the 2026-04-23 SF10 A/B Q01 timeout
// class on Q18.
func TestQ18NativeDAGShape_Regression(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	q18 := tpchPlanQueryMap[18]

	stages := sqlToStagesWithEnsure(t, cat, ctx, q18, 4)

	if len(stages) > 30 {
		t.Errorf("Q18 native-DAG plan has %d stages (>30 means a merge tree probably leaked through; legacy plan has 151)", len(stages))
		for i, s := range stages {
			t.Logf("  [%d] %s id=%s deps=%v", i, s.Type, s.ID, s.Dependencies)
		}
	}

	// No leaked merge-tree intermediates. ValidateNativeDAGShape catches this
	// at executeStageDAG entry, but assert here too so the planner-level test
	// surfaces the cause earlier in the diff.
	for _, s := range stages {
		if s.MergeGroupCount > 0 {
			t.Errorf("Q18 stage %s has MergeGroupCount=%d (intermediate of an uncollapsed tree)", s.ID, s.MergeGroupCount)
		}
	}

	if err := ValidateNativeDAGShape(stages); err != nil {
		t.Errorf("Q18 fails native-DAG shape validation: %v", err)
	}

	// Diagnostic dump: under -v, print FusedAggSpecs on scan stages so we can
	// see whether scan→aggregate fusion is present (necessary for the Q18 SF10
	// memory-constrained-scaling fix — see project_q18_sf10_native_dag_oom).
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if len(s.FusedAggSpecs) > 0 || len(s.FusedAggGroupBy) > 0 {
			t.Logf("scan %s: fused agg group=%v specs=%d files=%d", s.ID, s.FusedAggGroupBy, len(s.FusedAggSpecs), len(s.ScanFiles))
		} else {
			t.Logf("scan %s: NO fused agg, table=%s files=%d", s.ID, s.TableName, len(s.ScanFiles))
		}
	}
}

// sqlToStagesWithEnsure mirrors sqlToStages but turns on UseEnsureDistribution
// (the native-DAG path) so we see the actual production stage shape.
func sqlToStagesWithEnsure(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string, workerCount int) []Stage {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, plan) }
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planner := NewPlanner(cat)
	planner.WorkerCount = workerCount
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}
