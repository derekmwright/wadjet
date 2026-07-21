package physical

import (
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// TestScalarDeferAll_Q11_Q22 verifies that non-CTE uncorrelated scalar
// subqueries are deferred to distributed producer stages instead of being
// executed eagerly on the coordinator's single-process pipeline at plan
// time. Before this change Q11 spent ~39s and Q22 ~10s per SF100 execution
// inside resolveSubqueryAST→executeSubquery, silently, before "stage-DAG
// dispatch" — the DAG saw none of it. Deferral routes the subquery through
// the same producer-stage/ScalarDependencies path built for Q15's
// CTE-referencing case.
func TestScalarDeferAll_Q11_Q22(t *testing.T) {
	for _, tc := range []struct {
		name string
		qNum int
	}{
		{"Q11_having_sum_threshold", 11},
		{"Q22_where_avg_acctbal", 22},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, ctx := setupTPCHCatalog(t)

			sql := tpchPlanQueryMap[tc.qNum]
			parsed, err := plansql.Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			logicalPlan, err := logical.BuildFromSelect(info)
			if err != nil {
				t.Fatalf("logical plan: %v", err)
			}
			scanAnnotator := func(plan *logical.Node) {
				NewPlanner(cat).AnnotateScanColumns(ctx, plan)
			}
			scanAnnotator(logicalPlan)
			logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

			planner := NewPlanner(cat)
			planner.WorkerCount = 4

			stages, err := planner.PlanDistributed(ctx, logicalPlan)
			if err != nil {
				t.Fatalf("PlanDistributed: %v", err)
			}

			var filterStage *Stage
			for i := range stages {
				if len(stages[i].ScalarDependencies) > 0 {
					filterStage = &stages[i]
					break
				}
			}
			if filterStage == nil {
				t.Fatal("expected a stage with ScalarDependencies — scalar subquery was resolved eagerly at plan time instead of deferred")
			}

			stageByID := make(map[string]*Stage, len(stages))
			for i := range stages {
				stageByID[stages[i].ID] = &stages[i]
			}
			var sawPlaceholder bool
			for ph, producerID := range filterStage.ScalarDependencies {
				prod, ok := stageByID[producerID]
				if !ok {
					t.Fatalf("stage %s: placeholder %q references missing producer %q", filterStage.ID, ph, producerID)
				}
				if prod.Tasks != 1 {
					t.Errorf("producer stage %s: want Tasks=1 for singleton scalar output, got %d", producerID, prod.Tasks)
				}
				for _, expr := range filterStage.FilterExprs {
					if strings.Contains(expr, ":"+ph) {
						sawPlaceholder = true
					}
				}
			}
			if !sawPlaceholder {
				t.Errorf("stage %s: no FilterExpr contains the placeholder; FilterExprs=%v",
					filterStage.ID, filterStage.FilterExprs)
			}
		})
	}
}

// TestScalarDeferAll_KillSwitch verifies WADJET_SCALAR_DEFER=0 restores the
// legacy eager path for non-CTE subqueries (no ScalarDependencies emitted;
// the filter carries an inlined literal or the original subquery text).
func TestScalarDeferAll_KillSwitch(t *testing.T) {
	old := scalarDeferAll
	scalarDeferAll = false
	defer func() { scalarDeferAll = old }()

	cat, ctx := setupTPCHCatalog(t)
	sql := tpchPlanQueryMap[22]
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 4
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	for i := range stages {
		for ph := range stages[i].ScalarDependencies {
			if strings.HasPrefix(ph, "scalar_") {
				t.Errorf("kill switch off: stage %s still defers scalar %q", stages[i].ID, ph)
			}
		}
	}
}
