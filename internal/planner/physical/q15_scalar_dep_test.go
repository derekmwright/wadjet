package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestQ15_ScalarDependenciesWiring verifies that PlanDistributed with
// UseEnsureDistribution=true rewrites Q15's CTE-referencing scalar subquery
// as a deferred :scalar_N placeholder and emits a producer stage pointed to
// by the filter-carrying stage's ScalarDependencies. This is the planner-
// side gate for the Q15 SF0.1 float-drift fix — it must hold regardless of
// data size because it is a structural invariant.
//
// Root cause recap: under native-DAG the old code resolved the MAX subquery
// eagerly via executeSubquery (single-process cteCache), then substituted the
// float literal into the filter. The outer JOIN recomputed `revenue`
// distributedly, and the two paths differed in the low-order bits of the
// float64 accumulation — at SF0.1 this moved MAX past every supplier's
// distributed total_revenue and Q15 returned 0 rows.
func TestQ15_ScalarDependenciesWiring(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	sql := tpchPlanQueryMap[15]
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

	// There must be at least one stage carrying ScalarDependencies.
	var filterStage *Stage
	for i := range stages {
		if len(stages[i].ScalarDependencies) > 0 {
			filterStage = &stages[i]
			break
		}
	}
	if filterStage == nil {
		t.Fatal("expected at least one stage with ScalarDependencies populated for Q15")
	}

	// Every placeholder in ScalarDependencies must reference a stage that
	// exists in the plan.
	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}
	for ph, producerID := range filterStage.ScalarDependencies {
		if ph == "" {
			t.Errorf("empty placeholder name in stage %s", filterStage.ID)
		}
		if _, ok := stageByID[producerID]; !ok {
			t.Errorf("stage %s: scalar placeholder %q references missing producer %q", filterStage.ID, ph, producerID)
		}
	}

	// The filter stage's FilterExprs must contain the placeholder render
	// (":<name>") so the coordinator-side substitution has something to
	// rewrite. Otherwise the literal was eagerly inlined and the drift
	// bug would still occur.
	var sawPlaceholder bool
	for _, expr := range filterStage.FilterExprs {
		for ph := range filterStage.ScalarDependencies {
			if strings.Contains(expr, ":"+ph) {
				sawPlaceholder = true
				break
			}
		}
		if sawPlaceholder {
			break
		}
	}
	if !sawPlaceholder {
		t.Errorf("stage %s: no FilterExpr contains a :scalar_N placeholder; FilterExprs=%v",
			filterStage.ID, filterStage.FilterExprs)
	}

	// Producer must be a Singleton (Tasks==1) so its output is a single WSHF
	// file suitable for scalar extraction. Pick any producer for a sanity
	// check — in practice Q15 has exactly one.
	for _, producerID := range filterStage.ScalarDependencies {
		p := stageByID[producerID]
		if p.Tasks != 1 {
			t.Errorf("producer stage %s: want Tasks=1 for singleton scalar output, got %d", producerID, p.Tasks)
		}
	}
}

