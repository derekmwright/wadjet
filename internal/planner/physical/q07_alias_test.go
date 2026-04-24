package physical

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// TestExtractOutputRenames_TableQualifier verifies that SELECT-list
// renames preserve the table qualifier ("n1.n_name AS supp_nation"
// -> source "n1.n_name", not just "n_name"). This is the core of
// Bug A from the Q07 native-DAG investigation: the worker emits
// qualified column names from a self-join, and we need to match
// those, not the unqualified ColumnRef the parser hands us.
func TestExtractOutputRenames_TableQualifier(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// Q07 has both nation aliases (n1, n2) and a SUBSTR expression alias.
	sql := `SELECT
		n1.n_name AS supp_nation,
		n2.n_name AS cust_nation,
		SUBSTR(l_shipdate, 1, 4) AS l_year
		FROM lineitem
		JOIN supplier ON s_suppkey = l_suppkey
		JOIN nation n1 ON s_nationkey = n1.n_nationkey
		JOIN nation n2 ON s_nationkey = n2.n_nationkey`

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
	NewPlanner(cat).AnnotateScanColumns(ctx, logicalPlan)

	renames := extractOutputRenames(logicalPlan)
	want := map[string]string{
		"n1.n_name":                 "supp_nation",
		"n2.n_name":                 "cust_nation",
		"substr(l_shipdate, 1, 4)":  "l_year",
	}
	got := map[string]string{}
	for _, r := range renames {
		got[r.From] = r.To
	}
	for from, to := range want {
		if got[from] != to {
			t.Errorf("missing rename %q -> %q (got %q)", from, to, got[from])
		}
	}
	for from, to := range got {
		if want[from] != to {
			t.Errorf("unexpected rename %q -> %q", from, to)
		}
	}
}

// TestExtractOutputRenames_NoRenameWhenAliasMatchesSource skips
// projections whose alias equals the source — those would just
// rename a column to itself.
func TestExtractOutputRenames_NoRenameWhenAliasMatchesSource(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT n_name FROM nation`
	parsed, _ := plansql.Parse(sql)
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, _ := logical.BuildFromSelect(info)
	NewPlanner(cat).AnnotateScanColumns(ctx, logicalPlan)

	renames := extractOutputRenames(logicalPlan)
	if len(renames) != 0 {
		t.Errorf("expected no renames for `SELECT n_name`, got %v", renames)
	}
}

// TestExtractOutputRenames_SkipsAggregates verifies that aggregate
// projections (whose AggSpec.OutputCol is already the alias) do not
// produce renames — they already emit columns under the user's alias.
func TestExtractOutputRenames_SkipsAggregates(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_quantity) AS total FROM lineitem`
	parsed, _ := plansql.Parse(sql)
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, _ := logical.BuildFromSelect(info)
	NewPlanner(cat).AnnotateScanColumns(ctx, logicalPlan)

	renames := extractOutputRenames(logicalPlan)
	if len(renames) != 0 {
		t.Errorf("expected no renames for aggregate-only SELECT, got %v", renames)
	}
}

// TestPlanDistributed_GatherCarriesOutputRenames is the end-to-end
// planner check: PlanDistributed with UseEnsureDistribution=true must
// stamp the rename map onto the Gather stage so the coordinator can
// apply it on the result schema.
func TestPlanDistributed_GatherCarriesOutputRenames(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	sql := `SELECT n_name AS my_nation FROM nation WHERE n_name = 'FRANCE'`
	parsed, _ := plansql.Parse(sql)
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, _ := logical.BuildFromSelect(info)
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 4
	planner.UseEnsureDistribution = true

	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	var gather *Stage
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			gather = &stages[i]
			break
		}
	}
	if gather == nil {
		t.Fatal("no Gather stage emitted")
	}
	if len(gather.OutputRenames) != 1 {
		t.Fatalf("want 1 OutputRename on Gather, got %d: %v", len(gather.OutputRenames), gather.OutputRenames)
	}
	r := gather.OutputRenames[0]
	if r.From != "n_name" || r.To != "my_nation" {
		t.Errorf("rename = %+v; want {From:n_name To:my_nation}", r)
	}
}
