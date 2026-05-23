package physical

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// TestMarkCoPathingSelfJoinBuilds_Q07 verifies Q07's two nation joins (n1 and
// n2) both get QualifyAllBuildCols=true because their build-side scans target
// the same source table AND the joins are co-pathing (one transitively
// depends on the other).
func TestMarkCoPathingSelfJoinBuilds_Q07(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := tpchPlanQueryMap[7]
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
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

	stageByID := map[string]*Stage{}
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}

	// Walk all join stages, count how many target nation as build.
	var nationJoins []*Stage
	for i := range stages {
		s := &stages[i]
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin {
			continue
		}
		if s.RightDepStage == "" {
			continue
		}
		buildScan := stageByID[s.RightDepStage]
		// Walk through one Exchange wrapper if present.
		if buildScan != nil && buildScan.Type != StageScan && len(buildScan.Dependencies) > 0 {
			if d := stageByID[buildScan.Dependencies[0]]; d != nil && d.Type == StageScan {
				buildScan = d
			}
		}
		if buildScan != nil && buildScan.Type == StageScan && buildScan.TableName == "nation" {
			nationJoins = append(nationJoins, s)
		}
	}
	if len(nationJoins) != 2 {
		t.Fatalf("Q07: want 2 nation-join stages, got %d", len(nationJoins))
	}
	for _, j := range nationJoins {
		if !j.QualifyAllBuildCols {
			t.Errorf("nation join %s: want QualifyAllBuildCols=true (co-pathing self-join), got false", j.ID)
		}
	}
}

// TestMarkCoPathingSelfJoinBuilds_Q15_NoFalsePositive guards against the
// broader detector falsely marking Q15's joins. Q15 has lineitem scanned
// twice (CTE producer + scalar producer added by the late-bound scalar
// fix), but those scans don't co-path — they're independent producer
// chains. The narrower co-pathing detector must not mark them.
func TestMarkCoPathingSelfJoinBuilds_Q15_NoFalsePositive(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := tpchPlanQueryMap[15]
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
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
	for _, s := range stages {
		if s.QualifyAllBuildCols {
			t.Errorf("Q15: stage %s (%s) was marked QualifyAllBuildCols, expected none — Q15 has multiple lineitem scans but they don't co-path",
				s.ID, s.Type)
		}
	}
}

// TestLeafStages_ScalarDependencies verifies that a stage referenced via
// ScalarDependencies isn't returned as a leaf — Q15's late-bound producer
// chain would otherwise be picked up by the next sort/aggregate's
// leafStages call and incorrectly added as a Dependency.
func TestLeafStages_ScalarDependencies(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "agg-1", Type: "aggregate", Dependencies: []string{"scan-0"}},
		{ID: "join-2", Type: "hash_join", Dependencies: []string{"scan-0"},
			ScalarDependencies: map[string]string{"scalar_1": "agg-1"}},
	}
	leaves := leafStages(stages)
	for _, leaf := range leaves {
		if leaf == "agg-1" {
			t.Errorf("leafStages returned %q which is a ScalarDependency producer; want it filtered", leaf)
		}
	}
	// Sanity: join-2 should still be a leaf.
	var hasJoin bool
	for _, l := range leaves {
		if l == "join-2" {
			hasJoin = true
		}
	}
	if !hasJoin {
		t.Errorf("leafStages dropped join-2 too: %v", leaves)
	}
}

// TestParseJoinKeys_PreservesQualifiers locks in the parseJoinKeys behavior
// after the Q07 fix. Stripping qualifiers in cleanExpr would re-introduce
// the ambiguity that broke Q07/Q08 (probe-side lookup of unqualified
// "n_regionkey" matches both "n1.n_regionkey" and "n2.n_regionkey").
func TestParseJoinKeys_PreservesQualifiers(t *testing.T) {
	left, right := parseJoinKeys("n1.n_regionkey = r_regionkey")
	if len(left) != 1 || left[0] != "n1.n_regionkey" {
		t.Errorf("left = %v, want [n1.n_regionkey]", left)
	}
	if len(right) != 1 || right[0] != "r_regionkey" {
		t.Errorf("right = %v, want [r_regionkey]", right)
	}
	// Compound:
	left, right = parseJoinKeys("a.k1 = b.k1 AND a.k2 = b.k2")
	if len(left) != 2 || left[0] != "a.k1" || left[1] != "a.k2" {
		t.Errorf("compound left = %v, want [a.k1 a.k2]", left)
	}
	if len(right) != 2 || right[0] != "b.k1" || right[1] != "b.k2" {
		t.Errorf("compound right = %v, want [b.k1 b.k2]", right)
	}
	// Unqualified passes through:
	left, right = parseJoinKeys("s_suppkey = l_suppkey")
	if left[0] != "s_suppkey" || right[0] != "l_suppkey" {
		t.Errorf("unqualified: left=%v right=%v", left, right)
	}
}
