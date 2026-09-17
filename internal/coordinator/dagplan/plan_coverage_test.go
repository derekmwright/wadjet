// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// --- PrettyPrint ---

// --- EstimateCost ---

func TestEstimateCost_ScanStages(t *testing.T) {
	stages := []Stage{
		{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100, ScanFiles: []string{"a.parquet", "b.parquet"}},
		{Type: "scan", EstimatedBytes: 2000, EstimatedRows: 200, ScanFiles: []string{"c.parquet"}},
		{Type: "aggregate"}, // non-scan stages should be ignored
	}
	node := logical.NewScan("t", "")
	cost := EstimateCost(stages, node)
	if cost.TotalBytes != 3000 {
		t.Errorf("TotalBytes = %d, want 3000", cost.TotalBytes)
	}
	if cost.TotalRows != 300 {
		t.Errorf("TotalRows = %d, want 300", cost.TotalRows)
	}
	if cost.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", cost.TotalFiles)
	}
}

func TestEstimateCost_WithFilter(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	filter := logical.NewFilter(scan, []logical.Predicate{{Raw: "x > 1"}})

	cost := EstimateCost(stages, filter)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with filter node")
	}
}

func TestEstimateCost_WithLimit(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	limit := logical.NewLimit(scan, 10, 0)

	cost := EstimateCost(stages, limit)
	if !cost.HasLimit {
		t.Error("expected HasLimit=true with limit node")
	}
}

func TestEstimateCost_WithPartitionFilter(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	scan.PartitionFilter = map[string]string{"year": "2026"}

	cost := EstimateCost(stages, scan)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with partition filter")
	}
}

func TestEstimateCost_WithScanPredicates(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	scan.ScanPredicates = []logical.Predicate{{Column: "x", Op: "=", Value: 1}}

	cost := EstimateCost(stages, scan)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with scan predicates")
	}
}

// --- physical.HasFilterOrPartition ---

// --- HasLimit ---

// --- physical.FormatBytes ---

// --- enforceQueryLimits ---
//
// The guard reads the LOGICAL plan's scan cost (Planner.EstimatePlanScanCost),
// not a stage list, so these cells scan the fixture table — one file, 1024
// bytes, 100 rows — and set each threshold against those numbers. They used to
// hand-build Stage values with invented estimates, which stopped being the
// source the guard reads.

func TestEnforceQueryLimits_NilLimits(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	// No query limits — should pass
	err := planner.EnforceQueryLimits(context.Background(), logical.NewScan("events", ""))
	if err != nil {
		t.Errorf("expected nil error with nil limits, got %v", err)
	}
}

func TestEnforceQueryLimits_MaxScanBytes(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{MaxScanBytes: 500}

	err := planner.EnforceQueryLimits(ctx, logical.NewScan("events", ""))
	if err == nil {
		t.Error("expected error for exceeding MaxScanBytes")
	}
}

func TestEnforceQueryLimits_MaxScanRows(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{MaxScanRows: 50}

	err := planner.EnforceQueryLimits(ctx, logical.NewScan("events", ""))
	if err == nil {
		t.Error("expected error for exceeding MaxScanRows")
	}
}

func TestEnforceQueryLimits_MaxScanFiles(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{MaxScanFiles: 0}
	// 0 means "no file limit"; the cell needs a limit BELOW the fixture's
	// one file, and one file is the smallest a table can have — so the
	// threshold that fires is the byte one, and the file COUNT is asserted
	// through the message instead.
	planner.QueryLimits = &config.QueryLimits{MaxScanBytes: 1}
	err := planner.EnforceQueryLimits(ctx, logical.NewScan("events", ""))
	if err == nil {
		t.Fatal("expected error for exceeding MaxScanBytes")
	}
	if !strings.Contains(err.Error(), "across 1 files") {
		t.Errorf("the refusal does not report the file count it counted: %v", err)
	}
}

func TestEnforceQueryLimits_RequireFilterAboveBytes(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{RequireFilterAboveBytes: 100}

	scan := logical.NewScan("events", "") // no filter
	err := planner.EnforceQueryLimits(ctx, scan)
	if err == nil {
		t.Error("expected error for missing filter above byte threshold")
	}

	// With filter should pass
	filter := logical.NewFilter(scan, []logical.Predicate{{Raw: "x=1"}})
	err = planner.EnforceQueryLimits(ctx, filter)
	if err != nil {
		t.Errorf("expected no error with filter, got %v", err)
	}
}

func TestEnforceQueryLimits_RequireLimitAboveRows(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{RequireLimitAboveRows: 50}

	scan := logical.NewScan("events", "") // no limit
	err := planner.EnforceQueryLimits(ctx, scan)
	if err == nil {
		t.Error("expected error for missing limit above row threshold")
	}

	// With limit should pass
	limit := logical.NewLimit(scan, 10, 0)
	err = planner.EnforceQueryLimits(ctx, limit)
	if err != nil {
		t.Errorf("expected no error with limit, got %v", err)
	}
}

func TestEnforceQueryLimits_UnderLimits(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))
	planner.QueryLimits = &config.QueryLimits{
		MaxScanBytes: 5000,
		MaxScanRows:  500,
		MaxScanFiles: 10,
	}

	err := planner.EnforceQueryLimits(context.Background(), logical.NewScan("events", ""))
	if err != nil {
		t.Errorf("expected nil error under all limits, got %v", err)
	}
}

// --- physical.MapJoinType ---

// --- physical.MapExecJoinType ---

// --- physical.ParseJoinKeys ---

// --- physical.MatchesPartitionFilter ---

// --- physical.EvalFilterTyped ---

// --- physical.MapPredOp ---

// --- physical.DecimalFromBytes ---

// --- Distributed plan stage generation ---

func TestPlanDistributed_Distinct(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	distinct := logical.NewDistinct(scan)
	stages, err := planner.PlanDistributed(ctx, distinct)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	// Distinct should pass through to children
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
	if stages[0].Type != "scan" {
		t.Errorf("expected scan stage, got %q", stages[0].Type)
	}
}

func TestPlanDistributed_Project(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	proj := logical.NewProject(scan, []logical.Projection{{Column: "event_id", Alias: "id"}})
	stages, err := planner.PlanDistributed(ctx, proj)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
	if stages[0].Type != "scan" {
		t.Errorf("expected scan stage from project passthrough, got %q", stages[0].Type)
	}
}

func TestPlanDistributed_Filter(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	filter := logical.NewFilter(scan, []logical.Predicate{
		{Raw: "year = '2026'"},
	})
	stages, err := planner.PlanDistributed(ctx, filter)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
}

func TestPlanDistributed_LimitSort(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	sort := logical.NewSort(scan, []logical.OrderExpr{{Column: "ts", Desc: true}})
	limit := logical.NewLimit(sort, 10, 0)

	stages, err := planner.PlanDistributed(ctx, limit)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// Native-DAG: scan + sort (with limit fused) + exchange-gather.
	// merge_sort is collapsed by collapseRedundantFinalMergeSort.
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	foundSort := false
	for _, s := range stages {
		if s.Type == "sort" {
			foundSort = true
			if s.Limit != 10 {
				t.Errorf("expected sort stage limit=10, got %d", s.Limit)
			}
		}
	}
	if !foundSort {
		t.Error("expected a sort stage")
	}
}

// This used to assert only "2 scan stages for a union", which is exactly the
// #346 shape: both arms planned, nothing merging them, and the gather left
// reading whichever arm came last. A union must now emit a stage that
// consumes BOTH arms.
func TestPlanDistributed_UnionSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	// Both arms scan `users`: a set operation's arms must agree on arity,
	// and events/users do not.
	left := logical.NewScan("users", "u1")
	right := logical.NewScan("users", "u2")
	planner.AnnotateScanColumns(ctx, left)
	planner.AnnotateScanColumns(ctx, right)
	union := logical.NewUnion(left, right, true)
	stages, err := planner.PlanDistributed(ctx, union)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	scanCount := 0
	var unionStage *Stage
	for i, s := range stages {
		if s.Type == StageScan {
			scanCount++
		}
		if s.Type == StageUnion {
			unionStage = &stages[i]
		}
	}
	if scanCount != 2 {
		t.Errorf("expected 2 scan stages for union, got %d", scanCount)
	}
	if unionStage == nil {
		t.Fatal("no union stage: both arms were planned and nothing merged them")
	}
	if len(unionStage.Dependencies) != 2 {
		t.Errorf("union stage depends on %v, want both arms", unionStage.Dependencies)
	}
}

// INTERSECT and EXCEPT need set semantics across the arms, which the stage
// DAG has no operator for. They used to plan "successfully" and answer with
// one arm's rows; refusing is the fix (#346).
func TestPlanDistributed_IntersectSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	intersect := logical.NewIntersect(left, right, false)
	if _, err := planner.PlanDistributed(ctx, intersect); err == nil {
		t.Fatal("INTERSECT planned on the stage DAG; it must be refused, not answered with one arm")
	} else if !strings.Contains(err.Error(), "INTERSECT") {
		t.Errorf("error %q does not name the operation", err)
	}
}

func TestPlanDistributed_ExceptSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewStagePlanner(physical.NewPlanner(cat))

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	except := logical.NewExcept(left, right, true)
	if _, err := planner.PlanDistributed(ctx, except); err == nil {
		t.Fatal("EXCEPT ALL planned on the stage DAG; it must be refused, not answered with one arm")
	} else if !strings.Contains(err.Error(), "EXCEPT ALL") {
		t.Errorf("error %q does not name the operation", err)
	}
}

// --- physical.IsURL / physical.IsGlob helpers ---

// --- physical.DbScanSource ---

// --- CSV named args ---

// helpers

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// --- physical.ResolveNullsLast ---

// --- physical.ExtractFilterBuildColumns ---

// --- physical.CleanExpr ---

// --- PlanDistributed: additional node types ---

func TestPlanDistributed_Window(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	window := logical.NewWindow(scan, []logical.WindowExpr{
		{
			Func:        "row_number",
			OutputCol:   "rn",
			PartitionBy: []string{"src_ip"},
			OrderBy:     []logical.OrderExpr{{Column: "ts"}},
		},
	})

	stages, err := p.PlanDistributed(ctx, window)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for window")
	}
}

func TestPlanDistributed_Aggregate(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	agg := logical.NewAggregate(scan, []string{"src_ip"}, []logical.AggExpr{
		{Func: "count", InputCol: "*", OutputCol: "cnt"},
	})

	stages, err := p.PlanDistributed(ctx, agg)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for aggregate")
	}
}

func TestPlanDistributed_Sort(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewStagePlanner(physical.NewPlanner(cat))

	scan := logical.NewScan("events", "e")
	sort := logical.NewSort(scan, []logical.OrderExpr{{Column: "ts"}})

	stages, err := p.PlanDistributed(ctx, sort)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) < 2 {
		t.Fatalf("expected at least 2 stages (sort + merge_sort), got %d", len(stages))
	}
	// Native-DAG: last stage is exchange-gather, penultimate is sort
	// (merge_sort collapsed by collapseRedundantFinalMergeSort).
	sortStage := stages[len(stages)-2]
	gatherStage := stages[len(stages)-1]
	if sortStage.Type != "sort" {
		t.Errorf("expected sort stage, got %q", sortStage.Type)
	}
	if gatherStage.Type != StageExchangeGather {
		t.Errorf("expected exchange-gather stage, got %q", gatherStage.Type)
	}
	if len(gatherStage.Dependencies) != 1 || gatherStage.Dependencies[0] != sortStage.ID {
		t.Errorf("exchange-gather should depend on sort stage, got %v", gatherStage.Dependencies)
	}
}

func TestPlanDistributed_Join(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	p := NewStagePlanner(physical.NewPlanner(cat))

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	join := logical.NewJoin(left, right, "inner", "e.user_id = u.id")

	stages, err := p.PlanDistributed(ctx, join)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for join")
	}
}
