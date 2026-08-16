package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

func sqlToStagesWithDynamicFilters(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string, workerCount int, broadcastThreshold int64) []Stage {
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
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = workerCount
	planner.DynamicFiltersEnabled = true
	// broadcastThreshold: 0 keeps the planner default; -1 disables
	// broadcast so eligibility tests pin the shuffle regime and stay
	// deterministic as broadcast estimation improves (findScanNode
	// unwrapping Distinct made Q20's dimension joins broadcast-eligible
	// in this tiny-manifest env — correct planning, but it leaves
	// nothing for the DF pass to annotate).
	planner.BroadcastBytesThreshold = broadcastThreshold

	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}

// TestDynamicFilterPassQ17 verifies the planner does the right thing
// for Q17 (small-quantity-order) under DynamicFiltersEnabled.
//
// Original v1 test asserted Emit/Consume annotations were attached.
// After the filter-aware broadcast threshold change (CBO Phase 4),
// Q17's small filtered `part` build (~13K rows ≈ 1.5 MB) is below
// the broadcast threshold and the join becomes a broadcast_join
// instead of hash_join. Broadcast joins ship the build to every
// worker — so a separate dynamic-filter bloom is redundant (the
// build is already present everywhere). The planner correctly
// skips annotating broadcast joins.
//
// This test now confirms: Q17 plan contains a broadcast_join (the
// architecturally-correct shape) and ZERO dynamic-filter annotations.
// The pre-broadcast hash-shuffle + dynamic-filter combo is strictly
// worse — broadcast eliminates the lineitem shuffle that the bloom
// could only have partially mitigated.
func TestDynamicFilterPassQ17(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT
		SUM(l_extendedprice) / 7.0 as avg_yearly
	FROM lineitem
	JOIN part ON p_partkey = l_partkey
	WHERE p_brand = 'Brand#23'
		AND p_container = 'MED BOX'
		AND l_quantity < (
			SELECT 0.2 * AVG(l_quantity)
			FROM lineitem
			WHERE l_partkey = p_partkey
		)`

	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3, 0)

	hasBroadcast := false
	emits, consumes := 0, 0
	for _, s := range stages {
		if s.Type == StageBroadcastJoin {
			hasBroadcast = true
		}
		emits += len(s.EmitDynamicFilters)
		consumes += len(s.ConsumeDynamicFilters)
	}
	if !hasBroadcast {
		t.Errorf("expected at least one broadcast_join in Q17 plan; got none")
	}
	if emits != 0 || consumes != 0 {
		t.Errorf("expected zero dynamic-filter annotations (broadcast joins skipped); got emits=%d consumes=%d", emits, consumes)
	}
}

// TestDynamicFilterPassDisabled confirms the gate — flag off → no annotations.
func TestDynamicFilterPassDisabled(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT SUM(l_extendedprice)
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23'`
	stages := sqlToStages(t, cat, ctx, sql, 3) // default planner — flag off.
	for _, s := range stages {
		if len(s.EmitDynamicFilters) > 0 {
			t.Errorf("stage %s has EmitDynamicFilters when flag is off", s.ID)
		}
		if len(s.ConsumeDynamicFilters) > 0 {
			t.Errorf("stage %s has ConsumeDynamicFilters when flag is off", s.ID)
		}
	}
}

// TestDynamicFilterPass_AnnotatesShuffledJoin: a shuffled single-int-key
// inner join gets Emit/Consume annotations and bumps DynamicFiltersPlanned —
// the observability contract A/B runs rely on to prove the pass fired
// (the 2026-07-08 revisit pair was unverifiable without it).
func TestDynamicFilterPass_AnnotatesShuffledJoin(t *testing.T) {
	cat := setupJoinTables(t)
	ctx := context.Background()
	parsed, err := plansql.Parse("SELECT id, val, rval FROM smj_l JOIN smj_r ON id = rid")
	if err != nil {
		t.Fatal(err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatal(err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 3
	planner.BroadcastBytesThreshold = -1 // force the shuffled hash join
	planner.DynamicFiltersEnabled = true

	before := DynamicFiltersPlanned.Load()
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatal(err)
	}
	if got := DynamicFiltersPlanned.Load() - before; got == 0 {
		t.Fatal("DynamicFiltersPlanned did not move — the pass never annotated")
	}
	emits, consumes := 0, 0
	for _, s := range stages {
		emits += len(s.EmitDynamicFilters)
		consumes += len(s.ConsumeDynamicFilters)
	}
	if emits == 0 || consumes == 0 {
		t.Fatalf("expected emit+consume annotations, got emits=%d consumes=%d", emits, consumes)
	}
}
