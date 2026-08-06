package physical

import (
	"context"
	"testing"
)

// Q21's SF100 shape: join J probes the filtered lineitem scan with the
// supplier scan as primary build and nation (tiny, filtered) as a chained
// build keyed on s_nationkey — a column owned by the supplier scan. The
// cascade must wire nation→supplier (hop A) and supplier→lineitem (hop B)
// with stat-dep edges, post-filter AtOutput emits, and L2-capped blooms.
func cascadeFixture() []Stage {
	return []Stage{
		{ID: "scan-l1", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"}, FilterExprs: []string{"l_receiptdate > l_commitdate"},
			Columns: []string{"l_orderkey", "l_suppkey"}, EstimatedRows: 600_000_000},
		{ID: "scan-supp", Type: StageScan, TableName: "supplier",
			ScanFiles: []string{"f"}, Columns: []string{"s_suppkey", "s_nationkey", "s_name"},
			EstimatedRows: 1_000_000},
		{ID: "scan-nation", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, FilterExprs: []string{"n_name = 'SAUDI ARABIA'"},
			Columns: []string{"n_nationkey", "n_name"}, EstimatedRows: 25},
		{ID: "join", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"},
			LeftDepStage: "scan-l1", RightDepStage: "scan-supp",
			Dependencies: []string{"scan-l1", "scan-supp", "scan-nation"},
			ChainedJoins: []ChainedJoinSpec{{
				JoinType:      "inner",
				JoinLeftKeys:  []string{"s_nationkey"},
				JoinRightKeys: []string{"n_nationkey"},
				BuildDepStage: "scan-nation",
			}},
		},
	}
}

func TestDimensionCascadeMarksQ21Shape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := cascadeFixture()
	before := DimensionCascadesPlanned.Load()
	p.markDimensionCascade(ctx, stages)

	var nation, supp, l1 *Stage
	for i := range stages {
		switch stages[i].ID {
		case "scan-nation":
			nation = &stages[i]
		case "scan-supp":
			supp = &stages[i]
		case "scan-l1":
			l1 = &stages[i]
		}
	}
	if len(nation.EmitDynamicFilters) != 1 || !nation.EmitDynamicFilters[0].AtOutput ||
		nation.EmitDynamicFilters[0].KeyColumn != "n_nationkey" {
		t.Fatalf("hop A emit missing/wrong on nation: %+v", nation.EmitDynamicFilters)
	}
	if len(supp.ConsumeDynamicFilters) != 1 || supp.ConsumeDynamicFilters[0].TargetColumn != "s_nationkey" {
		t.Fatalf("hop A consume missing/wrong on supplier: %+v", supp.ConsumeDynamicFilters)
	}
	if len(supp.EmitDynamicFilters) != 1 || supp.EmitDynamicFilters[0].KeyColumn != "s_suppkey" {
		t.Fatalf("hop B emit missing/wrong on supplier: %+v", supp.EmitDynamicFilters)
	}
	if len(l1.ConsumeDynamicFilters) != 1 || l1.ConsumeDynamicFilters[0].TargetColumn != "l_suppkey" {
		t.Fatalf("hop B consume missing/wrong on lineitem: %+v", l1.ConsumeDynamicFilters)
	}
	if !containsString(supp.Dependencies, "scan-nation") || !containsString(l1.Dependencies, "scan-supp") {
		t.Fatalf("stat-dep edges missing: supp=%v l1=%v", supp.Dependencies, l1.Dependencies)
	}
	for _, e := range append(nation.EmitDynamicFilters, supp.EmitDynamicFilters...) {
		if e.BloomBits > cascadeBloomMaxBits {
			t.Fatalf("bloom %s exceeds residency cap: %d", e.FilterID, e.BloomBits)
		}
	}
	if DimensionCascadesPlanned.Load() == before {
		t.Fatal("counter did not increment")
	}
}

func TestDimensionCascadeNegatives(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cases := map[string]func([]Stage){
		"unfiltered dimension": func(s []Stage) { s[2].FilterExprs = nil },
		"oversized emitter":    func(s []Stage) { s[2].EstimatedRows = 50_000_000 },
		"outer join":           func(s []Stage) { s[3].JoinType = "right" },
		"unfiltered fact scan": func(s []Stage) { s[0].FilterExprs = nil },
		"chained key not on primary build": func(s []Stage) {
			s[3].ChainedJoins[0].JoinLeftKeys = []string{"o_orderdate"}
		},
	}
	for name, mutate := range cases {
		stages := cascadeFixture()
		mutate(stages)
		NewPlanner(cat).markDimensionCascade(ctx, stages)
		for i := range stages {
			if len(stages[i].ConsumeDynamicFilters) != 0 {
				t.Errorf("%s: stage %s unexpectedly marked", name, stages[i].ID)
			}
		}
	}
}

func TestDimensionCascadeKillSwitch(t *testing.T) {
	DimensionCascade.Store(false)
	defer DimensionCascade.Store(true)
	cat, ctx := setupTPCHCatalog(t)
	stages := cascadeFixture()
	NewPlanner(cat).markDimensionCascade(context.Background(), stages)
	_ = ctx
	for i := range stages {
		if len(stages[i].EmitDynamicFilters) != 0 {
			t.Fatal("kill switch must leave stages unmarked")
		}
	}
}

// The ACTUAL SF100 Q21 shape (results/20260806-110951 ground truth):
// orders is the PRIMARY build; supplier rides as a fused leg; nation is
// chained keyed on s_nationkey — a column owned by the SUPPLIER leg's
// build, not the primary. The generalized leg-pair matcher must wire
// nation→supplier→lineitem here too.
func TestDimensionCascadeMarksRealSF100Shape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := []Stage{
		{ID: "scan-l1", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"}, FilterExprs: []string{"l_receiptdate > l_commitdate"},
			Columns: []string{"l_orderkey", "l_suppkey"}, EstimatedRows: 600_000_000},
		{ID: "scan-orders", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f"}, FilterExprs: []string{"o_orderstatus = 'F'"},
			Columns: []string{"o_orderkey"}, EstimatedRows: 150_000_000},
		{ID: "scan-supp", Type: StageScan, TableName: "supplier",
			ScanFiles: []string{"f"}, Columns: []string{"s_suppkey", "s_nationkey", "s_name"},
			EstimatedRows: 1_000_000},
		{ID: "scan-nation", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, FilterExprs: []string{"n_name = 'SAUDI ARABIA'"},
			Columns: []string{"n_nationkey", "n_name"}, EstimatedRows: 25},
		{ID: "join", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "scan-l1", RightDepStage: "scan-orders",
			Dependencies: []string{"scan-l1", "scan-orders", "scan-supp", "scan-nation"},
			FusedJoins: []FusedJoinSpec{{
				JoinType:      "inner",
				JoinLeftKeys:  []string{"l_suppkey"},
				JoinRightKeys: []string{"s_suppkey"},
				BuildDepStage: "scan-supp",
			}},
			ChainedJoins: []ChainedJoinSpec{{
				JoinType:      "inner",
				JoinLeftKeys:  []string{"s_nationkey"},
				JoinRightKeys: []string{"n_nationkey"},
				BuildDepStage: "scan-nation",
			}},
		},
	}
	p := NewPlanner(cat)
	p.markDimensionCascade(ctx, stages)
	var nation, supp, l1 *Stage
	for i := range stages {
		switch stages[i].ID {
		case "scan-nation":
			nation = &stages[i]
		case "scan-supp":
			supp = &stages[i]
		case "scan-l1":
			l1 = &stages[i]
		}
	}
	if len(nation.EmitDynamicFilters) != 1 || nation.EmitDynamicFilters[0].KeyColumn != "n_nationkey" {
		t.Fatalf("hop A emit wrong: %+v", nation.EmitDynamicFilters)
	}
	if len(supp.ConsumeDynamicFilters) != 1 || supp.ConsumeDynamicFilters[0].TargetColumn != "s_nationkey" {
		t.Fatalf("hop A consume wrong: %+v", supp.ConsumeDynamicFilters)
	}
	if len(supp.EmitDynamicFilters) != 1 || supp.EmitDynamicFilters[0].KeyColumn != "s_suppkey" {
		t.Fatalf("hop B emit wrong: %+v", supp.EmitDynamicFilters)
	}
	if len(l1.ConsumeDynamicFilters) != 1 || l1.ConsumeDynamicFilters[0].TargetColumn != "l_suppkey" {
		t.Fatalf("hop B consume wrong: %+v", l1.ConsumeDynamicFilters)
	}
}
