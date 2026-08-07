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

// q05Fixture mirrors the SF10 Q05 plan shape (coord.log 2026-08-07): the
// PROBE ROOT is orders; lineitem arrives as a shuffle BUILD; supplier,
// nation, and region hang off chained legs; region is the only dimension
// with its own predicate. The two-hop matcher rejects every pairing here
// (nation/supplier: filters=0; supplier/lineitem targets are not the probe
// root); the fixpoint + generalized-target extension must wire the full
// region→nation→supplier→lineitem chain.
func q05Fixture() []Stage {
	return []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f"}, FilterExprs: []string{"o_orderdate >= date '1994-01-01'"},
			Columns: []string{"o_custkey", "o_orderdate", "o_orderkey"}, EstimatedRows: 15_000_000},
		{ID: "scan-3", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"},
			Columns:   []string{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}, EstimatedRows: 60_000_000},
		{ID: "exchange-repartition-5", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-3"},
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 32}},
		{ID: "scan-7", Type: StageScan, TableName: "supplier",
			ScanFiles: []string{"f"}, Columns: []string{"s_suppkey", "s_nationkey"}, EstimatedRows: 100_000},
		{ID: "scan-9", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, Columns: []string{"n_nationkey", "n_regionkey"}, EstimatedRows: 25},
		{ID: "scan-11", Type: StageScan, TableName: "region",
			ScanFiles: []string{"f"}, FilterExprs: []string{"r_name = 'ASIA'"},
			Columns: []string{"r_regionkey", "r_name"}, EstimatedRows: 5},
		{ID: "exchange-replicate-r", Type: StageExchangeReplicate,
			Dependencies: []string{"scan-11"}},
		{ID: "join-6", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"o_orderkey"}, JoinRightKeys: []string{"l_orderkey"},
			LeftDepStage: "scan-0", RightDepStage: "exchange-repartition-5",
			Dependencies: []string{"scan-0", "exchange-repartition-5", "scan-7", "scan-9", "exchange-replicate-r"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"}, BuildDepStage: "scan-7"},
				{JoinType: "inner", JoinLeftKeys: []string{"s_nationkey"}, JoinRightKeys: []string{"n_nationkey"}, BuildDepStage: "scan-9"},
				{JoinType: "inner", JoinLeftKeys: []string{"n_regionkey"}, JoinRightKeys: []string{"r_regionkey"}, BuildDepStage: "exchange-replicate-r"},
			},
		},
	}
}

// The fixpoint must wire Q05's full three-hop snowflake chain:
// region --r_regionkey--> nation --n_nationkey--> supplier --s_suppkey-->
// lineitem, with the last consume landing on the BUILD-side lineitem scan
// (generalized target) and every mid keeping its WAIT stat-dep.
func TestDimensionCascadeThreeHopQ05Shape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := q05Fixture()
	before := DimensionCascadesPlanned.Load()
	p.markDimensionCascade(ctx, stages)

	get := func(id string) *Stage {
		for i := range stages {
			if stages[i].ID == id {
				return &stages[i]
			}
		}
		t.Fatalf("stage %s missing", id)
		return nil
	}
	region, nation, supp, li, orders := get("scan-11"), get("scan-9"), get("scan-7"), get("scan-3"), get("scan-0")

	if len(region.EmitDynamicFilters) != 1 || region.EmitDynamicFilters[0].KeyColumn != "r_regionkey" {
		t.Fatalf("region must emit r_regionkey: %+v", region.EmitDynamicFilters)
	}
	if len(nation.ConsumeDynamicFilters) != 1 || nation.ConsumeDynamicFilters[0].TargetColumn != "n_regionkey" ||
		nation.ConsumeDynamicFilters[0].SourceStageID != "scan-11" {
		t.Fatalf("nation must consume region's bloom on n_regionkey: %+v", nation.ConsumeDynamicFilters)
	}
	if !containsString(nation.Dependencies, "scan-11") {
		t.Fatal("nation must keep the WAIT stat-dep on region")
	}
	if len(nation.EmitDynamicFilters) != 1 || nation.EmitDynamicFilters[0].KeyColumn != "n_nationkey" {
		t.Fatalf("nation must emit n_nationkey: %+v", nation.EmitDynamicFilters)
	}
	if len(supp.ConsumeDynamicFilters) != 1 || supp.ConsumeDynamicFilters[0].TargetColumn != "s_nationkey" {
		t.Fatalf("supplier must consume nation's bloom: %+v", supp.ConsumeDynamicFilters)
	}
	if !containsString(supp.Dependencies, "scan-9") {
		t.Fatal("supplier must keep the WAIT stat-dep on nation")
	}
	if len(supp.EmitDynamicFilters) != 1 || supp.EmitDynamicFilters[0].KeyColumn != "s_suppkey" {
		t.Fatalf("supplier must emit s_suppkey: %+v", supp.EmitDynamicFilters)
	}
	if len(li.ConsumeDynamicFilters) != 1 || li.ConsumeDynamicFilters[0].TargetColumn != "l_suppkey" ||
		li.ConsumeDynamicFilters[0].SourceStageID != "scan-7" {
		t.Fatalf("lineitem (build-side target) must consume supplier's bloom on l_suppkey: %+v", li.ConsumeDynamicFilters)
	}
	if len(li.EmitDynamicFilters) != 0 {
		t.Fatalf("lineitem is a chain TIP, never an emitter: %+v", li.EmitDynamicFilters)
	}
	if len(orders.ConsumeDynamicFilters) != 0 && orders.ConsumeDynamicFilters[0].TargetColumn == "l_suppkey" {
		t.Fatal("orders (probe root) must not receive the l_suppkey bloom")
	}
	if got := DimensionCascadesPlanned.Load() - before; got != 2 {
		t.Fatalf("chain = exactly 2 segments, counter moved %d", got)
	}
}

// Fixpoint termination: re-running the pass on an already-marked plan must
// mark nothing new (the segment dedup) — guards against oscillation.
func TestDimensionCascadeFixpointTerminates(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := q05Fixture()
	p.markDimensionCascade(ctx, stages)
	before := DimensionCascadesPlanned.Load()
	p.markDimensionCascade(ctx, stages)
	if DimensionCascadesPlanned.Load() != before {
		t.Fatal("second pass must be a no-op")
	}
}
