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

// sf100Q05Fixture mirrors the SF100 Q05 plan (zero-EC2 catalog repro,
// 2026-08-07, identical to production qid 1bf185d7): the probe root is the
// UNFILTERED lineitem scan feeding exchange-repartition as a shuffle build
// side — orders (filtered) is the BUILD of join-4, the reverse of the SF10
// shape q05Fixture pins. The fact gate must accept the exchange-fed
// pass-through root (the consume forwards into the exchange's tasks);
// gating on FilterExprs alone rejected every Q05 join at SF100 before leg
// enumeration, which is why the fixpoint shipped inert there.
func sf100Q05Fixture() []Stage {
	return []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"},
			Columns:   []string{"l_discount", "l_extendedprice", "l_orderkey", "l_suppkey"}, EstimatedRows: 600_000_000},
		{ID: "exchange-repartition-2", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 24}},
		{ID: "scan-1", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f"}, FilterExprs: []string{"o_orderdate >= '1994-01-01'"},
			Columns: []string{"o_custkey", "o_orderdate", "o_orderkey"}, EstimatedRows: 150_000_000},
		{ID: "join-4", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "exchange-repartition-2", RightDepStage: "scan-1",
			Dependencies: []string{"exchange-repartition-2", "scan-1", "exchange-replicate-supp"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"}, BuildDepStage: "exchange-replicate-supp"},
			},
		},
		{ID: "scan-5", Type: StageScan, TableName: "supplier",
			ScanFiles: []string{"f"}, Columns: []string{"s_nationkey", "s_suppkey"}, EstimatedRows: 1_000_000},
		{ID: "exchange-replicate-supp", Type: StageExchangeReplicate,
			Dependencies: []string{"scan-5"}},
		{ID: "scan-7", Type: StageScan, TableName: "customer",
			ScanFiles: []string{"f"}, Columns: []string{"c_custkey", "c_nationkey"}, EstimatedRows: 15_000_000},
		{ID: "exchange-repartition-9", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-7"},
			Exchange:     &ExchangeStage{Keys: []string{"c_custkey"}, Count: 24}},
		{ID: "join-10", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"},
			LeftDepStage: "join-4", RightDepStage: "exchange-repartition-9",
			Dependencies: []string{"join-4", "exchange-repartition-9", "scan-11", "exchange-replicate-region"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"s_nationkey"}, JoinRightKeys: []string{"n_nationkey"}, BuildDepStage: "scan-11"},
				{JoinType: "inner", JoinLeftKeys: []string{"n_regionkey"}, JoinRightKeys: []string{"r_regionkey"}, BuildDepStage: "exchange-replicate-region"},
			},
		},
		{ID: "scan-11", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, Columns: []string{"n_name", "n_nationkey", "n_regionkey"}, EstimatedRows: 25},
		{ID: "scan-13", Type: StageScan, TableName: "region",
			ScanFiles: []string{"f"}, FilterExprs: []string{"r_name = 'ASIA'"},
			Columns: []string{"r_name", "r_regionkey"}, EstimatedRows: 5},
		{ID: "exchange-replicate-region", Type: StageExchangeReplicate,
			Dependencies: []string{"scan-13"}},
	}
}

// Regression for the SF100 F gate: the exchange-fed UNFILTERED probe root
// must not block the cascade. Expect the full two-segment chain
// region→nation→supplier→LINEITEM with the tip consume on scan-0 and the
// oversized customer mid correctly excluded. Fails on the pre-fix matcher
// with zero marks.
func TestDimensionCascadeSF100Q05BuildHeavyShape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := sf100Q05Fixture()
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
	region, nation, supp, li, cust := get("scan-13"), get("scan-11"), get("scan-5"), get("scan-0"), get("scan-7")

	if len(region.EmitDynamicFilters) != 1 || region.EmitDynamicFilters[0].KeyColumn != "r_regionkey" {
		t.Fatalf("region must emit r_regionkey: %+v", region.EmitDynamicFilters)
	}
	if len(nation.ConsumeDynamicFilters) != 1 || nation.ConsumeDynamicFilters[0].TargetColumn != "n_regionkey" {
		t.Fatalf("nation must consume region's bloom: %+v", nation.ConsumeDynamicFilters)
	}
	if len(supp.ConsumeDynamicFilters) != 1 || supp.ConsumeDynamicFilters[0].TargetColumn != "s_nationkey" {
		t.Fatalf("supplier must consume nation's bloom: %+v", supp.ConsumeDynamicFilters)
	}
	if len(li.ConsumeDynamicFilters) != 1 || li.ConsumeDynamicFilters[0].TargetColumn != "l_suppkey" ||
		li.ConsumeDynamicFilters[0].SourceStageID != "scan-5" {
		t.Fatalf("lineitem (exchange-fed probe root) must consume supplier's bloom: %+v", li.ConsumeDynamicFilters)
	}
	if !containsString(li.Dependencies, "scan-5") {
		t.Fatal("lineitem must gain the WAIT stat-dep on supplier")
	}
	if len(li.EmitDynamicFilters) != 0 {
		t.Fatalf("lineitem is the chain TIP, never an emitter: %+v", li.EmitDynamicFilters)
	}
	if len(cust.EmitDynamicFilters) != 0 || len(cust.ConsumeDynamicFilters) != 0 {
		t.Fatalf("customer (15M) exceeds the emitter cap and must stay unmarked: %+v %+v",
			cust.EmitDynamicFilters, cust.ConsumeDynamicFilters)
	}
	if got := DimensionCascadesPlanned.Load() - before; got != 2 {
		t.Fatalf("chain = exactly 2 segments, counter moved %d", got)
	}
}

// q08Fixture mirrors the SF100 Q08 plan: nation is scanned TWICE with
// identical column sets — n1 (customer's nation, region-filtered) and n2
// (supplier's nation, which must NOT be filtered: it feeds the market-share
// CASE and needs every nation). Region's leg is keyed "n1.n_regionkey";
// bare-name ownership of n_regionkey is contested between the two nation
// scans, and only the alias qualifiers can tell them apart.
func q08Fixture() []Stage {
	return []Stage{
		{ID: "scan-li", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"},
			Columns:   []string{"l_extendedprice", "l_orderkey", "l_suppkey"}, EstimatedRows: 600_000_000},
		{ID: "er-li", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-li"},
			Exchange:     &ExchangeStage{Keys: []string{"l_orderkey"}, Count: 24}},
		{ID: "scan-orders", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f"}, FilterExprs: []string{"o_orderdate >= '1995-01-01'"},
			Columns: []string{"o_custkey", "o_orderdate", "o_orderkey"}, EstimatedRows: 150_000_000},
		{ID: "join-10", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "er-li", RightDepStage: "scan-orders",
			Dependencies: []string{"er-li", "scan-orders", "scan-supp", "scan-n2"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"}, BuildDepStage: "scan-supp"},
				{JoinType: "inner", JoinLeftKeys: []string{"s_nationkey"}, JoinRightKeys: []string{"n2.n_nationkey"}, BuildDepStage: "scan-n2"},
			},
		},
		{ID: "scan-supp", Type: StageScan, TableName: "supplier",
			ScanFiles: []string{"f"}, Columns: []string{"s_nationkey", "s_suppkey"}, EstimatedRows: 1_000_000},
		{ID: "scan-cust", Type: StageScan, TableName: "customer",
			ScanFiles: []string{"f"}, Columns: []string{"c_custkey", "c_nationkey"}, EstimatedRows: 15_000_000},
		{ID: "er-cust", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-cust"},
			Exchange:     &ExchangeStage{Keys: []string{"c_custkey"}, Count: 24}},
		{ID: "join-14", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"},
			LeftDepStage: "join-10", RightDepStage: "er-cust",
			Dependencies: []string{"join-10", "er-cust", "scan-n1", "scan-region"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"c_nationkey"}, JoinRightKeys: []string{"n1.n_nationkey"}, BuildDepStage: "scan-n1"},
				{JoinType: "inner", JoinLeftKeys: []string{"n1.n_regionkey"}, JoinRightKeys: []string{"r_regionkey"}, BuildDepStage: "scan-region"},
			},
		},
		{ID: "scan-n1", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, Columns: []string{"n_name", "n_nationkey", "n_regionkey"}, EstimatedRows: 25},
		{ID: "scan-n2", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, Columns: []string{"n_name", "n_nationkey", "n_regionkey"}, EstimatedRows: 25},
		{ID: "scan-region", Type: StageScan, TableName: "region",
			ScanFiles: []string{"f"}, FilterExprs: []string{"r_name = 'AMERICA'"},
			Columns: []string{"r_name", "r_regionkey"}, EstimatedRows: 5},
	}
}

// Regression for the hop-A provenance guard: region's bloom must land on
// n1 (whose leg keys carry the n1 qualifier) and NEVER on n2 — a
// region-filtered n2 build silently drops every non-AMERICA-supplier row
// from Q08's market-share denominator. Without the guard the wrong pairing
// marks on the second fixpoint sweep (after the correct n1 segment dedups).
func TestDimensionCascadeQ08AliasProvenance(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := q08Fixture()
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
	n1, n2, cust, supp := get("scan-n1"), get("scan-n2"), get("scan-cust"), get("scan-supp")

	if len(n1.ConsumeDynamicFilters) != 1 || n1.ConsumeDynamicFilters[0].TargetColumn != "n_regionkey" ||
		n1.ConsumeDynamicFilters[0].SourceStageID != "scan-region" {
		t.Fatalf("n1 must consume region's bloom: %+v", n1.ConsumeDynamicFilters)
	}
	if len(cust.ConsumeDynamicFilters) != 1 || cust.ConsumeDynamicFilters[0].TargetColumn != "c_nationkey" ||
		cust.ConsumeDynamicFilters[0].SourceStageID != "scan-n1" {
		t.Fatalf("customer must consume n1's bloom: %+v", cust.ConsumeDynamicFilters)
	}
	if len(n2.ConsumeDynamicFilters) != 0 || len(n2.EmitDynamicFilters) != 0 {
		t.Fatalf("n2 (supplier's nation, unfiltered by region) must stay unmarked: consumes=%+v emits=%+v",
			n2.ConsumeDynamicFilters, n2.EmitDynamicFilters)
	}
	for _, c := range supp.ConsumeDynamicFilters {
		if c.SourceStageID == "scan-n2" {
			t.Fatalf("supplier must not consume a bloom from the misbound n2: %+v", supp.ConsumeDynamicFilters)
		}
	}
}

// With UNQUALIFIED leg keys the contested n_regionkey ownership cannot be
// resolved — the guard must reject the pairing outright rather than guess.
func TestDimensionCascadeUnqualifiedAmbiguityRejects(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages := q08Fixture()
	for i := range stages {
		if stages[i].ID == "join-14" {
			stages[i].ChainedJoins[0].JoinRightKeys = []string{"n_nationkey"}
			stages[i].ChainedJoins[1].JoinLeftKeys = []string{"n_regionkey"}
		}
		if stages[i].ID == "join-10" {
			stages[i].ChainedJoins[1].JoinRightKeys = []string{"n_nationkey"}
		}
	}
	NewPlanner(cat).markDimensionCascade(ctx, stages)
	for i := range stages {
		if n := stages[i]; n.TableName == "nation" &&
			(len(n.ConsumeDynamicFilters) != 0 || len(n.EmitDynamicFilters) != 0) {
			t.Fatalf("%s: contested unqualified ownership must reject, got consumes=%+v emits=%+v",
				n.ID, n.ConsumeDynamicFilters, n.EmitDynamicFilters)
		}
	}
}

// sf100Q07Fixture mirrors the SF100 Q07 shape around join-12: lineitem
// (filtered probe root) → join-8 against exchange-fed orders → join-12
// against exchange-fed customer with n2 (filtered nation) chained on
// c_nationkey. Customer (15M) exceeds the dimension-class emitter cap but
// qualifies as an IN-FLOW mid: exchange-fed, and orders (150M) amortizes
// the added serialization 10×.
func sf100Q07Fixture() []Stage {
	return []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f"}, FilterExprs: []string{"l_shipdate >= '1995-01-01'"},
			Columns: []string{"l_orderkey", "l_suppkey"}, EstimatedRows: 600_000_000},
		{ID: "scan-5", Type: StageScan, TableName: "orders",
			ScanFiles: []string{"f"}, Columns: []string{"o_custkey", "o_orderkey"}, EstimatedRows: 150_000_000},
		{ID: "exchange-repartition-7", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-5"},
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 24}},
		{ID: "join-8", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			LeftDepStage: "scan-0", RightDepStage: "exchange-repartition-7",
			Dependencies: []string{"scan-0", "exchange-repartition-7"},
		},
		{ID: "scan-9", Type: StageScan, TableName: "customer",
			ScanFiles: []string{"f"}, Columns: []string{"c_custkey", "c_nationkey"}, EstimatedRows: 15_000_000},
		{ID: "exchange-repartition-11", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-9"},
			Exchange:     &ExchangeStage{Keys: []string{"c_custkey"}, Count: 24}},
		{ID: "scan-13", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f"}, FilterExprs: []string{"n2.n_name in ('GERMANY', 'FRANCE')"},
			Columns: []string{"n_name", "n_nationkey"}, EstimatedRows: 25},
		{ID: "exchange-replicate-n2", Type: StageExchangeReplicate,
			Dependencies: []string{"scan-13"}},
		{ID: "join-12", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"},
			LeftDepStage: "join-8", RightDepStage: "exchange-repartition-11",
			Dependencies: []string{"join-8", "exchange-repartition-11", "exchange-replicate-n2"},
			ChainedJoins: []ChainedJoinSpec{
				{JoinType: "inner", JoinLeftKeys: []string{"c_nationkey"}, JoinRightKeys: []string{"n2.n_nationkey"}, BuildDepStage: "exchange-replicate-n2"},
			},
		},
	}
}

// The in-flow mid class: customer (15M, exchange-fed) must emit c_custkey
// with InFlow=true — the coordinator keeps such stages OFF the priority
// lane — and orders (its exchange-fed target) must consume with a WAIT
// stat-dep. Fails pre-feature: the flat emitter cap rejected the mid.
func TestDimensionCascadeInFlowMidQ07Shape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	p := NewPlanner(cat)
	stages := sf100Q07Fixture()
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
	n2, cust, orders := get("scan-13"), get("scan-9"), get("scan-5")

	if len(n2.EmitDynamicFilters) != 1 || n2.EmitDynamicFilters[0].KeyColumn != "n_nationkey" ||
		n2.EmitDynamicFilters[0].InFlow {
		t.Fatalf("n2 must emit n_nationkey on the LANE (dimension class): %+v", n2.EmitDynamicFilters)
	}
	if len(cust.ConsumeDynamicFilters) != 1 || cust.ConsumeDynamicFilters[0].TargetColumn != "c_nationkey" {
		t.Fatalf("customer must consume n2's bloom: %+v", cust.ConsumeDynamicFilters)
	}
	if len(cust.EmitDynamicFilters) != 1 || cust.EmitDynamicFilters[0].KeyColumn != "c_custkey" ||
		!cust.EmitDynamicFilters[0].InFlow {
		t.Fatalf("customer must emit c_custkey as IN-FLOW: %+v", cust.EmitDynamicFilters)
	}
	if cust.EmitDynamicFilters[0].BloomBits > cascadeBloomMaxBits {
		t.Fatalf("in-flow bloom must stay residency-clamped: %d", cust.EmitDynamicFilters[0].BloomBits)
	}
	if len(orders.ConsumeDynamicFilters) != 1 || orders.ConsumeDynamicFilters[0].TargetColumn != "o_custkey" ||
		orders.ConsumeDynamicFilters[0].SourceStageID != "scan-9" {
		t.Fatalf("orders must consume customer's bloom on o_custkey: %+v", orders.ConsumeDynamicFilters)
	}
	if !containsString(orders.Dependencies, "scan-9") || !containsString(cust.Dependencies, "scan-13") {
		t.Fatalf("WAIT stat-deps missing: orders=%v cust=%v", orders.Dependencies, cust.Dependencies)
	}
	if got := DimensionCascadesPlanned.Load() - before; got != 1 {
		t.Fatalf("expected exactly 1 segment, counter moved %d", got)
	}
}

// In-flow eligibility negatives: each variant must reject the customer mid.
func TestDimensionCascadeInFlowNegatives(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cases := map[string]func([]Stage){
		"mid not dispatched (no exchange feed)": func(s []Stage) {
			for i := range s {
				if s[i].ID == "exchange-repartition-11" {
					s[i].Dependencies = []string{"somewhere-else"}
				}
			}
		},
		"target too small to amortize": func(s []Stage) {
			for i := range s {
				if s[i].ID == "scan-5" {
					s[i].EstimatedRows = 40_000_000 // < 4× the 15M mid
				}
			}
		},
		"mid above bloom-saturation bound": func(s []Stage) {
			for i := range s {
				if s[i].ID == "scan-9" {
					s[i].EstimatedRows = 150_000_000
				}
				if s[i].ID == "scan-5" {
					s[i].EstimatedRows = 800_000_000
				}
			}
		},
	}
	for name, mutate := range cases {
		stages := sf100Q07Fixture()
		mutate(stages)
		NewPlanner(cat).markDimensionCascade(ctx, stages)
		for i := range stages {
			if stages[i].ID == "scan-9" && len(stages[i].EmitDynamicFilters) != 0 {
				t.Errorf("%s: customer mid unexpectedly emits: %+v", name, stages[i].EmitDynamicFilters)
			}
		}
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
