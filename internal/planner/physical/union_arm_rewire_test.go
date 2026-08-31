package physical

import "testing"

// A stage-deleting pass rewires EVERY reference to the stage it deletes, and
// `UnionArm.DepStage` is one of them (METHOD 10).
//
// `ValidateNativeDAGShape` requires `UnionArms[i].DepStage == Dependencies[i]`
// on every union stage, so a pass that rewires the dependency list and not the
// arm builds a plan its own validator rejects at dispatch. Both
// `fuseScanShuffle` and `elideCoPartitionedExchanges` were taught to rewire it
// alongside `ChainedJoinSpec.BuildDepStage`, and the claim that the rewiring is
// COMPLETE is what needs a fixture.
//
// An SQL fixture cannot supply one. A corpus of eight union shapes — unions of
// joins, of grouped aggregates, of derived arms, UNION and UNION ALL, with and
// without a join above — reaches NEITHER loop, verified by panicking inside
// both of them and watching every shape pass. These two tests drive the passes
// directly instead, and each one asserts a different thing:
//
//   - fuseScanShuffle DECLINES, and the reason is condition 4 rather than
//     luck. A union stage lists its arms' producers in Dependencies (the
//     invariant above), so it is one of the exchange's consumers, and
//     condition 4 admits only hash_join, sort_merge_join and final_aggregate.
//     The loop in that pass is unreachable BY CONSTRUCTION; this pins the
//     construction, so a future widening of condition 4 that admits a union
//     fails here rather than silently relying on a rewire nothing ever ran.
//   - elideCoPartitionedExchanges has no consumer-type condition and IS
//     reachable. This drives it and asserts the arm was rewired, which is the
//     loop actually executing.
func TestFuseScanShuffleDeclinesAUnionArmsExchange(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			Columns:     []string{"id", "a"},
			FilterExprs: []string{"a > 1"}, // a DISPATCHED-shape scan: fusable
		},
		{
			ID: "exchange-repartition-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"id"}, Count: 4},
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4, Keys: []string{"id"}},
		},
		{
			ID: "union-2", Type: StageUnion,
			Dependencies: []string{"exchange-repartition-1"},
			UnionArms:    []UnionArm{{DepStage: "exchange-repartition-1"}},
		},
	}
	out := fuseScanShuffle(stages)
	if len(out) != len(stages) {
		t.Fatalf("fuseScanShuffle absorbed the exchange a UNION reads: %d stages, want %d.\n"+
			"Condition 4 admits only hash_join, sort_merge_join and final_aggregate, and a "+
			"union stage lists its arm's producer in Dependencies — so it is a consumer and "+
			"the fusion must decline. If this pass may now absorb it, the UnionArms rewiring "+
			"in fuseScanShuffle has become REACHABLE and needs a test that exercises it.",
			len(out), len(stages))
	}
	for _, s := range out {
		if s.ID != "union-2" {
			continue
		}
		if s.UnionArms[0].DepStage != "exchange-repartition-1" ||
			s.Dependencies[0] != "exchange-repartition-1" {
			t.Errorf("the union arm and its dependency disagree after a declined fusion: "+
				"arm=%q dep=%q", s.UnionArms[0].DepStage, s.Dependencies[0])
		}
	}
}

func TestElideCoPartitionedExchangeRewiresAUnionArm(t *testing.T) {
	// The producer is ALREADY hash-partitioned exactly as the exchange above
	// it would partition it — the identity re-shuffle this pass removes.
	stages := []Stage{
		{
			ID: "join-0", Type: StageHashJoin,
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4, Keys: []string{"id"}},
		},
		{
			ID: "exchange-repartition-1", Type: StageExchangeRepartition,
			Dependencies: []string{"join-0"},
			Exchange:     &ExchangeStage{Keys: []string{"id"}, Count: 4},
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4, Keys: []string{"id"}},
		},
		{
			ID: "scan-2", Type: StageScan, TableName: "t",
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4, Keys: []string{"id"}},
		},
		{
			ID: "union-3", Type: StageUnion,
			Dependencies: []string{"exchange-repartition-1", "scan-2"},
			UnionArms: []UnionArm{
				{DepStage: "exchange-repartition-1"},
				{DepStage: "scan-2"},
			},
		},
	}
	out := elideCoPartitionedExchanges(stages)
	if len(out) != len(stages)-1 {
		t.Fatalf("the identity exchange was not elided: %d stages, want %d — this test's "+
			"premise is gone and the rewiring below is no longer exercised",
			len(out), len(stages)-1)
	}
	var union *Stage
	for i := range out {
		if out[i].ID == "union-3" {
			union = &out[i]
		}
	}
	if union == nil {
		t.Fatal("the union stage disappeared")
	}
	// The whole point: Dependencies and UnionArms have to move TOGETHER, or
	// ValidateNativeDAGShape refuses the plan at dispatch.
	if union.Dependencies[0] != "join-0" {
		t.Errorf("Dependencies[0] = %q, want join-0", union.Dependencies[0])
	}
	if union.UnionArms[0].DepStage != "join-0" {
		t.Errorf("UnionArms[0].DepStage = %q, want join-0 — the elision rewired the "+
			"dependency and left the arm naming a stage it had just deleted, which "+
			"ValidateNativeDAGShape refuses at dispatch", union.UnionArms[0].DepStage)
	}
	if union.UnionArms[1].DepStage != union.Dependencies[1] {
		t.Errorf("the untouched arm drifted: arm=%q dep=%q",
			union.UnionArms[1].DepStage, union.Dependencies[1])
	}
}
