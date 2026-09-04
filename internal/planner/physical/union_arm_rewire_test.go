package physical

import "testing"

// A union stage keeps ONE record of each arm's producer (#715, METHOD 10).
//
// The arm used to carry a `DepStage` copy of `Dependencies[i]`, written at
// construction, and six passes had to keep the two in step. Whichever one
// moved a dependency without the copy left the pair inconsistent, and
// ValidateNativeDAGShape then refused the plan with a plain error that reached
// the client — for queries the single-process path answers. The copy is gone;
// `Stage.UnionArmDep(i)` reads `Dependencies[i]`, and these tests drive the
// three stage-rewriting passes directly to assert that a union's arms follow
// the dependency list with no per-arm rewiring anywhere.
//
// An SQL fixture cannot reach two of them: a corpus of eight union shapes —
// unions of joins, of grouped aggregates, of derived arms, UNION and UNION
// ALL, with and without a join above — reaches neither `fuseScanShuffle`'s nor
// `elideCoPartitionedExchanges`' union path, verified by panicking inside both
// and watching every shape pass. `collapseRedundantFinalMergeSort` IS reachable
// from SQL, and it is the pass that broke #715.

// fuseScanShuffle DECLINES to absorb an exchange a union reads, and the reason
// is condition 4 rather than luck: a union stage lists its arms' producers in
// Dependencies, so it is one of the exchange's consumers, and condition 4
// admits only hash_join, sort_merge_join and final_aggregate. Pinning the
// construction means a future widening that admits a union fails here.
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
			UnionArms:    []UnionArm{{}},
		},
	}
	out := fuseScanShuffle(stages)
	if len(out) != len(stages) {
		t.Fatalf("fuseScanShuffle absorbed the exchange a UNION reads: %d stages, want %d.\n"+
			"Condition 4 admits only hash_join, sort_merge_join and final_aggregate, and a "+
			"union stage lists its arm's producer in Dependencies — so it is a consumer and "+
			"the fusion must decline.", len(out), len(stages))
	}
	for _, s := range out {
		if s.ID != "union-2" {
			continue
		}
		if s.UnionArmDep(0) != "exchange-repartition-1" {
			t.Errorf("the union arm's producer moved after a declined fusion: %q", s.UnionArmDep(0))
		}
	}
}

// elideCoPartitionedExchanges has no consumer-type condition and IS reachable
// by construction: it rewrites Dependencies, and the arm's producer follows.
func TestElideCoPartitionedExchangeMovesAUnionArmsProducer(t *testing.T) {
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
			UnionArms:    []UnionArm{{}, {}},
		},
	}
	out := elideCoPartitionedExchanges(stages)
	if len(out) != len(stages)-1 {
		t.Fatalf("the identity exchange was not elided: %d stages, want %d — this test's "+
			"premise is gone", len(out), len(stages)-1)
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
	if union.UnionArmDep(0) != "join-0" {
		t.Errorf("arm 0's producer is %q, want join-0 — the elision deleted the stage it "+
			"named", union.UnionArmDep(0))
	}
	if union.UnionArmDep(1) != "scan-2" {
		t.Errorf("the untouched arm's producer moved: %q", union.UnionArmDep(1))
	}
}

// collapseRedundantFinalMergeSort is #715's own pass: it drops a trivial
// merge_sort and repoints its consumers at the sort below. It rewrote
// Dependencies, LeftDepStage and RightDepStage and NOT the arm's stored copy,
// which is what made "arm 0 names producer merge_sort-2 but Dependencies[0] is
// sort-1" reach the client for a union over two identical sorted producers.
//
// The two arms deliberately share ONE producer, which is the shape both #715
// and #660 turn on: identical subplans are deduped, and a CTE referenced twice
// is deduped by construction.
func TestCollapseFinalMergeSortMovesEveryUnionArmsProducer(t *testing.T) {
	stages := []Stage{
		{ID: "sort-1", Type: StageSort, Distribution: Distribution{Kind: DistSingleton}},
		{
			ID: "merge_sort-2", Type: StageMergeSort,
			Dependencies: []string{"sort-1"},
			Distribution: Distribution{Kind: DistSingleton},
		},
		{
			ID: "union-3", Type: StageUnion,
			Dependencies: []string{"merge_sort-2", "merge_sort-2"},
			UnionArms:    []UnionArm{{}, {}},
		},
	}
	out := collapseRedundantFinalMergeSort(stages)
	var union *Stage
	for i := range out {
		if out[i].ID == "union-3" {
			union = &out[i]
		}
	}
	if union == nil {
		t.Fatal("the union stage disappeared")
	}
	for i := range union.UnionArms {
		if got := union.UnionArmDep(i); got != "sort-1" {
			t.Errorf("arm %d's producer is %q, want sort-1 — the collapse deleted merge_sort-2",
				i, got)
		}
	}
	if err := ValidateNativeDAGShape(out); err != nil {
		t.Errorf("the collapsed plan does not validate: %v", err)
	}
}
