package physical

import (
	"testing"
)

// TestCollapseMergeTree_Aggregate rewrites a two-level merge_aggregate tree
// back into a single final_aggregate whose deps point directly at the
// leaves the intermediates consumed.
func TestCollapseMergeTree_Aggregate(t *testing.T) {
	// Tree shape mirroring emitMergeAggregateTree output for upstream=40 / fanout=16.
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "merge_aggregate-1-0", Type: "final_aggregate", Dependencies: []string{"scan-0"}, MergeGroup: 0, MergeGroupCount: 3, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}},
		{ID: "merge_aggregate-2-1", Type: "final_aggregate", Dependencies: []string{"scan-0"}, MergeGroup: 1, MergeGroupCount: 3, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}},
		{ID: "merge_aggregate-3-2", Type: "final_aggregate", Dependencies: []string{"scan-0"}, MergeGroup: 2, MergeGroupCount: 3, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}},
		{ID: "final_aggregate-4", Type: "final_aggregate", Dependencies: []string{"merge_aggregate-1-0", "merge_aggregate-2-1", "merge_aggregate-3-2"}, GroupByCols: []string{"k"}, AggSpecs: []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}},
	}

	got := collapseMergeTreesForNativeDAG(stages)

	if len(got) != 2 {
		t.Fatalf("expected 2 stages after collapse (scan + final), got %d: %+v", len(got), stageTypes(got))
	}
	final := got[1]
	if final.ID != "final_aggregate-4" {
		t.Fatalf("expected final_aggregate-4 to be preserved, got ID=%q", final.ID)
	}
	if len(final.Dependencies) != 1 || final.Dependencies[0] != "scan-0" {
		t.Errorf("final deps: got %v, want [scan-0]", final.Dependencies)
	}
	if final.MergeGroupCount != 0 || final.MergeGroup != 0 {
		t.Errorf("final MergeGroup/Count should be cleared: got group=%d count=%d", final.MergeGroup, final.MergeGroupCount)
	}
}

// TestCollapseMergeTree_Sort rewrites a two-level merge_sort tree.
func TestCollapseMergeTree_Sort(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "sort-1", Type: "sort", Dependencies: []string{"scan-0"}},
		{ID: "merge_sort-2-0", Type: "merge_sort", Dependencies: []string{"sort-1"}, MergeGroup: 0, MergeGroupCount: 2, SortKeys: []SortKeySpec{{Column: "v"}}},
		{ID: "merge_sort-3-1", Type: "merge_sort", Dependencies: []string{"sort-1"}, MergeGroup: 1, MergeGroupCount: 2, SortKeys: []SortKeySpec{{Column: "v"}}},
		{ID: "merge_sort-4", Type: "merge_sort", Dependencies: []string{"merge_sort-2-0", "merge_sort-3-1"}, SortKeys: []SortKeySpec{{Column: "v"}}},
	}
	got := collapseMergeTreesForNativeDAG(stages)
	if len(got) != 3 {
		t.Fatalf("expected 3 stages after collapse (scan + sort + final merge), got %d: %+v", len(got), stageTypes(got))
	}
	final := got[2]
	if final.ID != "merge_sort-4" {
		t.Fatalf("final ID: got %q want merge_sort-4", final.ID)
	}
	if len(final.Dependencies) != 1 || final.Dependencies[0] != "sort-1" {
		t.Errorf("final deps: got %v, want [sort-1]", final.Dependencies)
	}
}

// TestCollapseMergeTree_NoOp verifies that plans without a merge tree are
// passed through unchanged — including single-level merge_aggregate /
// merge_sort (where the tree collapse already happened implicitly).
func TestCollapseMergeTree_NoOp(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "final_aggregate-1", Type: "final_aggregate", Dependencies: []string{"scan-0"}},
	}
	got := collapseMergeTreesForNativeDAG(stages)
	if len(got) != 2 {
		t.Fatalf("single-level plan should pass through unchanged, got %d stages", len(got))
	}
}

// TestValidateNativeDAGShape_OK exercises the success path: a clean
// post-collapse, post-skip-fusion plan with the shapes the dispatchers expect.
func TestValidateNativeDAGShape_OK(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "scan-1", Type: "scan"},
		{ID: "join-2", Type: "hash_join", Dependencies: []string{"scan-0", "scan-1"}, LeftDepStage: "scan-0", RightDepStage: "scan-1"},
		{ID: "exchange-3", Type: "exchange-gather", Dependencies: []string{"join-2"}},
	}
	if err := ValidateNativeDAGShape(stages); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

// TestValidateNativeDAGShape_FusedJoinDeps reproduces the Q02 SF10 2026-04-23
// shape: a broadcast_join carrying FusedJoins ends up with >2 deps. Validator
// must catch it at plan time.
func TestValidateNativeDAGShape_FusedJoinDeps(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan"},
		{ID: "scan-1", Type: "scan"},
		{ID: "scan-2", Type: "scan"},
		{ID: "join-3", Type: "broadcast_join", Dependencies: []string{"scan-0", "scan-1", "scan-2"}, FusedJoins: []FusedJoinSpec{{BuildDepStage: "scan-2"}}},
	}
	err := ValidateNativeDAGShape(stages)
	if err == nil {
		t.Fatal("expected error on >2 deps, got nil")
	}
}

// TestValidateNativeDAGShape_IntermediateMergeStage catches a leaked
// merge-tree intermediate (collapseMergeTreesForNativeDAG didn't run or
// didn't recognize the shape).
func TestValidateNativeDAGShape_IntermediateMergeStage(t *testing.T) {
	stages := []Stage{
		{ID: "merge_aggregate-1-0", Type: "final_aggregate", Dependencies: []string{"scan-0"}, MergeGroup: 0, MergeGroupCount: 3},
	}
	err := ValidateNativeDAGShape(stages)
	if err == nil {
		t.Fatal("expected error on MergeGroupCount>0, got nil")
	}
}

// TestValidateNativeDAGShape_BadExchangeDeps catches Exchange stages with
// the wrong dep count (Exchange should always bridge exactly one child to
// its parent).
func TestValidateNativeDAGShape_BadExchangeDeps(t *testing.T) {
	stages := []Stage{
		{ID: "exchange-1", Type: "exchange-repartition", Dependencies: []string{"scan-0", "scan-1"}},
	}
	err := ValidateNativeDAGShape(stages)
	if err == nil {
		t.Fatal("expected error on multi-dep Exchange, got nil")
	}
}
