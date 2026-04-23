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
