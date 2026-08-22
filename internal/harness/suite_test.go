package harness

import "testing"

func TestSliceConfigs(t *testing.T) {
	if c := SliceConfigs[SliceSmall]; c.LineitemFiles != 4 {
		t.Errorf("small lineitem: want 4, got %d", c.LineitemFiles)
	}
	if c := SliceConfigs[SliceLarge]; !c.ExpectSpill {
		t.Error("large slice should ExpectSpill")
	}
	// GOMEMLIMIT alone never forces spill (the engine auto-detects a
	// per-task budget from cgroup/physical memory and floors it near 2 GB
	// regardless — cmd/wadjet/main.go minBudgetPerTask). A slice that
	// ExpectSpill must carry an explicit MemoryBudget to actually bypass
	// that auto-tuning, or the run-level spill_paths_exercised assertion
	// in Run() can never pass.
	if c := SliceConfigs[SliceSmall]; c.MemoryBudget != 0 {
		t.Errorf("small slice: want MemoryBudget 0 (unset), got %d", c.MemoryBudget)
	}
	if c := SliceConfigs[SliceLarge]; c.ExpectSpill && c.MemoryBudget <= 0 {
		t.Error("large slice ExpectSpill but MemoryBudget is unset — spill_paths_exercised can never pass")
	}
}

func TestSelectQueriesEmpty(t *testing.T) {
	got := SelectQueries(nil)
	if len(got) != 25 { // 22 TPC-H + 3 micros
		t.Errorf("want 22+3 queries, got %d", len(got))
	}
}

func TestLoadQueryValid(t *testing.T) {
	sql, err := LoadQuery("q05")
	if err != nil {
		t.Fatalf("LoadQuery q05: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL for q05")
	}
}

func TestLoadQueryInvalid(t *testing.T) {
	_, err := LoadQuery("q99")
	if err == nil {
		t.Error("expected error for q99")
	}
}
