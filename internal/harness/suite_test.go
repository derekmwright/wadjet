package harness

import "testing"

func TestSliceConfigs(t *testing.T) {
	if c := SliceConfigs[SliceSmall]; c.LineitemFiles != 4 {
		t.Errorf("small lineitem: want 4, got %d", c.LineitemFiles)
	}
	if c := SliceConfigs[SliceLarge]; !c.ExpectSpill {
		t.Error("large slice should ExpectSpill")
	}
}

func TestSelectQueriesEmpty(t *testing.T) {
	got := SelectQueries(nil)
	if len(got) != 23 {
		t.Errorf("want 22+1 queries, got %d", len(got))
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
