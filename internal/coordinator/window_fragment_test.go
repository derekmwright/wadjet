package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// derefType reads a wire OutputType, reporting -1 for an absent declaration
// so a nil never compares equal to BOOL (parquet.TypeID 0).
func derefType(p *int) int {
	if p == nil {
		return -1
	}
	return *p
}

// TestBuildWindowFragment pins the fragment a window stage dispatches as.
// Before #349 there was none: walkStages emitted the stage, no migration
// claimed it, and the task shipped with Operators == nil, which the worker
// rejects with "empty Operators".
func TestBuildWindowFragment(t *testing.T) {
	stage := physical.Stage{
		ID:           "window-1",
		Type:         physical.StageWindow,
		Dependencies: []string{"scan-0"},
		WindowCols: []physical.WindowColSpec{
			{
				Func:        "row_number",
				OutputCol:   "rn",
				OutputType:  parquet.TypeInt64,
				PartitionBy: []string{"n_regionkey"},
				OrderBy:     []physical.SortKeySpec{{Column: "n_nationkey", NullsLast: true}},
			},
			{
				Func:       "nth_value",
				InputCol:   "n_name",
				OutputCol:  "second",
				OutputType: parquet.TypeString,
				OrderBy:    []physical.SortKeySpec{{Column: "n_nationkey"}},
				Frame: &logical.WindowFrameSpec{
					Mode:  "rows",
					Start: logical.WindowBound{Type: "unbounded_preceding"},
					End:   logical.WindowBound{Type: "unbounded_following"},
				},
				NthValueN: 2,
			},
		},
	}
	task := &distributed.Task{ID: "t-0", DataBucket: "bkt"}
	taskInputs := map[string][]string{"scan-0": {"queries/q/scan-0/a.wshf"}}

	t.Run("canonical shape", func(t *testing.T) {
		ops, err := buildWindowFragment(stage, task, taskInputs, "")
		if err != nil {
			t.Fatalf("buildWindowFragment: %v", err)
		}
		wantTypes := []distributed.OpType{
			distributed.OpShuffleSource,
			distributed.OpWindow,
			distributed.OpUnpartitionedSink,
		}
		if len(ops) != len(wantTypes) {
			t.Fatalf("got %d ops, want %d: %+v", len(ops), len(wantTypes), ops)
		}
		for i, want := range wantTypes {
			if ops[i].Type != want {
				t.Errorf("op %d: got %q, want %q", i, ops[i].Type, want)
			}
		}
		// No OpSort ahead of the window: a window's ORDER BY defines its
		// frame, and exec.Window sorts each partition itself. An OpSort
		// here would be redundant work, and could not serve two columns
		// with different ORDER BYs anyway.
		for _, op := range ops {
			if op.Type == distributed.OpSort {
				t.Errorf("fragment carries an OpSort: %+v", ops)
			}
		}
		if ops[0].InputAlias != "scan-0" || len(ops[0].InputFiles) != 1 {
			t.Errorf("source op = %+v, want the dep's files under its own alias", ops[0])
		}
		if ops[0].InputBucket != "bkt" {
			t.Errorf("source bucket = %q, want the task's data bucket", ops[0].InputBucket)
		}

		cols := ops[1].WindowCols
		if len(cols) != 2 {
			t.Fatalf("got %d window columns, want 2", len(cols))
		}
		if cols[0].Func != "row_number" || cols[0].OutputCol != "rn" ||
			derefType(cols[0].OutputType) != int(parquet.TypeInt64) {
			t.Errorf("column 0 = %+v, want row_number → rn : Int64", cols[0])
		}
		if len(cols[0].PartitionBy) != 1 || cols[0].PartitionBy[0] != "n_regionkey" {
			t.Errorf("column 0 PartitionBy = %v, want [n_regionkey]", cols[0].PartitionBy)
		}
		if len(cols[0].OrderBy) != 1 || cols[0].OrderBy[0].Column != "n_nationkey" ||
			!cols[0].OrderBy[0].PlaceNullsLast() {
			t.Errorf("column 0 OrderBy = %+v, want n_nationkey NULLS LAST", cols[0].OrderBy)
		}
		// A window value function's output type IS its input column's type
		// (#345), and nothing downstream of the worker corrects a wrong
		// declaration — so the wire spec has to carry it.
		if derefType(cols[1].OutputType) != int(parquet.TypeString) {
			t.Errorf("column 1 OutputType = %v, want String (%d)", cols[1].OutputType, parquet.TypeString)
		}
		if cols[1].NthValueN != 2 {
			t.Errorf("column 1 NthValueN = %d, want 2 — the N lives in the SQL argument list "+
				"and the worker has no parser for it", cols[1].NthValueN)
		}
		if cols[1].Frame == nil || cols[1].Frame.Mode != "rows" ||
			cols[1].Frame.Start.Type != "unbounded_preceding" ||
			cols[1].Frame.End.Type != "unbounded_following" {
			t.Errorf("column 1 Frame = %+v, want the whole-partition ROWS frame", cols[1].Frame)
		}
	})

	t.Run("gather sink when fused", func(t *testing.T) {
		ops, err := buildWindowFragment(stage, task, taskInputs, "wadjet.gather.q-fused")
		if err != nil {
			t.Fatalf("buildWindowFragment: %v", err)
		}
		last := ops[len(ops)-1]
		if last.Type != distributed.OpGatherSink || last.ReplySubject != "wadjet.gather.q-fused" {
			t.Errorf("terminal op = %+v, want a gather sink on the fused subject", last)
		}
	})

	t.Run("a predicate above the window runs after it", func(t *testing.T) {
		filtered := *task
		filtered.PostFilterExprs = []string{"rn <= 3"}
		ops, err := buildWindowFragment(stage, &filtered, taskInputs, "")
		if err != nil {
			t.Fatalf("buildWindowFragment: %v", err)
		}
		// The predicate names the window's OUTPUT column, so it cannot run
		// before the operator — and dropping it would return every row.
		if len(ops) != 4 || ops[2].Type != distributed.OpFilter ||
			len(ops[2].Predicates) != 1 || ops[2].Predicates[0] != "rn <= 3" {
			t.Fatalf("ops = %+v, want [source, window, filter(rn <= 3), sink]", ops)
		}
	})

	t.Run("a declared BOOL is distinguishable from no declaration", func(t *testing.T) {
		// parquet.TypeID's zero value is BOOL, so an int OutputType would
		// ship LAG(bool_col) as "undeclared" and the worker would build a
		// Float64 vector for it — #345 for exactly one type.
		boolStage := stage
		boolStage.WindowCols = []physical.WindowColSpec{{
			Func: "lag", InputCol: "flag", OutputCol: "prev_flag", OutputType: parquet.TypeBool,
		}}
		ops, err := buildWindowFragment(boolStage, task, taskInputs, "")
		if err != nil {
			t.Fatalf("buildWindowFragment: %v", err)
		}
		got := ops[1].WindowCols[0].OutputType
		if got == nil {
			t.Fatal("OutputType is nil for a declared BOOL — indistinguishable from an absent declaration")
		}
		if *got != int(parquet.TypeBool) {
			t.Errorf("OutputType = %d, want Bool (%d)", *got, parquet.TypeBool)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		if _, err := buildWindowFragment(stage, task, map[string][]string{
			"scan-0": {"a.wshf"}, "scan-1": {"b.wshf"},
		}, ""); err == nil {
			t.Error("expected an error for a window stage with two input aliases")
		}
		bare := stage
		bare.WindowCols = nil
		if _, err := buildWindowFragment(bare, task, taskInputs, ""); err == nil {
			t.Error("expected an error for a window stage carrying no window columns")
		}
	})
}
