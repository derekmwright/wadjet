package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestBuildAggregateFragment_EmitEmptyIdentity pins WHICH aggregate in the
// DAG owes SQL an identity row over zero input (#329).
//
// Exactly one may: the UNGROUPED final. It is the query's answer for those
// aggregates and the planner distributes it as a Singleton, so the row it
// emits cannot be duplicated. Marking a partial or a merge_aggregate too
// would put N identity rows into the stage above — and would put a
// planner-typed row alongside runtime-typed siblings, which is the merge
// hazard the empty-output short-circuit exists to avoid.
func TestBuildAggregateFragment_EmitEmptyIdentity(t *testing.T) {
	aggs := []distributed.AggSpec{{
		Func: "sum", InputCol: "v", OutputCol: "total",
		OutputType: int(parquet.TypeFloat64),
	}}
	tests := []struct {
		name  string
		stage physical.Stage
		want  bool
	}{
		{"ungrouped final owes the row", physical.Stage{Type: "final_aggregate"}, true},
		{"grouped final owes nothing — no groups, no rows",
			physical.Stage{Type: "final_aggregate", GroupByCols: []string{"g"}}, false},
		{"DISTINCT final groups by every column",
			physical.Stage{Type: "final_aggregate", GroupByAll: true}, false},
		{"partial aggregate is absorbed by the final above it",
			physical.Stage{Type: "aggregate"}, false},
		{"merge_aggregate is absorbed by the final above it",
			physical.Stage{Type: "merge_aggregate"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &distributed.Task{DataBucket: "buk"}
			ops, err := buildAggregateFragment(tt.stage, task,
				map[string][]string{"upstream": {"f1.wshf"}}, aggs, nil, "")
			if err != nil {
				t.Fatalf("buildAggregateFragment: %v", err)
			}
			var agg *distributed.OpSpec
			for i := range ops {
				if ops[i].Type == distributed.OpHashAggregate {
					agg = &ops[i]
				}
			}
			if agg == nil {
				t.Fatalf("no OpHashAggregate in fragment: %+v", ops)
			}
			if agg.EmitEmptyIdentity != tt.want {
				t.Errorf("EmitEmptyIdentity = %v, want %v (stage %q, group by %v, group-by-all %v)",
					agg.EmitEmptyIdentity, tt.want, tt.stage.Type, tt.stage.GroupByCols, tt.stage.GroupByAll)
			}
		})
	}
}

// TestDecomposeAvg_LegOutputTypes pins the types of the synthetic legs AVG
// splits into. They are NOT AVG's own type: applyAvgFold reads the count leg
// straight out of Int64Data, so inheriting AVG's float64 declaration hands
// the fold the wrong vector — and over an empty input the count leg is the
// only thing standing between AVG and a divide it must not perform.
func TestDecomposeAvg_LegOutputTypes(t *testing.T) {
	in := []distributed.AggSpec{{
		Func: "avg", InputCol: "v", OutputCol: "a",
		OutputType: int(parquet.TypeFloat64),
	}}
	got := decomposeAvg(in)
	if len(got) != 2 {
		t.Fatalf("decomposeAvg produced %d specs, want 2 (sum, count): %+v", len(got), got)
	}
	if got[0].OutputCol != avgSumPrefix+"a" || got[0].OutputType != int(parquet.TypeFloat64) {
		t.Errorf("sum leg: got %q type %v, want %q type %v",
			got[0].OutputCol, parquet.TypeID(got[0].OutputType), avgSumPrefix+"a", parquet.TypeFloat64)
	}
	if got[1].OutputCol != avgCountPrefix+"a" || got[1].OutputType != int(parquet.TypeInt64) {
		t.Errorf("count leg: got %q type %v, want %q type %v",
			got[1].OutputCol, parquet.TypeID(got[1].OutputType), avgCountPrefix+"a", parquet.TypeInt64)
	}

	physIn := []physical.AggSpec{{
		Func: "avg", InputCol: "v", OutputCol: "a", OutputType: parquet.TypeFloat64,
	}}
	physGot := decomposeAvgPhysical(physIn)
	if len(physGot) != 2 {
		t.Fatalf("decomposeAvgPhysical produced %d specs, want 2: %+v", len(physGot), physGot)
	}
	if physGot[0].OutputType != parquet.TypeFloat64 || physGot[1].OutputType != parquet.TypeInt64 {
		t.Errorf("physical legs: sum %v, count %v; want %v, %v",
			physGot[0].OutputType, physGot[1].OutputType, parquet.TypeFloat64, parquet.TypeInt64)
	}
}
