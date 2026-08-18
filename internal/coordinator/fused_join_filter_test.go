package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// A fused join's own predicates must survive op-spec construction.
//
// Broadcast-join fusion absorbs a join stage into its consumer, carrying the
// absorbed stage's FilterExprs on the fused spec — the planner sets them and
// the wire format has the field. buildJoinFragment copied every OTHER field
// into the probe op and dropped FilterExprs on the floor, so fusing a
// filter-carrying join silently discarded its WHERE clause.
//
// `WHERE c_nationkey = s_nationkey` over TPC-H Q05's join survived while its
// stage stood alone, and vanished the moment a fifth table made that stage
// fusable: COUNT(*) 2450 -> 60000, exactly the unfiltered row count, and Q05
// revenues ~25x inflated with the wrong sum frozen into
// baseline-local-small.json as the correctness oracle (#312).
func TestFusedJoinFiltersReachTheFragment(t *testing.T) {
	stage := physical.Stage{ID: "join-8", Type: physical.StageHashJoin}
	task := &distributed.Task{
		BuildTableAlias: "orders",
		JoinType:        "inner",
		JoinLeftKeys:    []string{"o_orderkey"},
		JoinRightKeys:   []string{"l_orderkey"},
		DataBucket:      "test",
	}
	wireFused := []distributed.FusedJoinSpec{{
		JoinType:        "inner",
		JoinLeftKeys:    []string{"l_suppkey"},
		JoinRightKeys:   []string{"s_suppkey"},
		BuildTableAlias: "supplier",
		BuildFiles:      []string{"queries/q/supplier.wshf"},
		FilterExprs:     []string{"c_nationkey = s_nationkey"},
	}}

	ops, err := buildJoinFragment(stage, task, map[string][]string{"orders": {"build.wshf"}, "join-4": {"probe.wshf"}},
		wireFused, nil, nil, distributed.OpSpec{Type: distributed.OpUnpartitionedSink}, false)
	if err != nil {
		t.Fatalf("buildJoinFragment: %v", err)
	}

	probeAt := -1
	filterAt := -1
	for i, op := range ops {
		if op.Type == distributed.OpBroadcastProbe && probeAt < 0 {
			probeAt = i
		}
		for _, p := range op.Predicates {
			if p == "c_nationkey = s_nationkey" {
				filterAt = i
			}
		}
	}
	if probeAt < 0 {
		t.Fatalf("no broadcast probe op emitted for the fused join: %+v", opTypes(ops))
	}
	if filterAt < 0 {
		t.Fatalf("the fused join's predicate reached no op — the join would return "+
			"every row it probes. ops = %v", opTypes(ops))
	}
	// The predicate belongs after its own probe: that is where the absorbed
	// stage would have applied it.
	if filterAt <= probeAt {
		t.Fatalf("filter at op %d precedes its probe at op %d: %v", filterAt, probeAt, opTypes(ops))
	}
}

func opTypes(ops []distributed.OpSpec) []distributed.OpType {
	out := make([]distributed.OpType, 0, len(ops))
	for _, o := range ops {
		out = append(out, o.Type)
	}
	return out
}
