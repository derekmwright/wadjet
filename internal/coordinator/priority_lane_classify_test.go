package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// The priority lane's memory contract only covers planner-bounded tiny
// scans: a stage whose ONLY emits are in-flow (cascade mid class) must
// dispatch as ordinary bulk work, while any lane-class emit keeps the
// stage on the lane.
func TestStageHasLaneEmit(t *testing.T) {
	cases := []struct {
		name  string
		emits []physical.DynamicFilterEmit
		want  bool
	}{
		{"no emits", nil, false},
		{"lane emit", []physical.DynamicFilterEmit{{FilterID: "a"}}, true},
		{"in-flow only", []physical.DynamicFilterEmit{{FilterID: "a", InFlow: true}}, false},
		{"mixed", []physical.DynamicFilterEmit{{FilterID: "a", InFlow: true}, {FilterID: "b"}}, true},
	}
	for _, tc := range cases {
		if got := stageHasLaneEmit(physical.Stage{EmitDynamicFilters: tc.emits}); got != tc.want {
			t.Errorf("%s: stageHasLaneEmit=%t want %t", tc.name, got, tc.want)
		}
	}
}
