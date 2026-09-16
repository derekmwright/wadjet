// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
)

// The priority lane's memory contract only covers planner-bounded tiny
// scans: a stage whose ONLY emits are in-flow (cascade mid class) must
// dispatch as ordinary bulk work, while any lane-class emit keeps the
// stage on the lane.
func TestStageHasLaneEmit(t *testing.T) {
	cases := []struct {
		name  string
		emits []dagplan.DynamicFilterEmit
		want  bool
	}{
		{"no emits", nil, false},
		{"lane emit", []dagplan.DynamicFilterEmit{{FilterID: "a"}}, true},
		{"in-flow only", []dagplan.DynamicFilterEmit{{FilterID: "a", InFlow: true}}, false},
		{"mixed", []dagplan.DynamicFilterEmit{{FilterID: "a", InFlow: true}, {FilterID: "b"}}, true},
	}
	for _, tc := range cases {
		if got := stageHasLaneEmit(dagplan.Stage{EmitDynamicFilters: tc.emits}); got != tc.want {
			t.Errorf("%s: stageHasLaneEmit=%t want %t", tc.name, got, tc.want)
		}
	}
}
