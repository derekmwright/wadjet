// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
)

// A server with a coordinator still prints the stage DAG in EXPLAIN VERBOSE.
//
// The local planner stopped emitting stages when the two planners were
// separated (ADR-0037): PhysicalPlan.PrettyPrint is the pipeline's own line
// now. This door renders the DAG from the distributed planner instead, so its
// EXPLAIN text is what it was before the split — the same "Stage <id> [<type>]
// (<n> tasks)" lines, with the same dependency suffix.
func TestTheDistributedExplainStillPrintsTheStageList(t *testing.T) {
	stages := []dagplan.Stage{
		{ID: "scan-0", Type: "scan", Tasks: 3},
		{ID: "final_aggregate-1", Type: "final_aggregate", Tasks: 1, Dependencies: []string{"scan-0"}},
	}
	got := dagplan.PrettyPrintStages(stages)
	want := "Stage scan-0 [scan] (3 tasks)\n" +
		"Stage final_aggregate-1 [final_aggregate] (1 tasks) <- depends on scan-0"
	if got != want {
		t.Errorf("the stage rendering changed:\n got %q\nwant %q", got, want)
	}
	if !strings.Contains(got, "Stage ") {
		t.Error("the distributed EXPLAIN prints no stage line")
	}
	if empty := dagplan.PrettyPrintStages(nil); empty != "Single-stage local execution" {
		t.Errorf("a stageless plan renders %q, want the pipeline's own line", empty)
	}
}
