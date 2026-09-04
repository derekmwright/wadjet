package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// TestRefuseReplannedSQLTextGuardsEveryDispatchSite.
//
// Six coordinator sites put a statement's SQL TEXT on a task
// (SubmitSQL's pipeline dispatch, createPipelineTasks' two, the gather
// synthesis, the build-cache pre-scan, the aggregate shuffle and the
// repartition orchestration), and `worker/executor.go`'s executePipeline
// re-plans that text with no policy in reach. The guard therefore sits at
// Scheduler.PublishTasks — the one choke point every dispatcher goes through
// — and this asserts the CONDITION it fires on, which is exactly the
// condition executePipeline triggers on: SQL text, no operator fragment, no
// upstream inputs (#859 round 2, review P2).
func TestRefuseReplannedSQLTextGuardsEveryDispatchSite(t *testing.T) {
	policed := logical.ContextWithPolicyEnforced(context.Background())
	plain := context.Background()

	for _, tc := range []struct {
		name    string
		ctx     context.Context
		task    distributed.Task
		refused bool
	}{
		{
			name:    "policed query, bare SQL text — the worker will re-plan it",
			ctx:     policed,
			task:    distributed.Task{SQLText: "SELECT ssn FROM e7emp"},
			refused: true,
		},
		{
			name: "policed query, an operator fragment — the plan travelled",
			ctx:  policed,
			task: distributed.Task{
				SQLText:   "SELECT ssn FROM e7emp",
				Operators: []distributed.OpSpec{{Type: distributed.OpFilter}},
			},
		},
		{
			name: "policed query, upstream inputs — it reads files, not text",
			ctx:  policed,
			task: distributed.Task{
				SQLText: "SELECT ssn FROM e7emp",
				Inputs:  map[string][]string{"scan-0": {"queries/q/part-0.wshf"}},
			},
		},
		{
			name: "policed query, no SQL text at all",
			ctx:  policed,
			task: distributed.Task{TableName: "e7emp"},
		},
		{
			name: "UNPOLICED query, bare SQL text — the ordinary async path",
			ctx:  plain,
			task: distributed.Task{SQLText: "SELECT ssn FROM e7emp"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseReplannedSQLText(tc.ctx, "this dispatch path", tc.task)
			if tc.refused && err == nil {
				t.Fatal("a task a worker will re-plan from TEXT was dispatched under a policy")
			}
			if !tc.refused && err != nil {
				t.Fatalf("refused a task that carries its plan: %v", err)
			}
			if tc.refused && !strings.Contains(err.Error(), "security policy") {
				t.Fatalf("error %v does not name the reason", err)
			}
		})
	}
}
