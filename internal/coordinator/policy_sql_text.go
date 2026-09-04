package coordinator

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseReplannedSQLText refuses a task whose SQL TEXT a worker will RE-PLAN,
// when a policy shaped this query's plan.
//
// `worker/executor.go`'s executePipeline parses, builds and optimizes
// `task.SQLText` with no policy in reach, so the plan the coordinator enforced
// does not survive the hop — that is #859's async-door mechanism. The guard
// belongs at every site that PUTS the text on a task, not at one caller: six
// sites do (SubmitSQL's pipeline dispatch, createPipelineTasks, the gather
// synthesis, the build-cache pre-scan, the aggregate shuffle and the
// repartition orchestration), and a seventh added later would otherwise
// inherit the leak in silence.
//
// It fires on exactly the condition executePipeline triggers on — SQL text
// with no Operators and no Inputs — so a task that carries an operator
// fragment or reads upstream files, which is every task on the synchronous
// DAG, is untouched.
func refuseReplannedSQLText(ctx context.Context, site string, t distributed.Task) error {
	if !logical.PolicyEnforced(ctx) {
		return nil
	}
	if t.SQLText == "" || len(t.Operators) > 0 || len(t.Inputs) > 0 {
		return nil
	}
	return sqlerr.Wrap("0A000", fmt.Errorf(
		"this query is not available for this identity on the distributed path: a row or "+
			"column security policy applies, and %s dispatches the statement's TEXT to a "+
			"worker that re-plans it where the policy is not in reach", site))
}
