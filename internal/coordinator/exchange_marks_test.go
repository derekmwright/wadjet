// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// A producer's reported marks reach every exchange read of its files at
// dispatch — an operator's input and build files and a shuffle / gather
// task's own — and are forgotten with the query (ADR-0024 §10). A failed
// attempt's report, and a file nobody reported, stamp nothing.
func TestExchangeMarksRideTheFile(t *testing.T) {
	r := newExchangeMarkRegistry()
	r.record(distributed.ResultNotification{QueryID: "q1", Success: true,
		ResultFiles: []string{"queries/q1/s1/a.wshf"}, UnconstrainedColumns: []string{"v"}})
	r.record(distributed.ResultNotification{QueryID: "sh-probe-q1", Success: true,
		ResultFiles: []string{"queries/q1/s2/b.wshf"}, UnconstrainedColumns: []string{"w", "v"}})
	r.record(distributed.ResultNotification{QueryID: "q1", Success: false,
		ResultFiles: []string{"queries/q1/s3/c.wshf"}, UnconstrainedColumns: []string{"x"}})

	task := distributed.Task{
		Files: []string{"queries/q1/s1/a.wshf"},
		Operators: []distributed.OpSpec{
			{InputFiles: []string{"queries/q1/s2/b.wshf", "queries/q1/s1/a.wshf"}, BuildFiles: []string{"queries/q1/s3/c.wshf"}},
			{InputFiles: []string{"tables/t/x.parquet"}},
		},
	}
	stampTaskUnconstrained(&task, r)
	if !reflect.DeepEqual(task.Unconstrained, []string{"v"}) {
		t.Errorf("task marks %v, want [v]", task.Unconstrained)
	}
	if !reflect.DeepEqual(task.Operators[0].Unconstrained, []string{"v", "w"}) {
		t.Errorf("input marks %v, want [v w]", task.Operators[0].Unconstrained)
	}
	if task.Operators[0].BuildUnconstrained != nil || task.Operators[1].Unconstrained != nil {
		t.Errorf("unreported files stamped: build %v, base table %v",
			task.Operators[0].BuildUnconstrained, task.Operators[1].Unconstrained)
	}
	r.forgetQuery("q1")
	if m := r.marks([]string{"queries/q1/s1/a.wshf", "queries/q1/s2/b.wshf"}); m != nil {
		t.Errorf("marks survived the query: %v", m)
	}
}
