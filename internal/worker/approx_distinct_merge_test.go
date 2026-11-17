// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// TestBuildFragmentHashAggregateRefusesApproxDistinctMerge pins the merge of
// an approx_distinct partial (#1288). The partial a worker emits for
// approx_distinct is a COUNT (exec's distinctSet.count(); the set itself
// never travels), so a merge-mode aggregate that re-ran approx_distinct over
// the per-task counts answered the number of DISTINCT COUNTS: 2 for
// `SELECT approx_distinct(c_i32) FROM typemx WHERE id < 20` on every DAG arm,
// where the true answer is 20. There is no correct merge of counts, so the
// worker must refuse the merge rather than answer it; the planner routes
// approx_distinct through the one-level RawInputAggregate shape instead
// (dagplan.aggNeedsWholeInput).
func TestBuildFragmentHashAggregateRefusesApproxDistinctMerge(t *testing.T) {
	e := &Executor{}
	for _, grouped := range []bool{false, true} {
		spec := distributed.OpSpec{
			MergeMode: true,
			Aggregates: []distributed.AggSpec{
				{Func: "count", InputCol: "", OutputCol: "n"},
				{Func: "approx_distinct", InputCol: "c_i32", OutputCol: "v"},
			},
		}
		if grouped {
			spec.GroupByCols = []string{"g"}
		}
		agg, err := e.buildFragmentHashAggregate(context.Background(), spec)
		if err == nil {
			t.Fatalf("grouped=%v: merge-mode approx_distinct built a HashAggregate (%v over input %q): "+
				"it counts the distinct per-task COUNTS, not the distinct values",
				grouped, agg.Aggs[1].Func, agg.Aggs[1].InputCol)
		}
		if !strings.Contains(err.Error(), "approx_distinct") {
			t.Errorf("grouped=%v: refusal does not name the aggregate: %v", grouped, err)
		}
	}
}

// TestBuildFragmentHashAggregateApproxDistinctRawInput is the control: over
// RAW rows (MergeMode=false, which is what the RawInputAggregate final runs)
// approx_distinct builds, reading its own input column.
func TestBuildFragmentHashAggregateApproxDistinctRawInput(t *testing.T) {
	e := &Executor{}
	agg, err := e.buildFragmentHashAggregate(context.Background(), distributed.OpSpec{
		Aggregates: []distributed.AggSpec{
			{Func: "approx_distinct", InputCol: "c_i32", OutputCol: "v"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Aggs[0].Func != exec.AggApproxDistinct || agg.Aggs[0].InputCol != "c_i32" {
		t.Errorf("raw approx_distinct resolved to %v over %q, want AggApproxDistinct over \"c_i32\"",
			agg.Aggs[0].Func, agg.Aggs[0].InputCol)
	}
}
