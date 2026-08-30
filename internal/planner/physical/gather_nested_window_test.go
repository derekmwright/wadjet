package physical

import (
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestGatherRefusesANestedWrappedWindow pins what the output-reachability
// check does with the one shape #656's placement work cannot compute.
//
// `SELECT x FROM (SELECT SUM(a) OVER () + 1 AS x FROM t) s` wraps a window in
// an expression ONE LEVEL DOWN. At the query's own root the gather evaluates
// such an expression from OutputRename.Expr, which extractOutputRenames fills
// in when the projection references a synthetic window output. Nested, the
// outer item is a bare forward of a COMPUTED alias, so the walk stops there —
// there is no source column to point at — and no Expr is produced.
//
// The DAG used to answer it with the window stage's raw output, `__win_0`
// included, where the single-process pipeline answers one column `x`: a wrong
// RESULT SET, silently. It is now REFUSED at plan time and the coordinator
// routes it local, so the client gets the right answer.
//
// The pin fails the day the DAG computes it, which is the signal to move the
// shape into fcpCorpus.
func TestGatherRefusesANestedWrappedWindow(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT x FROM (SELECT SUM(n_nationkey) OVER () + 1 AS x FROM nation) s`
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	node, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatal(err)
	}
	annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
	annotate(node)
	node = logical.Optimize(node, annotate)
	p := NewPlanner(cat)
	p.WorkerCount = 3
	if _, err = p.PlanDistributed(ctx, node); err == nil {
		t.Errorf("the DAG now computes the SELECT list for %q, so this shape no longer needs "+
			"the local route. Move it into fcpCorpus and delete this pin.", sql)
		return
	}
	if !errors.Is(err, ErrUnreachableGatherOutput) {
		t.Fatalf("PlanDistributed refused %q for some OTHER reason than the pinned one: %v", sql, err)
	}
}
