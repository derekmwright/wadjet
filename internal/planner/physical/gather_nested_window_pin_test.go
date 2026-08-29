package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestGatherCannotEvaluateANestedWrappedWindow pins the one shape the
// output-reachability check finds that #656's placement work does NOT fix, so
// the residual is recorded rather than discovered again.
//
// `SELECT x FROM (SELECT SUM(a) OVER () + 1 AS x FROM t) s` wraps a window in
// an expression ONE LEVEL DOWN. At the query's own root the gather evaluates
// such an expression from OutputRename.Expr, which extractOutputRenames fills
// in when the projection references a synthetic window output. Nested, the
// outer item is a bare forward of a COMPUTED alias, so the walk stops there —
// there is no source column to point at — and no Expr is produced: the client
// gets the window stage's raw output, `__win_0` included, where the
// single-process pipeline answers one column `x`.
//
// That is the GATHER's expression layer, not the Filter/Project placement
// this file's sibling gate covers: the projection was never attachable to a
// stage at all, it has to be evaluated after the gather. Fixing it means
// teaching extractOutputRenames to pull a nested computed alias's AST, which
// is its own change with its own blast radius.
//
// The pin FAILS the day it starts working, which is the signal to move the
// shape into fcpCorpus.
func TestGatherCannotEvaluateANestedWrappedWindow(t *testing.T) {
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
	stages, err := p.PlanDistributed(ctx, node)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	gather, emitted, modelled := gatherOutputSources(stages)
	if !modelled {
		t.Skip("no gather producer this check models")
	}
	for _, r := range gather.OutputRenames {
		if r.Expr != nil || r.From == "" {
			continue
		}
		if _, ok := lookupEmittedColumn(emitted, r.From); !ok {
			return // the residual is still here, exactly as documented
		}
	}
	t.Errorf("the gather now resolves every source for %q, so the nested wrapped-window "+
		"residual is FIXED. Move this shape into fcpCorpus and delete this pin.\n  emitted: %v",
		sql, sortedNames(emitted))
}
