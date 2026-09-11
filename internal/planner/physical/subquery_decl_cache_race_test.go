package physical

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestTheScalarSubqueryDeclarationMemoIsOwnedByOneBuild is the reviewer's #1018
// round-6 B2 probe, promoted.
//
// The memo added in round 6 (annotateSubqueryColumnDecls → scalarSubqueryColumnDecl)
// is a plain map on the Planner. forSubquery SHALLOW-COPIES the Planner, so
// before the reset this test pins, every child planner inherited the PARENT's
// map header — and the subquery runner baked into a compiled expression is
// reached from every parallel pipeline goroutine. Planning a parent that HOLDS
// a scalar subquery is what initializes the map; eight concurrent child plans
// then wrote it at once, and `-race` reported concurrent map access inside
// scalarSubqueryColumnDecl. Go can also turn that into a fatal concurrent map
// write, which the round-6 recover() cannot catch: a fatal error is not a
// panic.
//
// It uses the same seam as #334's TestSubqueryRunnerConcurrent — the real
// p.subqueryRunner, not a stub — because the seam is the claim: a child
// planner must not write build scratch its parent or its siblings can see.
//
// RUN IT WITH -race. Without the detector the shape passes either way.
func TestTheScalarSubqueryDeclarationMemoIsOwnedByOneBuild(t *testing.T) {
	ctx := context.Background()
	cat := scanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.planCtx = ctx
	// A parent that holds a scalar subquery, so the annotation pass runs and
	// the memo exists before any child asks for it.
	plan := planWithPlanner(t, p, "SELECT (SELECT MAX(id) FROM items) AS v FROM items")
	defer plan.Pipeline.Close()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Distinct texts: the defect is the SHARED map, not a contended
			// key, and distinct keys make every goroutine WRITE.
			q := fmt.Sprintf("SELECT (SELECT %d FROM items WHERE id=0) AS v FROM items WHERE id=0", i)
			if _, err := p.subqueryRunner(q); err != nil {
				t.Errorf("subquery %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
}

// TestANestedScalarSubqueryPlansConcurrentlyThroughChildPlanners is the same
// claim one level deeper: the subquery a child planner runs itself holds a
// scalar subquery, so the CHILD's own annotation pass writes the memo while
// its seven siblings write theirs. Before the reset all eight wrote the
// parent's.
func TestANestedScalarSubqueryPlansConcurrentlyThroughChildPlanners(t *testing.T) {
	ctx := context.Background()
	cat := scanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.planCtx = ctx
	plan := planWithPlanner(t, p, "SELECT (SELECT MAX(id) FROM items) AS v FROM items")
	defer plan.Pipeline.Close()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			q := fmt.Sprintf(
				"SELECT (SELECT (SELECT MAX(id)+%d FROM items) FROM items WHERE id=0) AS v FROM items WHERE id=0", i)
			if _, err := p.subqueryRunner(q); err != nil {
				t.Errorf("nested subquery %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
