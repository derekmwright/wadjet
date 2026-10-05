// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestTheScalarSubqueryDeclarationMemoIsOwnedByOneBuild is the recorded #1018
// the earlier implementation B2 probe, promoted.
//
// The memo added in the earlier implementation (annotateSubqueryColumnDecls → scalarSubqueryColumnDecl)
// is a plain map on the Planner. forSubquery SHALLOW-COPIES the Planner, so
// before the reset this test pins, every child planner inherited the PARENT's
// map header — and the subquery runner baked into a compiled expression is
// reached from every parallel pipeline goroutine. Planning a parent that HOLDS
// a scalar subquery is what initializes the map; eight concurrent child plans
// then wrote it at once, and `-race` reported concurrent map access inside
// scalarSubqueryColumnDecl. Go can also turn that into a fatal concurrent map
// write, which the earlier implementation recover() cannot catch: a fatal error is not a
// panic.
//
// It uses the same seam as #334's TestSubqueryRunnerConcurrent — the real
// p.subqueryRunner, not a stub — because the seam is the claim: a child
// planner must not write build scratch its parent or its siblings can see.
//
// RUN IT WITH -race. Without the detector the shape passes either way.
func TestTheScalarSubqueryDeclarationMemoIsOwnedByOneBuild(t *testing.T) {
	ctx := context.Background()
	cat := ScanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.PlanCtx = ctx
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
	cat := ScanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.PlanCtx = ctx
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

// TestTheBuildsDeclarationMemoIsLockedForConcurrentReruns is the path the two
// tests above do not take: a correlated scalar subquery's re-run asks the
// BUILD'S OWN planner — not a child — for the arity and the declaration of
// its per-row text (CorrelatedScalarSubquery.Eval → SubqueryOutputArity /
// SubqueryOutputColumn), from every parallel pipeline goroutine, and each
// re-plan annotates the nested subquery that text holds into the build's
// memo. Before the lock, `-race` reported the memo inside
// scalarSubqueryColumnDecl on `SELECT t.id, (SELECT (SELECT extract(year FROM
// t.d) …) + q.m FROM ss_i q …) FROM ss_t t` (#1422's per-row re-run; the
// coordinator cell r12/c31_outerValue_year), and a five-arm run died of
// `fatal error: concurrent map read and map write`.
//
// RUN IT WITH -race, as the tests above.
func TestTheBuildsDeclarationMemoIsLockedForConcurrentReruns(t *testing.T) {
	ctx := context.Background()
	cat := ScanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.PlanCtx = ctx
	plan := planWithPlanner(t, p, "SELECT (SELECT MAX(id) FROM items) AS v FROM items")
	defer plan.Pipeline.Close()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for k := 0; k < 4; k++ {
				// Distinct per-row texts, as a re-run spells each outer value.
				q := fmt.Sprintf("SELECT (SELECT MAX(id) + %d FROM items) + %d FROM items WHERE id = 0", i, k)
				if n, ok := p.SubqueryOutputArity(q); !ok || n != 1 {
					t.Errorf("arity %d/%d: %d %v", i, k, n, ok)
				}
				if _, ok := p.SubqueryOutputColumn(q); !ok {
					t.Errorf("declaration %d/%d: not declared", i, k)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
