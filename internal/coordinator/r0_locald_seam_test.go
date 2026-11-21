// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// TestR0LocalDSeamRunsD is the localD seam's negative control (ADR-0051, R0):
//
//  1. the seam runs D, not L: L and D print alike (PrettyPrint shows no
//     binder stamp), so the corpus guards the seam twice — its D text equals
//     the plan text the DAG arm's ExecuteSQL reports, and the plan it executed
//     carries no binder stamp. With the test-only fault that makes the seam
//     plan L instead, the second guard fires on every statement whose L
//     carries a stamp;
//  2. the frozen copy shares no state with the executed D, and a shallow copy
//     (the fault the deep copy exists to avoid) would;
//  3. a statement with a scalar subquery the stage planner plans after D is
//     reported late-subplan, never established.
func TestR0LocalDSeamRunsD(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands a cluster up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	c := r0Cluster(t, ctx, "own", nil)

	var checked int
	for _, cell := range r0ReadCells(t) {
		if !containsString(cell.fixtures, "own") {
			continue
		}
		res, err := c.ExecuteSQL(ctx, cell.sql)
		if err != nil || res == nil {
			continue
		}
		dagPlan := r0PlanText(res.Plan)
		_, rec, _ := r0LocalD(c, ctx, cell.sql)
		if rec.Frozen == nil || rec.DText != dagPlan {
			continue // refused before planning, or routed (a route reports L's text)
		}
		if rec.BoundRefs != 0 {
			t.Errorf("%s: the seam's D carries %d binder stamps", cell.name, rec.BoundRefs)
		}
		r0LocalDFault.Store("bound")
		_, faulted, _ := r0LocalD(c, ctx, cell.sql)
		r0LocalDFault.Store("")
		if faulted.BoundRefs == 0 {
			// L prints like D and carries no stamp here (no bindable
			// reference): this statement cannot tell them apart.
			continue
		}
		checked++
		if len(rec.SharedPointers) > 0 {
			t.Errorf("%s: frozen copy shares %v with the executed D", cell.name, rec.SharedPointers)
		}
	}
	if checked == 0 {
		t.Fatal("no statement lets the guard tell L from D: the control would be vacuous")
	}
	t.Logf("the seam planning L is caught by the binder-stamp guard on %d statements", checked)

	// 2: a shallow copy shares state; the deep copy does not.
	cctx, _ := c.statementAuth(ctx)
	cctx = statementGuard.WithIncarnationGuard(cctx, c.catalog)
	p, err := c.planStatement(cctx, "SELECT p.id, s.r FROM tb_d p LEFT JOIN (SELECT id, coalesce(v,0)+1 AS r FROM tb_bd) s ON p.id = s.id", false, c.statementPlanOptions(cctx))
	if err != nil {
		t.Fatal(err)
	}
	shallow := *p.node
	if len(r0SharedPointers(&shallow, p.node)) == 0 {
		t.Fatal("a shallow copy reports no shared state: the disjointness check is vacuous")
	}
	deep := r0DeepCopy(p.node)
	if s := r0SharedPointers(deep, p.node); len(s) > 0 {
		t.Fatalf("the deep copy shares %v", s)
	}
	if r0PlanText(deep.PrettyPrint(0)) != r0PlanText(p.node.PrettyPrint(0)) {
		t.Fatal("the deep copy prints differently from its original")
	}

	// 3: a late subplan is never reported established.
	_, rec, err := r0LocalD(c, ctx, "SELECT id FROM tb_d WHERE v > (SELECT avg(v) FROM tb_bd)")
	if err != nil {
		t.Fatalf("late-subplan statement: %v", err)
	}
	if rec.Status != "late-subplan" {
		t.Fatalf("a scalar subquery the stage planner plans after D reports status %q (%s)", rec.Status, rec.LateSubplan)
	}
}
