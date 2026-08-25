package tpch

import (
	"context"
	"testing"
	"time"
)

// TestInSubqueryRefusalRoutesLocalAndStillAnswers is the other half of #524's
// gate: what happens when the planner CANNOT materialize the set.
//
// The corpus in TestTwoPathInvariance covers every shape it can — and asserts
// the refusal fires for none of them. This one forces the refusal by shrinking
// the inline bound to a single row, and requires two things of it: the query
// still ANSWERS (a refusal is a handoff to the coordinator-local pipeline, not
// an outcome), and it answers the same thing the materialized path did.
//
// Without the handoff this shape failed the client outright with "IN subquery
// requires a SubqueryRunner", which is what #524 measured.
func TestInSubqueryRefusalRoutesLocalAndStillAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	_, dag := setupTwoPathCluster(t, ctx)

	// Three values, one row of headroom: the set cannot be inlined.
	const sql = `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
		(SELECT b.n_nationkey FROM nation b ORDER BY b.n_nationkey LIMIT 3)`

	before := dag.InSubqueryLocalRoutes()
	t.Setenv("WADJET_IN_SET_MAX", "1")

	rows, _, err := runArm(t, ctx, dag, sql)
	if err != nil {
		t.Fatalf("a refused IN-subquery must still answer on the local route, got: %v", err)
	}
	if routed := dag.InSubqueryLocalRoutes() - before; routed != 1 {
		t.Fatalf("InSubqueryLocalRoutes advanced by %d, want 1 — the refusal did not fire, "+
			"so this test is no longer exercising the route it exists for", routed)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 (scalar COUNT)", len(rows))
	}
	if got := cellNum(rows[0], "c"); got != 3 {
		t.Errorf("COUNT(*) = %v, want 3 (PostgreSQL 17) — the local route must answer what the "+
			"materialized path answers", got)
	}

	// And with the bound restored, the same query takes the DAG.
	t.Setenv("WADJET_IN_SET_MAX", "")
	before = dag.InSubqueryLocalRoutes()
	rows, _, err = runArm(t, ctx, dag, sql)
	if err != nil {
		t.Fatalf("query error with the bound restored: %v", err)
	}
	if routed := dag.InSubqueryLocalRoutes() - before; routed != 0 {
		t.Errorf("the refusal fired with the default bound — a 3-row set must materialize")
	}
	if got := cellNum(rows[0], "c"); got != 3 {
		t.Errorf("COUNT(*) = %v, want 3 (PostgreSQL 17)", got)
	}
}
