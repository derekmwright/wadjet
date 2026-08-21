package physical

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Regression coverage for issue #359: a per-row correlated subquery has no
// distributed lowering, and before the typed refusal existed the stage DAG's
// scalar-deferral path executed the subquery as a standalone producer stage
// whose dangling outer reference evaluated NULL — the query answered 0,
// silently. PlanDistributed must refuse every such shape with
// ErrCorrelatedSubqueryDistributed (the coordinator routes on that error), and
// must NOT refuse the shapes it can run: uncorrelated subqueries (deferred to
// producer stages) and equality correlations (decorrelated into joins before
// planning).

// planDistributed builds, optimizes and distributed-plans sql exactly the way
// the coordinator does.
func planDistributed(t *testing.T, p *Planner, sql string) ([]Stage, error) {
	t.Helper()
	ctx := context.Background()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	lp, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical build: %v", err)
	}
	p.AnnotateScanColumns(ctx, lp)
	lp = logical.Optimize(lp, func(n *logical.Node) { p.AnnotateScanColumns(ctx, n) })
	return p.PlanDistributed(ctx, lp)
}

func TestPlanDistributedRefusesCorrelatedSubqueries(t *testing.T) {
	cat := scanCacheFixture(t, 50)

	refused := []struct {
		name string
		sql  string
	}{
		{"ScalarInWhere", `SELECT COUNT(*) AS n FROM items i1
			WHERE i1.id > (SELECT AVG(id) FROM items i2 WHERE i2.id2 < i1.id2)`},
		{"ScalarInSelectList", `SELECT id,
			(SELECT AVG(id) FROM items i2 WHERE i2.id2 < i1.id2) AS a
			FROM items i1`},
		{"Exists", `SELECT COUNT(*) AS n FROM items i1
			WHERE EXISTS (SELECT 1 FROM items i2 WHERE i2.id2 < i1.id2 AND i2.id > 40)`},
		{"NotExists", `SELECT COUNT(*) AS n FROM items i1
			WHERE NOT EXISTS (SELECT 1 FROM items i2 WHERE i2.id2 < i1.id2 AND i2.id > 40)`},
		{"NestedTwoDeep", `SELECT COUNT(*) AS n FROM items i1
			WHERE i1.id > (SELECT AVG(i2.id) FROM items i2
				WHERE i2.id > (SELECT AVG(i3.id) FROM items i3 WHERE i3.id2 < i1.id2))`},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPlanner(cat)
			p.WorkerCount = 3
			stages, err := planDistributed(t, p, tc.sql)
			if err == nil {
				t.Fatalf("PlanDistributed produced %d stages; it must refuse a per-row correlated subquery\n  SQL: %s",
					len(stages), tc.sql)
			}
			if !errors.Is(err, ErrCorrelatedSubqueryDistributed) {
				t.Fatalf("refusal is not the typed error the coordinator routes on: %v", err)
			}
		})
	}

	// The shapes the DAG CAN run must keep planning — over-refusal would
	// silently downgrade every subquery-bearing query to single-process.
	accepted := []struct {
		name string
		sql  string
	}{
		// Deferred to a scalar producer stage (Q11/Q22's shape).
		{"UncorrelatedScalar", `SELECT COUNT(*) AS n FROM items i1
			WHERE i1.id > (SELECT AVG(id) FROM items)`},
		// #334's shape: the unqualified id2 binds to the SUBQUERY's own
		// FROM, so this is uncorrelated however the outer table is aliased.
		{"UncorrelatedByInnerScope", `SELECT COUNT(*) AS n FROM items i1
			WHERE i1.id > (SELECT AVG(id2) FROM items)`},
		// Equality correlation: decorrelated into a join by the logical
		// optimizer before planning — Q17's shape, never per-row.
		{"EqualityCorrelationDecorrelates", `SELECT COUNT(*) AS n FROM items i1
			WHERE i1.id > (SELECT AVG(id) FROM items i2 WHERE i2.id2 = i1.id2)`},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPlanner(cat)
			p.WorkerCount = 3
			stages, err := planDistributed(t, p, tc.sql)
			if err != nil {
				t.Fatalf("PlanDistributed refused a shape it can run: %v\n  SQL: %s", err, tc.sql)
			}
			if len(stages) == 0 {
				t.Fatalf("no stages emitted\n  SQL: %s", tc.sql)
			}
		})
	}
}

// The deferral-seam backstop: a subquery that is not self-contained must never
// become a producer stage, even when it reaches resolveFilterSubqueries
// without the pre-pass having seen it. DanglingTableRefs is the analysis; this
// pins its verdicts on the shapes that matter.
func TestDanglingTableRefsBackstop(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		dangling bool
	}{
		{`SELECT AVG(c_acctbal) FROM customer c2 WHERE c2.c_nationkey < c1.c_nationkey`, true},
		{`SELECT AVG(c2.x) FROM customer c2 WHERE c2.x > (SELECT AVG(c3.x) FROM customer c3 WHERE c3.k < c1.k)`, true},
		{`SELECT AVG(c_acctbal) FROM customer c2 WHERE c2.c_nationkey < 7`, false},
		{`SELECT SUM(a) FROM (SELECT x AS a FROM t) d WHERE d.a > 0`, false},
		{`SELECT MAX(total) FROM revenue0`, false},
	} {
		refs := plansql.DanglingTableRefs(tc.sql)
		if got := len(refs) > 0; got != tc.dangling {
			t.Errorf("DanglingTableRefs(%q) = %v, want dangling=%v", tc.sql, refs, tc.dangling)
		}
	}
}

// A refusal must carry an actionable message: the construct, the outer
// columns, and the fact that the coordinator runs the query single-process.
func TestCorrelatedRefusalMessage(t *testing.T) {
	cat := scanCacheFixture(t, 20)
	p := NewPlanner(cat)
	p.WorkerCount = 3
	_, err := planDistributed(t, p, `SELECT COUNT(*) AS n FROM items i1
		WHERE i1.id > (SELECT AVG(id) FROM items i2 WHERE i2.id2 < i1.id2)`)
	if err == nil {
		t.Fatal("expected refusal")
	}
	for _, want := range []string{"i1.id2", "scalar subquery", "single-process"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not mention %q", err, want)
		}
	}
}
