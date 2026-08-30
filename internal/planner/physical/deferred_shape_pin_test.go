package physical

import (
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The two shapes #656 DEFERS rather than plans, pinned so the deferral cannot
// outlive its reason.
//
// Both answer correctly today — the coordinator routes them to its local
// single-process engine — and both are a query that left the DAG. A pin that
// FAILS when the DAG plans the shape is what turns "we know" into "we will
// notice": deleting it is the fix's proof.

// dspPlan plans one query distributed and returns the refusal, or nil.
func dspPlan(t *testing.T, sql string) error {
	t.Helper()
	cat, ctx := setupTPCHCatalog(t)
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	node, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical: %v", err)
	}
	annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
	annotate(node)
	node = logical.Optimize(node, annotate)
	p := NewPlanner(cat)
	p.WorkerCount = 3
	stages, err := p.PlanDistributed(ctx, node)
	if err != nil {
		return err
	}
	return ValidateNativeDAGShape(stages)
}

// TestOrderedProjectionUnderAnAggregateIsDeferredToLocal pins #716.
//
// `SELECT COUNT(*) FROM (SELECT k * 2 AS d FROM … ORDER BY d) x` orders on a
// SELECT-list ALIAS inside a derived table whose consumer is an aggregate. The
// outer COUNT(*) needs no columns, so the projection that computes the key is
// pruned — attachScanSelectProjections declines because findOutputProjectionNode
// stops at the aggregate — and the sort keys on a name nothing emits. Before
// the refusal this failed at DISPATCH with `sort: key column "d" does not exist
// in the input schema`, for every producer class including a plain scan.
//
// TODO(#716): materialize the key on the DAG. When that lands this test fails,
// and the fix is to delete it and assert the distributed plan instead.
func TestOrderedProjectionUnderAnAggregateIsDeferredToLocal(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"scan", `SELECT COUNT(*) AS n FROM (SELECT n_nationkey * 2 AS d FROM nation ORDER BY d) x`},
		{"aggregate", `SELECT COUNT(*) AS n FROM (SELECT k * 2 AS d FROM ` +
			`(SELECT n_regionkey + 1 AS k, COUNT(*) AS v FROM nation GROUP BY n_regionkey + 1) s ` +
			`ORDER BY d) x`},
		{"distinct", `SELECT COUNT(*) AS n FROM (SELECT k * 2 AS d FROM ` +
			`(SELECT DISTINCT n_nationkey AS k FROM nation) s ORDER BY d) x`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := dspPlan(t, tc.sql)
			if err == nil {
				t.Fatalf("the DAG now PLANS this shape, so #716 has landed — delete this pin "+
					"and assert the distributed plan\n  SQL: %s", tc.sql)
			}
			if !errors.Is(err, ErrUnreachableGatherOutput) {
				t.Fatalf("refused, but not with the sentinel the coordinator routes on — the "+
					"client would get a hard error: %v\n  SQL: %s", err, tc.sql)
			}
			if !strings.Contains(err.Error(), "orders on") {
				t.Errorf("the refusal does not name the sort key as the cause: %v", err)
			}
		})
	}
}

// TestGatherRefusesANestedWrappedWindowPlanTime is the second deferral: a
// window WRAPPED in an expression one level down. At the query's own root the
// gather evaluates such an item from OutputRename.Expr; nested, the outer item
// is a bare forward of a COMPUTED alias, the walk stops there, and no Expr is
// produced. The DAG used to hand the client the window stage's raw output,
// `__win_0` included, for a query that asked for one column.
//
// TODO(#717): give the nested case the same treatment as the root case.
func TestGatherRefusesANestedWrappedWindowPlanTime(t *testing.T) {
	sql := `SELECT x FROM (SELECT SUM(n_nationkey) OVER () + 1 AS x FROM nation) s`
	err := dspPlan(t, sql)
	if err == nil {
		t.Fatalf("the DAG now plans a nested wrapped window — delete this pin (#717)")
	}
	if !errors.Is(err, ErrUnreachableGatherOutput) {
		t.Fatalf("refused without the sentinel, so the client gets a hard error: %v", err)
	}
}

// TestUnionArmProducerAndDependencyDisagree pins #715, which is NOT this
// class: two arms reading the same sorted producer leave UnionArm.DepStage
// naming `merge_sort-2` while Dependencies[0] is `sort-1`, and the shape check
// refuses with a plain error — so nothing routes it local and it reaches the
// client. Pre-existing and byte-identical on the parent commit.
//
// TODO(#715): derive DepStage from Dependencies, or repoint both together.
func TestUnionArmProducerAndDependencyDisagree(t *testing.T) {
	sql := `SELECT k FROM (SELECT n_nationkey AS k FROM nation ORDER BY n_nationkey) a ` +
		`UNION ALL SELECT k FROM (SELECT n_nationkey AS k FROM nation ORDER BY n_nationkey) b`
	err := dspPlan(t, sql)
	if err == nil {
		t.Fatalf("this shape now plans — #715 is fixed, delete this pin")
	}
	if !strings.Contains(err.Error(), "names producer") {
		t.Fatalf("refused for a different reason than #715's producer/dependency mismatch: %v", err)
	}
	if errors.Is(err, ErrUnreachableGatherOutput) {
		t.Fatalf("this refusal now routes local, which ANSWERS the query — that is a fix, " +
			"so update the pin to assert the answer instead")
	}
}
