package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestExtractOutputRenames_WrappedAggregate verifies Q17's wrapped-
// aggregate path: SELECT SUM(...) / 7.0 AS x. The logical layer
// rewrites the projection to BinaryOp{ColRef("__agg_0"), /, 7.0};
// extractOutputRenames must surface this as an Expr-bearing rename so
// the coordinator's gather rewrite compiles + evaluates the divisor.
//
// The key narrowness check: the eval path triggers ONLY when the
// rewritten AST references a __agg_N synthetic column. Pure scalar
// expressions like SUBSTR(o_orderdate, 1, 4) are computed by the
// worker's GROUP BY pipeline and surface as a plain column name —
// those need a name rename, not eval (and eval would mistype the
// SUBSTR result as float64).
func TestExtractOutputRenames_WrappedAggregate(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem`
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	NewPlanner(cat).AnnotateScanColumns(ctx, logicalPlan)

	renames := extractOutputRenames(logicalPlan)
	if len(renames) != 1 {
		t.Fatalf("want 1 rename, got %v", renames)
	}
	r := renames[0]
	if r.To != "avg_yearly" {
		t.Errorf("To = %q, want avg_yearly", r.To)
	}
	if r.Expr == nil {
		t.Fatalf("Expr should be set for wrapped-aggregate projection")
	}
	if !referencesSyntheticAgg(r.Expr) {
		t.Errorf("Expr should reference a __agg_N column, got %s", r.Expr.String())
	}
}

// TestExtractOutputRenames_PureExpressionGetsRenameNotEval verifies the
// narrowness check — pure scalar expressions (no synthetic-agg ref) take
// the rename path so the worker's GROUP-BY-computed column gets renamed
// rather than re-evaluated as float64.
func TestExtractOutputRenames_PureExpressionGetsRenameNotEval(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUBSTR(o_orderdate, 1, 4) AS o_year, SUM(o_totalprice) AS total
		FROM orders GROUP BY SUBSTR(o_orderdate, 1, 4)`
	parsed, _ := plansql.Parse(sql)
	info, _ := plansql.ExtractSelect(parsed)
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	NewPlanner(cat).AnnotateScanColumns(ctx, logicalPlan)

	renames := extractOutputRenames(logicalPlan)
	if len(renames) != 2 {
		t.Fatalf("want 2 renames, got %v", renames)
	}
	for _, r := range renames {
		if r.Expr != nil && !referencesSyntheticAgg(r.Expr) {
			t.Errorf("rename %q has Expr but doesn't reference __agg_N: should be a name rename, got Expr=%s",
				r.To, r.Expr.String())
		}
	}
}

// TestReferencesSyntheticAgg covers the predicate's main shapes.
func TestReferencesSyntheticAgg(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{"__agg_0 / 7.0", true},
		{"__agg_0 + __agg_1", true},
		{"sum(x) / 7.0", false}, // un-rewritten — has FuncCall, not ColRef
		{"substr(o_orderdate, 1, 4)", false},
		{"x", false},
		{"x + y * 2", false},
	}
	for _, c := range cases {
		ast, err := plansql.ParseExpression(c.expr)
		if err != nil {
			t.Errorf("parse %q: %v", c.expr, err)
			continue
		}
		got := referencesSyntheticAgg(ast)
		if got != c.want {
			t.Errorf("referencesSyntheticAgg(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}
