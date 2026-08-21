package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// #384 regression suite: the Filter-Project swap must substitute a computed
// or renamed alias's defining expression into the predicate before pushing
// it below the Project — or decline the push when substitution is unsound.

// nullifProjections is the repro's Project:
// SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region
func nullifProjections() []Projection {
	return []Projection{
		{Column: "r_regionkey", Expr: "r_regionkey"},
		{Alias: "rk2", Expr: "NULLIF(r_regionkey, 2)", ASTExpr: &plansql.FuncCallNode{
			Name: "nullif",
			Args: []plansql.Node{
				&plansql.ColRef{Column: "r_regionkey"},
				&plansql.Lit{Value: "2", Kind: plansql.LitNumber},
			},
		}},
	}
}

// predGT builds `col > 1` with an AST, the shape the builder produces for a
// WHERE clause.
func predGT(table, col string) Predicate {
	ast := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Table: table, Column: col},
		Op:    ">",
		Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
	}
	return Predicate{Raw: ast.String(), ASTExpr: ast}
}

func TestFilterProjectSwap_SubstitutesComputedAlias(t *testing.T) {
	scan := NewScan("region", "t")
	proj := NewProject(scan, nullifProjections())
	filter := NewFilter(proj, []Predicate{predGT("", "rk2")})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	pushed := result.Children[0]
	if pushed.Type != NodeFilter {
		t.Fatalf("expected Filter below Project, got %s", pushed.Type)
	}
	if pushed.Children[0] != scan {
		t.Fatal("expected pushed Filter directly above the scan")
	}
	if len(pushed.Predicates) != 1 {
		t.Fatalf("expected 1 pushed predicate, got %d", len(pushed.Predicates))
	}
	got := pushed.Predicates[0]
	if strings.Contains(strings.ToLower(got.Raw), "rk2") {
		t.Errorf("pushed predicate still references the alias: %q", got.Raw)
	}
	if !strings.Contains(strings.ToLower(got.Raw), "nullif(r_regionkey, 2)") {
		t.Errorf("pushed predicate does not carry the defining expression: %q", got.Raw)
	}
	refs := map[string]bool{}
	collectASTColumnRefs(got.ASTExpr, refs)
	if refs["rk2"] {
		t.Errorf("pushed AST still references rk2: %v", refs)
	}
	if !refs["r_regionkey"] {
		t.Errorf("pushed AST does not reference the source column: %v", refs)
	}
}

func TestFilterProjectSwap_SubstitutesQualifiedComputedAlias(t *testing.T) {
	// The join spelling: WHERE r.rk2 > 1 routed onto the build subquery.
	scan := NewScan("region", "r")
	proj := NewProject(scan, nullifProjections())
	filter := NewFilter(proj, []Predicate{predGT("r", "rk2")})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	got := result.Children[0].Predicates[0]
	if strings.Contains(strings.ToLower(got.Raw), "rk2") {
		t.Errorf("pushed predicate still references the qualified alias: %q", got.Raw)
	}
	if !strings.Contains(strings.ToLower(got.Raw), "nullif") {
		t.Errorf("pushed predicate does not carry the defining expression: %q", got.Raw)
	}
}

func TestFilterProjectSwap_SubstitutesRename(t *testing.T) {
	scan := NewScan("region", "t")
	proj := NewProject(scan, []Projection{
		{Column: "r_regionkey", Alias: "k", Expr: "r_regionkey",
			ASTExpr: &plansql.ColRef{Column: "r_regionkey"}},
	})
	filter := NewFilter(proj, []Predicate{predGT("", "k")})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	got := result.Children[0].Predicates[0]
	if got.Raw != "r_regionkey > 1" {
		t.Errorf("renamed predicate = %q, want %q", got.Raw, "r_regionkey > 1")
	}
}

func TestFilterProjectSwap_MixedPredicateRewritesOnlyAliasRefs(t *testing.T) {
	// WHERE rk2 >= r_regionkey: the alias ref is substituted, the
	// passthrough ref is untouched.
	scan := NewScan("region", "t")
	proj := NewProject(scan, nullifProjections())
	ast := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "rk2"},
		Op:    ">=",
		Right: &plansql.ColRef{Column: "r_regionkey"},
	}
	filter := NewFilter(proj, []Predicate{{Raw: ast.String(), ASTExpr: ast}})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	got := result.Children[0].Predicates[0]
	want := "(nullif(r_regionkey, 2)) >= r_regionkey"
	if got.Raw != want {
		t.Errorf("mixed predicate = %q, want %q", got.Raw, want)
	}
}

func TestFilterProjectSwap_PassthroughPredicateUnchanged(t *testing.T) {
	// A predicate on a passthrough column crosses exactly as before #384.
	scan := NewScan("region", "t")
	proj := NewProject(scan, nullifProjections())
	orig := predGT("", "r_regionkey")
	filter := NewFilter(proj, []Predicate{orig})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	got := result.Children[0].Predicates[0]
	if got.Raw != orig.Raw || got.ASTExpr != orig.ASTExpr {
		t.Errorf("passthrough predicate was rewritten: %q (AST identity changed: %v)",
			got.Raw, got.ASTExpr != orig.ASTExpr)
	}
}

func TestFilterProjectSwap_DeclinesVolatileDefinition(t *testing.T) {
	// SELECT random() AS r FROM region ... WHERE r < 0.5 — substituting
	// would evaluate random() a second time with a different value, so the
	// push is declined and the Filter stays above the Project.
	scan := NewScan("region", "t")
	proj := NewProject(scan, []Projection{
		{Alias: "r", Expr: "random()", ASTExpr: &plansql.FuncCallNode{Name: "random"}},
	})
	ast := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "r"},
		Op:    "<",
		Right: &plansql.Lit{Value: "0.5", Kind: plansql.LitNumber},
	}
	filter := NewFilter(proj, []Predicate{{Raw: ast.String(), ASTExpr: ast}})

	result := pushdownPredicates(filter)

	if result.Type != NodeFilter {
		t.Fatalf("expected the Filter to stay above the Project, got %s", result.Type)
	}
	if result.Children[0] != proj {
		t.Fatal("expected the declined Filter to keep the Project as its child")
	}
	if result.Predicates[0].Raw != ast.String() {
		t.Errorf("declined predicate was rewritten: %q", result.Predicates[0].Raw)
	}
}

func TestFilterProjectSwap_DeclinesAggregateOutput(t *testing.T) {
	// An aggregate output has no row-wise defining expression.
	scan := NewScan("region", "t")
	proj := NewProject(scan, []Projection{
		{Alias: "s", Expr: "SUM(r_regionkey)", IsAgg: true},
	})
	filter := NewFilter(proj, []Predicate{predGT("", "s")})

	result := pushdownPredicates(filter)

	if result.Type != NodeFilter {
		t.Fatalf("expected the Filter to stay above the Project, got %s", result.Type)
	}
}

func TestFilterProjectSwap_DeclinesSubqueryBearingPredicate(t *testing.T) {
	// The predicate correlates a subquery on the computed alias; the
	// rewriter cannot see into the subquery's SQL, so the push declines.
	scan := NewScan("region", "t")
	proj := NewProject(scan, nullifProjections())
	ast := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "rk2"},
		Op:    ">",
		Right: &plansql.SubqueryNode{SQL: "SELECT AVG(n_regionkey) FROM nation WHERE n_regionkey = rk2"},
	}
	filter := NewFilter(proj, []Predicate{{Raw: ast.String(), ASTExpr: ast}})

	result := pushdownPredicates(filter)

	if result.Type != NodeFilter {
		t.Fatalf("expected the Filter to stay above the Project, got %s", result.Type)
	}
}

func TestFilterProjectSwap_SplitsPushableFromDeclined(t *testing.T) {
	// One pushable predicate and one declined predicate: the pushable one
	// crosses (substituted), the declined one stays above the Project.
	scan := NewScan("region", "t")
	proj := NewProject(scan, []Projection{
		{Column: "r_regionkey", Expr: "r_regionkey"},
		{Alias: "rk2", Expr: "NULLIF(r_regionkey, 2)", ASTExpr: &plansql.FuncCallNode{
			Name: "nullif",
			Args: []plansql.Node{
				&plansql.ColRef{Column: "r_regionkey"},
				&plansql.Lit{Value: "2", Kind: plansql.LitNumber},
			},
		}},
		{Alias: "r", Expr: "random()", ASTExpr: &plansql.FuncCallNode{Name: "random"}},
	})
	volatilePred := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "r"},
		Op:    "<",
		Right: &plansql.Lit{Value: "0.5", Kind: plansql.LitNumber},
	}
	filter := NewFilter(proj, []Predicate{
		predGT("", "rk2"),
		{Raw: volatilePred.String(), ASTExpr: volatilePred},
	})

	result := pushdownPredicates(filter)

	if result.Type != NodeFilter {
		t.Fatalf("expected kept Filter at top, got %s", result.Type)
	}
	if len(result.Predicates) != 1 || !strings.Contains(result.Predicates[0].Raw, "r <") {
		t.Fatalf("kept Filter carries %v, want the volatile predicate", result.Predicates)
	}
	projNode := result.Children[0]
	if projNode.Type != NodeProject {
		t.Fatalf("expected Project below kept Filter, got %s", projNode.Type)
	}
	pushed := projNode.Children[0]
	if pushed.Type != NodeFilter {
		t.Fatalf("expected pushed Filter below Project, got %s", pushed.Type)
	}
	if len(pushed.Predicates) != 1 || !strings.Contains(strings.ToLower(pushed.Predicates[0].Raw), "nullif") {
		t.Fatalf("pushed Filter carries %v, want the substituted rk2 predicate", pushed.Predicates)
	}
	if pushed.Children[0] != scan {
		t.Fatal("expected pushed Filter directly above the scan")
	}
}

func TestFilterProjectSwap_ThreeValuedLogicSurvivesCaseSubstitution(t *testing.T) {
	// CASE WHEN r_regionkey < 2 THEN NULL ELSE r_regionkey END AS bucket,
	// WHERE bucket > 2: the substituted predicate must keep the CASE (and
	// its NULL arm) intact, not simplify it away.
	scan := NewScan("nation", "t")
	caseExpr := &plansql.CaseNode{
		Whens: []plansql.WhenClause{{
			Cond: &plansql.CmpExpr{
				Left:  &plansql.ColRef{Column: "n_regionkey"},
				Op:    "<",
				Right: &plansql.Lit{Value: "2", Kind: plansql.LitNumber},
			},
			Result: &plansql.Lit{Kind: plansql.LitNull},
		}},
		Else: &plansql.ColRef{Column: "n_regionkey"},
	}
	proj := NewProject(scan, []Projection{
		{Column: "n_name", Expr: "n_name"},
		{Alias: "bucket", Expr: "CASE ...", ASTExpr: caseExpr},
	})
	ast := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "bucket"},
		Op:    ">",
		Right: &plansql.Lit{Value: "2", Kind: plansql.LitNumber},
	}
	filter := NewFilter(proj, []Predicate{{Raw: ast.String(), ASTExpr: ast}})

	result := pushdownPredicates(filter)

	if result.Type != NodeProject {
		t.Fatalf("expected Project at top after pushdown, got %s", result.Type)
	}
	got := result.Children[0].Predicates[0]
	raw := strings.ToLower(got.Raw)
	if !strings.Contains(raw, "case") || !strings.Contains(raw, "null") {
		t.Errorf("substituted predicate lost the CASE/NULL structure: %q", got.Raw)
	}
	// The substituted CASE must be parenthesized so the reparsed Raw keeps
	// its precedence.
	cmp, ok := got.ASTExpr.(*plansql.CmpExpr)
	if !ok {
		t.Fatalf("substituted AST is %T, want *CmpExpr", got.ASTExpr)
	}
	if _, ok := cmp.Left.(*plansql.ParenNode); !ok {
		t.Errorf("substituted definition is %T, want parenthesized", cmp.Left)
	}
}

func TestFilterProjectSwap_SimpleFormPredicateOnAliasDeclines(t *testing.T) {
	// A Column/Op/Value predicate (no AST) naming a computed alias cannot
	// be rewritten consistently — it must not cross.
	scan := NewScan("region", "t")
	proj := NewProject(scan, nullifProjections())
	filter := NewFilter(proj, []Predicate{{Column: "rk2", Op: ">", Value: 1}})

	result := pushdownPredicates(filter)

	if result.Type != NodeFilter {
		t.Fatalf("expected the Filter to stay above the Project, got %s", result.Type)
	}
}
