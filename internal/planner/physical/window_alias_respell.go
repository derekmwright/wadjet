package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// respellWindowKeyExprs rewrites each materialized window-key EXPRESSION so
// its column references name what the window stage's input really carries.
//
// resolveWindowKeys is shared by both paths, so a key expression written over
// a derived table's or CTE's SELECT-list alias (`SUM(v * 2) OVER ()` above
// `SELECT c_i64 AS v`) is correct for the single-process pipeline, where the
// Project below the window is a real operator. On the DAG that Project emits
// no stage, so `v` names nothing: the fragment's pre-window projection
// evaluated it to NULL and the window wrote NULL in every row — #672's other
// half, and the ARGUMENT sibling of #658's PARTITION BY key.
//
// Only a reference the alias walk RESOLVES is rewritten; a spec whose
// expression cannot be re-parsed, or that names nothing derived, comes back
// exactly as it was.
func respellWindowKeyExprs(specs []ProjectExprSpec, child *logical.Node) []ProjectExprSpec {
	if len(specs) == 0 || child == nil {
		return specs
	}
	for i := range specs {
		ast, err := plansql.ParseExpression(specs[i].Expr)
		if err != nil {
			continue
		}
		if rewritten, changed := respellDerivedAliasRefs(ast, child); changed {
			specs[i].Expr = rewritten.String()
		}
	}
	return specs
}

// respellDerivedAliasRefs replaces every column reference naming a derived
// table's or CTE's SELECT-list RENAME with the source column the DAG's streams
// carry. Copy-on-write; a reference the alias walk does not resolve comes back
// exactly as it was.
//
// It resolves a rename only. A COMPUTED alias has no source column to point at
// — `derivedAliasSourceColumn` answers "" for one — and the expression that
// defines it is what respellAggInputExpr substitutes instead.
func respellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	out, changed, _ := rewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		src := derivedAliasSourceColumn(ref.String(), child)
		if src == "" && ref.Table == "" {
			src = derivedAliasSourceColumn(ref.Column, child)
		}
		if src == "" {
			return nil, false
		}
		return &plansql.ColRef{Column: cleanExpr(src)}, true
	})
	return out, changed
}

// respellAggInputExpr rewrites an aggregate's ARGUMENT expression so that every
// column reference in it names what the stage BELOW the aggregate really
// emits.
//
// walkStages emits no stage for an ordinary Project, so a derived table's
// SELECT list never happens on the DAG (ADR-0025). The aggregate's argument is
// shipped to the worker as TEXT and compiled there against the batch the stage
// hands it — which carries the SCAN's columns, not the derived table's names.
// `SUM(CASE WHEN s = 'x' THEN twice ELSE 0 END)` over
// `(SELECT s, id * 2 AS twice FROM t)` therefore read `twice` off a batch that
// has no such column, `expr.ColRef.Eval` answered nil for every row, and the
// SUM came back as the total of the CASE's ELSE branch — 0 where PostgreSQL
// answers 2. It is TPC-H Q08's exact shape and it is type-independent: a plain
// rename triggers it too, and a rename that SHADOWS a base column answered a
// different wrong number rather than a zero (#702).
//
// resolveAggInputName already does this for an argument that IS a name; this
// is the same resolution applied one level down, to each reference inside an
// argument that is an EXPRESSION. Both outcomes it can report are used:
//
//	a RENAME       — the reference becomes the source column;
//	a COMPUTED     — the reference becomes the expression that defines it,
//	  alias          PARENTHESIZED, because the definition is substituted into
//	                 a larger expression and `id * 2` spliced bare into `x * 3`
//	                 would re-associate.
//
// The single-process pipeline runs that Project as a real operator, so it is
// already right and this rewrite is DAG-only: it is applied to the stage spec's
// text, never to the logical node the local engine executes.
func respellAggInputExpr(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	if n == nil || child == nil {
		return n, false
	}
	out, changed, _ := rewriteColRefs(n, func(ref *plansql.ColRef) (plansql.Node, bool) {
		if resolved, expr, _, renamed := resolveAggInputName(ref.String(), child); renamed {
			if expr != nil {
				return &plansql.ParenNode{Inner: expr}, true
			}
			if !strings.EqualFold(resolved, ref.String()) {
				return &plansql.ColRef{Column: cleanExpr(resolved)}, true
			}
			return nil, false
		}
		// A ROW FIELD PATH whose CONTAINER is the rename: `rw.b` over
		// `SELECT c_row AS rw`. The whole spelling names no column, and the
		// field `b` is not a column either — only the QUALIFIER is a name to
		// resolve, and resolving it gives the path the stage's stream really
		// carries. Without this the reference reached the worker as `rw.b`,
		// which nothing there can look up.
		//
		// It runs only AFTER the whole spelling has failed, which is
		// ADR-0022 §1's order: a derived table that emits a column named
		// `rw.b`, or whose own alias is `rw`, is resolved above and never
		// reaches here. A qualifier that is a FROM alias rather than a
		// rename resolves to nothing and is left exactly as written.
		if ref.Table == "" {
			return nil, false
		}
		qual, qexpr, _, qrenamed := resolveAggInputName(ref.Table, child)
		if !qrenamed || qexpr != nil || strings.EqualFold(qual, ref.Table) {
			return nil, false
		}
		return &plansql.ColRef{Table: cleanExpr(qual), Column: ref.Column}, true
	})
	return out, changed
}
