package physical

import (
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
// table's or CTE's SELECT-list alias with the source column the DAG's streams
// carry. Copy-on-write over the node kinds an expression key can hold; an
// unrecognized kind is left alone, which keeps today's behaviour rather than
// inventing a different expression.
func respellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	switch e := n.(type) {
	case nil:
		return nil, false
	case *plansql.ColRef:
		src := derivedAliasSourceColumn(e.String(), child)
		if src == "" && e.Table == "" {
			src = derivedAliasSourceColumn(e.Column, child)
		}
		if src == "" {
			return n, false
		}
		return &plansql.ColRef{Column: cleanExpr(src)}, true
	case *plansql.BinaryOp:
		l, lc := respellDerivedAliasRefs(e.Left, child)
		r, rc := respellDerivedAliasRefs(e.Right, child)
		if !lc && !rc {
			return n, false
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, c := respellDerivedAliasRefs(e.Inner, child)
		if !c {
			return n, false
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.ParenNode:
		in, c := respellDerivedAliasRefs(e.Inner, child)
		if !c {
			return n, false
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, c := respellDerivedAliasRefs(e.Inner, child)
		if !c {
			return n, false
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true
	case *plansql.FuncCallNode:
		args := make([]plansql.Node, len(e.Args))
		changed := false
		for i, a := range e.Args {
			na, c := respellDerivedAliasRefs(a, child)
			args[i] = na
			changed = changed || c
		}
		if !changed {
			return n, false
		}
		out := *e
		out.Args = args
		return &out, true
	}
	return n, false
}
