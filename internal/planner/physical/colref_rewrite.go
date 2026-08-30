package physical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// rewriteColRefs rebuilds n with every column reference sub claims replaced by
// what sub returns, copy-on-write: a subtree nothing changed comes back as the
// same pointer, so a caller that changed nothing has changed nothing.
//
// It exists because THREE respell sites had grown their own walk over the
// expression AST, each covering the node kinds its own defect happened to
// need. A walk that does not descend into a node kind is not a no-op: the
// references inside that kind are left naming something the consuming stage
// does not carry, `expr.ColRef.Eval` answers nil for them, and the query comes
// back with a wrong number rather than an error. `respellDerivedAliasRefs`
// handled arithmetic, a paren, a cast and a function call — so
// `SUM(v * 2) OVER ()` over a derived alias was respelled and
// `SUM(CASE WHEN s = 'x' THEN v ELSE 0 END)` over the same alias was not
// (#702, TPC-H Q08's exact shape).
//
// complete reports whether the walk UNDERSTOOD every node it met. It is false
// for a subquery, an EXISTS and a window call — each carries raw SQL or a
// clause structure this walk deliberately does not rewrite — and for any node
// kind added to the AST since. A caller may still use the partially rewritten
// expression; what it may not do is treat a `complete == false` walk as proof
// that every reference now names something. That proof is the schema assert's
// job (assertCarrierSchemaResolves), which reads the finished plan and refuses
// it when a name resolves to nothing.
func rewriteColRefs(n plansql.Node, sub func(*plansql.ColRef) (plansql.Node, bool)) (
	out plansql.Node, changed, complete bool,
) {
	switch e := n.(type) {
	case nil:
		return nil, false, true

	case *plansql.ColRef:
		if rep, ok := sub(e); ok {
			return rep, true, true
		}
		return n, false, true

	// Leaves: nothing to descend into, and nothing this walk can misread.
	case *plansql.Lit, *plansql.StarNode, *plansql.IntervalLit, *plansql.LiteralPlaceholder:
		return n, false, true

	case *plansql.BinaryOp:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		r, rc, rok := rewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true, lok && rok

	case *plansql.UnaryOp:
		in, c, ok := rewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true, ok

	case *plansql.ParenNode:
		in, c, ok := rewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.ParenNode{Inner: in}, true, ok

	case *plansql.CastNode:
		in, c, ok := rewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true, ok

	case *plansql.NotNode:
		in, c, ok := rewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.NotNode{Inner: in}, true, ok

	case *plansql.CmpExpr:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		r, rc, rok := rewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true, lok && rok

	case *plansql.AndNode:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		r, rc, rok := rewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.AndNode{Left: l, Right: r}, true, lok && rok

	case *plansql.OrNode:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		r, rc, rok := rewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.OrNode{Left: l, Right: r}, true, lok && rok

	case *plansql.IsExpr:
		l, c, ok := rewriteColRefs(e.Left, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.IsExpr{Left: l, Not: e.Not, Check: e.Check}, true, ok

	case *plansql.LikeExpr:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		p, pc, pok := rewriteColRefs(e.Pattern, sub)
		if !lc && !pc {
			return n, false, lok && pok
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true, lok && pok

	case *plansql.BetweenExpr:
		l, lc, lok := rewriteColRefs(e.Left, sub)
		lo, loc, look := rewriteColRefs(e.Low, sub)
		hi, hic, hiok := rewriteColRefs(e.High, sub)
		ok := lok && look && hiok
		if !lc && !loc && !hic {
			return n, false, ok
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true, ok

	case *plansql.InExpr:
		l, changed, ok := rewriteColRefs(e.Left, sub)
		vals := make([]plansql.Node, len(e.Values))
		for i, v := range e.Values {
			nv, c, vok := rewriteColRefs(v, sub)
			vals[i] = nv
			changed = changed || c
			ok = ok && vok
		}
		if !changed {
			return n, false, ok
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: vals}, true, ok

	case *plansql.AnyAllExpr:
		l, changed, ok := rewriteColRefs(e.Left, sub)
		vals := make([]plansql.Node, len(e.Values))
		for i, v := range e.Values {
			nv, c, vok := rewriteColRefs(v, sub)
			vals[i] = nv
			changed = changed || c
			ok = ok && vok
		}
		if !changed {
			return n, false, ok
		}
		return &plansql.AnyAllExpr{Left: l, Op: e.Op, Modifier: e.Modifier, Values: vals}, true, ok

	case *plansql.FuncCallNode:
		changed, ok := false, true
		args := make([]plansql.Node, len(e.Args))
		for i, a := range e.Args {
			na, c, aok := rewriteColRefs(a, sub)
			args[i] = na
			changed = changed || c
			ok = ok && aok
		}
		if !changed {
			return n, false, ok
		}
		out := *e
		out.Args = args
		return &out, true, ok

	case *plansql.ArrayLitNode:
		changed, ok := false, true
		els := make([]plansql.Node, len(e.Elements))
		for i, el := range e.Elements {
			ne, c, eok := rewriteColRefs(el, sub)
			els[i] = ne
			changed = changed || c
			ok = ok && eok
		}
		if !changed {
			return n, false, ok
		}
		return &plansql.ArrayLitNode{Elements: els}, true, ok

	case *plansql.TupleNode:
		changed, ok := false, true
		els := make([]plansql.Node, len(e.Elements))
		for i, el := range e.Elements {
			ne, c, eok := rewriteColRefs(el, sub)
			els[i] = ne
			changed = changed || c
			ok = ok && eok
		}
		if !changed {
			return n, false, ok
		}
		return &plansql.TupleNode{Elements: els}, true, ok

	case *plansql.CaseNode:
		// The kind whose absence #702 is: TPC-H Q08 aggregates a CASE whose
		// THEN branch is a bare reference to a column a derived table
		// computes, and a walk that stopped at the CASE left that reference
		// naming nothing the stage carries.
		subj, changed, ok := rewriteColRefs(e.Subject, sub)
		whens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			nc, cc, cok := rewriteColRefs(w.Cond, sub)
			nr, rc, rok := rewriteColRefs(w.Result, sub)
			whens[i] = plansql.WhenClause{Cond: nc, Result: nr}
			changed = changed || cc || rc
			ok = ok && cok && rok
		}
		els, ec, eok := rewriteColRefs(e.Else, sub)
		changed = changed || ec
		ok = ok && eok
		if !changed {
			return n, false, ok
		}
		return &plansql.CaseNode{Subject: subj, Whens: whens, Else: els}, true, ok
	}

	// A subquery, an EXISTS, a window call, or a node kind added since. Each
	// either carries raw SQL this walk will not re-parse or has a clause
	// structure of its own; leaving it alone is right, and SAYING so is what
	// keeps a caller from reading silence as coverage.
	return n, false, false
}
