// SPDX-License-Identifier: MIT

package physical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// RewriteColRefs replaces column references claimed by sub, copy-on-write:
// unchanged subtrees retain their pointer (#702).
// complete means every node was understood. Subqueries, EXISTS, window calls
// and unknown node kinds return false; their raw SQL/clause structures are
// not rewritten. A caller may use partial results, but complete=false cannot
// prove references resolve. assertCarrierSchemaResolves checks the finished
// plan and refuses unresolved names.
func RewriteColRefs(n plansql.Node, sub func(*plansql.ColRef) (plansql.Node, bool)) (
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
		l, lc, lok := RewriteColRefs(e.Left, sub)
		r, rc, rok := RewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true, lok && rok

	case *plansql.UnaryOp:
		in, c, ok := RewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true, ok

	case *plansql.ParenNode:
		in, c, ok := RewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.ParenNode{Inner: in}, true, ok

	case *plansql.CastNode:
		in, c, ok := RewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true, ok

	case *plansql.NotNode:
		in, c, ok := RewriteColRefs(e.Inner, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.NotNode{Inner: in}, true, ok

	case *plansql.CmpExpr:
		l, lc, lok := RewriteColRefs(e.Left, sub)
		r, rc, rok := RewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true, lok && rok

	case *plansql.AndNode:
		l, lc, lok := RewriteColRefs(e.Left, sub)
		r, rc, rok := RewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.AndNode{Left: l, Right: r}, true, lok && rok

	case *plansql.OrNode:
		l, lc, lok := RewriteColRefs(e.Left, sub)
		r, rc, rok := RewriteColRefs(e.Right, sub)
		if !lc && !rc {
			return n, false, lok && rok
		}
		return &plansql.OrNode{Left: l, Right: r}, true, lok && rok

	case *plansql.IsExpr:
		l, c, ok := RewriteColRefs(e.Left, sub)
		if !c {
			return n, false, ok
		}
		return &plansql.IsExpr{Left: l, Not: e.Not, Check: e.Check}, true, ok

	case *plansql.LikeExpr:
		l, lc, lok := RewriteColRefs(e.Left, sub)
		p, pc, pok := RewriteColRefs(e.Pattern, sub)
		if !lc && !pc {
			return n, false, lok && pok
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true, lok && pok

	case *plansql.BetweenExpr:
		l, lc, lok := RewriteColRefs(e.Left, sub)
		lo, loc, look := RewriteColRefs(e.Low, sub)
		hi, hic, hiok := RewriteColRefs(e.High, sub)
		ok := lok && look && hiok
		if !lc && !loc && !hic {
			return n, false, ok
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true, ok

	case *plansql.InExpr:
		l, changed, ok := RewriteColRefs(e.Left, sub)
		vals := make([]plansql.Node, len(e.Values))
		for i, v := range e.Values {
			nv, c, vok := RewriteColRefs(v, sub)
			vals[i] = nv
			changed = changed || c
			ok = ok && vok
		}
		if !changed {
			return n, false, ok
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: vals}, true, ok

	case *plansql.AnyAllExpr:
		l, changed, ok := RewriteColRefs(e.Left, sub)
		vals := make([]plansql.Node, len(e.Values))
		for i, v := range e.Values {
			nv, c, vok := RewriteColRefs(v, sub)
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
			na, c, aok := RewriteColRefs(a, sub)
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
			ne, c, eok := RewriteColRefs(el, sub)
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
			ne, c, eok := RewriteColRefs(el, sub)
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
		subj, changed, ok := RewriteColRefs(e.Subject, sub)
		whens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			nc, cc, cok := RewriteColRefs(w.Cond, sub)
			nr, rc, rok := RewriteColRefs(w.Result, sub)
			whens[i] = plansql.WhenClause{Cond: nc, Result: nr}
			changed = changed || cc || rc
			ok = ok && cok && rok
		}
		els, ec, eok := RewriteColRefs(e.Else, sub)
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
