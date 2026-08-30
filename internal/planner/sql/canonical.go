package sql

import "strings"

// The identity of an expression, and the name a GROUP BY key is emitted
// under.
//
// A grouped query says the same expression twice — once in GROUP BY, once in
// the SELECT list, and again in HAVING or ORDER BY — and every consumer above
// the aggregate has to decide which SELECT item IS which group key. That
// decision used to be made on the RENDERED TEXT of the two spellings, and SQL
// does not promise that two spellings of one expression render alike:
// `(g + 1)` and `g + 1` are the same expression, `G + 1` and `g + 1` are the
// same expression, and `"g + 1"` is not an expression at all but the NAME of
// one column. Every site had its own approximation of "the same" — one
// case-insensitive, one paren-insensitive, one neither — so which spelling
// was used decided which path answered wrongly, silently, with a NULL key
// column (#723) or a group that never matched its HAVING (#720) or a key that
// shipped to the worker with its quote characters attached (#725).
//
// ExprIdentity is the ONE answer. Two expressions are the same group key when
// their identities are equal, and nothing else decides it.

// ExprIdentity renders an expression in the canonical form that answers "are
// these the same expression?".
//
// Three spelling differences are erased, and only these three:
//
//   - PARENTHESES. `(g + 1)`, `((g) + 1)` and `g + 1` are one identity. The
//     parse tree already records the grouping, so a ParenNode carries no
//     information the tree does not.
//   - IDENTIFIER CASE. `G + 1` and `g + 1` are one identity, the way
//     PostgreSQL folds an unquoted identifier.
//   - WHITESPACE. `g+1` and `g + 1` are one identity, which the AST rendering
//     already gave.
//
// Nothing else is erased. Associativity in particular is NOT: `g - 1 - 2` is
// `(g - 1) - 2` and `g - (1 - 2)` is itself, and the two identities differ,
// because the two expressions differ. That is why the rendering is fully
// parenthesised at every infix node rather than relying on String()'s
// precedence-free output — dropping a ParenNode and printing `a * b + c` for
// `a * (b + c)` would make two DIFFERENT expressions one identity, which is
// the wrong answer in the more dangerous direction.
func ExprIdentity(n Node) string {
	c := canonicalExpr(n, true)
	if c == nil {
		return ""
	}
	return c.String()
}

// GroupKeyName is the column name a GROUP BY term's value is published under
// by the aggregate, on both execution paths.
//
// A bare column reference keeps its own name with any delimiters stripped, so
// `GROUP BY "g + 1"` names the column `g + 1` and not the four-token string
// `"g + 1"` the worker's hash aggregate cannot find in a batch (#725).
// Anything else — a computed key — is named by its own rendered text with
// redundant outer parentheses removed, so `GROUP BY (g + 1)` and
// `GROUP BY g + 1` publish ONE name and a SELECT item spelled either way
// resolves to it.
//
// Case is PRESERVED: this is a name a batch column is matched against by
// bytes, so the name must be what the value is actually published under.
// ExprIdentity, which is only ever compared, is the one that folds case.
func GroupKeyName(n Node) string {
	u := Unparen(n)
	if u == nil {
		return ""
	}
	if ref, ok := u.(*ColRef); ok {
		return NormalizeIdentRef(ref.String())
	}
	return u.String()
}

// Unparen strips redundant outer parentheses from an expression. `(g)` is
// `g`; `(a) + (b)` is unchanged, because its parentheses are not outer.
func Unparen(n Node) Node {
	for {
		p, ok := n.(*ParenNode)
		if !ok || p.Inner == nil {
			return n
		}
		n = p.Inner
	}
}

// canonicalExpr rebuilds n in the shape ExprIdentity renders: every ParenNode
// dropped, every infix node's non-leaf operands re-wrapped so the rendering
// stays unambiguous, and — when fold is set — every identifier and function
// name lower-cased.
//
// A node kind this does not know is returned as it stands rather than
// guessed at. That is the conservative answer: an unrecognised expression
// simply keeps whatever identity its own String() gives it, which is exactly
// the behaviour every caller had before this function existed.
func canonicalExpr(n Node, fold bool) Node {
	switch e := n.(type) {
	case nil:
		return nil
	case *ParenNode:
		return canonicalExpr(e.Inner, fold)
	case *ColRef:
		if !fold {
			return e
		}
		return &ColRef{Table: strings.ToLower(e.Table), Column: strings.ToLower(e.Column)}
	case *BinaryOp:
		return &BinaryOp{Left: canonicalOperand(e.Left, fold), Op: strings.ToLower(e.Op),
			Right: canonicalOperand(e.Right, fold)}
	case *UnaryOp:
		return &UnaryOp{Op: e.Op, Inner: canonicalOperand(e.Inner, fold)}
	case *CmpExpr:
		return &CmpExpr{Left: canonicalOperand(e.Left, fold), Op: e.Op,
			Right: canonicalOperand(e.Right, fold)}
	case *AndNode:
		return &AndNode{Left: canonicalOperand(e.Left, fold), Right: canonicalOperand(e.Right, fold)}
	case *OrNode:
		return &OrNode{Left: canonicalOperand(e.Left, fold), Right: canonicalOperand(e.Right, fold)}
	case *NotNode:
		return &NotNode{Inner: canonicalOperand(e.Inner, fold)}
	case *IsExpr:
		return &IsExpr{Left: canonicalOperand(e.Left, fold), Not: e.Not, Check: strings.ToLower(e.Check)}
	case *LikeExpr:
		return &LikeExpr{Left: canonicalOperand(e.Left, fold), Not: e.Not,
			Pattern: canonicalOperand(e.Pattern, fold)}
	case *BetweenExpr:
		return &BetweenExpr{Left: canonicalOperand(e.Left, fold), Not: e.Not,
			Low: canonicalOperand(e.Low, fold), High: canonicalOperand(e.High, fold)}
	case *InExpr:
		out := &InExpr{Left: canonicalOperand(e.Left, fold), Not: e.Not,
			Values: make([]Node, len(e.Values))}
		for i, v := range e.Values {
			out.Values[i] = canonicalExpr(v, fold)
		}
		return out
	case *AnyAllExpr:
		out := &AnyAllExpr{Left: canonicalOperand(e.Left, fold), Op: e.Op,
			Modifier: strings.ToUpper(e.Modifier), Values: make([]Node, len(e.Values))}
		for i, v := range e.Values {
			out.Values[i] = canonicalExpr(v, fold)
		}
		return out
	case *FuncCallNode:
		name := e.Name
		if fold {
			name = strings.ToLower(name)
		}
		out := &FuncCallNode{Name: name, Distinct: e.Distinct, Star: e.Star,
			Args: make([]Node, len(e.Args))}
		for i, a := range e.Args {
			out.Args[i] = canonicalExpr(a, fold)
		}
		return out
	case *CastNode:
		return &CastNode{Inner: canonicalExpr(e.Inner, fold), TypeName: strings.ToUpper(e.TypeName)}
	case *CaseNode:
		out := &CaseNode{Subject: canonicalExpr(e.Subject, fold), Else: canonicalExpr(e.Else, fold),
			Whens: make([]WhenClause, len(e.Whens))}
		for i, w := range e.Whens {
			out.Whens[i] = WhenClause{Cond: canonicalExpr(w.Cond, fold), Result: canonicalExpr(w.Result, fold)}
		}
		return out
	case *ArrayLitNode:
		out := &ArrayLitNode{Elements: make([]Node, len(e.Elements))}
		for i, el := range e.Elements {
			out.Elements[i] = canonicalExpr(el, fold)
		}
		return out
	case *TupleNode:
		out := &TupleNode{Elements: make([]Node, len(e.Elements))}
		for i, el := range e.Elements {
			out.Elements[i] = canonicalExpr(el, fold)
		}
		return out
	}
	return n
}

// canonicalOperand is canonicalExpr for a position where the rendering must
// stay unambiguous: an infix node's operand is re-wrapped in parentheses when
// it is itself infix, so `(g - 1) - 2` and `g - (1 - 2)` render differently.
func canonicalOperand(n Node, fold bool) Node {
	c := canonicalExpr(n, fold)
	if isInfixNode(c) {
		return &ParenNode{Inner: c}
	}
	return c
}

// isInfixNode reports whether a node renders as operands around an operator,
// with no delimiters of its own. Those are the ones whose nesting the
// rendering has to state; a function call, a CAST, a CASE and a tuple already
// carry their own brackets.
func isInfixNode(n Node) bool {
	switch n.(type) {
	case *BinaryOp, *UnaryOp, *CmpExpr, *AndNode, *OrNode, *NotNode,
		*IsExpr, *LikeExpr, *BetweenExpr, *InExpr, *AnyAllExpr:
		return true
	}
	return false
}

// ReplaceGroupKeyRefs rewrites every subexpression that IS one of an
// aggregate's GROUP BY keys into a reference to the column the aggregate
// publishes that key under. keys maps ExprIdentity to published name.
//
// This is what makes a HAVING over a computed group key mean anything. Above
// the aggregate the input columns are gone and only the key's own output
// column carries the value, so `HAVING g + 1 > 2` written as arithmetic over
// `g` evaluated to UNKNOWN on every row — and a filter admits only TRUE, so
// the query returned no rows at all where PostgreSQL returns five (#720).
//
// The walk is TOP-DOWN and stops at the first whole-term match, so the
// LARGEST expression that is a key is the one replaced: over
// `GROUP BY g + 1`, the predicate `g + 1 > 2` becomes `"g + 1" > 2` rather
// than descending to a `g` the aggregate does not emit.
//
// It never enters an aggregate call. Inside one, `SUM(g + 1)`, the expression
// is evaluated over the aggregate's INPUT rows, where `g` is exactly the
// column that does exist; replacing it with the grouped output would compute
// something else entirely.
func ReplaceGroupKeyRefs(node Node, keys map[string]string) Node {
	if node == nil || len(keys) == 0 {
		return node
	}
	// A bare column reference is never re-pointed: a plain group key is
	// already published under its own name, and a ROW field path resolves
	// through the same dotted spelling on both engines.
	if _, isRef := node.(*ColRef); !isRef {
		if name, ok := keys[ExprIdentity(node)]; ok {
			return &ColRef{Column: name}
		}
	}
	switch n := node.(type) {
	case *ParenNode:
		return &ParenNode{Inner: ReplaceGroupKeyRefs(n.Inner, keys)}
	case *BinaryOp:
		return &BinaryOp{Left: ReplaceGroupKeyRefs(n.Left, keys), Op: n.Op,
			Right: ReplaceGroupKeyRefs(n.Right, keys)}
	case *UnaryOp:
		return &UnaryOp{Op: n.Op, Inner: ReplaceGroupKeyRefs(n.Inner, keys)}
	case *CmpExpr:
		return &CmpExpr{Left: ReplaceGroupKeyRefs(n.Left, keys), Op: n.Op,
			Right: ReplaceGroupKeyRefs(n.Right, keys)}
	case *AndNode:
		return &AndNode{Left: ReplaceGroupKeyRefs(n.Left, keys),
			Right: ReplaceGroupKeyRefs(n.Right, keys)}
	case *OrNode:
		return &OrNode{Left: ReplaceGroupKeyRefs(n.Left, keys),
			Right: ReplaceGroupKeyRefs(n.Right, keys)}
	case *NotNode:
		return &NotNode{Inner: ReplaceGroupKeyRefs(n.Inner, keys)}
	case *IsExpr:
		return &IsExpr{Left: ReplaceGroupKeyRefs(n.Left, keys), Not: n.Not, Check: n.Check}
	case *LikeExpr:
		return &LikeExpr{Left: ReplaceGroupKeyRefs(n.Left, keys), Not: n.Not,
			Pattern: ReplaceGroupKeyRefs(n.Pattern, keys)}
	case *BetweenExpr:
		return &BetweenExpr{Left: ReplaceGroupKeyRefs(n.Left, keys), Not: n.Not,
			Low: ReplaceGroupKeyRefs(n.Low, keys), High: ReplaceGroupKeyRefs(n.High, keys)}
	case *InExpr:
		out := &InExpr{Left: ReplaceGroupKeyRefs(n.Left, keys), Not: n.Not,
			Values: make([]Node, len(n.Values))}
		for i, v := range n.Values {
			out.Values[i] = ReplaceGroupKeyRefs(v, keys)
		}
		return out
	case *CastNode:
		return &CastNode{Inner: ReplaceGroupKeyRefs(n.Inner, keys), TypeName: n.TypeName}
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			return node
		}
		out := &FuncCallNode{Name: n.Name, Distinct: n.Distinct, Star: n.Star,
			Args: make([]Node, len(n.Args))}
		for i, a := range n.Args {
			out.Args[i] = ReplaceGroupKeyRefs(a, keys)
		}
		return out
	case *CaseNode:
		out := &CaseNode{Subject: ReplaceGroupKeyRefs(n.Subject, keys),
			Else: ReplaceGroupKeyRefs(n.Else, keys), Whens: make([]WhenClause, len(n.Whens))}
		for i, w := range n.Whens {
			out.Whens[i] = WhenClause{Cond: ReplaceGroupKeyRefs(w.Cond, keys),
				Result: ReplaceGroupKeyRefs(w.Result, keys)}
		}
		return out
	case *ArrayLitNode:
		out := &ArrayLitNode{Elements: make([]Node, len(n.Elements))}
		for i, e := range n.Elements {
			out.Elements[i] = ReplaceGroupKeyRefs(e, keys)
		}
		return out
	case *TupleNode:
		out := &TupleNode{Elements: make([]Node, len(n.Elements))}
		for i, e := range n.Elements {
			out.Elements[i] = ReplaceGroupKeyRefs(e, keys)
		}
		return out
	}
	// Anything else — a subquery, an EXISTS, a window call — has its own
	// scope and is left exactly as it stands.
	return node
}
