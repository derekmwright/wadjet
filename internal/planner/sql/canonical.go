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

// ExprIdentityUnqualified is ExprIdentity with TABLE QUALIFIERS erased as
// well, so `typemx.g + 1` and `g + 1` are one identity (#738).
//
// It is a SEPARATE function and not a fourth rule in ExprIdentity, because
// erasing a qualifier needs a SCOPE that this file does not have: `a.x` and
// `b.x` over a join are two expressions and `t.x` and `x` in a single-relation
// block are one. Only a caller holding the block's FROM list can tell them
// apart, and exactly one does — physical.groupCheck, which uses this when the
// block has ONE source and ExprIdentity when it has more.
//
// Erasing it unconditionally would make two different expressions one
// identity, which is the failure this file's header calls "the wrong answer in
// the more dangerous direction".
func ExprIdentityUnqualified(n Node) string {
	c := canonicalExpr(stripQualifiers(n), true)
	if c == nil {
		return ""
	}
	return c.String()
}

// stripQualifiers rebuilds n with every ColRef's table qualifier removed. It
// walks through canonicalExpr's own rebuild rather than duplicating the node
// switch: canonicalExpr copies every node it knows, so replacing the ColRef
// arm's input is enough.
func stripQualifiers(n Node) Node {
	switch e := n.(type) {
	case nil:
		return nil
	case *ColRef:
		return &ColRef{Column: e.Column}
	case *ParenNode:
		return &ParenNode{Inner: stripQualifiers(e.Inner)}
	case *BinaryOp:
		return &BinaryOp{Left: stripQualifiers(e.Left), Op: e.Op, Right: stripQualifiers(e.Right)}
	case *UnaryOp:
		return &UnaryOp{Op: e.Op, Inner: stripQualifiers(e.Inner)}
	case *CastNode:
		return &CastNode{Inner: stripQualifiers(e.Inner), TypeName: e.TypeName}
	case *FuncCallNode:
		args := make([]Node, len(e.Args))
		for i, a := range e.Args {
			args[i] = stripQualifiers(a)
		}
		c := *e
		c.Args = args
		return &c
	}
	// A node kind this does not know keeps its qualifiers, which is the
	// conservative answer: the identity then differs from the bare spelling
	// and the check declines rather than admitting a reference it cannot see
	// through.
	return n
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
		return &CastNode{Inner: canonicalExpr(e.Inner, fold), TypeName: canonicalTypeName(e.TypeName)}
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

// canonicalTypeName folds a CAST's destination to one spelling per TYPE, so
// `CAST(g AS INT)` and `CAST(g AS INTEGER)` are one identity (#738).
//
// The set is PostgreSQL's own, measured rather than assumed — a synonym pair
// there is a pair `SELECT CAST(g AS a) FROM t GROUP BY CAST(g AS b)` answers
// for, and a non-pair is one it refuses with 42803:
//
//	INT / INTEGER / INT4          SMALLINT / INT2
//	BIGINT / INT8                 REAL / FLOAT4
//	DOUBLE PRECISION / FLOAT8     DEC / DECIMAL / NUMERIC
//	BOOL / BOOLEAN                CHARACTER VARYING / VARCHAR
//
// VARCHAR and TEXT are NOT a pair and are deliberately absent: PostgreSQL
// refuses that spelling too, and wadjet refusing it is right in kind. Getting
// the set wrong in the other direction makes two DIFFERENT expressions one
// identity, which is the property this whole file exists to keep.
//
// The PARAMETERS are folded with the name — `DEC(9,2)` and `DECIMAL(9, 2)` are
// one destination — because whitespace inside them is spelling and nothing
// else, exactly as it is outside them.
func canonicalTypeName(name string) string {
	base, params := splitTypeParams(name)
	if canon, ok := typeNameSynonyms[base]; ok {
		base = canon
	}
	if params == "" {
		return base
	}
	return base + "(" + params + ")"
}

// splitTypeParams separates a type name from its parenthesised parameters and
// normalizes the whitespace in both: `dec( 9 , 2 )` becomes ("DEC", "9,2").
func splitTypeParams(name string) (string, string) {
	t := strings.TrimSpace(name)
	open := strings.IndexByte(t, '(')
	if open < 0 || !strings.HasSuffix(t, ")") {
		return strings.ToUpper(strings.Join(strings.Fields(t), " ")), ""
	}
	base := strings.ToUpper(strings.Join(strings.Fields(t[:open]), " "))
	inner := t[open+1 : len(t)-1]
	parts := strings.Split(inner, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return base, strings.Join(parts, ",")
}

// typeNameSynonyms maps every spelling of one type to a single canonical one.
// Keys and values are upper-cased with runs of whitespace collapsed, which is
// the form splitTypeParams produces.
var typeNameSynonyms = map[string]string{
	"INTEGER":           "INT",
	"INT4":              "INT",
	"INT":               "INT",
	"INT8":              "BIGINT",
	"BIGINT":            "BIGINT",
	"INT2":              "SMALLINT",
	"SMALLINT":          "SMALLINT",
	"FLOAT4":            "REAL",
	"REAL":              "REAL",
	"FLOAT8":            "DOUBLE PRECISION",
	"DOUBLE PRECISION":  "DOUBLE PRECISION",
	"DECIMAL":           "DECIMAL",
	"DEC":               "DECIMAL",
	"NUMERIC":           "DECIMAL",
	"BOOLEAN":           "BOOL",
	"BOOL":              "BOOL",
	"CHARACTER VARYING": "VARCHAR",
	"VARCHAR":           "VARCHAR",
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

// groupKeyRefLookup finds the published name for an expression that IS one of
// the aggregate's group keys.
//
// It tries the ordinary identity first and the QUALIFIER-ERASED one second, so
// `SELECT typemx.g + 1 ... GROUP BY g + 1` substitutes (#738). The second
// lookup is safe because the map only ever CONTAINS an unqualified entry when
// the builder registered one, and it registers one only for a block whose FROM
// has a single relation — the scope in which a qualifier is spelling. Over a
// join the map holds qualified identities alone, so an unqualified probe finds
// nothing and the substitution declines, which is what keeps `GROUP BY zzj.d92`
// from licensing `SELECT zzp.d92`.
func groupKeyRefLookup(keys map[string]string, n Node) (string, bool) {
	if name, ok := keys[ExprIdentity(n)]; ok {
		return name, true
	}
	name, ok := keys[ExprIdentityUnqualified(n)]
	return name, ok
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
	return RewriteExpr(node, func(n Node) (Node, bool) {
		// A bare column reference is never re-pointed: a plain group key is
		// already published under its own name, and a ROW field path
		// resolves through the same dotted spelling on both engines.
		if _, isRef := n.(*ColRef); isRef {
			return nil, false
		}
		if name, ok := groupKeyRefLookup(keys, n); ok {
			return &ColRef{Column: name}, true
		}
		return nil, false
	})
}

// RewriteExpr rebuilds an expression, offering every node to fn TOP-DOWN. fn
// returns (replacement, true) to substitute a node and stop descending into
// it, or (nil, false) to leave it and have its children visited.
//
// It never enters an aggregate call: inside one the expression is evaluated
// over the aggregate's INPUT rows, which is a different namespace from the
// grouped output every caller of this is rewriting for.
//
// A node kind it does not know is returned as it stands rather than guessed
// at — the conservative answer, and the one every caller relied on before
// this walk was shared.
func RewriteExpr(node Node, fn func(Node) (Node, bool)) Node {
	if node == nil {
		return nil
	}
	if repl, done := fn(node); done {
		return repl
	}
	switch n := node.(type) {
	case *ParenNode:
		return &ParenNode{Inner: RewriteExpr(n.Inner, fn)}
	case *BinaryOp:
		return &BinaryOp{Left: RewriteExpr(n.Left, fn), Op: n.Op,
			Right: RewriteExpr(n.Right, fn)}
	case *UnaryOp:
		return &UnaryOp{Op: n.Op, Inner: RewriteExpr(n.Inner, fn)}
	case *CmpExpr:
		return &CmpExpr{Left: RewriteExpr(n.Left, fn), Op: n.Op,
			Right: RewriteExpr(n.Right, fn)}
	case *AndNode:
		return &AndNode{Left: RewriteExpr(n.Left, fn), Right: RewriteExpr(n.Right, fn)}
	case *OrNode:
		return &OrNode{Left: RewriteExpr(n.Left, fn), Right: RewriteExpr(n.Right, fn)}
	case *NotNode:
		return &NotNode{Inner: RewriteExpr(n.Inner, fn)}
	case *IsExpr:
		return &IsExpr{Left: RewriteExpr(n.Left, fn), Not: n.Not, Check: n.Check}
	case *LikeExpr:
		return &LikeExpr{Left: RewriteExpr(n.Left, fn), Not: n.Not,
			Pattern: RewriteExpr(n.Pattern, fn)}
	case *BetweenExpr:
		return &BetweenExpr{Left: RewriteExpr(n.Left, fn), Not: n.Not,
			Low: RewriteExpr(n.Low, fn), High: RewriteExpr(n.High, fn)}
	case *InExpr:
		out := &InExpr{Left: RewriteExpr(n.Left, fn), Not: n.Not,
			Values: make([]Node, len(n.Values))}
		for i, v := range n.Values {
			out.Values[i] = RewriteExpr(v, fn)
		}
		return out
	case *CastNode:
		return &CastNode{Inner: RewriteExpr(n.Inner, fn), TypeName: n.TypeName}
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			return node
		}
		out := &FuncCallNode{Name: n.Name, Distinct: n.Distinct, Star: n.Star,
			Args: make([]Node, len(n.Args))}
		for i, a := range n.Args {
			out.Args[i] = RewriteExpr(a, fn)
		}
		return out
	case *CaseNode:
		out := &CaseNode{Subject: RewriteExpr(n.Subject, fn),
			Else: RewriteExpr(n.Else, fn), Whens: make([]WhenClause, len(n.Whens))}
		for i, w := range n.Whens {
			out.Whens[i] = WhenClause{Cond: RewriteExpr(w.Cond, fn),
				Result: RewriteExpr(w.Result, fn)}
		}
		return out
	case *ArrayLitNode:
		out := &ArrayLitNode{Elements: make([]Node, len(n.Elements))}
		for i, e := range n.Elements {
			out.Elements[i] = RewriteExpr(e, fn)
		}
		return out
	case *TupleNode:
		out := &TupleNode{Elements: make([]Node, len(n.Elements))}
		for i, e := range n.Elements {
			out.Elements[i] = RewriteExpr(e, fn)
		}
		return out
	}
	// Anything else — a subquery, an EXISTS, a window call — has its own
	// scope and is left exactly as it stands.
	return node
}
