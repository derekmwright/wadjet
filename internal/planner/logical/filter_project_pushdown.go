package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// #384: pushdownPredicates' Filter-Project swap used to be unconditional. A
// predicate referencing a column the Project COMPUTES (`NULLIF(x, 2) AS rk2`
// ... `WHERE rk2 > 1`) was pushed below the Project without substitution, so
// the filter ran against a schema that does not carry the alias: the
// single-process pipeline errored ("filter column does not exist"), and the
// stage DAG's scan-stage filter matched nothing and silently returned 0 rows.
//
// The classic fix is predicate substitution: rewrite each reference to a
// computed (or renamed) output with the alias's defining expression, then
// push — the substituted predicate names only source columns, so it also
// rides scan pushdown. Where substitution is unsound — aggregate outputs,
// volatile functions, subquery-bearing definitions, or expressions the
// rewriter cannot see through — the predicate DECLINES the push and stays
// above the Project, which is its original, trivially correct position.
// Three-valued logic holds through substitution: the predicate evaluates the
// exact defining expression the Project would have produced, NULLs included.

// projOutput describes one Project output as seen by a predicate above the
// Project: what to substitute for a reference to it when the predicate moves
// below (def), and whether such a move must be declined (unsafe).
type projOutput struct {
	// def is the expression substituted for a ColRef naming this output.
	// nil means the output passes its source column through under the same
	// name, so a reference needs no rewrite.
	def plansql.Node
	// unsafe means a predicate referencing this output must not be pushed
	// below the Project at all.
	unsafe bool
}

// projectSubstitutions builds the output-name → substitution map for a
// Project's projections.
func projectSubstitutions(projs []Projection) map[string]projOutput {
	outs := make(map[string]projOutput, len(projs))
	for _, p := range projs {
		name := strings.ToLower(p.Alias)
		if name == "" {
			name = bareColumnName(p.Column)
		}
		if name == "" {
			// An expression without an alias has no name a predicate's
			// ColRef can carry; nothing to map.
			continue
		}
		out := classifyProjection(p, name)
		if prev, dup := outs[name]; dup {
			// Two outputs share a name: a reference is ambiguous unless
			// both are the same passthrough.
			if prev.def != nil || prev.unsafe || out.def != nil || out.unsafe {
				outs[name] = projOutput{unsafe: true}
			}
			continue
		}
		outs[name] = out
	}
	return outs
}

// classifyProjection decides what a reference to this projection's output
// becomes below the Project.
func classifyProjection(p Projection, name string) projOutput {
	if p.IsAgg {
		// An aggregate output has no row-wise defining expression to
		// substitute below the Project.
		return projOutput{unsafe: true}
	}
	if p.Column != "" {
		if bareColumnName(p.Column) == name {
			return projOutput{} // passthrough: same name below
		}
		// Rename: substitute a reference to the source column. Prefer the
		// projection's own AST (it may carry a table qualifier); fall back
		// to the Column string.
		if ref := simpleColRef(p.ASTExpr); ref != nil {
			return projOutput{def: ref}
		}
		if q, col, ok := plansql.SplitIdentRef(p.Column); ok {
			return projOutput{def: &plansql.ColRef{Table: q, Column: col}}
		}
		return projOutput{def: &plansql.ColRef{Column: p.Column}}
	}
	// Computed output. Substitution needs the defining AST, and it must be
	// deterministic and self-contained.
	if p.ASTExpr == nil || substitutionUnsafe(p.ASTExpr) {
		return projOutput{unsafe: true}
	}
	if ref := simpleColRef(p.ASTExpr); ref != nil {
		return projOutput{def: ref} // rename spelled without Column
	}
	// Parenthesize so the regenerated Raw string keeps the expression's
	// precedence when a worker re-parses it.
	return projOutput{def: &plansql.ParenNode{Inner: p.ASTExpr}}
}

// bareColumnName returns the lower-cased unqualified name of a column
// reference string ("t.x" → "x").
func bareColumnName(col string) string {
	if col == "" {
		return ""
	}
	if _, name, ok := plansql.SplitIdentRef(col); ok {
		return strings.ToLower(name)
	}
	return strings.ToLower(col)
}

// simpleColRef unwraps parens and returns the node if it is a plain column
// reference, else nil.
func simpleColRef(n plansql.Node) *plansql.ColRef {
	for {
		switch e := n.(type) {
		case *plansql.ColRef:
			return e
		case *plansql.ParenNode:
			n = e.Inner
		default:
			return nil
		}
	}
}

// substitutionUnsafe reports whether substituting this defining expression
// into a predicate could change the query's meaning: volatile functions
// (duplicated evaluation), subqueries and window functions (not row-wise
// self-contained), or any node the walker does not recognize.
func substitutionUnsafe(n plansql.Node) bool {
	if n == nil {
		return false
	}
	switch e := n.(type) {
	case *plansql.ColRef, *plansql.Lit, *plansql.IntervalLit:
		return false
	case *plansql.CmpExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.AndNode:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.OrNode:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.BinaryOp:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.UnaryOp:
		return substitutionUnsafe(e.Inner)
	case *plansql.NotNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.ParenNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.CastNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.FuncCallNode:
		if volatileFuncs[strings.ToLower(e.Name)] {
			return true
		}
		for _, arg := range e.Args {
			if substitutionUnsafe(arg) {
				return true
			}
		}
		return false
	case *plansql.InExpr:
		if substitutionUnsafe(e.Left) {
			return true
		}
		for _, v := range e.Values {
			if substitutionUnsafe(v) {
				return true
			}
		}
		return false
	case *plansql.BetweenExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Low) || substitutionUnsafe(e.High)
	case *plansql.LikeExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Pattern)
	case *plansql.IsExpr:
		return substitutionUnsafe(e.Left)
	case *plansql.CaseNode:
		if substitutionUnsafe(e.Subject) || substitutionUnsafe(e.Else) {
			return true
		}
		for _, w := range e.Whens {
			if substitutionUnsafe(w.Cond) || substitutionUnsafe(w.Result) {
				return true
			}
		}
		return false
	case *plansql.ArrayLitNode:
		for _, el := range e.Elements {
			if substitutionUnsafe(el) {
				return true
			}
		}
		return false
	case *plansql.TupleNode:
		for _, el := range e.Elements {
			if substitutionUnsafe(el) {
				return true
			}
		}
		return false
	default:
		// SubqueryNode, ExistsNode, AnyAllExpr, WindowFuncNode, StarNode,
		// and anything added later: not row-wise self-contained (or not
		// understood) — do not substitute it.
		return true
	}
}

// volatileFuncs lists functions whose result differs across evaluations, so
// duplicating them via substitution would change the query's meaning.
var volatileFuncs = map[string]bool{
	"rand":            true,
	"random":          true,
	"uuid":            true,
	"gen_random_uuid": true,
}

// splitFilterForProjectPush partitions a Filter's predicates for the
// Filter-Project swap: `pushed` may cross below the Project (rewritten where
// they referenced renamed or computed outputs), `kept` must stay above it.
func splitFilterForProjectPush(preds []Predicate, projs []Projection) (pushed, kept []Predicate) {
	outs := projectSubstitutions(projs)
	for _, pred := range preds {
		newAST, ok := rewritePredThroughProject(pred, outs)
		if !ok {
			kept = append(kept, pred)
			continue
		}
		if newAST != nil {
			pred.ASTExpr = newAST
			pred.Raw = newAST.String()
			// The simple-form fields, if set, named the alias; the AST is
			// now the authority.
			pred.Column, pred.Op, pred.Value = "", "", nil
		}
		pushed = append(pushed, pred)
	}
	return pushed, kept
}

// rewritePredThroughProject decides one predicate's fate for the swap.
// Returns (nil, true) to push unchanged, (ast, true) to push with the
// rewritten expression, (nil, false) to decline the push.
func rewritePredThroughProject(pred Predicate, outs map[string]projOutput) (plansql.Node, bool) {
	// A simple-form predicate (Column/Op/Value) naming a renamed or
	// computed output has no AST to rewrite consistently: decline.
	if col := bareColumnName(pred.Column); col != "" {
		if o, ok := outs[col]; ok && (o.def != nil || o.unsafe) {
			return nil, false
		}
	}
	if pred.ASTExpr == nil {
		// Nothing to analyze — the pre-#384 behavior (push unchanged) only
		// arises for predicates that never named a Project alias.
		return nil, true
	}
	refs := make(map[string]bool)
	collectASTColumnRefs(pred.ASTExpr, refs)
	needs := false
	for r := range refs {
		if strings.Contains(r, ".") {
			continue // the collector records the bare name alongside
		}
		o, ok := outs[r]
		if !ok {
			continue
		}
		if o.unsafe {
			return nil, false
		}
		if o.def != nil {
			needs = true
		}
	}
	if !needs {
		return nil, true
	}
	newAST, ok := substituteColRefs(pred.ASTExpr, outs)
	if !ok {
		return nil, false
	}
	return newAST, true
}

// substituteColRefs returns expr with every column reference to a renamed or
// computed Project output replaced by its definition (copy-on-write: shared,
// unchanged subtrees are reused, matching ReplaceAllAggregates). ok=false
// means the rewrite could not be done soundly and the caller must decline
// the push.
func substituteColRefs(n plansql.Node, outs map[string]projOutput) (plansql.Node, bool) {
	if n == nil {
		return nil, true
	}
	switch e := n.(type) {
	case *plansql.ColRef:
		o, ok := outs[strings.ToLower(e.Column)]
		if !ok || (o.def == nil && !o.unsafe) {
			return n, true
		}
		if o.unsafe {
			return nil, false
		}
		return o.def, true
	case *plansql.Lit, *plansql.IntervalLit:
		return n, true
	case *plansql.CmpExpr:
		l, lok := substituteColRefs(e.Left, outs)
		r, rok := substituteColRefs(e.Right, outs)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true
	case *plansql.AndNode:
		l, lok := substituteColRefs(e.Left, outs)
		r, rok := substituteColRefs(e.Right, outs)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.AndNode{Left: l, Right: r}, true
	case *plansql.OrNode:
		l, lok := substituteColRefs(e.Left, outs)
		r, rok := substituteColRefs(e.Right, outs)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.OrNode{Left: l, Right: r}, true
	case *plansql.BinaryOp:
		l, lok := substituteColRefs(e.Left, outs)
		r, rok := substituteColRefs(e.Right, outs)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, ok := substituteColRefs(e.Inner, outs)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.NotNode:
		in, ok := substituteColRefs(e.Inner, outs)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.NotNode{Inner: in}, true
	case *plansql.ParenNode:
		in, ok := substituteColRefs(e.Inner, outs)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, ok := substituteColRefs(e.Inner, outs)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(e.Args))
		changed := false
		for i, arg := range e.Args {
			na, ok := substituteColRefs(arg, outs)
			if !ok {
				return nil, false
			}
			newArgs[i] = na
			if na != arg {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.FuncCallNode{Name: e.Name, Args: newArgs, Distinct: e.Distinct, Star: e.Star}, true
	case *plansql.InExpr:
		l, lok := substituteColRefs(e.Left, outs)
		if !lok {
			return nil, false
		}
		newVals := make([]plansql.Node, len(e.Values))
		changed := l != e.Left
		for i, v := range e.Values {
			nv, ok := substituteColRefs(v, outs)
			if !ok {
				return nil, false
			}
			newVals[i] = nv
			if nv != v {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: newVals}, true
	case *plansql.BetweenExpr:
		l, lok := substituteColRefs(e.Left, outs)
		lo, look := substituteColRefs(e.Low, outs)
		hi, hok := substituteColRefs(e.High, outs)
		if !lok || !look || !hok {
			return nil, false
		}
		if l == e.Left && lo == e.Low && hi == e.High {
			return n, true
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true
	case *plansql.LikeExpr:
		l, lok := substituteColRefs(e.Left, outs)
		p, pok := substituteColRefs(e.Pattern, outs)
		if !lok || !pok {
			return nil, false
		}
		if l == e.Left && p == e.Pattern {
			return n, true
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true
	case *plansql.IsExpr:
		l, ok := substituteColRefs(e.Left, outs)
		if !ok {
			return nil, false
		}
		if l == e.Left {
			return n, true
		}
		return &plansql.IsExpr{Left: l, Not: e.Not, Check: e.Check}, true
	case *plansql.CaseNode:
		subj, sok := substituteColRefs(e.Subject, outs)
		els, eok := substituteColRefs(e.Else, outs)
		if !sok || !eok {
			return nil, false
		}
		changed := subj != e.Subject || els != e.Else
		newWhens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			cond, cok := substituteColRefs(w.Cond, outs)
			res, rok := substituteColRefs(w.Result, outs)
			if !cok || !rok {
				return nil, false
			}
			newWhens[i] = plansql.WhenClause{Cond: cond, Result: res}
			if cond != w.Cond || res != w.Result {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.CaseNode{Subject: subj, Whens: newWhens, Else: els}, true
	case *plansql.ArrayLitNode:
		newEls := make([]plansql.Node, len(e.Elements))
		changed := false
		for i, el := range e.Elements {
			ne, ok := substituteColRefs(el, outs)
			if !ok {
				return nil, false
			}
			newEls[i] = ne
			if ne != el {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.ArrayLitNode{Elements: newEls}, true
	case *plansql.TupleNode:
		newEls := make([]plansql.Node, len(e.Elements))
		changed := false
		for i, el := range e.Elements {
			ne, ok := substituteColRefs(el, outs)
			if !ok {
				return nil, false
			}
			newEls[i] = ne
			if ne != el {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.TupleNode{Elements: newEls}, true
	default:
		// Opaque to the rewriter (subqueries, EXISTS, window functions,
		// future node types): sound to leave in place only if it names no
		// renamed, computed, or unsafe output — collectASTColumnRefs sees
		// into subquery outer references, so correlation on an alias is
		// caught here.
		refs := make(map[string]bool)
		collectASTColumnRefs(n, refs)
		for r := range refs {
			if strings.Contains(r, ".") {
				continue
			}
			if o, ok := outs[r]; ok && (o.def != nil || o.unsafe) {
				return nil, false
			}
		}
		return n, true
	}
}
