// This file holds filter plan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"strings"
)

// RecordBatch type alias for convenience
type RecordBatch = batch.RecordBatch

// buildFilterOp compiles one predicate into a filter operator. It returns an
// error only for a predicate naming a function that does not exist: every other
// compile failure falls through to the raw-string and column-compare paths
// below, which is what makes those fallbacks useful. An unknown function has
// nothing to fall through TO — the string parser would not recognize it either,
// so the predicate would quietly become nil and the filter would vanish,
// admitting every row (#341).
func (p *Planner) buildFilterOp(pred logical.Predicate, outerTables map[string]bool, outerCols map[string]string) (exec.UnaryOperator, error) {
	// Try to compile from AST expression first (full expression engine)
	if pred.ASTExpr != nil {
		var compiled expr.Expr
		var err error
		if len(outerTables) > 0 {
			if len(outerCols) > 0 {
				compiled, err = expr.CompileWithScopeResolver(pred.ASTExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, err = expr.CompileWithScope(pred.ASTExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
		} else {
			compiled, err = expr.CompileWithRunner(pred.ASTExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
		}
		if expr.IsCompileRefusal(err) {
			return nil, err
		}
		if err == nil {
			// Try to extract vectorized filter for simple comparison patterns.
			// First try full vectorization, then partial (vectorize what we can
			// from AND chains, keep the rest as row-at-a-time predicates).
			if vf := tryVectorizeFilter(compiled); vf != nil {
				return vf, nil
			}
			// The #147 guard for the ROW evaluator, which KernelFilter has
			// had since and this path did not: a predicate naming a column
			// the input does not carry is UNKNOWN on every row, and a WHERE
			// admits only TRUE, so it answers zero rows in silence (#653).
			// Declined for a CORRELATED predicate, whose outer references
			// resolve outside this batch by design.
			check := rowFilterColumnCheck(pred.ASTExpr, outerTables)
			if vf := tryPartialVectorize(compiled, check); vf != nil {
				return vf, nil
			}
			f := exec.NewFilter(wrapPredicate(compiled))
			f.Check = check
			return f, nil
		}
	}

	// Fall back to raw string parsing
	if pred.Raw != "" {
		p := parseSimplePredicate(pred.Raw)
		if p != nil {
			return p, nil
		}
	}

	if pred.Column != "" && pred.Op != "" {
		op := parseCompareOp(pred.Op)
		return exec.NewFilter(exec.ColumnCompareLit(pred.Column, op, pred.Value, pred.ValueText)), nil
	}

	return nil, nil
}

// tryVectorizeFilter inspects a compiled expression tree and returns a vectorized
// filter operator when the pattern is a simple comparison (col op col, col op const)
// or an AND chain of such comparisons. Returns nil for complex expressions.
func tryVectorizeFilter(e expr.Expr) exec.UnaryOperator {
	ops := extractFilterOps(e, false)
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return exec.NewChainFilter(ops)
}

// rowFilterColumnCheck returns the first-batch column-existence check for a
// row-evaluated predicate, or nil where the guard cannot apply: a correlated
// predicate (its outer references resolve outside the batch by design) or one
// carrying a node FilterColumnRefs declines to enumerate.
func rowFilterColumnCheck(ast plansql.Node, outerTables map[string]bool) func(*batch.RecordBatch) error {
	if ast == nil || len(outerTables) > 0 {
		return nil
	}
	refs, ok := expr.FilterColumnRefs(ast)
	if !ok || len(refs) == 0 {
		return nil
	}
	return func(b *batch.RecordBatch) error { return expr.CheckFilterColumns(b, refs) }
}

// tryPartialVectorize handles AND chains where some operands are vectorizable and
// some are not. Vectorized operands run first (narrowing the selection vector),
// followed by row-at-a-time predicates for the rest. This is better than falling
// back entirely to row-at-a-time when any part of an AND chain isn't vectorizable.
func tryPartialVectorize(e expr.Expr, check func(*batch.RecordBatch) error) exec.UnaryOperator {
	parts := flattenAnds(e)
	if len(parts) < 2 {
		return nil // not an AND chain
	}
	var vectorized []exec.UnaryOperator
	var nonVectorized []expr.Expr
	for _, part := range parts {
		ops := extractFilterOps(part, false)
		if ops != nil {
			vectorized = append(vectorized, ops...)
		} else {
			nonVectorized = append(nonVectorized, part)
		}
	}
	if len(vectorized) == 0 {
		return nil
	}
	// Put vectorized filters first to narrow selection, then slow predicates
	allOps := make([]exec.UnaryOperator, 0, len(vectorized)+len(nonVectorized))
	allOps = append(allOps, vectorized...)
	for _, e := range nonVectorized {
		f := exec.NewFilter(wrapPredicate(e))
		// The whole predicate's references, not this conjunct's: every op in
		// the chain reads ONE batch, so its schema answers for all of them.
		f.Check = check
		check = nil // once is enough
		allOps = append(allOps, f)
	}
	if len(allOps) == 1 {
		return allOps[0]
	}
	return exec.NewChainFilter(allOps)
}

// flattenAnds recursively flattens nested AND expressions into a flat list.
func flattenAnds(e expr.Expr) []expr.Expr {
	if and, ok := e.(*expr.And); ok {
		return append(flattenAnds(and.Left), flattenAnds(and.Right)...)
	}
	return []expr.Expr{e}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND combinations.
// kernelFilterWithRowFallback builds a typed kernel filter; for dotted
// column names ("attrs.score") it attaches the compiled comparison as a
// row-at-a-time fallback so ROW-field access works (the kernel resolves
// qualified table refs by stripping the prefix, but cannot reach into ROW
// children — issue #147).
// colColFilterWithRowFallback builds a col-col kernel filter carrying the
// compiled comparison as a row-at-a-time fallback. The kernel requires both
// columns to share a storage type; when they differ (e.g. FLOAT64 <>
// INT32), the fallback evaluates the comparison with SQL numeric coercion
// instead of the kernel indexing the wrong typed slice (issue #375).
func colColFilterWithRowFallback(left, right string, op exec.CompareOp, cmp expr.Expr) *exec.ColColFilter {
	f := exec.NewColColFilter(left, right, op)
	f.RowFallback = wrapPredicate(cmp)
	return f
}

// fieldPathColRef returns node as a *plansql.ColRef when it is one, seeing
// through parentheses — the shape colDecls.field resolves against. A nil
// answer simply resolves to no field.
func fieldPathColRef(node plansql.Node) *plansql.ColRef {
	for {
		switch n := node.(type) {
		case *plansql.ColRef:
			return n
		case *plansql.ParenNode:
			node = n.Inner
		default:
			return nil
		}
	}
}

// nullCheckWithRowFallback and likeFilterWithRowFallback are
// kernelFilterWithRowFallback for the two vectorized filters that had no
// fallback at all. Both resolved a dotted name by stripping the qualifier and
// then, finding nothing, matched NO ROWS silently — so `WHERE rw.f IS NULL`
// and `WHERE rw.s LIKE 'x%'` over a ROW field answered an empty result
// indistinguishable from real data (#568). The comparison filters have had
// this delegation since #147.
func nullCheckWithRowFallback(name string, checkNull bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewNullCheckFilter(name, checkNull)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func likeFilterWithRowFallback(name, pattern string, negate bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewLikeFilter(name, pattern, negate)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func kernelFilterWithRowFallback(name string, op exec.CompareOp, lit *expr.Lit, cmp expr.Expr) exec.UnaryOperator {
	if lit.Val == nil {
		// A comparison against a NULL literal is UNKNOWN for every row, so no
		// row qualifies. It cannot be lowered to a value comparison at all:
		// the kernel takes its constant as a box and every typed coercion
		// reads nil as that type's ZERO, which answered `WHERE c_i64 = NULL`
		// with the rows where the column is 0 (#450).
		return exec.NewMatchNothingFilter()
	}
	// The literal's own TEXT travels with its box. A DECIMAL column's kernel
	// converts the text at the column's scale, which is the only way a
	// literal past a float64's ~15-16 significant digits reaches the
	// comparison as the number that was written (#452).
	kf := exec.NewKernelFilterLit(name, op, lit.Val, lit.Text)
	if strings.Contains(name, ".") {
		kf.RowFallback = wrapPredicate(cmp)
	}
	return kf
}

// kernelOrNothing is the `col <op> constant` operator for a constant that did
// not arrive inside an expr.Lit — a BETWEEN bound, or a value parsed out of
// raw predicate text. Same NULL rule as kernelFilterWithRowFallback.
func kernelOrNothing(col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	return kernelOrNothingRef(nil, col, op, val, text)
}

// kernelOrNothingRef is kernelOrNothing carrying the reference the constant is
// compared against, so a dotted name can be given the row-at-a-time fallback
// kernelFilterWithRowFallback gives the `col <op> lit` shape. Without it a
// BETWEEN over a ROW field path failed with `filter column "rw.f" does not
// exist in the input schema` — loud, but a query PostgreSQL answers (#568).
//
// Each HALF of a BETWEEN gets its own fallback comparison rather than the
// whole predicate: the two halves are separate operators, and handing both
// the same conjunction would evaluate it twice.
func kernelOrNothingRef(ref *expr.ColRef, col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	if val == nil {
		return exec.NewMatchNothingFilter()
	}
	kf := exec.NewKernelFilterLit(col, op, val, text)
	if ref != nil && strings.Contains(col, ".") {
		kf.RowFallback = wrapPredicate(expr.NewCmp(ref, &expr.Lit{Val: val, Text: text}, execToCmpOp(op)))
	}
	return kf
}

// execToCmpOp is cmpToExecOp's inverse, for the sites that lower a comparison
// to a kernel and then have to rebuild the equivalent expression as a
// fallback.
func execToCmpOp(op exec.CompareOp) expr.CmpOp {
	switch op {
	case exec.OpEq:
		return expr.CmpEq
	case exec.OpNe:
		return expr.CmpNe
	case exec.OpLt:
		return expr.CmpLt
	case exec.OpLe:
		return expr.CmpLe
	case exec.OpGt:
		return expr.CmpGt
	default:
		return expr.CmpGe
	}
}

// inFilterForList builds the IN / NOT IN operator for a list of literals,
// applying SQL's NULL rule to the LIST — which is not the same rule as for a
// scalar comparison, and is the one that surprises people:
//
//	`x IN (a, NULL)` is TRUE where x = a and UNKNOWN everywhere else, because
//	TRUE dominates the disjunction. A NULL member therefore drops out; with
//	nothing else left the whole test is UNKNOWN and nothing qualifies.
//
//	`x NOT IN (a, NULL)` is `x <> a AND x <> NULL`, and the second conjunct is
//	UNKNOWN for every row: the result is FALSE or UNKNOWN, never TRUE. A NULL
//	anywhere in a NOT IN list empties the answer (#450).
//
// An empty list with no NULL in it is left alone — that is a different shape
// and the set kernel already answers it.
//
// RESIDUAL (real NOT IN + NULL + over-range literal only): `real NOT IN (1e40,
// NULL)` short-circuits to MatchNothing below on the NULL rule (#450) before any
// literal is examined, so PostgreSQL's 22003 for the over-range 1e40 in the
// real[] cast is not raised — wadjet answers empty. The positive `IN (1e40,
// NULL)` is NOT affected: it keeps the over-range literal, carries the syntactic
// arity of 2 (SetSyntacticLen below), narrows to real[], and raises 22003 like
// PostgreSQL. Surfacing the error on the NOT-IN path would mean checking the
// over-range literal before the MatchNothing short-circuit; left as a documented
// residual (obscure — a NULL in a NOT IN already empties the answer).
func inFilterForList(col string, values []any, texts []string, negate bool) exec.UnaryOperator {
	kept := make([]any, 0, len(values))
	keptTexts := make([]string, 0, len(texts))
	hadNull := false
	for i, v := range values {
		if v == nil {
			hadNull = true
			continue
		}
		kept = append(kept, v)
		if i < len(texts) {
			keptTexts = append(keptTexts, texts[i])
		}
	}
	if hadNull && (negate || len(kept) == 0) {
		return exec.NewMatchNothingFilter()
	}
	inf := exec.NewInFilterLit(col, kept, keptTexts, negate)
	// The comparison WIDTH of a FLOAT32 IN list is decided by the SYNTACTIC
	// element count, not the count that survives the NULL strip above: PostgreSQL
	// casts the whole `{...}` array literal — NULLs included — to real[] whenever
	// there is more than one element, so `real IN (0.1, NULL)` narrows to real
	// and matches, while `real IN (0.1)` widens to double and does not (#549).
	inf.SetSyntacticLen(len(values))
	return inf
}

// negateCmpOp inverts a comparison for an enclosing NOT. Under SQL's
// three-valued logic NOT (a = b) is TRUE exactly where a <> b is TRUE — both
// are UNKNOWN when either side is NULL, and every filter kernel already skips
// NULL rows — so the inverted operator is the whole of the negation for a
// WHERE, which admits only TRUE. The second result is false for an operator
// with no inverse in this set: the signal to leave the predicate to the row
// evaluator rather than lower it wrongly.
func negateCmpOp(op exec.CompareOp) (exec.CompareOp, bool) {
	switch op {
	case exec.OpEq:
		return exec.OpNe, true
	case exec.OpNe:
		return exec.OpEq, true
	case exec.OpLt:
		return exec.OpGe, true
	case exec.OpLe:
		return exec.OpGt, true
	case exec.OpGt:
		return exec.OpLe, true
	case exec.OpGe:
		return exec.OpLt, true
	default:
		return op, false
	}
}

// maybeNegate applies an enclosing NOT to an already-mapped comparison.
func maybeNegate(op exec.CompareOp, neg bool) (exec.CompareOp, bool) {
	if !neg {
		return op, true
	}
	return negateCmpOp(op)
}

// negatedExpr is the expression a lowered operator's row-at-a-time fallback
// must evaluate. That fallback is the ORIGINAL comparison, so under a NOT it
// has to be wrapped: handing it the un-negated node is the same dropped
// negation one layer down (#461).
func negatedExpr(e expr.Expr, neg bool) expr.Expr {
	if !neg {
		return e
	}
	return &expr.Not{Operand: e}
}

// orOfOps unions two extracted operand lists into one OR filter. Nil on
// either side means that side is not vectorizable, and an OR is only as
// vectorizable as both of its arms.
func orOfOps(leftOps, rightOps []exec.UnaryOperator) []exec.UnaryOperator {
	if leftOps == nil || rightOps == nil {
		return nil
	}
	one := func(ops []exec.UnaryOperator) exec.UnaryOperator {
		if len(ops) == 1 {
			return ops[0]
		}
		return exec.NewChainFilter(ops)
	}
	return []exec.UnaryOperator{exec.NewOrFilter(one(leftOps), one(rightOps))}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND
// combinations.
//
// neg carries an enclosing NOT: what comes back is the negation of e. That is
// what the *expr.Not case used to drop — it returned the operand's own
// operators, so `WHERE NOT (k = 131)` was executed as `WHERE k = 131`, the
// complement of the answer, silently and on both engines (#461). Anything
// that cannot be negated returns nil instead, and the caller's residual
// row-at-a-time filter evaluates the NOT itself, where three-valued logic
// survives (expr.Not.EvalBoolNull).
func extractFilterOps(e expr.Expr, neg bool) []exec.UnaryOperator {
	switch v := e.(type) {
	case *expr.Cmp:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		// col op col
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					// Possible ROW-field access — the col-col kernel can't
					// evaluate it; leave this comparison row-at-a-time.
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
		}
		// col op const
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
		// const op col → flip
		if lit, lok := v.Left.(*expr.Lit); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(rc.Name, flipOp(op), lit, fb)}
			}
		}
	case *expr.CmpNetworkLit:
		// Bare column vs. a string literal compileCmp pre-parsed as an IPv4
		// or MAC address (tryNetworkLit/CmpNetworkLit in expr/compile.go).
		// This case was missing entirely, so every `ipv4_col <op> 'lit'` /
		// `mac_col <op> 'lit'` predicate fell through to nil here and ran
		// row-at-a-time, losing the vectorized kernel a plain *expr.Cmp node
		// got on this exact shape before compileCmp started emitting
		// CmpNetworkLit (measured +43% on 400k rows).
		//
		// v.Col's type isn't known here — extractFilterOps has no schema,
		// same as the *expr.Cmp arm above — so this builds the identical
		// "col op const" kernel filter that arm would have built for the
		// original `col op 'lit'`/`'lit' op col`, from v.Lit (the literal's
		// original text) rather than the pre-parsed ipv4/mac int64s on the
		// node: ResolveFilterKernel (exec/kernel/compare.go) dispatches
		// purely on the column's REAL runtime type, parsing v.Lit itself via
		// parseIPv4ToInt64/parseMACToInt64 for an actual network column and
		// falling to compareFilterString for anything else. That is also
		// why tryNetworkLit does not need to be, and cannot be, restricted
		// to network-typed columns at compile time: a STRING column whose
		// literal happens to parse as an address (`s = '10.1.2.3'`) rides
		// this same case and gets exactly its normal compareFilterString
		// kernel — using the pre-parsed int64s directly here, bypassing
		// that dispatch, would misinterpret a STRING vector as encoded
		// IPv4/MAC int64 data.
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		kOp := op
		if v.Flip {
			kOp = flipOp(op)
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, kOp, &expr.Lit{Val: v.Lit}, fb)}
	case *expr.CmpInt64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.CmpFloat64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.And:
		if neg {
			// De Morgan: NOT (a AND b) is NOT a OR NOT b, which holds in
			// Kleene logic as well as Boolean.
			return orOfOps(extractFilterOps(v.Left, true), extractFilterOps(v.Right, true))
		}
		leftOps := extractFilterOps(v.Left, false)
		if leftOps == nil {
			return nil
		}
		rightOps := extractFilterOps(v.Right, false)
		if rightOps == nil {
			return nil
		}
		return append(leftOps, rightOps...)
	case *expr.Or:
		if neg {
			// De Morgan the other way: NOT (a OR b) is NOT a AND NOT b, and
			// an AND is the chained intersection of the two selections.
			leftOps := extractFilterOps(v.Left, true)
			if leftOps == nil {
				return nil
			}
			rightOps := extractFilterOps(v.Right, true)
			if rightOps == nil {
				return nil
			}
			return append(leftOps, rightOps...)
		}
		return orOfOps(extractFilterOps(v.Left, false), extractFilterOps(v.Right, false))
	case *expr.Between:
		// col BETWEEN low AND high → two kernel filters: col >= low AND col <= high
		// col NOT BETWEEN low AND high → col < low OR col > high
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if lo, lok := v.Low.(*expr.Lit); lok {
				if hi, hok := v.Hi.(*expr.Lit); hok {
					// SQL defines `x NOT BETWEEN a AND b` as `NOT (x BETWEEN
					// a AND b)`, so an enclosing NOT is the same flag.
					// A NULL bound makes its own half UNKNOWN and leaves the
					// other half standing. BETWEEN then admits nothing, and
					// NOT BETWEEN reduces to the surviving comparison —
					// `x NOT BETWEEN NULL AND h` is TRUE exactly where
					// x > h, because a FALSE conjunct makes the conjunction
					// FALSE whatever the UNKNOWN one says (#450).
					if v.Not != neg {
						return []exec.UnaryOperator{exec.NewOrFilter(
							kernelOrNothingRef(col, col.Name, exec.OpLt, lo.Val, lo.Text),
							kernelOrNothingRef(col, col.Name, exec.OpGt, hi.Val, hi.Text),
						)}
					}
					return []exec.UnaryOperator{
						kernelOrNothingRef(col, col.Name, exec.OpGe, lo.Val, lo.Text),
						kernelOrNothingRef(col, col.Name, exec.OpLe, hi.Val, hi.Text),
					}
				}
			}
		}
	case *expr.In:
		// col IN (lit, lit, ...) or col NOT IN (lit, lit, ...)
		if col, ok := v.Expr.(*expr.ColRef); ok {
			values := make([]any, 0, len(v.Values))
			texts := make([]string, 0, len(v.Values))
			for _, val := range v.Values {
				if lit, ok := val.(*expr.Lit); ok {
					values = append(values, lit.Val)
					texts = append(texts, lit.Text)
				} else {
					return nil // non-literal in IN list
				}
			}
			f := inFilterForList(col.Name, values, texts, v.Not != neg)
			if inf, ok := f.(*exec.InFilter); ok && strings.Contains(col.Name, ".") {
				inf.RowFallback = wrapPredicate(negatedExpr(v, neg))
			}
			return []exec.UnaryOperator{f}
		}
	case *expr.Like:
		// col LIKE 'pattern' or col NOT LIKE 'pattern'
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if pat, ok := v.Pattern.(*expr.Lit); ok {
				if pat.Val == nil {
					// `col LIKE NULL` is UNKNOWN for every row, negated or
					// not — there is no pattern to match against (#450).
					return []exec.UnaryOperator{exec.NewMatchNothingFilter()}
				}
				if s, ok := pat.Val.(string); ok {
					return []exec.UnaryOperator{likeFilterWithRowFallback(col.Name, s, v.Not != neg, negatedExpr(v, neg))}
				}
			}
		}
	case *expr.IsNull:
		// col IS NULL / col IS NOT NULL — vectorized null bitmap scan
		if col, ok := v.Operand.(*expr.ColRef); ok {
			return []exec.UnaryOperator{nullCheckWithRowFallback(col.Name, v.Not == neg, negatedExpr(v, neg))}
		}
	case *expr.ColIsNull:
		// Offsets-shape rewrite of `col IS [NOT] NULL` (expr/shape_funcs.go).
		// Same kernel the *expr.IsNull case builds — without this the filter
		// would silently drop to row-at-a-time evaluation.
		return []exec.UnaryOperator{nullCheckWithRowFallback(v.Col.Name, v.Not == neg, negatedExpr(v, neg))}
	case *expr.ColEmptyStr:
		// Offsets-shape rewrite of a column compared against the empty
		// string literal (expr/shape_funcs.go). Reproduces exactly what the
		// *expr.Cmp "col op const" branch built for the pre-rewrite node.
		op := exec.OpEq
		if v.Not != neg {
			op = exec.OpNe
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, op, &expr.Lit{Val: ""}, negatedExpr(v, neg))}
	case *expr.Not:
		// NOT (expr) — vectorize the NEGATION of the inner expression. This
		// case used to return the inner expression's own operators, which
		// applied the predicate positively and answered the complement (#461).
		return extractFilterOps(v.Operand, !neg)
	}
	return nil
}

func cmpToExecOp(op expr.CmpOp) exec.CompareOp {
	switch op {
	case expr.CmpEq:
		return exec.OpEq
	case expr.CmpNe:
		return exec.OpNe
	case expr.CmpLt:
		return exec.OpLt
	case expr.CmpLe:
		return exec.OpLe
	case expr.CmpGt:
		return exec.OpGt
	case expr.CmpGe:
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func flipOp(op exec.CompareOp) exec.CompareOp {
	switch op {
	case exec.OpLt:
		return exec.OpGt
	case exec.OpLe:
		return exec.OpGe
	case exec.OpGt:
		return exec.OpLt
	case exec.OpGe:
		return exec.OpLe
	default:
		return op
	}
}

// collectTableAliases recursively collects all table names and aliases from
// scan nodes in a logical plan subtree. Used to provide outer scope context
// for correlated subquery detection.
func collectTableAliases(node *logical.Node) map[string]bool {
	aliases := make(map[string]bool)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			// Derived-table aliases count: this is the outer scope a
			// correlated subquery's references are resolved against (#489).
			for _, name := range n.ScopeNames() {
				aliases[strings.ToLower(name)] = true
			}
		}
		// So does a CTE reference. It records its scope on the SUBTREE ROOT
		// rather than on the scans below (subtreeNamesRelation says why), so
		// a walk that reads only NodeScan never sees it and `WHERE EXISTS
		// (… WHERE t.k = u.did)` over a CTE `u` was not recognized as
		// correlated at all (#535).
		if n.CTEName != "" {
			aliases[strings.ToLower(n.CTEName)] = true
		}
		if n.CTERefAlias != "" {
			aliases[strings.ToLower(n.CTERefAlias)] = true
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return aliases
}
