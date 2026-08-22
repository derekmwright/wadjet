package expr

import "github.com/derekmwright/wadjet/internal/engine/batch"

// FilterPredicate compiles a boolean expression into the per-row predicate a
// filter loop calls, resolving the evaluation protocol ONCE here rather than
// on every row.
//
// A WHERE admits only TRUE, so its answer is always the two-valued collapse
// `val && !null`. For the comparisons and set predicates that collapse IS
// their EvalBool — a wrapper whose whole body is a call to EvalBoolNull and
// a drop of the second result, which #370 left behind when it made the
// three-valued form the definition. Reached through an interface the wrapper
// is a call frame the compiler cannot remove, so the row loop pays it on
// every row; taking EvalBoolNull directly here answers identically with the
// frame gone.
//
// The connectives stay on EvalBool deliberately. And/Or collapse each
// operand BEFORE the operator, so they stop at an UNKNOWN left operand where
// the three-valued form must evaluate the right one to tell FALSE from
// UNKNOWN. Same answer either way (Kleene min/max agrees with the collapse),
// but different work — and a right operand that raises, `1/0`, would start
// raising. IS NULL / IS TRUE are excluded for the mirror reason: there
// EvalBool is the definition and EvalBoolNull is the wrapper.
func FilterPredicate(e Expr) func(b *batch.RecordBatch, row int) bool {
	if ne, ok := e.(BoolNullExpr); ok && evalBoolIsCollapse(e) {
		return func(b *batch.RecordBatch, row int) bool {
			val, null := ne.EvalBoolNull(b, row)
			return val && !null
		}
	}
	if be, ok := e.(BoolExpr); ok {
		return func(b *batch.RecordBatch, row int) bool {
			return be.EvalBool(b, row)
		}
	}
	return func(b *batch.RecordBatch, row int) bool {
		v := e.Eval(b, row)
		if v == nil {
			return false
		}
		if bv, ok := v.(bool); ok {
			return bv
		}
		return false
	}
}

// evalBoolIsCollapse lists the operators whose EvalBool is literally
// `val && !null` of EvalBoolNull, so the two can be exchanged at the filter
// entry. TestEvalBoolIsCollapse holds the list to that definition — an
// operator that gains its own EvalBool must leave this list.
func evalBoolIsCollapse(e Expr) bool {
	switch e.(type) {
	case *CmpTemporalLit, *In, *Between, *Like, *Not, *IsDistinctFrom, *ColEmptyStr:
		return true
	}
	return false
}
