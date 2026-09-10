// This file holds expr special; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Special SQL expressions ---

// In checks if a value is in a set.
type In struct {
	Expr   Expr
	Values []Expr
	Not    bool
	// dec binds a DECIMAL column against an all-numeric-literal list; see
	// NewCmp and decimal_literal.go.
	dec *decimalLitCmp
	// f32 binds a FLOAT32 column against a multi-element all-numeric-literal
	// list, which PostgreSQL compares at REAL width and this path compared at
	// double (#633); see real_in_width.go.
	f32 *realLitSet
	// pairs is one declaration-driven binding per LIST MEMBER, for the lists
	// dec declines: a mixed list, a member that is not a literal, or a column
	// that is not a DECIMAL. `x IN (v)` is `x = v` chained with OR, so it has
	// to take the same comparison rule — `s = 2.00` answered one row while
	// `s IN (2.00)` answered none, one predicate with two readings (#504
	// review, non-blocker a).
	pairs []*boxedPair
	// disarm caches "none of pairs carries a declaration-driven rule",
	// collapsing the per-row cost to one atomic load once every member has
	// settled — see pairSetState. Mixed lists (some member armed) keep
	// dispatching through pairs[i].compare exactly as before.
	disarm pairSetState
}

// NewIn builds a set-membership test, binding the DECIMAL-column-against-
// numeric-literals shape, the FLOAT32-column-against-a-multi-element-list
// shape, and one boxed pair per member.
func NewIn(e Expr, values []Expr, not bool) *In {
	pairs := make([]*boxedPair, len(values))
	for i, v := range values {
		pairs[i] = newBoxedPair(e, v)
	}
	return &In{Expr: e, Values: values, Not: not,
		dec: bindDecimalList(e, values), f32: bindRealLitList(e, values), pairs: pairs}
}

func (e *In) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *In) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: `x IN (a, b, NULL)` is the chained OR of comparisons, so a
// match answers TRUE, and a miss with a NULL anywhere in the list is
// UNKNOWN — never FALSE. NOT IN is its Kleene negation, which is why
// `1 NOT IN (2, NULL)` must not answer true: PostgreSQL's reading is "I
// don't know, so no" (#370).
func (e *In) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true
			}
			// Every member is a numeric literal here, so the list holds no
			// NULL and the three-valued reading below cannot arise.
			for i := range e.Values {
				if e.dec.order(vec, row, i) == 0 {
					return !e.Not, false
				}
			}
			return e.Not, false
		}
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}
	// A REAL-typed operand against a multi-element literal list compares at
	// REAL width, not at the double width this boxed path would otherwise use
	// (#633). A FLOAT32 column boxes as float64(float32), so narrowing it back
	// recovers the stored value exactly; a CAST to REAL boxes as a float32
	// already. The list's own NULL rule is unchanged — a miss with a NULL
	// member is UNKNOWN, never FALSE.
	if e.f32.applies(b) {
		if f, ok := realBox(lv); ok {
			if e.f32.contains(f) {
				return !e.Not, false
			}
			if e.f32.sawNull {
				return false, true
			}
			return e.Not, false
		}
	}
	sawNull := false
	// Once every pair on this node is confirmed disarmed, skip the per-pair
	// dispatch entirely — one atomic load instead of one boxedPair.compare
	// call per member per row (see pairSetState).
	fast := e.disarm.disarmed(e.pairs)
	for i, v := range e.Values {
		rv := v.Eval(b, row)
		if rv == nil {
			sawNull = true
			continue
		}
		var eq bool
		switch {
		case fast:
			eq = compare(lv, rv, CmpEq)
		case i < len(e.pairs):
			eq = e.pairs[i].compare(b, lv, rv, CmpEq)
		default:
			eq = compare(lv, rv, CmpEq)
		}
		if eq {
			return !e.Not, false
		}
	}
	if sawNull {
		return false, true
	}
	return e.Not, false
}

// Between checks if a value is between two bounds.
type Between struct {
	Expr    Expr
	Low, Hi Expr
	Not     bool
	// dec binds a DECIMAL column against two numeric-literal bounds; see
	// NewCmp and decimal_literal.go.
	dec *decimalLitCmp
	// loPair/hiPair are the declaration-driven bindings for the two bounds,
	// for the shapes dec declines. `x BETWEEN a AND b` is `x >= a AND x <= b`
	// and must read its bounds the way those two comparisons do.
	loPair, hiPair *boxedPair
	// pairs is {loPair, hiPair}, built once so the disarm check below never
	// allocates a slice on the hot path.
	pairs []*boxedPair
	// disarm caches "neither bound carries a declaration-driven rule" — see
	// pairSetState and In.disarm.
	disarm pairSetState
}

// NewBetween builds a range test, binding the DECIMAL-column-against-
// numeric-literals shape and one boxed pair per bound.
func NewBetween(e, low, hi Expr, not bool) *Between {
	loPair := newBoxedPair(e, low)
	hiPair := newBoxedPair(e, hi)
	return &Between{Expr: e, Low: low, Hi: hi, Not: not,
		dec:    bindDecimalList(e, []Expr{low, hi}),
		loPair: loPair, hiPair: hiPair,
		pairs: []*boxedPair{loPair, hiPair}}
}

func (e *Between) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Between) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: BETWEEN is defined as (x >= lo AND x <= hi), so a NULL
// bound does not force UNKNOWN — the other half can still answer FALSE
// (`5 BETWEEN NULL AND 2` is false, and NOT BETWEEN flips it to true).
func (e *Between) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true
			}
			// Both bounds are numeric literals here, so neither half can be
			// UNKNOWN and BETWEEN is the plain conjunction.
			if e.dec.order(vec, row, 0) < 0 || e.dec.order(vec, row, 1) > 0 {
				return e.Not, false
			}
			return !e.Not, false
		}
	}
	v := e.Expr.Eval(b, row)
	if v == nil {
		return false, true
	}
	lo := e.Low.Eval(b, row)
	hi := e.Hi.Eval(b, row)
	// Three-valued AND over the two half-comparisons.
	geNull := lo == nil
	leNull := hi == nil
	// Once both bounds are confirmed disarmed, skip the per-bound dispatch —
	// one atomic load instead of two boxedPair.compare calls per row (see
	// pairSetState).
	fast := e.disarm.disarmed(e.pairs)
	if !geNull {
		var ge bool
		if fast {
			ge = compare(v, lo, CmpGe)
		} else {
			ge = e.loPair.compare(b, v, lo, CmpGe)
		}
		if !ge {
			return e.Not, false
		}
	}
	if !leNull {
		var le bool
		if fast {
			le = compare(v, hi, CmpLe)
		} else {
			le = e.hiPair.compare(b, v, hi, CmpLe)
		}
		if !le {
			return e.Not, false
		}
	}
	if geNull || leNull {
		return false, true
	}
	return !e.Not, false
}

// Like performs SQL LIKE pattern matching.
type Like struct {
	Expr    Expr
	Pattern Expr
	Not     bool
}

func (e *Like) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Like) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: LIKE with NULL on either side is UNKNOWN, and NOT LIKE
// stays UNKNOWN with it (#370). A container-shaped operand is a query ERROR
// (#522) rather than a value, matched or not — see containerLikeKind.
func (e *Like) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	v := e.Expr.Eval(b, row)
	p := e.Pattern.Eval(b, row)
	if v == nil || p == nil {
		return false, true
	}
	if kind, ok := containerLikeKind(e.Expr, v); ok {
		raiseNoLikeOperator(kind)
	}
	result := matchLike(toString(boxedTextOperand(b, row, e.Expr, v)), toString(p))
	if e.Not {
		return !result, false
	}
	return result, false
}

// containerLikeKind reports whether v is a container-shaped LIKE operand and
// names its PostgreSQL-ish kind, for raiseNoLikeOperator (#522: PostgreSQL
// has no `~~` operator for any composite or array type, and wadjet has never
// committed to a text form for one either — kernel.ResolveLikeFilterKernel
// makes the matching refusal on the WHERE-clause kernel path).
//
// A bare column reference resolves exactly, from its declared type. Every
// other operand shape (a nested expression, a literal) falls back to the
// boxed value's Go shape: []any is ARRAY (or MAP, which boxes as a list of
// entry ROWs the same way — GetValue's TypeArray/TypeMap case — so the
// fallback cannot tell them apart and names the more common of the two),
// map[string]any is ROW, and []float32 is VECTOR.
func containerLikeKind(operand Expr, v any) (string, bool) {
	if cr, ok := operand.(*ColRef); ok && cr.structField == "" {
		switch cr.typ {
		case batch.TypeArray:
			return "array", true
		case batch.TypeMap:
			return "map", true
		case batch.TypeRow:
			return "record", true
		case batch.TypeVector:
			return "vector", true
		}
	}
	switch v.(type) {
	case []any:
		return "array", true
	case map[string]any:
		return "record", true
	case []float32:
		return "vector", true
	}
	return "", false
}

// Case is a CASE WHEN ... THEN ... ELSE ... END expression.
type Case struct {
	Operand Expr       // optional: CASE <operand> WHEN ...
	Whens   []CaseWhen // WHEN condition THEN result
	Else    Expr       // optional ELSE clause

	// arms holds one refusal and one declaration-driven comparison per WHEN
	// — see caseArms for why the arming is per-WHEN and not per-CASE. Armed
	// lazily, like IsDistinctFrom's, and for the same reason.
	arms atomic.Pointer[caseArms]

	// dch is the DECIMAL box mode: whether the result branches fold to a
	// DECIMAL, so a branch that answers an INTEGER hands over the value's
	// TEXT rather than a carrier (choice_decimal.go, #695).
	dch decimalChoice
}

// boxMode reports what this CASE's chosen box must be rewritten to so it
// survives the vector the plan declared. The arms slice is built only on the
// resolution path — Go evaluates a call's arguments when the call is reached,
// and the fast path returns first — so the row loop allocates nothing.
func (e *Case) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, caseResultArms(e))
}

func (e *Case) armed() *caseArms {
	if r := e.arms.Load(); r != nil {
		return r
	}
	r := &caseArms{
		refuse: make([]*refuseArm, len(e.Whens)),
		pairs:  make([]*boxedPair, len(e.Whens)),
	}
	for i, w := range e.Whens {
		r.refuse[i] = armRefusal(e.Operand, w.Cond)
		r.pairs[i] = newBoxedPair(e.Operand, w.Cond)
	}
	e.arms.Store(r)
	return r
}

// CaseWhen is a single WHEN clause in a CASE expression.
type CaseWhen struct {
	Cond   Expr // the condition (or value to compare against operand)
	Result Expr
}

func (e *Case) Eval(b *batch.RecordBatch, row int) any {
	v := e.eval(b, row)
	if v == nil {
		return v
	}
	return choiceBox(e.boxMode(b), v)
}

func (e *Case) eval(b *batch.RecordBatch, row int) any {
	if e.Operand != nil {
		// Simple CASE: CASE x WHEN v1 THEN r1 ...
		opVal := e.Operand.Eval(b, row)
		arms := e.armed()
		for i, w := range e.Whens {
			whenVal := w.Cond.Eval(b, row)
			// The WHEN value's exact source text settles a match a float64
			// box cannot: `CASE d WHEN <a literal naming d exactly>` answered
			// the ELSE branch (#465). A WHEN value that is not a number is a
			// query ERROR against a DECIMAL operand, never a silent ELSE
			// (#463/#505): `CASE d WHEN 'abc'` answered 0 instead of refusing.
			//
			// And when BOTH sides are DECIMAL columns, neither box says so —
			// two rendered DECIMALs are two strings as far as compare() can
			// tell, so `CASE d_2 WHEN d_4` took the ELSE branch for the row
			// where the two hold the same number at different scales (#506).
			// The pair is bound from the operands' declarations instead.
			if opVal != nil && whenVal != nil {
				arms.refuse[i].check(b)
				if arms.pairs[i].compare(b, opVal, whenVal, CmpEq) {
					return w.Result.Eval(b, row)
				}
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond1 THEN r1 ...
		for _, w := range e.Whens {
			if toBool(w.Cond, b, row) {
				return w.Result.Eval(b, row)
			}
		}
	}
	if e.Else != nil {
		return e.Else.Eval(b, row)
	}
	return nil
}

// Coalesce returns the first non-null argument.
type Coalesce struct {
	Args []Expr

	// dch is the DECIMAL box mode — see Case.dch (#695).
	dch decimalChoice
}

func (e *Coalesce) Eval(b *batch.RecordBatch, row int) any {
	for _, arg := range e.Args {
		v := arg.Eval(b, row)
		if v == nil {
			continue
		}
		return choiceBox(e.boxMode(b), v)
	}
	return nil
}

func (e *Coalesce) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, e.Args)
}
