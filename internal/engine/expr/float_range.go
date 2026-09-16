// SPDX-License-Identifier: MIT

package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// PostgreSQL's float8 RANGE rule, applied to the arithmetic kernels (#1082).
//
// `1e308 * 10` answered +Inf here and PostgreSQL 17.11 raises
// `22003 value out of range: overflow`; a CTAS then stored the infinity, which
// is the class ADR-0024 item 4 closes for DECIMAL — a value with no carrier is
// a loud error, never a plausible stand-in. The engine already raised it from
// EXP and POWER (math_domain_refusal.go), so it was not even uniform with
// itself.
//
// The rule is float.c's, operator by operator, and the operand exemptions are
// the point: an INFINITY that arrives as an operand is a VALUE, and every
// result it produces is one too, so `'Infinity'::float8 * 10` is Infinity on
// both engines. Only a non-finite result from FINITE operands is a range
// error, and only a zero from non-zero ones is an underflow.
//
//	+  -    float8pl / float8mi: overflow when the result is infinite and
//	        neither operand was. No underflow rule — a sum that rounds to zero
//	        is the answer.
//	*       float8mul: the same overflow rule, plus underflow when the product
//	        is zero and neither operand was.
//	/       float8div: both rules again, with BOTH operands exempted — so
//	        `1 / Infinity` is zero while `1e308 / 1e-308`, where neither
//	        operand is infinite, is an overflow. A zero DIVISOR is 22012 and
//	        the callers raise it before they reach here.
//
// Unary minus has no rule at all — negation cannot leave the range — which is
// why `-1e308` answers and `-1e308 * 10` does not (both measured).
//
// The non-finite test is the EXPONENT BITS, not math.IsInf and not `r-r != 0`.
// Both of those cost the float unit — two compares against ±MaxFloat64, or a
// subtract whose latency the next iteration waits on — and this test runs once
// per row on the hottest arithmetic path in the engine. `bits & 0x7FF…` is a
// bitcast the compiler emits no instruction for, an AND and a compare, on the
// integer unit, with no dependency on the float pipeline. It is true for both
// infinities and for NaN; a NaN result from finite operands is impossible for
// these four operators, so routing it to the overflow refusal reaches no value
// a query can produce.

// floatExpMask is a float64's exponent field: all ones for an infinity and a
// NaN, and never all ones for a finite value.
const floatExpMask = 0x7FF0000000000000

// nonFiniteFloat reports whether f is an infinity or a NaN, by its exponent
// bits. See the file comment for why this and not math.IsInf.
func nonFiniteFloat(f float64) bool {
	return math.Float64bits(f)&floatExpMask == floatExpMask
}

// floatNeedsRangeCheck is nonFiniteFloat OR zero, in one unsigned compare: a
// magnitude of 0 wraps to the maximum and every non-finite magnitude is at
// least the exponent mask, so the two conditions the multiply and the divide
// care about collapse into one subtract and one comparison.
func floatNeedsRangeCheck(f float64) bool {
	m := math.Float64bits(f) &^ uint64(1<<63)
	return m-1 >= floatExpMask-1
}

func pgFloatAdd(a, b float64) float64 {
	r := a + b
	if nonFiniteFloat(r) {
		refuseFloatSum(a, b)
	}
	return r
}

func pgFloatSub(a, b float64) float64 {
	r := a - b
	if nonFiniteFloat(r) {
		refuseFloatSum(a, b)
	}
	return r
}

func pgFloatMul(a, b float64) float64 {
	r := a * b
	if floatNeedsRangeCheck(r) {
		refuseFloatProduct(a, b, r)
	}
	return r
}

// pgFloatDiv is the quotient for a divisor the caller has already established
// is non-zero: a zero divisor is 22012 and is raised there, where the NULL
// rows have already been separated from the genuine zeros.
func pgFloatDiv(a, b float64) float64 {
	r := a / b
	if floatNeedsRangeCheck(r) {
		refuseFloatProduct(a, b, r)
	}
	return r
}

// refuseFloatSum and refuseFloatProduct are the COLD arms, out of line so the
// four guards above stay inside the inliner's budget: with the operand test
// and the panic inlined into them, pgFloatAdd cost 135 against a budget of 80,
// and a guard that becomes a call costs more than the arithmetic it protects
// (the same shape int_overflow.go's mulInt64Wide takes).
//
//go:noinline
func refuseFloatSum(a, b float64) {
	if !nonFiniteFloat(a) && !nonFiniteFloat(b) {
		raiseFloatOverflow()
	}
}

//go:noinline
func refuseFloatProduct(a, b, r float64) {
	if r == 0 {
		// A zero product or quotient is the ANSWER when an operand was zero,
		// and an underflow otherwise. The divisor's own exemption is the
		// reason `1 / Infinity` is zero rather than a refusal.
		if a != 0 && b != 0 && !nonFiniteFloat(b) {
			raiseFloatUnderflow()
		}
		return
	}
	if !nonFiniteFloat(a) && !nonFiniteFloat(b) {
		raiseFloatOverflow()
	}
}

// checkFusedFloatRange applies the rule to a buffer the FUSED column-constant
// loops filled, by re-deriving only the rows that could be refused.
//
// The loops above it are monomorphic per column type and per operator, and
// putting the operand test inside all twenty of them would cost more in
// duplication than it saves. Instead each result is asked ONE question — is
// it non-finite, or (for the two operators with an underflow rule) zero — and
// only a row that answers yes is recomputed from its source value, which the
// caller still has. On an ordinary batch that is one integer test per row and
// nothing else; on a column of zeros under `*` it is a switch and a multiply
// for those rows, which is still far short of re-evaluating the expression.
func checkFusedFloatRange(v *batch.Vector, typ batch.TypeID, c float64, op arithOp, constFirst bool, dst []float64, n int) {
	if n > len(dst) {
		n = len(dst)
	}
	// Two loops rather than one with a loop-invariant bool in its test: only
	// `*` and `/` have an underflow rule, and the ordinary row should pay ONE
	// integer test.
	switch op {
	case arithAdd, arithSub:
		for i := 0; i < n; i++ {
			if nonFiniteFloat(dst[i]) {
				recheckFusedRow(v, typ, c, op, constFirst, i)
			}
		}
	case arithMul, arithDiv:
		for i := 0; i < n; i++ {
			if floatNeedsRangeCheck(dst[i]) {
				recheckFusedRow(v, typ, c, op, constFirst, i)
			}
		}
	}
}

// recheckFusedRow re-derives one suspect row and raises if the rule refuses
// it. Out of line so the scanning loops above stay tight.
//
//go:noinline
func recheckFusedRow(v *batch.Vector, typ batch.TypeID, c float64, op arithOp, constFirst bool, i int) {
	x, ok := fusedSrcFloat(v, typ, i)
	if !ok {
		return
	}
	l, r := x, c
	if constFirst {
		l, r = c, x
	}
	switch op {
	case arithAdd:
		pgFloatAdd(l, r)
	case arithSub:
		pgFloatSub(l, r)
	case arithMul:
		pgFloatMul(l, r)
	case arithDiv:
		pgFloatDiv(l, r)
	}
}

// fusedSrcFloat reads one source value back at the width the fused loop read
// it. ok=false for a column shape that has no fused loop, which cannot reach
// here.
func fusedSrcFloat(v *batch.Vector, typ batch.TypeID, i int) (float64, bool) {
	switch typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[i]), true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return float64(v.Int64Data[i]), true
	case batch.TypeFloat64:
		return v.Float64Data[i], true
	case batch.TypeFloat32:
		return float64(v.Float32Data[i]), true
	}
	return 0, false
}

// floatSignMask is a float64's sign bit; clearing it leaves the MAGNITUDE,
// which orders the same way the unsigned integer does — so one comparison per
// row inside an arithmetic loop answers "did any result leave the type"
// without a second pass over the result buffer (fused_float64_range.go).
const floatSignMask = uint64(1) << 63
