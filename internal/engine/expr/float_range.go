package expr

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
// error.
//
//   - and - : overflow when the result is infinite and neither operand was
//     (float8pl/float8mi). No underflow rule — a sum that rounds to
//     zero is the answer.
//   - : the same overflow rule, plus underflow when the product is zero
//     and neither operand was (float8mul).
//     /       : overflow when the result is infinite and the DIVIDEND was not,
//     underflow when the result is zero and the dividend was not
//     (float8div). The divisor is not exempted: `1e308 / 1e-308` is an
//     overflow on both engines, and a zero divisor is 22012, raised by
//     the callers before they reach here.
//
// Unary minus has no rule at all — negation cannot leave the range — which is
// why `-1e308` answers and `-1e308 * 10` does not (both measured).
//
// The checks are written as `r-r != 0` rather than math.IsInf where the extra
// branch would cost more than the arithmetic it guards: that expression is a
// subtract and a compare, is false for every finite operand including zero,
// and is true for both infinities and for NaN. A NaN result from finite
// operands is impossible for these four operators, so routing it to the
// overflow refusal costs nothing and reaches no reachable value.
func pgFloatAdd(a, b float64) float64 {
	r := a + b
	if r-r != 0 && a-a == 0 && b-b == 0 {
		raiseFloatOverflow()
	}
	return r
}

func pgFloatSub(a, b float64) float64 {
	r := a - b
	if r-r != 0 && a-a == 0 && b-b == 0 {
		raiseFloatOverflow()
	}
	return r
}

func pgFloatMul(a, b float64) float64 {
	r := a * b
	if r == 0 {
		if a != 0 && b != 0 {
			raiseFloatUnderflow()
		}
		return r
	}
	if r-r != 0 && a-a == 0 && b-b == 0 {
		raiseFloatOverflow()
	}
	return r
}

// pgFloatDiv is the quotient for a divisor the caller has already established
// is non-zero: a zero divisor is 22012 and is raised there, where the NULL
// rows have already been separated from the genuine zeros.
func pgFloatDiv(a, b float64) float64 {
	r := a / b
	if r == 0 {
		if a != 0 && b-b == 0 {
			raiseFloatUnderflow()
		}
		return r
	}
	if r-r != 0 && a-a == 0 && b-b == 0 {
		raiseFloatOverflow()
	}
	return r
}

// pgFloatArith applies the rule for a resolved opcode. It is the form the
// row-at-a-time nodes take; the tight loops keep their own unrolled arms.
func pgFloatArith(op arithOp, a, b float64) (float64, bool) {
	switch op {
	case arithAdd:
		return pgFloatAdd(a, b), true
	case arithSub:
		return pgFloatSub(a, b), true
	case arithMul:
		return pgFloatMul(a, b), true
	case arithDiv:
		return pgFloatDiv(a, b), true
	}
	return 0, false
}

// floatVecSuspect reports whether a filled result buffer holds anything the
// range rule might refuse: a non-finite value for every operator, and a zero
// for the two that have an underflow rule.
//
// It is one pass with no branch on the common row, and it exists so the
// vectorized kernels pay a compare rather than the operand test: a batch whose
// results are all ordinary finite non-zero numbers — every batch TPC-H and
// ClickBench produce — answers false here and never re-reads its operands. A
// batch that answers true is re-evaluated ROW BY ROW through the checked
// scalar path, which has the operands in hand and raises with the right rule
// for the right row. Re-evaluating is what keeps the two paths from
// disagreeing: the loop does not carry enough state to tell an infinite
// OPERAND from an infinite RESULT.
func floatVecSuspect(dst []float64, n int, op arithOp) bool {
	if n > len(dst) {
		n = len(dst)
	}
	switch op {
	case arithMul, arithDiv:
		for i := 0; i < n; i++ {
			v := dst[i]
			if v == 0 || v-v != 0 {
				return true
			}
		}
	case arithAdd, arithSub:
		for i := 0; i < n; i++ {
			if v := dst[i]; v-v != 0 {
				return true
			}
		}
	}
	return false
}
