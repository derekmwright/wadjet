package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The math functions' DOMAIN refusals, which used to be a NULL or an infinity.
//
// #840 named three (LN(0), SQRT(-1), and the temporal cast); the census the
// arc ran over the whole function table found the class is nine sites in one
// file, all with the same shape: the argument is outside the function's
// domain, PostgreSQL raises, and this engine manufactured a value. A NULL
// there is a wrong ANSWER — `WHERE LN(x) IS NULL` counted the rows where x is
// zero as if x had been NULL — and an infinity is worse, because it
// propagates arithmetically and nothing downstream can see where it came from.
//
// PostgreSQL uses FOUR codes here and they are four different answers to a
// client. Every one measured live on postgres:17.11:
//
//	LN(0), LOG(0), LOG(b,0), LOG(0,x)   2201E  cannot take logarithm of zero
//	LN(-1), LOG(-1), LOG(-1,x)          2201E  cannot take logarithm of a negative number
//	LOG(1, x)                           22012  division by zero
//	SQRT(-1)                            2201F  cannot take square root of a negative number
//	POWER(0, -1)                        2201F  zero raised to a negative power is undefined
//	POWER(-1, 0.5)                      2201F  a negative number raised to a non-integer power
//	                                           yields a complex result
//	POWER(2, 10000), EXP(1000)          22003  value out of range: overflow
//	POWER(2, -10000), EXP(-1000)        22003  value out of range: underflow
//	ASIN(2), ACOS(2)                    22003  input is out of range
//	MOD(1, 0)                           22012  division by zero
//
// NaN and the infinities are VALUES, not domain failures, and PostgreSQL
// passes them through: SQRT('NaN') is NaN, LN('Infinity') is Infinity,
// SQRT('Infinity') is Infinity, SQRT(-0.0) is -0. Each helper below therefore
// tests the failing condition and nothing else, so a NaN never reaches a
// refusal — the boundary `math_domain_test.go` attempts from the outside.

// raiseLogarithmDomain refuses a logarithm argument that is zero or negative.
// A NaN or a positive infinity passes through: both are values PostgreSQL
// answers with.
func raiseLogarithmDomain(v float64) {
	if v > 0 || math.IsNaN(v) {
		return
	}
	if v == 0 {
		panic(fatalEval{sqlerr.New("2201E", "cannot take logarithm of zero")})
	}
	panic(fatalEval{sqlerr.New("2201E", "cannot take logarithm of a negative number")})
}

// raiseSquareRootDomain refuses a negative square-root argument. `-0.0 < 0` is
// false in IEEE arithmetic and in PostgreSQL, which answers -0 for it.
func raiseSquareRootDomain(v float64) {
	if v < 0 {
		panic(fatalEval{sqlerr.New("2201F", "cannot take square root of a negative number")})
	}
}

// raiseTrigDomain refuses an ASIN/ACOS argument outside [-1, 1]. The
// comparisons are written so a NaN — which is neither less nor greater —
// falls through to the value PostgreSQL answers with, NaN itself.
func raiseTrigDomain(v float64) {
	if v < -1 || v > 1 {
		panic(fatalEval{sqlerr.New("22003", "input is out of range")})
	}
}

// raisePowerDomain refuses the two operand pairs PostgreSQL's dpow() calls
// undefined, in its own order. Neither fires for a NaN or an infinity.
func raisePowerDomain(base, exp float64) {
	if math.IsNaN(base) || math.IsNaN(exp) {
		return
	}
	if base == 0 && exp < 0 && !math.IsInf(exp, 0) {
		panic(fatalEval{sqlerr.New("2201F", "zero raised to a negative power is undefined")})
	}
	if base < 0 && !math.IsInf(base, 0) && !math.IsInf(exp, 0) && exp != math.Trunc(exp) {
		panic(fatalEval{sqlerr.New("2201F",
			"a negative number raised to a non-integer power yields a complex result")})
	}
}

// int32Count narrows a Go count to the int4 the length family declares,
// raising 22003 rather than WRAPPING past 2^31 (#637).
//
// #530 made LENGTH / OCTET_LENGTH / BIT_LENGTH / CARDINALITY answer int4,
// which is PostgreSQL's declaration for all four, and the conversion was a
// bare `int32(len(...))`: past the boundary it produces a negative number
// under a right type, which is the class ADR-0012 item 9 calls a different
// number wearing the right type. PostgreSQL raises `integer out of range`.
//
// The inputs that reach the boundary are extreme — BIT_LENGTH needs a string
// past 256 MB, OCTET_LENGTH one past 2 GB, CARDINALITY an array of 2.1 billion
// elements — which is exactly why the wrap was silent: no fixture has one, and
// the check costs a compare that the branch predictor answers for free.
func int32Count(n int) int32 {
	if n > math.MaxInt32 || n < math.MinInt32 {
		panic(fatalEval{sqlerr.New("22003", "integer out of range")})
	}
	return int32(n)
}

// raiseFloatOverflow and raiseFloatUnderflow are PostgreSQL's two float8 range
// refusals, `float_overflow_error` and `float_underflow_error`.
//
// They are two bare raises rather than one "result check" helper, and that is
// the correction the review forced: the helper took a single `finite` argument
// and a `nonZero` bool, which read as though one predicate fitted every
// caller. It does not. `dexp` excludes an INFINITE ARGUMENT from both checks
// (per POSIX `exp(-Inf)` is zero, not an underflow), `dpow` excludes an
// infinite EITHER operand from the overflow, and `dpow`'s underflow does not
// apply here at all — see fnPow. Each caller states its own rule beside the
// call, where the reader can compare it to float.c.
func raiseFloatOverflow() {
	panic(fatalEval{sqlerr.New("22003", "value out of range: overflow")})
}

func raiseFloatUnderflow() {
	panic(fatalEval{sqlerr.New("22003", "value out of range: underflow")})
}
