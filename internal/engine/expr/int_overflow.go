package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Checked int64 arithmetic: an integer sum, difference or product that leaves
// the range is a query ERROR, not a wrapped number (#637).
//
// PostgreSQL refuses every one of these — `9223372036854775807::bigint + 1` is
// `bigint out of range`, SQLSTATE 22003 — and wadjet wrapped, which is the
// same class of silent wrong answer ADR-0024 item 4 closes for DECIMAL: a
// wrapped total is a different number wearing the right type, and nothing
// downstream can see that it is wrong. `BinOpNumeric`'s own doc said so out
// loud ("Int64 overflow wraps (Go semantics)") — recorded, but still an answer
// no client should get.
//
// Division and modulo need no check here: the only int64 division that
// overflows is MinInt64 / -1, which the div arm handles beside its zero-divisor
// refusal.

// addInt64Checked returns a + b, raising 22003 when the exact sum has no
// int64.
//
// The test is the standard two's-complement one: a signed addition overflows
// exactly when both operands share a sign and the result takes the other.
func addInt64Checked(a, b int64) int64 {
	s := a + b
	if (a < 0) == (b < 0) && (s < 0) != (a < 0) {
		raiseBigintOutOfRange()
	}
	return s
}

// subInt64Checked returns a - b, raising 22003 when the exact difference has
// no int64. It overflows exactly when the operands' signs DIFFER and the
// result takes the subtrahend's.
func subInt64Checked(a, b int64) int64 {
	d := a - b
	if (a < 0) != (b < 0) && (d < 0) != (a < 0) {
		raiseBigintOutOfRange()
	}
	return d
}

// mulInt64Checked returns a * b, raising 22003 when the exact product has no
// int64.
//
// The check divides back rather than widening: a/b in int64 is exact for the
// quotient, so `p/a != b` catches every overflow. The two special cases are
// a == 0 (never overflows) and a == -1 with b == MinInt64, whose product is
// 2^63 and which the division test cannot see because MinInt64 / -1 overflows
// in its own right.
func mulInt64Checked(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a == -1 && b == math.MinInt64 {
		raiseBigintOutOfRange()
	}
	if b == -1 && a == math.MinInt64 {
		raiseBigintOutOfRange()
	}
	p := a * b
	if p/a != b {
		raiseBigintOutOfRange()
	}
	return p
}

// divInt64Checked returns a / b, truncating toward zero. A zero divisor is
// 22012 and MinInt64 / -1 is 22003 — its quotient is 2^63, the one division
// with no int64.
func divInt64Checked(a, b int64) int64 {
	if b == 0 {
		raiseDivisionByZero()
	}
	if a == math.MinInt64 && b == -1 {
		raiseBigintOutOfRange()
	}
	return a / b
}

// modInt64Checked returns a % b with the dividend's sign, PostgreSQL's rule
// and Go's. A zero divisor is 22012; MinInt64 % -1 is 0 and needs no refusal,
// but Go traps the division it is computed from, so it is answered directly.
func modInt64Checked(a, b int64) int64 {
	if b == 0 {
		raiseDivisionByZero()
	}
	if b == -1 {
		return 0
	}
	return a % b
}

// raiseBigintOutOfRange is PostgreSQL's refusal for an int8 result with no
// value in the type, SQLSTATE 22003.
//
// It says `bigint` and not `integer` because every integer expression in this
// engine computes in int64: an INT32 column is read as an int64 and integer
// arithmetic DECLARES Int64 (physical.intArithAllInt), so the range that can
// actually be left is int8's. An int4 sum that leaves int4's range and stays
// inside int8's is answered here and refused by PostgreSQL — a SUPERSET, the
// direction ADR-0012 records as acceptable.
func raiseBigintOutOfRange() {
	panic(fatalEval{sqlerr.New("22003", "bigint out of range")})
}
