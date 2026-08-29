package expr

import (
	"math"
	"math/bits"

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
// The test is the branch-free two's-complement one: a signed addition
// overflows exactly when both operands share a sign and the result takes the
// other, which `(a^s) & (b^s) < 0` says in three ALU ops. The four-comparison
// spelling says the same thing and costs enough MORE that the whole function
// misses the inliner's budget — and a guard that cannot be inlined costs more
// than the arithmetic it protects.
func addInt64Checked(a, b int64) int64 {
	s := a + b
	if (a^s)&(b^s) < 0 {
		raiseBigintOutOfRange()
	}
	return s
}

// subInt64Checked returns a - b, raising 22003 when the exact difference has
// no int64. It overflows exactly when the operands' signs DIFFER and the
// result takes the subtrahend's, which is `(a^b) & (a^d) < 0`.
func subInt64Checked(a, b int64) int64 {
	d := a - b
	if (a^b)&(a^d) < 0 {
		raiseBigintOutOfRange()
	}
	return d
}

// mulInt64Checked returns a * b, raising 22003 when the exact product has no
// int64.
//
// Two paths, and neither divides. The first cut checked with `p/a != b`, which
// is correct and costs an integer DIVISION on every row — 20-40 cycles, where
// the multiply itself is 3 — and measured +39% on the typed int arithmetic
// benchmark (`id*2+10`, the shape ClickBench's `i*k` takes). The check has to
// be as cheap as the operation it guards or it is not a check, it is a tax.
//
//   - Both magnitudes below 2^31: the product is below 2^62 and cannot
//     overflow, so the bare multiply stands. This is every ordinary row.
//   - Otherwise the full 128-bit product of the magnitudes, from one
//     bits.Mul64, and the answer fits exactly when its high word is zero and
//     the low word is inside the signed range for the product's sign — 2^63-1
//     positive, 2^63 negative, that one extra value being MinInt64 itself.
func mulInt64Checked(a, b int64) int64 {
	// Both operands inside int32 means the product is inside int62 and
	// cannot overflow. The round-trip conversion is the cheapest spelling of
	// that test that still INLINES: a branch-free `uint64(a>>31+1)|…` form
	// costs the inliner more than the two comparisons do, and a guard that
	// misses the budget becomes a call — which is what made these cost more
	// than the arithmetic they protect in the first place.
	if a == int64(int32(a)) && b == int64(int32(b)) {
		return a * b
	}
	return mulInt64Wide(a, b)
}

// mulInt64Wide is the multiply for operands the 32-bit test did not clear. It
// is a function of its own so mulInt64Checked stays inside the inliner's
// budget: these guards run once per operand per row, and a call that cannot be
// inlined costs more than the arithmetic it protects.
//
//go:noinline
func mulInt64Wide(a, b int64) int64 {
	ma, na := int64Magnitude(a)
	mb, nb := int64Magnitude(b)
	hi, lo := bits.Mul64(ma, mb)
	if hi != 0 {
		raiseBigintOutOfRange()
	}
	if na != nb {
		if lo > 1<<63 {
			raiseBigintOutOfRange()
		}
		// lo == 2^63 converts to MinInt64, whose negation is itself — which
		// is the value being asked for, so this is right at the edge too.
		return -int64(lo)
	}
	if lo > 1<<63-1 {
		raiseBigintOutOfRange()
	}
	return int64(lo)
}

// int64Magnitude returns |v| and whether v was negative. uint64(-v) is exact
// for MinInt64 as well: -v wraps back to MinInt64 and its unsigned bits ARE
// 2^63, the magnitude wanted.
func int64Magnitude(v int64) (uint64, bool) {
	if v < 0 {
		return uint64(-v), true
	}
	return uint64(v), false
}

// divInt64Checked returns a / b, truncating toward zero. A zero divisor is
// 22012 and MinInt64 / -1 is 22003 — its quotient is 2^63, the one division
// with no int64.
func divInt64Checked(a, b int64) int64 {
	if b == 0 || (a == math.MinInt64 && b == -1) {
		divInt64Refuse(b)
	}
	return a / b
}

// divInt64Refuse carries the two refusals out of the hot function so it stays
// inlinable; b decides which one, since the caller has already established
// that one of them applies.
//
//go:noinline
func divInt64Refuse(b int64) {
	if b == 0 {
		raiseDivisionByZero()
	}
	raiseBigintOutOfRange()
}

// modInt64Checked returns a % b with the dividend's sign, PostgreSQL's rule
// and Go's. A zero divisor is 22012; MinInt64 % -1 is 0 and needs no refusal,
// but Go traps the division it is computed from, so it is answered directly.
func modInt64Checked(a, b int64) int64 {
	if b == 0 {
		divInt64Refuse(0)
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
//
//go:noinline
func raiseBigintOutOfRange() {
	panic(fatalEval{sqlerr.New("22003", "bigint out of range")})
}
