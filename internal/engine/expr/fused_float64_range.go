// SPDX-License-Identifier: MIT

package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// fusedFloat64Range is the fused column-constant loop for a FLOAT64 source,
// carrying PostgreSQL's range rule (float_range.go) inside the arithmetic
// rather than in a pass over the result buffer.
//
// It is the hot one — TPC-H Q14's `100.00 * SUM(…)`, ClickBench Q30's ninety
// `SUM(col + k)` expressions — and the second pass the first cut used cost a
// full re-read of a 16 KB buffer: 4.6 µs → 6.2 µs on
// BenchmarkCompiledFloatArithVec, +35%. What replaces it is one comparison
// per row against the largest result MAGNITUDE seen so far, which the
// compiler emits as a cmov: a magnitude orders the same way its unsigned
// integer does, and every non-finite one is at least the exponent mask. The
// operand exemptions are asked ONCE, after the loop, and only for a buffer
// that actually holds a non-finite result — so an ordinary batch never reads
// its source values a second time. 4.6 µs → 3.6 µs, −20% against the base.
//
// The UNDERFLOW test stays inside the two operators that have one, with the
// constant's own half hoisted out of the loop: `c != 0` for the product and
// `c` finite for the quotient are loop invariants, and with them hoisted the
// per-row test is the same magnitude comparison the loop already made. That
// matters for correctness as much as for cost — `f8 / 1e400` has an INFINITE
// constant, whose quotient is a zero PostgreSQL answers rather than refuses.
//
// It lives in its own file, and that is a MEASURED decision rather than a
// tidy one. Inlined into fusedColConstFloat64 this same code made
// BenchmarkBinOpNumericEvalFloat — a per-row evaluator that never calls it —
// 20% slower than the base where the unpatched tree is 8% slower; moved here,
// with no other change, that benchmark reads 7% and this one keeps its win.
// The per-row path cannot execute a line of this function, so the difference
// is where the package's code SITS, not what it does. See the arc's round-2
// notes for the four-tree measurement.
func fusedFloat64Range(v *batch.Vector, c float64, op arithOp, constFirst bool, dst []float64, n int) (bool, bool) {
	src := v.Float64Data
	var mx uint64
	switch {
	case op == arithAdd:
		for i := 0; i < n; i++ {
			r := src[i] + c
			dst[i] = r
			if m := math.Float64bits(r) &^ floatSignMask; m > mx {
				mx = m
			}
		}
	case op == arithSub && !constFirst:
		for i := 0; i < n; i++ {
			r := src[i] - c
			dst[i] = r
			if m := math.Float64bits(r) &^ floatSignMask; m > mx {
				mx = m
			}
		}
	case op == arithSub:
		for i := 0; i < n; i++ {
			r := c - src[i]
			dst[i] = r
			if m := math.Float64bits(r) &^ floatSignMask; m > mx {
				mx = m
			}
		}
	case op == arithMul:
		// float8mul's underflow: a zero product from two NON-ZERO operands.
		cNonZero := c != 0
		for i := 0; i < n; i++ {
			r := src[i] * c
			dst[i] = r
			m := math.Float64bits(r) &^ floatSignMask
			if m > mx {
				mx = m
			}
			if m == 0 && cNonZero && src[i] != 0 {
				raiseFloatUnderflow()
			}
		}
	default:
		// float8div's, with the divisor exempted: `1 / Infinity` is zero on
		// both engines, and the caller has already refused a zero constant.
		cFinite := !nonFiniteFloat(c)
		for i := 0; i < n; i++ {
			r := src[i] / c
			dst[i] = r
			m := math.Float64bits(r) &^ floatSignMask
			if m > mx {
				mx = m
			}
			if m == 0 && cFinite && src[i] != 0 {
				raiseFloatUnderflow()
			}
		}
	}
	if mx >= floatExpMask {
		// A non-finite RESULT is where the operand exemption starts to
		// matter, and only for the rows that produced one.
		checkFusedFloatRange(v, batch.TypeFloat64, c, op, constFirst, dst, n)
	}
	return true, v.Nulls.HasNulls()
}
