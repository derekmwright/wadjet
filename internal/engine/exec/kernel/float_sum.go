// SPDX-License-Identifier: MIT

package kernel

// The float SUM's two range rules, both PostgreSQL's and both measured on
// 17.11.
//
// RANGE (#1082). `sum(float8)` is float8pl applied row by row, so a total that
// leaves the type is `22003 value out of range: overflow` and never the
// +Infinity this engine answered — the same rule the arithmetic kernels now
// carry (expr/float_range.go) and the same one the integer and DECIMAL sums
// have carried since #637 and #455. An infinity that ARRIVES as an input is a
// value on both engines, so the operand exemption is what keeps
// `sum(x)` over a column holding Infinity answering Infinity.
//
// WIDTH (#950). `sum(real)` is REAL on PostgreSQL — float4pl, accumulated at
// float4's width — while `avg(real)` is double precision and totals each value
// at float8's (#760). Over 2, 0.1, 12.75, 16777216 and -20 the two answers
// differ in the last digit the type can hold: 1.677721e+07 for the real
// accumulation and 1.6777211e+07 for a float8 total narrowed once at the end.
// Both are "the sum"; only one is the server's.
//
// The width is carried on the running float64 rather than in a second field
// because a float64 that holds a float32 value holds it EXACTLY: the sum of
// two float32s has a float64, so rounding that sum back to float32 is the
// correctly-rounded float32 addition, and a partial that spills, merges or
// clones stays float32-exact through every one of those paths.

// foldFloatSum adds v into a running float8 total, reporting an overflow
// rather than raising: the aggregate's error channel is the emit-time check
// (exec.aggEmitErr), which is where IntOverflow and DecOverflow are read.
func foldFloatSum(sum, v float64) (float64, bool) {
	s := sum + v
	return s, s-s != 0 && sum-sum == 0 && v-v == 0
}

// foldRealSum is foldFloatSum at float4's width — PostgreSQL's float4pl.
func foldRealSum(sum float64, v float32) (float64, bool) {
	a := float32(sum)
	s := a + v
	return float64(s), s-s != 0 && a-a == 0 && v-v == 0
}

// FoldRealSum is foldRealSum for the SoA grouped path, which lives in package
// exec and holds its running totals in a []float64 of its own.
func FoldRealSum(sum float64, v float32) (float64, bool) { return foldRealSum(sum, v) }

// FoldFloatSum is foldFloatSum for the same callers.
func FoldFloatSum(sum, v float64) (float64, bool) { return foldFloatSum(sum, v) }
