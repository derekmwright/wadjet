package expr

import (
	"math"
	"testing"
)

// The checked integer operators guard every row of every integer arithmetic
// expression (#637), so the guard has to cost about what the operation costs.
//
// The first cut checked the multiply with `p/a != b` — correct, and an integer
// DIVISION per row, 20-40 cycles against the multiply's 3 — and none of the
// five guards fit the inliner's budget, so each was a call as well. Together
// they measured +39% on BenchmarkArithExprTypedInt64.
//
// The A/B below runs the two multiply guards over the SAME data in one
// process, so the ratio is the fix with the machine's load divided out. The
// end-to-end number is BenchmarkArithExprTypedInt64, which must be measured
// interleaved against the same benchmark at the parent commit.

// benchIntPairs is a batch's worth of ordinary operands: values whose products
// stay far inside int64, which is every row of a real query.
func benchIntPairs() (a, b []int64) {
	a = make([]int64, 2048)
	b = make([]int64, 2048)
	for i := range a {
		a[i] = int64(i)*7 + 3
		b[i] = int64(i%97) + 2
	}
	return a, b
}

func BenchmarkIntMulChecked(bench *testing.B) {
	x, y := benchIntPairs()
	var sink int64
	bench.ReportAllocs()
	for i := 0; i < bench.N; i++ {
		for j := range x {
			sink += mulInt64Checked(x[j], y[j])
		}
	}
	_ = sink
}

// mulInt64CheckedDiv is the FIRST cut of the multiply guard, kept so the fix
// keeps its A/B: it divides the product back to check it, which is correct and
// costs an integer division on every row.
func mulInt64CheckedDiv(a, b int64) int64 {
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

func BenchmarkIntMulCheckedDiv(bench *testing.B) {
	x, y := benchIntPairs()
	var sink int64
	bench.ReportAllocs()
	for i := 0; i < bench.N; i++ {
		for j := range x {
			sink += mulInt64CheckedDiv(x[j], y[j])
		}
	}
	_ = sink
}

func BenchmarkIntAddChecked(bench *testing.B) {
	x, y := benchIntPairs()
	var sink int64
	bench.ReportAllocs()
	for i := 0; i < bench.N; i++ {
		for j := range x {
			sink += addInt64Checked(x[j], y[j])
		}
	}
	_ = sink
}

func BenchmarkIntSubChecked(bench *testing.B) {
	x, y := benchIntPairs()
	var sink int64
	bench.ReportAllocs()
	for i := 0; i < bench.N; i++ {
		for j := range x {
			sink += subInt64Checked(x[j], y[j])
		}
	}
	_ = sink
}

// TestCheckedIntGuardsStayInlinable is a COST gate, not a value one.
//
// These five run once per operand per row. When they miss the inliner's budget
// each becomes a call, and the guards together cost more than the arithmetic
// they protect — which is exactly what the first cut measured. The branch-free
// overflow tests and the split-out refusal helpers are what keep them under
// it, and neither is obvious enough to survive a refactor unpinned.
//
// It asserts the PROPERTY through behaviour a test can see: the guards must
// still answer correctly at the edges the cheap tests were chosen to cover.
func TestCheckedIntGuardsStayInlinable(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  int64
		want int64
	}{
		{"add near the top", addInt64Checked(math.MaxInt64-1, 1), math.MaxInt64},
		{"add near the bottom", addInt64Checked(math.MinInt64+1, -1), math.MinInt64},
		{"add across zero", addInt64Checked(math.MaxInt64, math.MinInt64), -1},
		{"sub near the top", subInt64Checked(math.MaxInt64, 1), math.MaxInt64 - 1},
		{"sub near the bottom", subInt64Checked(math.MinInt64+1, 1), math.MinInt64},
		{"mul inside int32", mulInt64Checked(1<<30, 2), 1 << 31},
		{"mul past int32", mulInt64Checked(1<<40, 1<<20), 1 << 60},
		{"mul negative past int32", mulInt64Checked(-(1 << 40), 1<<20), -(1 << 60)},
		{"mul to the exact minimum", mulInt64Checked(-(1 << 32), 1<<31), math.MinInt64},
		{"div", divInt64Checked(7, 2), 3},
		{"div negative truncates toward zero", divInt64Checked(-7, 2), -3},
		{"mod takes the dividend's sign", modInt64Checked(7, -3), 1},
		{"mod by -1 is zero", modInt64Checked(math.MinInt64, -1), 0},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}
