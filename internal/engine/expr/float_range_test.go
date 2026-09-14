package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1082: the float8 arithmetic kernels answered ±Inf for a result PostgreSQL
// 17.11 refuses, and a CTAS stored the infinity. Every expectation below was
// measured on the live server; see float_range.go for the rule, operator by
// operator.

func floatRangeBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "tiny", Type: parquet.TypeFloat64},
		{Name: "inf", Type: parquet.TypeFloat64},
		{Name: "z", Type: parquet.TypeFloat64},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, 1e308)
	b.Columns[1].SetValue(0, 1e-300)
	b.Columns[2].SetValue(0, math.Inf(1))
	b.Columns[3].SetValue(0, 0.0)
	return b
}

// TestFloatArithmeticRefusesAResultOutsideTheType is the refusal half: a
// non-finite (or flushed-to-zero) result from FINITE operands.
func TestFloatArithmeticRefusesAResultOutsideTheType(t *testing.T) {
	b := floatRangeBatch(t)
	for _, c := range []struct{ name, sql, msg string }{
		{"product_of_a_column_and_a_literal", "f * 10", "value out of range: overflow"},
		{"product_of_two_columns", "f * f", "value out of range: overflow"},
		{"sum_of_two_columns", "f + f", "value out of range: overflow"},
		{"difference_across_the_range", "f - (0 - f)", "value out of range: overflow"},
		{"quotient_by_a_fraction", "f / 0.5", "value out of range: overflow"},
		{"negated_then_multiplied", "-f * 10", "value out of range: overflow"},
		{"quotient_by_a_tiny_divisor", "f / tiny", "value out of range: overflow"},
		{"product_that_flushes_to_zero", "tiny * tiny", "value out of range: underflow"},
		{"quotient_that_flushes_to_zero", "tiny / 1e300", "value out of range: underflow"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := intDomainCompile(t, c.sql)
			state, msg := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
			if state != "22003" || msg != c.msg {
				t.Errorf("%s raised [%s] %s, want [22003] %s\n"+
					"PostgreSQL 17.11 refuses this expression; an infinity here is a "+
					"value from nowhere and a CTAS stores it (#1082).",
					c.sql, state, msg, c.msg)
			}
		})
	}
}

// TestAnInfiniteOperandIsAValue is the other half and the reason the rule
// tests the OPERANDS and not only the result: PostgreSQL's float8pl exempts an
// infinite input, so every one of these ANSWERS on both engines.
func TestAnInfiniteOperandIsAValue(t *testing.T) {
	b := floatRangeBatch(t)
	for _, c := range []struct {
		name, sql string
		want      float64
	}{
		{"infinity_times_ten", "inf * 10", math.Inf(1)},
		{"infinity_plus_one", "inf + 1", math.Inf(1)},
		{"infinity_over_two", "inf / 2", math.Inf(1)},
		{"negated_infinity", "-inf", math.Inf(-1)},
		{"a_zero_operand_may_produce_zero", "z * f", 0},
		{"a_zero_dividend_may_produce_zero", "z / f", 0},
		{"one_over_infinity_is_zero", "1 / inf", 0},
		{"negation_never_leaves_the_range", "-f", -1e308},
		{"an_ordinary_product_still_answers", "tiny * 1e10", 1e-290},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			f, ok := got.(float64)
			if !ok || f != c.want {
				t.Errorf("%s = %T(%v), want float64(%v) — measured on PostgreSQL 17.11",
					c.sql, got, got, c.want)
			}
		})
	}
}

// TestTheVectorizedFloatKernelRefusesTheSameRows is the arm the scalar test
// cannot reach: a projection over a whole batch takes EvalFloat64Vec, whose
// tight loops keep no operands, so the rule lives in a scan of the results and
// a per-row re-evaluation. A batch that overflows on ONE row must refuse, and
// one whose infinities all came from an infinite INPUT must not.
func TestTheVectorizedFloatKernelRefusesTheSameRows(t *testing.T) {
	const n = 8
	mk := func(t *testing.T, fill func(i int) (float64, float64)) *batch.RecordBatch {
		t.Helper()
		b := batch.NewRecordBatch([]parquet.Column{
			{Name: "a", Type: parquet.TypeFloat64},
			{Name: "bb", Type: parquet.TypeFloat64},
		}, n)
		b.Len = n
		for i := 0; i < n; i++ {
			x, y := fill(i)
			b.Columns[0].SetValue(i, x)
			b.Columns[1].SetValue(i, y)
		}
		return b
	}
	// The node is built directly: a two-COLUMN product compiles to
	// BinOpNumeric, whose float mode delegates to this one per row, so
	// reaching the vectorized loops at all takes the typed node itself.
	colCol := func() *BinOpFloat64 {
		return &BinOpFloat64{Left: &ColRef{Name: "a"}, Right: &ColRef{Name: "bb"}, Op: "*"}
	}
	colConst := func() *BinOpFloat64 {
		return &BinOpFloat64{Left: &ColRef{Name: "a"}, Right: &Lit{Val: 10.0}, Op: "*"}
	}

	t.Run("one_overflowing_row_in_a_batch_refuses", func(t *testing.T) {
		b := mk(t, func(i int) (float64, float64) {
			if i == 5 {
				return 1e308, 10
			}
			return float64(i + 1), 2
		})
		dst := make([]float64, n)
		state, msg := recoverFatalEvalForTest(t, func() {
			colCol().EvalFloat64Vec(b, dst, n)
		})
		if state != "22003" || msg != "value out of range: overflow" {
			t.Errorf("the vectorized kernel raised [%s] %s, want [22003] value out of "+
				"range: overflow", state, msg)
		}
	})

	t.Run("an_infinite_input_keeps_the_whole_batch", func(t *testing.T) {
		b := mk(t, func(i int) (float64, float64) {
			if i == 5 {
				return math.Inf(1), 10
			}
			return float64(i + 1), 2
		})
		dst := make([]float64, n)
		colCol().EvalFloat64Vec(b, dst, n)
		if !math.IsInf(dst[5], 1) {
			t.Errorf("row 5 = %v, want +Inf: an infinite OPERAND is a value on both engines", dst[5])
		}
		if dst[0] != 2 {
			t.Errorf("row 0 = %v, want 2", dst[0])
		}
	})

	t.Run("the_fused_column_constant_loop_is_checked_too", func(t *testing.T) {
		b := mk(t, func(i int) (float64, float64) {
			if i == 3 {
				return 1e308, 0
			}
			return float64(i + 1), 0
		})
		dst := make([]float64, n)
		state, _ := recoverFatalEvalForTest(t, func() {
			colConst().EvalFloat64Vec(b, dst, n)
		})
		if state != "22003" {
			t.Errorf("the fused col-const loop raised [%s], want [22003]", state)
		}
	})

	t.Run("an_ordinary_batch_is_untouched", func(t *testing.T) {
		b := mk(t, func(i int) (float64, float64) { return float64(i + 1), 2 })
		dst := make([]float64, n)
		colCol().EvalFloat64Vec(b, dst, n)
		for i := 0; i < n; i++ {
			if want := float64(i+1) * 2; dst[i] != want {
				t.Fatalf("row %d = %v, want %v", i, dst[i], want)
			}
		}
	})
}
