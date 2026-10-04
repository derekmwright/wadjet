// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestRoundDoublePrecisionHalfToEven pins PostgreSQL's rounding rule for
// DOUBLE PRECISION (#381): ROUND rounds a NUMERIC operand half AWAY from
// zero, but a DOUBLE PRECISION operand half TO EVEN. Both CAST(x AS ...)
// and the x::type postfix spelling reach the same compiled *Cast node, so
// both are covered; REAL and FLOAT collapse to the same runtime float64 as
// DOUBLE PRECISION in this engine (no float32/numeric tower) and round the
// same way in PostgreSQL, so they're covered too.
func TestRoundDoublePrecisionHalfToEven(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want float64
	}{
		{"ROUND(CAST(0.5 AS double precision))", 0.0},
		{"ROUND(CAST(1.5 AS double precision))", 2.0},
		{"ROUND(CAST(2.5 AS double precision))", 2.0},
		{"ROUND(CAST(-0.5 AS double precision))", -0.0},
		{"ROUND(CAST(-1.5 AS double precision))", -2.0},
		{"ROUND(0.5::double precision)", 0.0},
		{"ROUND(2.5::double precision)", 2.0},
		{"ROUND(CAST(0.5 AS real))", 0.0},
		{"ROUND(CAST(2.5 AS real))", 2.0},
		{"ROUND(CAST(0.5 AS float))", 0.0},
		{"ROUND(CAST(2.5 AS float))", 2.0},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			got := e.Eval(b, 0)
			if got != c.want {
				t.Errorf("Eval(%q) = %#v, want %#v", c.sql, got, c.want)
			}
		})
	}
}

// TestRoundNumericStillHalfAwayFromZero guards the NUMERIC side of #381: a
// bare literal, a column, and an explicit NUMERIC/DECIMAL cast must all keep
// rounding half AWAY from zero — the DOUBLE PRECISION routing must not leak
// onto them.
//
// A bare literal argument boxes as a decimal string, not a float64: since
// #1252's round 5 (`9b096b9e`) a fractional literal declares DECIMAL
// wherever it sits, and round 7's B1 fix moved scalarFnDeclaredDecimal's own
// declaration with it, so `ROUND(2.5)` now answers a decimal "3" the way
// PostgreSQL's numeric does (review r5 B1, #1252). A bare, unparameterized
// `CAST(x AS numeric/decimal)` over an operand with an exact type is that
// type at the carrier's width (#1386, Cast.bareDecimalType), so it answers
// the same decimal text.
func TestRoundNumericStillHalfAwayFromZero(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want any
	}{
		{"ROUND(0.5)", "1"},
		{"ROUND(1.5)", "2"},
		{"ROUND(2.5)", "3"},
		{"ROUND(-0.5)", "-1"},
		{"ROUND(-1.5)", "-2"},
		{"ROUND(CAST(0.5 AS numeric))", "1"},
		{"ROUND(CAST(2.5 AS decimal))", "3"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			got := e.Eval(b, 0)
			if got != c.want {
				t.Errorf("Eval(%q) = %#v, want %#v", c.sql, got, c.want)
			}
		})
	}
}

// TestRoundsHalfEvenDecision pins the ONE decision every rounding site makes
// (rounding_rule.go): the operand's PostgreSQL category picks the rule, and
// only a category this layer cannot name falls back to the carrier's reading
// — a numeric literal is numeric, every other float64 a double. At 89cea148
// ROUND decided by the SPELLING of its argument (a CAST to a float type), so
// a DOUBLE PRECISION column, a float expression and a scalar subquery rounded
// half away from zero (#381).
func TestRoundsHalfEvenDecision(t *testing.T) {
	lit := &Lit{Val: 2.5}
	cases := []struct {
		name string
		cat  PGCategory
		op   Expr
		even bool
	}{
		{"float8 column", PGCatFloat8, &ColRef{Name: "f"}, true},
		{"float8 cast of a literal", PGCatFloat8, &Cast{Operand: lit, DestType: "double precision"}, true},
		// A literal whose plan says float8 (a folded CAST) is float8: the
		// category decides before the box does.
		{"float8 literal box", PGCatFloat8, lit, true},
		{"numeric column", PGCatNumeric, &ColRef{Name: "n"}, false},
		{"numeric literal", PGCatNumeric, lit, false},
		{"float-carried numeric", PGCatNumeric, &BinOp{Op: "/", Left: &Lit{Val: int64(5)}, Right: &Lit{Val: 2.0}}, false},
		// An integer has no fraction: either rule answers it, and the
		// decision keeps the carrier's reading.
		{"integer column", PGCatInteger, &ColRef{Name: "i"}, true},
		{"integer literal", PGCatInteger, &Lit{Val: int64(2)}, false},
		{"unknown literal", PGCatUnknown, lit, false},
		{"unknown negated literal", PGCatUnknown, &UnaryOp{Op: "-", Operand: lit}, false},
		{"unknown column", PGCatUnknown, &ColRef{Name: "x"}, true},
		{"unknown cast", PGCatUnknown, &Cast{Operand: lit, DestType: "real"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := roundsHalfEven(c.cat, c.op); got != c.even {
				t.Errorf("roundsHalfEven(%v, %T) = %v, want %v", c.cat, c.op, got, c.even)
			}
		})
	}
}

// TestRoundRuleFollowsTheDecision checks that ROUND's two kernels and the
// integer CAST answer by the decision, at every value class: the half-way
// values, the doubles beside 2.5, 2^52 + 0.5 (no fraction left), 1e300, NaN
// and the infinities.
func TestRoundRuleFollowsTheDecision(t *testing.T) {
	cases := []struct {
		v          float64
		even, away float64
	}{
		{0.5, 0, 1}, {1.5, 2, 2}, {2.5, 2, 3}, {3.5, 4, 4}, {-0.5, math.Copysign(0, -1), -1},
		{-1.5, -2, -2}, {-2.5, -2, -3}, {2.4999999999999996, 2, 2}, {2.5000000000000004, 3, 3},
		{4503599627370496.5, 4503599627370496, 4503599627370496}, {1e300, 1e300, 1e300},
		{math.Inf(1), math.Inf(1), math.Inf(1)}, {math.Inf(-1), math.Inf(-1), math.Inf(-1)},
	}
	for _, c := range cases {
		even := fnRoundHalfEven([]any{c.v}).(float64)
		away := fnRound([]any{c.v}).(float64)
		if even != c.even || math.Signbit(even) != math.Signbit(c.even) {
			t.Errorf("round half to even(%v) = %v, want %v", c.v, even, c.even)
		}
		if away != c.away {
			t.Errorf("round half away(%v) = %v, want %v", c.v, away, c.away)
		}
	}
	if !math.IsNaN(fnRoundHalfEven([]any{math.NaN()}).(float64)) {
		t.Error("round half to even(NaN) is not NaN")
	}
	if got := castFloatToInt64Even(2.5, "bigint"); got != 2 {
		t.Errorf("castFloatToInt64Even(2.5) = %d, want 2", got)
	}
	if got := castFloatToInt64(2.5, "bigint"); got != 3 {
		t.Errorf("castFloatToInt64(2.5) = %d, want 3", got)
	}
}

// TestRoundOverAFloatColumnHalfToEven is the column door of #381: ROUND over
// a FLOAT64 column, row by row and vectorized, rounds half to even. At
// 89cea148 it answered 1, 3, -1 for 0.5, 2.5, -0.5 (PostgreSQL 0, 2, -0).
func TestRoundOverAFloatColumnHalfToEven(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "f", Type: parquet.TypeFloat64}}, 3)
	for i, v := range []float64{0.5, 2.5, -0.5} {
		b.Columns[0].SetValue(i, v)
	}
	want := []float64{0, 2, math.Copysign(0, -1)}
	e := compileExprSQL(t, "ROUND(f)")
	// The compiled node is the exact DECIMAL form, whose fallback — the
	// FuncCall every non-DECIMAL argument takes — carries the vector path.
	fc := e.(*decimalScalarFn).fallback
	out := batch.NewVector(batch.TypeFloat64, 3)
	fc.EvalVec(b, out, 3)
	for i, w := range want {
		if got := e.Eval(b, i); got != w || math.Signbit(got.(float64)) != math.Signbit(w) {
			t.Errorf("row %d: ROUND(f) = %v, want %v", i, got, w)
		}
		if got := out.Float64Data[i]; got != w || math.Signbit(got) != math.Signbit(w) {
			t.Errorf("row %d: vectorized ROUND(f) = %v, want %v", i, got, w)
		}
	}
}
