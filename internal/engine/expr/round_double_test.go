// SPDX-License-Identifier: MIT

package expr

import "testing"

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
