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
func TestRoundNumericStillHalfAwayFromZero(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want float64
	}{
		{"ROUND(0.5)", 1.0},
		{"ROUND(1.5)", 2.0},
		{"ROUND(2.5)", 3.0},
		{"ROUND(-0.5)", -1.0},
		{"ROUND(-1.5)", -2.0},
		{"ROUND(CAST(0.5 AS numeric))", 1.0},
		{"ROUND(CAST(2.5 AS decimal))", 3.0},
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

// TestIsBinaryFloatCast covers the compile-time routing signal directly.
func TestIsBinaryFloatCast(t *testing.T) {
	cases := []struct {
		name string
		e    Expr
		want bool
	}{
		{"double_cast", &Cast{Operand: &Lit{Val: 0.5}, DestType: "double"}, true},
		{"real_cast", &Cast{Operand: &Lit{Val: 0.5}, DestType: "real"}, true},
		{"float_cast", &Cast{Operand: &Lit{Val: 0.5}, DestType: "float"}, true},
		{"numeric_cast", &Cast{Operand: &Lit{Val: 0.5}, DestType: "numeric"}, false},
		{"decimal_cast", &Cast{Operand: &Lit{Val: 0.5}, DestType: "decimal"}, false},
		{"int_cast", &Cast{Operand: &Lit{Val: 5}, DestType: "int"}, false},
		{"bare_literal", &Lit{Val: 0.5}, false},
		{"col_ref", &ColRef{Name: "x"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isBinaryFloatCast(c.e); got != c.want {
				t.Errorf("isBinaryFloatCast(%v) = %v, want %v", c.e, got, c.want)
			}
		})
	}
}
