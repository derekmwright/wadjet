// SPDX-License-Identifier: MIT

package expr

import "testing"

// REAL AND DOUBLE PRECISION READ TEXT AS float4in / float8in DO (#1411
// measured case B3).
//
// strconv.ParseFloat is Go's literal grammar: it takes a `_` digit
// separator, and it rounds a value below the type's smallest subnormal to
// zero with no error. So `CAST('1_0' AS REAL)` answered 10 and
// `CAST('1e-46' AS REAL)` 0, and TABLESAMPLE BERNOULLI ('1e-46') — whose
// untyped argument is read by this function — sampled 0 % where PostgreSQL
// 17.11 raises 22003. Every want is PostgreSQL's, measured live.
func TestCastTextToFloatTakesPostgresGrammar(t *testing.T) {
	b := castRefusalBatch(t)
	for _, c := range []struct {
		dest, in   string
		state, msg string
	}{
		{"real", "1_0", "22P02", `invalid input syntax for type real: "1_0"`},
		{"real", "1e1_0", "22P02", `invalid input syntax for type real: "1e1_0"`},
		{"real", "1__0", "22P02", `invalid input syntax for type real: "1__0"`},
		{"double precision", "1_0", "22P02", `invalid input syntax for type double precision: "1_0"`},
		{"float(10)", "1_0", "22P02", `invalid input syntax for type real: "1_0"`},
		{"real", "1e-46", "22003", `"1e-46" is out of range for type real`},
		{"real", "-1e-46", "22003", `"-1e-46" is out of range for type real`},
		{"real", " 1e-46 ", "22003", `"1e-46" is out of range for type real`},
		{"real", " 1e400 ", "22003", `"1e400" is out of range for type real`},
		{"double precision", "1e-400", "22003", `"1e-400" is out of range for type double precision`},
		// a hexadecimal mantissa's digits include letters, and `e` is one
		{"real", "0xAp-2000", "22003", `"0xAp-2000" is out of range for type real`},
		{"real", "-0x1p-200", "22003", `"-0x1p-200" is out of range for type real`},
		{"real", "0xep-2000", "22003", `"0xep-2000" is out of range for type real`},
		{"double precision", "0xAp-2000", "22003", `"0xAp-2000" is out of range for type double precision`},
	} {
		state, msg := recoverFatalEvalForTest(t, func() {
			(&Cast{Operand: &Lit{Val: c.in}, DestType: c.dest}).Eval(b, 0)
		})
		if state != c.state || msg != c.msg {
			t.Errorf("CAST(%q AS %s) raised [%s] %s, want [%s] %s (PostgreSQL 17.11)",
				c.in, c.dest, state, msg, c.state, c.msg)
		}
	}
	// A subnormal is a value, and a zero spelled any way is zero.
	for _, c := range []struct {
		dest, in string
		zero     bool
	}{
		{"real", "1e-45", false},
		{"double precision", "1e-320", false},
		{"real", "0.000e-999", true},
		{"double precision", "0e-500", true},
		{"real", "-0", true},
		{"real", "0x0p-2000", true},
		{"real", "0x1p-149", false},
		{"real", "0x1.8p1", false},
	} {
		got := (&Cast{Operand: &Lit{Val: c.in}, DestType: c.dest}).Eval(b, 0)
		f, ok := got.(float64)
		if !ok {
			if f32, ok32 := got.(float32); ok32 {
				f, ok = float64(f32), true
			}
		}
		if !ok || (f == 0) != c.zero {
			t.Errorf("CAST(%q AS %s) = %v; PostgreSQL answers a %s", c.in, c.dest, got,
				map[bool]string{true: "zero", false: "nonzero subnormal"}[c.zero])
		}
	}
	// numeric takes `_` since PostgreSQL 16; this function reads numeric
	// text only as a fallback and keeps that.
	if got := (&Cast{Operand: &Lit{Val: "1_000"}, DestType: "numeric"}).Eval(b, 0); got == nil {
		t.Errorf("CAST('1_000' AS NUMERIC) = NULL; PostgreSQL answers 1000")
	}
}
