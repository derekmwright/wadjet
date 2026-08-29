package kernel

import "testing"

// TestRealOverflowTextMatchesPostgresNumericOutput pins the 22003 message's
// DIGITS. PostgreSQL's `real IN (1e40, 3.1)` fails casting the array to real[],
// and the cast that fails is numeric->real — so the message names the numeric's
// own text, which is expanded and never in exponent form, whatever the query
// spelled. Every want below is `<literal>::numeric::text` on postgres:17.
func TestRealOverflowTextMatchesPostgresNumericOutput(t *testing.T) {
	cases := []struct{ in, want string }{
		{"1e40", "10000000000000000000000000000000000000000"},
		{"1e+40", "10000000000000000000000000000000000000000"},
		{"1E40", "10000000000000000000000000000000000000000"},
		{"-1e40", "-10000000000000000000000000000000000000000"},
		{"1.5e40", "15000000000000000000000000000000000000000"},
		// A trailing zero in the mantissa is absorbed by the exponent shift,
		// exactly as PostgreSQL's numeric absorbs it into its dscale.
		{"1.50e40", "15000000000000000000000000000000000000000"},
		{"4e38", "400000000000000000000000000000000000000"},
		{"10000000000000000000000000000000000000000",
			"10000000000000000000000000000000000000000"},
		// Not overflow shapes, but the renderer must not mangle them: it is
		// the same function every 22003 for this type goes through.
		{"12.75", "12.75"},
		{"12.750", "12.750"},
		{"0.5", "0.5"},
		{"1e-3", "0.001"},
		{"1.5e-3", "0.0015"},
		{"-0.25", "-0.25"},
		{"007", "7"},
		// Not a number at all: rendered verbatim rather than guessed at.
		{"abc", "abc"},
		{"", ""},
	}
	for _, c := range cases {
		if got := RealOverflowText(c.in); got != c.want {
			t.Errorf("RealOverflowText(%q) = %q, want %q (PostgreSQL numeric output)", c.in, got, c.want)
		}
	}
}

// TestRealLitTextOverflow separates the three answers the 22003 rule needs: a
// finite literal past real's range (raise), a literal that names infinity (a
// legal real, no raise), and text that is not a number (nothing to raise
// about — a different refusal owns that).
func TestRealLitTextOverflow(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"1e40", true},
		{"-1e40", true},
		{"1e39", true},
		// Past float64's own range: still a finite number the user wrote, and
		// still not a real. Reading the digits is what keeps this apart from
		// an infinity — the float64 box for it IS +Inf.
		{"1e400", true},
		{"3.5e38", true},
		// Inside real's range (FLT_MAX is about 3.4028235e38).
		{"3.4e38", false},
		{"3.4028234e38", false},
		{"0", false},
		{"3.1", false},
		{"-3.4e38", false},
		// Denormal: representable, and the underflow question is the CAST's,
		// not the array literal's.
		{"1e-45", false},
		{"1e-50", false},
		// Infinity IS a real value; PostgreSQL accepts 'Infinity'::real.
		{"Infinity", false},
		{"-Infinity", false},
		{"inf", false},
		// Not a number.
		{"abc", false},
		{"", false},
	}
	for _, c := range cases {
		if got := RealLitTextOverflow(c.in); got != c.want {
			t.Errorf("RealLitTextOverflow(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
