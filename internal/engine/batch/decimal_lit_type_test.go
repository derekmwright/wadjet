package batch

import "testing"

// TestDecimalTextTypeIsTheLiteralsSpelling is ADR-0024 item 3's rule for a
// numeric literal: its (p,s) is what the user WROTE, not the range of a type
// it might fit.
//
// It is what makes `d * 2` a multiply by DECIMAL(1,0) — so the product keeps
// the column's own scale — where taking 2 as the INT32 range's DECIMAL(10,0)
// would declare eight integer digits nobody asked for. An integer COLUMN is
// the other rule (DecimalTypeOf) and does bring its whole range, because a
// column's values are not one spelling.
func TestDecimalTextTypeIsTheLiteralsSpelling(t *testing.T) {
	for _, tc := range []struct {
		text string
		want DecimalType
		ok   bool
	}{
		{"0", DecimalType{Precision: 1}, true},
		{"2", DecimalType{Precision: 1}, true},
		{"-2", DecimalType{Precision: 1}, true},
		{"100", DecimalType{Precision: 3}, true},
		{"12.75", DecimalType{Precision: 4, Scale: 2}, true},
		{"-12.75", DecimalType{Precision: 4, Scale: 2}, true},
		{"0.5", DecimalType{Precision: 1, Scale: 1}, true},
		// A value below 1 still needs its scale's worth of fraction digits,
		// and no more: 0.0015 is numeric(4,4), whose bound is |v| < 1.
		{"0.0015", DecimalType{Precision: 4, Scale: 4}, true},
		// Trailing zeros change no value, so they change no type. PostgreSQL
		// keeps them in its per-value dscale; a wadjet DECIMAL column has one
		// declared scale and the oracle compares canonical digits, so this is
		// the same number either way (ADR-0012 item 12's recorded class).
		{"12.750", DecimalType{Precision: 4, Scale: 2}, true},
		{"1.00", DecimalType{Precision: 1}, true},
		// The exponent form normalizes first, so the same value written two
		// ways declares one type.
		{"1.5e3", DecimalType{Precision: 4}, true},
		{"1500", DecimalType{Precision: 4}, true},
		{"1.5e-3", DecimalType{Precision: 4, Scale: 4}, true},
		{"0.0015", DecimalType{Precision: 4, Scale: 4}, true},
		// The full carrier width.
		{"12345678901234567890123456789012345678", DecimalType{Precision: 38}, true},

		// Past what a DECIMAL can declare: REFUSED rather than clamped, so
		// the caller keeps the float path instead of silently truncating
		// digits the user wrote.
		{"1e39", DecimalType{}, false},
		{"0.000000000000000000000000000000000000000001", DecimalType{}, false},
		{"abc", DecimalType{}, false},
		{"", DecimalType{}, false},
		{"1.2.3", DecimalType{}, false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			got, ok := DecimalTextType(tc.text)
			if ok != tc.ok || got != tc.want {
				t.Errorf("DecimalTextType(%q) = (%+v, %v), want (%+v, %v)",
					tc.text, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestDecimalTextTypeHoldsItsOwnValue is the invariant that makes the type
// usable: a literal must have an exact carrier AT the type this function
// names, or an operand resolved through it would lose digits before any
// arithmetic ran.
func TestDecimalTextTypeHoldsItsOwnValue(t *testing.T) {
	for _, text := range []string{
		"0", "2", "-2", "100", "12.75", "-12.75", "0.5", "0.0015", "1.5e3",
		"1.5e-3", "12345678901234567890123456789012345678",
	} {
		typ, ok := DecimalTextType(text)
		if !ok {
			t.Fatalf("DecimalTextType(%q) declined", text)
		}
		d, ok := DecimalTextAt(text, typ.Scale)
		if !ok || d.Residual != 0 || d.Sat != 0 {
			t.Errorf("%q has no exact carrier at its own scale %d (ok=%v residual=%d sat=%d)",
				text, typ.Scale, ok, d.Residual, d.Sat)
		}
		if !DecimalFitsPrecision(d.Unscaled, typ.Precision) {
			t.Errorf("%q does not fit its own precision %d", text, typ.Precision)
		}
	}
}
