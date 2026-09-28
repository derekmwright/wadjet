// SPDX-License-Identifier: MIT

package batch

import (
	"strings"
	"testing"
)

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
		// TRAILING ZEROS ARE KEPT: they are fraction digits the user wrote,
		// and PostgreSQL's numeric carries them in its per-value dscale.
		// `12.75 * 100.0` renders 1275.000 there — three fraction digits,
		// because the literal contributed one — and folding them away made the
		// product's declared scale 2 where PostgreSQL's is 3 (#555 review, R2).
		{"12.750", DecimalType{Precision: 5, Scale: 3}, true},
		{"1.00", DecimalType{Precision: 3, Scale: 2}, true},
		{"100.0", DecimalType{Precision: 4, Scale: 1}, true},
		{"0.00", DecimalType{Precision: 2, Scale: 2}, true},
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

// TestDecimalValueTypeIsTheValuesOwnSpelling: a literal compared by VALUE
// (a membership's outer operand, #1372) is typed by DecimalTextType of the
// value's shortest spelling when its written spelling is wider than a
// DECIMAL only by zeros; a value no DECIMAL(38,s) holds is refused.
func TestDecimalValueTypeIsTheValuesOwnSpelling(t *testing.T) {
	z := strings.Repeat
	for _, tc := range []struct {
		text string
		want DecimalType
		ok   bool
	}{
		{"12.5", DecimalType{Precision: 3, Scale: 1}, true},
		{"12.50", DecimalType{Precision: 4, Scale: 2}, true}, // the spelling, when it fits
		{"12.5" + z("0", 40), DecimalType{Precision: 3, Scale: 1}, true},
		{z("0", 40) + "12.5", DecimalType{Precision: 3, Scale: 1}, true},
		{"14" + z("0", 40) + "e-40", DecimalType{Precision: 2}, true},
		{"0." + z("0", 45), DecimalType{Precision: 1}, true},
		{"1.25000000000000001e13", DecimalType{Precision: 18, Scale: 4}, true},
		{"14." + z("0", 39) + "1", DecimalType{}, false},
		{"0." + z("0", 38) + "1", DecimalType{}, false},
		{"1e40", DecimalType{}, false},
		{"NaN", DecimalType{}, false},
		{"zz", DecimalType{}, false},
	} {
		got, ok := DecimalValueType(tc.text)
		if ok != tc.ok || got != tc.want {
			t.Errorf("DecimalValueType(%q) = (%+v, %v), want (%+v, %v)", tc.text, got, ok, tc.want, tc.ok)
		}
	}
}

// TestDecimalTextRoundedAtRoundsOnceFromTheDigits: the value at a target
// scale, rounded half away from zero by the first digit past it, whatever
// width the text was written in — PostgreSQL's numeric(p,s) input.
func TestDecimalTextRoundedAtRoundsOnceFromTheDigits(t *testing.T) {
	z := strings.Repeat
	for _, tc := range []struct {
		text      string
		scale     int
		want      string
		sat, isOK bool
	}{
		{"12.755", 2, "12.76", false, true},
		{"-12.755", 2, "-12.76", false, true},
		{"12.754999", 2, "12.75", false, true},
		{"14." + z("0", 39) + "1", 4, "14.0000", false, true},
		{"14." + z("0", 4) + "5" + z("0", 40), 4, "14.0001", false, true},
		{"9.99995", 4, "10.0000", false, true},
		{"1e-40", 4, "0.0000", false, true},
		{"0.00005", 4, "0.0001", false, true},
		{"0.00004", 4, "0.0000", false, true},
		{"1e40", 0, "", true, true},
		{"1e1000", 4, "", true, true},
		{"0e1000", 4, "0.0000", false, true},
		{"zz", 2, "", false, false},
		{"NaN", 2, "", false, false},
	} {
		v, sat, ok := DecimalTextRoundedAt(tc.text, tc.scale)
		if ok != tc.isOK || sat != tc.sat {
			t.Errorf("DecimalTextRoundedAt(%q, %d): sat %v ok %v, want sat %v ok %v", tc.text, tc.scale, sat, ok, tc.sat, tc.isOK)
			continue
		}
		if ok && !sat {
			if got := v.FormatDecimal(tc.scale); got != tc.want {
				t.Errorf("DecimalTextRoundedAt(%q, %d) = %s, want %s", tc.text, tc.scale, got, tc.want)
			}
		}
	}
}
