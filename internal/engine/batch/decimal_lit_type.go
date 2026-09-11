package batch

import (
	"strconv"
	"strings"
)

// DecimalTextType derives a literal's (p,s) from its spelling (ADR-0024 item 3).
// Keep trailing zeros and normalize exponent notation before counting digits;
// never infer a literal from the full integer-column range (DecimalTypeOf).
// Invalid text, excess scale or excess digits returns ok=false, so callers
// fall back rather than truncate a value to fit a guessed DECIMAL type.
// See docs/internals/batch-decimal-literal-type.md for the design.
func DecimalTextType(s string) (DecimalType, bool) {
	digits, exp, ok := decimalLitParts(s)
	if !ok {
		return DecimalType{}, false
	}
	if exp >= 0 {
		// A non-negative exponent means trailing zeros the digit string does
		// not carry: 1.5e3 is "15" at exp 2, which is 1500 — four integer
		// digits and no fraction.
		return decLitType(len(digits)+exp, 0)
	}
	scale := -exp
	if scale > MaxDecimalScale {
		return DecimalType{}, false
	}
	// The precision must cover the whole number, and a value below 1 still
	// needs its scale's worth of fraction digits: 0.0015 is DECIMAL(4,4).
	return decLitType(max(len(digits), scale), scale)
}

// decimalLitParts splits numeric text into its SIGNIFICANT digits and the
// power of ten they must be multiplied by, keeping the trailing zeros the user
// wrote as digits rather than folding them into the exponent.
//
// It is deliberately not decimalParts: that one exists for the COMPARISON
// path, where a value's magnitude is all that matters and "1.50" and "1.5" are
// the same number, so it normalizes both away. Here the spelling IS the type.
func decimalLitParts(s string) (digits string, exp int, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", 0, false
	}
	switch s[0] {
	case '-', '+':
		s = s[1:]
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return "", 0, false
		}
		// A magnitude past what any DECIMAL declares is refused rather than
		// clamped: the caller keeps the float path instead of running on a
		// number with fewer digits than the user wrote.
		if e < -2*MaxDecimalPrecision || e > 2*MaxDecimalPrecision {
			return "", 0, false
		}
		exp = e
		s = s[:i]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if !allDigits(intPart) || !allDigits(fracPart) || intPart+fracPart == "" {
		return "", 0, false
	}
	// LEADING zeros carry no information and no scale, so they go; trailing
	// ones stay, because they are fraction digits the user asked for.
	digits = strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		// The value is zero. Its scale is still what was written — `0.00` is
		// DECIMAL(2,2) — so keep one digit and let the exponent carry it.
		digits = "0"
	}
	return digits, exp - len(fracPart), true
}

// decLitType finishes a literal's type, refusing rather than clamping what a
// DECIMAL cannot declare. Clamping would keep the query running on a number
// with fewer digits than the user wrote, which is the silent class ADR-0024
// exists to close.
func decLitType(p, s int) (DecimalType, bool) {
	if p < 1 {
		p = 1
	}
	if p > MaxDecimalPrecision || s > MaxDecimalScale || s > p {
		return DecimalType{}, false
	}
	return DecimalType{Precision: p, Scale: s}, true
}

// allDigits reports whether s is entirely ASCII digits. An empty string
// counts (an absent fraction is not a defect); the caller rejects the case
// where BOTH halves are empty. The numeric grammar itself lives in
// parquet.DecimalTextParts since #647; this helper only classifies a
// literal's SPELLING for its (p,s).
func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
