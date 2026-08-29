package batch

import (
	"strconv"
	"strings"
)

// DecimalTextType reports the DECIMAL type a numeric LITERAL names — ADR-0024
// item 3's "a numeric literal's (p,s) is its spelling".
//
// PostgreSQL types an unadorned `12.75` as numeric, and the (p,s) a finite
// carrier needs for it is read off the digits the user actually WROTE: 12.75
// is DECIMAL(4,2), 2 is DECIMAL(1,0), 0.5 is DECIMAL(1,1). That is what makes
// `d * 2` a multiply by DECIMAL(1,0) — result scale 2 — rather than by the
// INT32 range's DECIMAL(10,0), which would declare eight integer digits nobody
// wrote. An integer COLUMN is the other rule and keeps its whole range
// (DecimalTypeOf), because a column's values are not one spelling.
//
// TRAILING ZEROS ARE KEPT, and that is the whole reason this reads the text
// itself rather than going through decimalParts: `100.0` is DECIMAL(4,1), not
// (3,0). PostgreSQL's numeric carries a per-value dscale that the zeros are
// part of — `12.75 * 100.0` renders 1275.000, three fraction digits, because
// the literal contributed one — and folding them away made the product's
// declared scale 2 where PostgreSQL's is 3. They cost nothing and they are
// what the user wrote.
//
// The exponent form is normalized first: `1.5e3` is 1500, DECIMAL(4,0), and
// `1.5e-3` is 0.0015, DECIMAL(4,4) — the same value written two ways gets the
// same type, which is what keeps `d + 1.5e3` and `d + 1500` from declaring
// different columns.
//
// ok=false for text that names no number, and for one whose scale or digit
// count is past what a DECIMAL can declare — a literal with 40 fraction digits
// has no fixed-point type here, and the caller must fall back rather than
// truncate it.
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
