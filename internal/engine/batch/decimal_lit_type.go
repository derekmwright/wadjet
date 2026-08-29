package batch

// DecimalTextType reports the DECIMAL type a numeric LITERAL names — ADR-0024
// item 3's "a numeric literal's (p,s) is its spelling".
//
// PostgreSQL types an unadorned `12.75` as numeric, and the (p,s) a finite
// carrier needs for it is read off the digits the user actually wrote: 12.75 is
// DECIMAL(4,2), 2 is DECIMAL(1,0), 0.5 is DECIMAL(1,1). That is what makes
// `d * 2` a multiply by DECIMAL(1,0) — result scale 2 — rather than by the
// INT32 range's DECIMAL(10,0), which would declare eight integer digits nobody
// wrote. An integer COLUMN is the other rule and keeps its whole range
// (DecimalTypeOf), because a column's values are not one spelling.
//
// The exponent form is normalized first: `1.5e3` is 1500, DECIMAL(4,0), and
// `1.5e-3` is 0.0015, DECIMAL(5,4) — the same value written two ways gets the
// same type, which is what keeps `d + 1.5e3` and `d + 1500` from declaring
// different columns.
//
// ok=false for text that names no number, and for one whose scale or digit
// count is past what a DECIMAL can declare — a literal with 40 fraction digits
// has no fixed-point type here, and the caller must fall back to float rather
// than truncate it.
func DecimalTextType(s string) (DecimalType, bool) {
	neg, digits, exp, ok := decimalParts(s)
	_ = neg
	if !ok {
		return DecimalType{}, false
	}
	digits, exp = trimDecimalDigits(digits, exp)
	// decimalParts hands back the significant digits and the exponent of the
	// LAST one, so the scale is how far that digit sits below the point.
	scale := -exp
	if scale < 0 {
		// A positive exponent means trailing zeros the digit string does not
		// carry: 1.5e3 is "15" at exp 2, which is 1500 — four integer digits
		// and no fraction.
		return decLitType(len(digits)+exp, 0)
	}
	if scale > MaxDecimalScale {
		return DecimalType{}, false
	}
	// The precision must cover the whole number, and a value below 1 still
	// needs its scale's worth of digits: 0.0015 is DECIMAL(5,4), not (2,4).
	return decLitType(max(len(digits), scale), scale)
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
