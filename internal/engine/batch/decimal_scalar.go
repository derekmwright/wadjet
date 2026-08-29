package batch

// Exact execution of the scalar math functions that answer in their argument's
// own domain — abs/ceil/floor/round/trunc/sign over a DECIMAL.
//
// This is the execution half of DecimalScalarType, which decides the (p,s) the
// answer is declared at; the two must agree, because the declaration sizes the
// output vector and this produces the digits that go into it.
//
// Rounding is half AWAY FROM ZERO wherever it happens, PostgreSQL's numeric
// rounding: round(2.5) is 3 and round(-2.5) is -3. CEIL and FLOOR are not
// roundings at all — they move to the next integer in a FIXED direction, which
// Rescale cannot express — so they read the remainder of an exact QuoRem and
// step by one when it is non-zero on their own side.

// DecimalScalar executes op over one unscaled carrier at inScale and returns
// the exact result at the declared DECIMAL(p, s).
//
// digits is round/trunc's second argument and is ignored by the other ops.
// PostgreSQL's one-argument round and trunc are these at digits = 0.
//
// A NEGATIVE digits rounds or truncates to a power of ten ABOVE the point:
// `round(1234.56, -2)` is 1200 and `round(1250, -2)` is 1300. It is done in ONE
// rescale, by reading the value at the wider scale inScale-digits so that
// rescaling to 0 lands exactly on the power of ten being rounded to. Rounding
// to scale 0 first and adjusting afterwards would round TWICE and turn 1249
// into 1300.
//
// The status is DecimalOverflow when the answer has no Int128 or no place in
// DECIMAL(p,s) — ADR-0024 item 4's bound, the same one the ...At arithmetic
// wrappers apply — and never a saturated or wrapped value.
func DecimalScalar(op DecimalScalarOp, v Int128, inScale, digits, p, s int) (Int128, DecimalStatus) {
	if inScale < 0 || s < 0 {
		return Int128{}, DecimalInvalidScale
	}
	switch op {
	case DecimalScalarAbs:
		mag, ok := absInt128(v)
		if !ok {
			// -2^127 has no Int128 magnitude, so |v| is outside every
			// declaration the carrier can express.
			return Int128{}, DecimalOverflow
		}
		out, ok := Rescale(mag, inScale, s)
		return decimalAtPrecision(out, statusOf(ok), p)
	case DecimalScalarSign:
		sign := Int128{}
		switch {
		case v.IsNegative():
			sign = Int128From(-1)
		case !v.IsZero():
			sign = Int128From(1)
		}
		out, ok := Rescale(sign, 0, s)
		return decimalAtPrecision(out, statusOf(ok), p)
	case DecimalScalarCeil:
		out, ok := decCeilFloor(v, inScale, s, true)
		return decimalAtPrecision(out, statusOf(ok), p)
	case DecimalScalarFloor:
		out, ok := decCeilFloor(v, inScale, s, false)
		return decimalAtPrecision(out, statusOf(ok), p)
	case DecimalScalarRound:
		out, ok := decRoundTrunc(v, inScale, digits, s, true)
		return decimalAtPrecision(out, statusOf(ok), p)
	case DecimalScalarTrunc:
		out, ok := decRoundTrunc(v, inScale, digits, s, false)
		return decimalAtPrecision(out, statusOf(ok), p)
	}
	return Int128{}, DecimalInvalidScale
}

// statusOf lifts a fits-the-carrier bool into the public status. The only
// failure these functions have is the carrier's range: a zero divisor cannot
// arise (every divisor here is a power of ten) and the scales are validated up
// front.
func statusOf(ok bool) DecimalStatus {
	if ok {
		return DecimalOK
	}
	return DecimalOverflow
}

// decCeilFloor moves v to the next integer in ONE direction — up for ceil,
// down for floor — and then out to outScale.
func decCeilFloor(v Int128, inScale, outScale int, up bool) (Int128, bool) {
	if inScale == 0 {
		return Rescale(v, 0, outScale)
	}
	div, ok := DecimalPow10(inScale)
	if !ok {
		// A scale past 38 leaves no integer digits at all: every value the
		// carrier holds is strictly inside (-1, 1), so ceil is 1 for a
		// positive value, floor is -1 for a negative one, and both are 0
		// otherwise.
		switch {
		case v.IsZero():
			return Int128{}, true
		case up && !v.IsNegative():
			return Rescale(Int128From(1), 0, outScale)
		case !up && v.IsNegative():
			return Rescale(Int128From(-1), 0, outScale)
		}
		return Int128{}, true
	}
	q, r, ok := v.QuoRem(div)
	if !ok {
		return Int128{}, false
	}
	if !r.IsZero() {
		// QuoRem truncates toward zero and the remainder takes the dividend's
		// sign, so a step is needed only on the side that truncation moved
		// away from: ceil of a positive value, floor of a negative one.
		step := Int128{}
		if up && !r.IsNegative() {
			step = Int128From(1)
		} else if !up && r.IsNegative() {
			step = Int128From(-1)
		}
		if !step.IsZero() {
			if q, ok = q.AddChecked(step); !ok {
				return Int128{}, false
			}
		}
	}
	return Rescale(q, 0, outScale)
}

// decRoundTrunc keeps `digits` fraction digits — rounding half away from zero,
// or cutting toward zero — and lands the answer at outScale.
func decRoundTrunc(v Int128, inScale, digits, outScale int, round bool) (Int128, bool) {
	if digits < 0 {
		wide := inScale - digits
		if wide > MaxDecimalPrecision {
			// The power of ten being rounded to is past 10^38, which no
			// Int128 magnitude reaches, so every value goes to zero.
			return Int128{}, true
		}
		var kept Int128
		var ok bool
		if round {
			kept, ok = Rescale(v, wide, 0)
		} else {
			kept, ok = decTruncate(v, wide)
		}
		if !ok {
			return Int128{}, false
		}
		shifted, ok := kept.MulPow10(-digits)
		if !ok {
			return Int128{}, false
		}
		return Rescale(shifted, 0, outScale)
	}
	if digits >= inScale {
		// Nothing is dropped: the value only widens, which is exact.
		return Rescale(v, inScale, outScale)
	}
	var kept Int128
	var ok bool
	if round {
		kept, ok = Rescale(v, inScale, digits)
	} else {
		kept, ok = decTruncate(v, inScale-digits)
	}
	if !ok {
		return Int128{}, false
	}
	return Rescale(kept, digits, outScale)
}

// decTruncate divides an unscaled value by 10^drop and discards the remainder
// — cutting TOWARD ZERO, which is what TRUNC means and what separates it from
// Rescale's rounding.
func decTruncate(v Int128, drop int) (Int128, bool) {
	if drop <= 0 {
		return v, true
	}
	if drop > MaxDecimalPrecision {
		// The divisor is 10^39 or wider and no Int128 magnitude reaches it,
		// so every value cuts to zero.
		return Int128{}, true
	}
	q, _, ok := v.QuoRem(pow10Int128[drop])
	return q, ok
}
