package batch

// The DECIMAL RESULT-TYPE rules of ADR-0024, in one place.
//
// Before this file the same question was answered five times over, each time
// from scratch: the grouped aggregate (physical.aggSpecOutputDecimal), the
// set operation's DAG path (physical.setOpDecimalTarget), the set operation's
// single-process path (physical.unifySetOpSchemas, with a DIFFERENT rule —
// max(precision) rather than a rebuilt integer part, so the two paths declared
// different types for the same UNION), the window, and the cast. ADR-0024
// items 2 and 3 settle one table of rules, and it has two halves: the
// COMMON-TYPE rule for the constructs that CHOOSE BETWEEN their operands
// (item 2) is here, and the ARITHMETIC rule for the ones that compute a new
// number from them (item 3) is DecimalResultType / AdjustDecimalPrecision
// Scale in decimal_arith.go, beside the kernels that execute it. Neither rule
// is written twice.
//
// Everything here is a function of the INPUT TYPES alone. No value is
// consulted, so the same query over more rows can never change the type of
// its own output column.

// Int32DecimalDigits and Int64DecimalDigits are how many decimal digits an
// integer type's whole range needs, which is what an integer operand
// contributes to a common DECIMAL type: INT32 spans 10 digits, INT64 spans 19
// (ADR-0024 item 2 — "an integer is DECIMAL(10,0) / (19,0)").
const (
	Int32DecimalDigits = 10
	Int64DecimalDigits = 19
)

// DecimalType is a DECIMAL's declared (precision, scale) — the two facts a
// bare TypeID cannot express. It is the batch-level twin of
// logical.DecimalMeta and parquet.Column's Precision/Scale pair; the planner
// converts at its own boundary rather than this package importing either.
type DecimalType struct {
	Precision int
	Scale     int
}

// DecimalTypeOf returns the DecimalType an operand of type t contributes to a
// result-type computation, and whether t participates at all. DECIMAL brings
// its own declaration (handed in by the caller, which is the only holder of
// it); the integer types bring their whole range at scale 0.
func DecimalTypeOf(t TypeID, dec DecimalType) (DecimalType, bool) {
	switch t {
	case TypeDecimal:
		return dec, true
	case TypeInt32:
		return DecimalType{Precision: Int32DecimalDigits}, true
	case TypeInt64:
		return DecimalType{Precision: Int64DecimalDigits}, true
	}
	return DecimalType{}, false
}

// DecimalCommon uses max(scale) and max(precision-scale)+scale, capped at 38
// precision while retaining scale (ADR-0024 item 2; #533, ADR-0018 §4).
// Apply to set arms, CASE and COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST.
// Never apply arithmetic's p>38 fractional adjustment to a choice: it would
// drop digits already stored in an operand (ADR-0024 items 3, 7).
// The precision cap reduces range; out-of-type values require per-value 22003,
// not wrapping or plan-time rejection of fitting rows (ADR-0024 item 4; #552).
// Unknown (p,s) or nonnumeric operands decline; callers must not guess DECIMAL.
// See docs/internals/batch-decimal-choice-common-type.md for the design.
func DecimalCommon(in []DecimalType) (DecimalType, bool) {
	if len(in) == 0 {
		return DecimalType{}, false
	}
	scale, intDigits := 0, 0
	for _, m := range in {
		if m.Precision <= 0 {
			// Precision 0 is the codebase's "unconstrained" sentinel for a
			// DECIMAL nothing could type (#458). Taken at face value it
			// would widen every operand to scale 0 and truncate all of them.
			return DecimalType{}, false
		}
		if m.Scale > scale {
			scale = m.Scale
		}
		if d := m.Precision - m.Scale; d > intDigits {
			intDigits = d
		}
	}
	prec := intDigits + scale
	if prec > MaxDecimalPrecision {
		prec = MaxDecimalPrecision
	}
	if prec < 1 {
		prec = 1
	}
	return DecimalType{Precision: prec, Scale: scale}, true
}

// DecimalScalarOp names a scalar math function whose DECIMAL result type this
// package decides. They are the functions that answer a number IN THE SAME
// DOMAIN as their argument — PostgreSQL's abs/ceil/floor/round/trunc/sign over
// a numeric all return numeric — as opposed to the transcendental ones
// (sqrt/exp/ln/power/log), which PostgreSQL also answers in numeric and which
// wadjet deliberately keeps in float64: an exact fixed-point tower is what
// those need, and ADR-0012 item 9 already records that class of divergence for
// STDDEV and friends.
type DecimalScalarOp uint8

const (
	// DecimalScalarAbs keeps the argument's type exactly: |v| is a value the
	// same column holds.
	DecimalScalarAbs DecimalScalarOp = iota
	// DecimalScalarCeil and DecimalScalarFloor drop the fraction and may
	// CARRY into a new integer digit — ceil(9.9) is 10 — so the integer part
	// grows by one.
	DecimalScalarCeil
	DecimalScalarFloor
	// DecimalScalarRound rounds half away from zero to `digits` fraction
	// digits and can carry, like ceil.
	DecimalScalarRound
	// DecimalScalarTrunc cuts at `digits` fraction digits and cannot carry.
	DecimalScalarTrunc
	// DecimalScalarSign answers -1, 0 or 1 whatever the argument's width.
	DecimalScalarSign
)

// DecimalScalarType returns the (precision, scale) of a scalar math function's
// DECIMAL result, per ADR-0024 items 2 and 3.
//
// digits is the SECOND argument of round(x, n) / trunc(x, n) and is ignored by
// the one-argument ops. PostgreSQL's one-argument round and trunc are the
// two-argument ones at n = 0, so a caller with no second argument passes 0 and
// gets the same answer PostgreSQL gives (`round(12.75::numeric)` is 13).
//
// A NEGATIVE n rounds to a power of ten ABOVE the point — PostgreSQL's
// `round(1234.56, -2)` is 1200 and `round(1250, -2)` is 1300, half away from
// zero like every other numeric rounding here — and the result has no fraction
// at all, so the scale is 0. It is not a range reduction: 1200 still needs its
// four integer digits.
//
// ok=false when the input carries no usable declaration (precision 0 is the
// codebase's "unconstrained" sentinel, #458), and then the caller must decline
// to declare a DECIMAL rather than guess one — the same clause DecimalCommon
// has, for the same reason.
func DecimalScalarType(op DecimalScalarOp, in DecimalType, digits int) (DecimalType, bool) {
	if in.Precision <= 0 {
		return DecimalType{}, false
	}
	p, s := normalizeDecimalPS(in.Precision, in.Scale)
	intDigits := p - s
	switch op {
	case DecimalScalarAbs:
		return DecimalType{Precision: p, Scale: s}, true
	case DecimalScalarSign:
		// -1, 0, 1: one digit, no fraction, whatever the argument was.
		return DecimalType{Precision: 1}, true
	case DecimalScalarCeil, DecimalScalarFloor:
		// They drop the fraction and may CARRY — ceil(9.9) is 10 — so the
		// integer part grows by one.
		return decScalarType(intDigits+1, 0), true
	case DecimalScalarRound:
		return decScalarType(intDigits+roundCarry(digits, s), max(digits, 0)), true
	case DecimalScalarTrunc:
		// Truncation cannot carry, so the integer part never grows.
		return decScalarType(intDigits, max(digits, 0)), true
	}
	return DecimalType{}, false
}

// roundCarry is the extra integer digit a ROUND may need: one when it actually
// drops fraction digits, because rounding up can carry (9.9 to one fewer digit
// is 10), and none when the requested scale already keeps everything the value
// has. A NEGATIVE digits always rounds to a power of ten above the point, so
// it always can.
func roundCarry(digits, inScale int) int {
	if digits >= inScale {
		return 0
	}
	return 1
}

// decScalarType builds a scalar result's type from its INTEGER digits and the
// fraction digits asked for, giving up FRACTION digits when the two do not fit
// the carrier.
//
// The direction is the opposite of AdjustDecimalPrecisionScale's and it is
// deliberate. There the scale is a computed by-product of an arithmetic rule
// and the integer part is what the value needs, so the rule spends fraction
// digits down to a floor and only then narrows the range. Here the integer
// part is what the ARGUMENT already holds — dropping one would change the
// value outright — and the scale is a REQUEST, so the request is what yields.
//
// Applying the arithmetic rule here instead is what `round(d_wide + d_4, 9)`
// caught: a (38,9) argument has 29 integer digits, the +1 carry makes 30, and
// the fraction floor of min(9,6) took the scale to 8 — one digit fewer than
// the caller asked for and than PostgreSQL keeps, on a round that drops
// nothing at all (#555 review, S1).
func decScalarType(intDigits, s int) DecimalType {
	if intDigits < 1 {
		intDigits = 1
	}
	if intDigits > MaxDecimalPrecision {
		intDigits = MaxDecimalPrecision
	}
	if s < 0 {
		s = 0
	}
	if room := MaxDecimalPrecision - intDigits; s > room {
		s = room
	}
	if s > MaxDecimalScale {
		s = MaxDecimalScale
	}
	return DecimalType{Precision: intDigits + s, Scale: s}
}
