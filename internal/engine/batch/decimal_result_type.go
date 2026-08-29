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

// DecimalCommon is the COMMON DECIMAL type of a set of operands (ADR-0024
// item 2): the type every one of them can be moved into without dropping a
// digit it holds.
//
//	scale     = max over the operands
//	precision = max over the operands of (precision - scale), plus that scale
//
// The scale is the maximum because that is the only choice that moves no
// value: a narrower one would DROP digits a wider operand holds, which is the
// truncating half of #533. Precision is then reconstructed from the widest
// INTEGER part rather than taken as max(precision), because max(precision) is
// not a bound on the widened values — DECIMAL(18,2) alongside DECIMAL(9,4)
// needs 16 integer digits at scale 4, i.e. 20, where max(precision) would
// declare 18 and hand the parquet writer a leaf too small for the value
// (ADR-0018 §4's encoding rule keys off precision).
//
// This is the rule for every construct that CHOOSES BETWEEN its operands
// rather than computing a new number from them: a set operation's arms,
// CASE's branches, COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST's arguments.
//
// The result is capped at the carrier's full width — 38 digits is what an
// Int128 holds. The cap reduces the PRECISION and leaves the scale, so it is
// a RANGE reduction, which is why a value with no Int128 at the output type
// is then an ERROR at the moment of coercion rather than a wrapped number
// (ADR-0024 items 4 and 7; #552 records the cost).
//
// ok=false means an operand contributed nothing this rule can use — a
// computed DECIMAL whose (p,s) nobody resolved, or a non-numeric type. The
// caller must then decline to declare a DECIMAL at all rather than guess one.
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
