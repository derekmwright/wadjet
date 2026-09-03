package expr

import (
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// CAST(x AS DECIMAL(p,s)) / NUMERIC(p,s) / bare DECIMAL / ::numeric, done
// EXACTLY — ADR-0024 items 3 and 4, #555's cast half.
//
// Before this the evaluator had two answers and neither was a DECIMAL:
// `"decimal", "numeric"` returned ToFloat64(v), and `DECIMAL(10,2)` matched no
// case at all and fell to `default: return v` — the value passed through
// unchanged, the (p,s) silently ignored, no rounding and no rescale, so
// `CAST(numeric(18,4) '12.7501' AS DECIMAL(9,2))` answered 12.7501 where
// PostgreSQL answers 12.75. The declared type followed: `inferCastType` had
// NUMERIC/DECIMAL in its float8 arm, and `"DECIMAL(10,2)"` in none of them, so
// the projection allocated a STRING column.
//
// The conversion goes through the value's exact DECIMAL TEXT and one rescale,
// never through a float64. Rounding is half away from zero, PostgreSQL's
// numeric rounding, and it happens exactly once — the target scale is the only
// place any digit is dropped.

// decimalDest is a parsed DECIMAL cast destination.
type decimalDest struct {
	// params is false for a bare DECIMAL/NUMERIC, which takes the OPERAND's
	// own (p,s) — ADR-0024 item 3's "CAST(x AS DECIMAL): the operand's own
	// (p,s); (38,0) from an integer".
	params bool
	typ    batch.DecimalType
	// overWidth marks a (p,s) PostgreSQL accepts and an Int128 cannot hold —
	// numeric allows precision to 1000, the carrier stops at 38. The cast is
	// REFUSED rather than answered at a narrower type, ADR-0024 item 4's rule
	// applied to the declaration instead of to a value.
	overWidth bool
}

// parseDecimalDest reads a CAST destination type name. ok=false for a
// destination that is not DECIMAL/NUMERIC at all.
//
// The name arrives as the parser produced it — `decimal(10, 2)`, `NUMERIC(9,2)`,
// `numeric` — so the whitespace and case normalization is here rather than at
// the call site, which runs per row.
func parseDecimalDest(dest string) (decimalDest, bool) {
	d := strings.TrimSpace(strings.ToLower(dest))
	name, args, hasArgs := strings.Cut(d, "(")
	switch strings.TrimSpace(name) {
	case "decimal", "numeric", "dec":
	default:
		return decimalDest{}, false
	}
	if !hasArgs {
		return decimalDest{}, true // bare: the operand decides
	}
	args = strings.TrimSuffix(strings.TrimSpace(args), ")")
	pText, sText, hasScale := strings.Cut(args, ",")
	p, err := strconv.Atoi(strings.TrimSpace(pText))
	if err != nil {
		return decimalDest{}, false
	}
	s := 0
	if hasScale {
		if s, err = strconv.Atoi(strings.TrimSpace(sText)); err != nil {
			return decimalDest{}, false
		}
	}
	if p < 1 || p > batch.MaxDecimalPrecision || s < 0 || s > p {
		// A declaration this carrier cannot express — PostgreSQL's numeric
		// allows precision to 1000 and an Int128 stops at 38 (ADR-0024 item
		// 1). It is still a DECIMAL destination, so the caller REFUSES it
		// rather than passing the value through untouched, which is what the
		// evaluator's `default` arm did with every parameterized DECIMAL
		// before this file existed.
		return decimalDest{params: true, overWidth: true,
			typ: batch.DecimalType{Precision: p, Scale: s}}, true
	}
	return decimalDest{params: true, typ: batch.DecimalType{Precision: p, Scale: s}}, true
}

// DecimalCastDest reports the (precision, scale) a DECIMAL cast destination
// names, for the planner's declared-type layer. hasParams is false for a bare
// DECIMAL/NUMERIC, whose type comes from the operand; ok is false for a
// destination that is not DECIMAL at all, or whose (p,s) no DECIMAL can hold.
func DecimalCastDest(dest string) (prec, scale int, hasParams, ok bool) {
	d, ok := parseDecimalDest(dest)
	if !ok || d.overWidth {
		return 0, 0, false, false
	}
	return d.typ.Precision, d.typ.Scale, d.params, true
}

// castDecimalState is the Cast node's resolved DECIMAL destination, published
// once. It rides beside boolSrc for the same reason: the destination is fixed
// for the query, and re-parsing the type name per row cost a string walk on
// every value.
type castDecimalState struct {
	ready atomic.Bool
	dest  decimalDest
	is    bool
}

// decimalDestination resolves this cast's DECIMAL destination once.
func (e *Cast) decimalDestination() (decimalDest, bool) {
	if e.decDest.ready.Load() {
		return e.decDest.dest, e.decDest.is
	}
	d, ok := parseDecimalDest(e.DestType)
	e.decDest.dest, e.decDest.is = d, ok
	e.decDest.ready.Store(true)
	return d, ok
}

// castToDecimal converts one value to the destination's exact DECIMAL, boxed
// as its rendered text — the same box a DECIMAL COLUMN produces, so a cast
// result and a stored value reach every consumer in one shape.
//
// The four source families and what PostgreSQL does with each, all verified
// live on 17.11:
//
//	numeric  12.7501::numeric(9,2)  = 12.75      (rounds, half away from zero)
//	integer  1::numeric(10,2)       = 1.00
//	float8   0.1::float8::numeric(10,4) = 0.1000 (the double's own decimal)
//	text     '12.75'::numeric(9,2)  = 12.75      ('abc' is 22P02)
//
// A float is spelled as its SHORTEST ROUND-TRIP decimal first — the unique
// decimal that reads back as the same double, which is also what PostgreSQL
// prints for it — and then resolved through the same exact text path as
// everything else. Going through the binary value instead would carry the
// double's 55-digit exact expansion, which rounds differently at the target
// scale than the number the user can see.
//
// NaN and the infinities are 22003 with a message naming ADR-0024 item 6: a
// wadjet DECIMAL is an Int128 with no bit pattern for them, and PostgreSQL's
// numeric does store NaN — a documented divergence, refused loudly rather than
// stored as something else.
func (e *Cast) castToDecimal(b *batch.RecordBatch, row int, v any, d decimalDest) any {
	if d.overWidth {
		panic(fatalEval{sqlerr.New("22003",
			"NUMERIC precision %d is out of range for this engine: a DECIMAL is a 128-bit "+
				"unscaled integer, so the widest declaration it can hold is %d digits "+
				"(ADR-0024 item 1)", d.typ.Precision, batch.MaxDecimalPrecision)})
	}
	if _, isBool := v.(bool); isBool {
		// PostgreSQL has no boolean-to-numeric cast in ANY spelling, so the
		// refusal cannot live inside castDecimalValue: a BARE destination
		// declines to name a type before it ever gets there, and the float
		// fallback below then answered 1 (#555 review, N3).
		raiseCannotCastToNumeric("boolean")
	}
	typ, ok := e.castDecimalTarget(b, row, v, d)
	if !ok {
		// A bare DECIMAL over an operand whose own (p,s) nothing here can
		// resolve. Answering the float64 this arm answered before ADR-0024 is
		// the honest fallback: it is what the planner declared for the same
		// shape, so the two still agree.
		//
		// TEXT still has to be READ, not assumed: `CAST('abc' AS NUMERIC)`
		// reached ToFloat64 and answered 0 — a number where PostgreSQL raises
		// 22P02, and the PARAMETERIZED spelling one line down has raised it
		// since #555. One destination cannot have two answers depending on
		// whether the caller wrote the (p,s) (#839's census).
		if f, isText := castFloatText(v, "numeric", 64); isText {
			return f
		}
		return ToFloat64(v)
	}
	unscaled, ok := castDecimalValue(v, typ.Scale)
	if !ok {
		return nil
	}
	if !batch.DecimalFitsPrecision(unscaled, typ.Precision) {
		raiseNumericFieldOverflow(typ.Precision, typ.Scale)
	}
	return unscaled.FormatDecimal(typ.Scale)
}

// castDecimalTarget is the (p,s) this cast produces for THIS value: the
// destination's own when it named one, and otherwise the operand's — the
// value's natural scale at the carrier's full width, or (38,0) for an integer
// (ADR-0024 item 3).
func (e *Cast) castDecimalTarget(b *batch.RecordBatch, row int, v any, d decimalDest) (batch.DecimalType, bool) {
	if d.params {
		return d.typ, true
	}
	// A bare DECIMAL over an operand with an exact form keeps that form.
	if o, ok := e.Operand.(decimalOperand); ok {
		if t, ok := o.decimalType(b); ok {
			return batch.DecimalType{Precision: batch.MaxDecimalPrecision, Scale: t.Scale}, true
		}
	}
	switch v.(type) {
	case int64, int32, int:
		return batch.DecimalType{Precision: batch.MaxDecimalPrecision}, true
	}
	// Everything else has no scale this layer can name, and the DECLARATION
	// says so: physical.castDeclaredDecimal declines a bare destination over a
	// FLOAT or TEXT operand and the projection allocates a FLOAT64 vector.
	// Answering a decimal box here anyway is what made `CAST(text AS DECIMAL)`
	// fail at the store with "cannot store string into FLOAT64 vector" — the
	// declaration and the value disagreeing, which is the one thing this whole
	// layer exists to prevent (#555 review, R3). A per-VALUE scale would not
	// close it either: the vector is built once, from the type.
	_ = row
	return batch.DecimalType{}, false
}

// castDecimalValue reads a boxed value as an exact unscaled carrier at scale,
// rounding half away from zero exactly once. ok=false is SQL NULL.
func castDecimalValue(v any, scale int) (batch.Int128, bool) {
	switch tv := v.(type) {
	case nil:
		return batch.Int128{}, false
	case int64:
		return castDecimalFromText(strconv.FormatInt(tv, 10), scale), true
	case int32:
		return castDecimalFromText(strconv.FormatInt(int64(tv), 10), scale), true
	case int:
		return castDecimalFromText(strconv.FormatInt(int64(tv), 10), scale), true
	case float64:
		return castDecimalFromText(strconv.FormatFloat(tv, 'f', -1, 64), scale), true
	case float32:
		return castDecimalFromText(strconv.FormatFloat(float64(tv), 'f', -1, 32), scale), true
	case string:
		return castDecimalFromText(tv, scale), true
	case bool:
		// Unreachable through Cast (castToDecimal refuses a boolean before
		// naming a type), kept so every caller of this conversion gets the
		// same refusal rather than a 0/1 nobody asked for.
		_ = tv
		raiseCannotCastToNumeric("boolean")
	}
	raiseInvalidTextRepresentation("numeric", toString(v))
	return batch.Int128{}, false
}

// castDecimalFromText is the one conversion every source family funnels
// through: read the text at ITS OWN scale, then rescale ONCE to the target.
//
// Reading at the natural scale first is what makes the rounding single.
// batch.DecimalTextAt at the TARGET scale truncates and reports a residual
// instead, which is right for its own caller — a comparison bound, where a
// literal finer than the column still has a place in the order (#462) — and
// wrong for a value: `12.755::numeric(9,2)` is 12.76 in PostgreSQL, not 12.75.
func castDecimalFromText(text string, scale int) batch.Int128 {
	nat, ok := batch.DecimalTextType(text)
	if !ok {
		// Not a number this carrier can name. NaN and the infinities are the
		// interesting half: PostgreSQL's numeric DOES hold them and an Int128
		// has no bit pattern for either, so they are refused as a VALUE with
		// the SQLSTATE that says the range is the problem (ADR-0024 item 6).
		if isNonFiniteNumericText(text) {
			panic(fatalEval{nonFiniteDecimalError(text)})
		}
		// A well-formed number that is simply TOO WIDE is a range condition,
		// not a syntax one: PostgreSQL answers `CAST('1e40' AS numeric(38,0))`
		// with 22003 numeric field overflow, and reporting 22P02 sends a
		// client hunting a typo in a number it read correctly (#555 review,
		// S1).
		if _, isNumber := batch.CanonicalDecimalText(text); isNumber {
			raiseNumericFieldOverflow(0, scale)
		}
		raiseInvalidTextRepresentation("numeric", text)
	}
	d, ok := batch.DecimalTextAt(text, nat.Scale)
	if !ok || d.Residual != 0 {
		raiseInvalidTextRepresentation("numeric", text)
	}
	if d.Sat != 0 {
		// The value has no Int128 even at its own scale: 10^39 written out.
		raiseNumericFieldOverflow(0, nat.Scale)
	}
	out, ok := batch.Rescale(d.Unscaled, nat.Scale, scale)
	if !ok {
		raiseNumericFieldOverflow(0, scale)
	}
	return out
}

// isNonFiniteNumericText reports whether text names NaN or an infinity in one
// of PostgreSQL's spellings for numeric input.
func isNonFiniteNumericText(text string) bool {
	switch strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "+"))) {
	case "nan", "inf", "-inf", "infinity", "-infinity":
		return true
	}
	return false
}

// --- The cast as an arithmetic operand --------------------------------------
//
// A cast with an explicit (p,s) produces an exact DECIMAL, so it can be an
// operand of exact arithmetic: `CAST(x AS DECIMAL(10,2)) * 2` is numeric in
// PostgreSQL and exact here. A BARE DECIMAL cast cannot — its (p,s) is the
// operand's, which this layer resolves per VALUE — so it declines and the
// expression stays where it was.

func (e *Cast) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	d, ok := e.decimalDestination()
	if !ok || !d.params {
		return batch.DecimalType{}, false
	}
	_ = b
	return d.typ, true
}

func (e *Cast) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	d, ok := e.decimalDestination()
	if !ok || !d.params {
		return batch.Int128{}, false
	}
	v := e.Operand.Eval(b, row)
	if v == nil {
		return batch.Int128{}, false
	}
	unscaled, ok := castDecimalValue(v, d.typ.Scale)
	if !ok {
		return batch.Int128{}, false
	}
	if !batch.DecimalFitsPrecision(unscaled, d.typ.Precision) {
		raiseNumericFieldOverflow(d.typ.Precision, d.typ.Scale)
	}
	return unscaled, true
}

func (e *Cast) decimalVec(_ *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	// No materialized column of its own; the caller reads it per row through
	// evalDecimal, unboxed.
	return kernel.DecimalOperandVec{}, false
}

// castIsExactDecimal reports whether this cast produces a DECIMAL at a type it
// names itself — the test operandIsDecimalTyped makes to decide whether the
// arithmetic around it is exact.
func castIsExactDecimal(e *Cast) bool {
	d, ok := e.decimalDestination()
	return ok && d.params
}

// --- Casting a DECIMAL to an INTEGER ----------------------------------------

// castDecimalToInt rounds an exact DECIMAL to an integer, half away from zero,
// and refuses a value with no int64 — PostgreSQL's `integer out of range` /
// `bigint out of range`, SQLSTATE 22003.
//
// It exists because the generic integer arm reads a string operand through
// strconv.ParseFloat, which loses every digit past a double's sixteenth: a
// DECIMAL(38,10) holding 493827160549382.7160549350 came back as the nearest
// double's integer part, and a value past 2^63 came back as whatever the
// float conversion produced rather than as the refusal PostgreSQL gives.
//
// ok=false means this operand has no exact form and the generic arm answers.
func castDecimalToInt(v any, dest string) (int64, bool) {
	s, ok := stringOperand(v)
	if !ok {
		return 0, false
	}
	nat, ok := batch.DecimalTextType(s)
	if !ok {
		return 0, false // not decimal text: the generic arm reports 22P02
	}
	d, ok := batch.DecimalTextAt(s, nat.Scale)
	if !ok || d.Residual != 0 || d.Sat != 0 {
		return 0, false
	}
	rounded, ok := batch.Rescale(d.Unscaled, nat.Scale, 0)
	if !ok || !rounded.FitsInt64() {
		raiseIntegerOutOfRange(dest)
	}
	out := rounded.ToInt64()
	// Each spelling names its own range; `bigint` and `signed` keep the int64
	// one the value already has.
	return castIntInRange(out, dest), true
}

// castIntInRange applies an integer destination's own RANGE — PostgreSQL's
// `smallint out of range` / `integer out of range`, SQLSTATE 22003.
//
// It is the one place that bound lives, so every SOURCE reaches the same
// refusal. An integer box used to return from the cast before any check at
// all, so `CAST(99999 AS SMALLINT)` answered 99999 where PostgreSQL refuses.
func castIntInRange(v int64, dest string) int64 {
	switch dest {
	case "int", "integer", "int4":
		if v < -(1<<31) || v > (1<<31)-1 {
			raiseIntegerOutOfRange(dest)
		}
	case "smallint", "int2":
		if v < -(1<<15) || v > (1<<15)-1 {
			raiseIntegerOutOfRange(dest)
		}
	}
	return v
}
