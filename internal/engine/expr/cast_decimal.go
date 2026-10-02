// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
//
// The resolution is published as ONE pointer to an immutable value. One
// compiled Cast is evaluated from many goroutines at once (a parallel filter,
// a DAG worker, a parallel correlated re-run), and the lazy path wrote two
// plain fields and then stored an atomic.Bool: two first evaluations wrote the
// fields concurrently, and a reader that saw the flag from one writer read
// fields the other was still writing — a data race. A pointer store publishes
// the whole value, and every racing first evaluation parses the same string to
// the same value, so whichever store lands last is equally correct.
//
// The Cast is not resolved at construction because it is built as a struct
// literal at several sites (the compiler, the UDF body compiler, array and
// membership element casts); a constructor-time field one of them missed
// would read as "not a DECIMAL destination" and answer a wrong value.
type castDecimalState struct {
	res atomic.Pointer[castDecimalResolved]
}

type castDecimalResolved struct {
	dest decimalDest
	is   bool
}

// decimalDestination resolves this cast's DECIMAL destination once.
func (e *Cast) decimalDestination() (decimalDest, bool) {
	if r := e.decDest.res.Load(); r != nil {
		return r.dest, r.is
	}
	d, ok := parseDecimalDest(e.DestType)
	e.decDest.res.Store(&castDecimalResolved{dest: d, is: ok})
	return d, ok
}

// castToDecimal returns exact DECIMAL as rendered TEXT, matching stored-column boxes.
// Numeric values round half away from zero; integers scale exactly; text parses
// through the exact path and invalid syntax refuses 22P02.
// Render floats as their SHORTEST ROUND-TRIP decimal before exact target-scale conversion;
// the full binary expansion can round differently from the visible decimal value.
// NaN and infinities refuse 22003 naming ADR-0024 item 6: Int128 has no carrier for them,
// a deliberate divergence from PostgreSQL numeric's special values.
// See docs/internals/decimal-cast-source-value-contract.md for the design.
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
	// A wide numeric LITERAL reaches this cast as the float64 compileLit
	// boxed it, and past ~17 significant digits that box has already lost the
	// value: `CAST(9007199254740993.25 AS DECIMAL(30,2))` answered
	// 9007199254740994.00 where PostgreSQL 17.11 answers 9007199254740993.25,
	// and the fraction was gone before this conversion ever saw it (#1037).
	//
	// The literal's own SOURCE TEXT is kept for exactly this reason — it is
	// what arithmetic over the same literal already reads (ADR-0012 item 6)
	// — so a cast reads it too, and reads it FIRST, before the box.
	src, litText := v, ""
	if text, ok := castExactSourceText(e.Operand); ok {
		src, litText = text, text
	}
	typ, ok := e.castDecimalTarget(b, row, src, litText, d)
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
	unscaled, ok := castDecimalValue(src, typ.Scale)
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
func (e *Cast) castDecimalTarget(b *batch.RecordBatch, row int, v any, litText string, d decimalDest) (batch.DecimalType, bool) {
	if d.params {
		return d.typ, true
	}
	// A bare DECIMAL over a quoted literal is its spelling's (#1386).
	if t, ok := quotedLitBareDecimal(e.Operand); ok {
		return t, true
	}
	// A bare DECIMAL over an operand with an exact form keeps that form.
	if o, ok := decimalOperandOf(e.Operand, b); ok {
		if t, ok := o.decimalType(b); ok {
			return batch.DecimalType{Precision: batch.MaxDecimalPrecision, Scale: t.Scale}, true
		}
	}
	// A LITERAL's own source text names its scale — its SPELLING, ADR-0024
	// item 2's rule for a numeric literal — so a BARE destination over one
	// keeps every digit it was written with rather than declining to the
	// float fallback below (#1037). litText is empty for every other operand,
	// and a TEXT COLUMN stays on that fallback: the planner declines a bare
	// destination over one and allocates a FLOAT64 vector, so answering a
	// decimal box here would meet the #361 store guard.
	if litText != "" {
		if t, ok := batch.DecimalTextType(litText); ok {
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
// through: the text's value rounded ONCE, half away from zero, at the target
// scale (batch.DecimalTextRoundedAt).
//
// batch.DecimalTextAt at the target scale truncates and reports a residual
// instead, which is right for its own caller — a comparison bound, where a
// literal finer than the column still has a place in the order (#462) — and
// wrong for a value: `12.755::numeric(9,2)` is 12.76 in PostgreSQL, not 12.75.
// The value is read from the digits, never from a DECIMAL named by the
// SPELLING: a text written wider than 38 digits still has a value at the
// target scale — `CAST('14.' || 40 zeros AS numeric(18,4))` is 14.0000 in
// PostgreSQL, and so is a 42-digit '14.000…0001', which the target's scale
// rounds; reading through the spelling's own DECIMAL refused both 22003.
func castDecimalFromText(text string, scale int) batch.Int128 {
	// NaN and the infinities: PostgreSQL's numeric DOES hold them and an
	// Int128 has no bit pattern for either, so they are refused as a VALUE
	// with the SQLSTATE that says the range is the problem (ADR-0024 item 6).
	if isNonFiniteNumericText(text) {
		panic(fatalEval{nonFiniteDecimalError(text)})
	}
	out, sat, ok := batch.DecimalTextRoundedAt(text, scale)
	if !ok {
		raiseInvalidTextRepresentation("numeric", text)
	}
	if sat {
		// A well-formed number too wide for the carrier at this scale is a
		// range condition, not a syntax one: PostgreSQL answers
		// `CAST('1e40' AS numeric(38,0))` with 22003 numeric field overflow,
		// and reporting 22P02 sends a client hunting a typo in a number it
		// read correctly (#555).
		raiseNumericFieldOverflow(0, scale)
	}
	return out
}

// isNonFiniteNumericText reports whether text names NaN or an infinity in one
// of PostgreSQL's spellings for numeric input — the classifier the
// comparison bound reads too (batch.DecimalSpecialText). A SIGNED NaN is no
// such spelling: PostgreSQL refuses '+NaN' and '-NaN' as input syntax
// (22P02), which the cast's own input function then raises.
func isNonFiniteNumericText(text string) bool {
	return batch.DecimalSpecialText(text) != batch.DecimalFinite
}

// --- The cast as an arithmetic operand --------------------------------------
//
// A cast with an explicit (p,s) produces an exact DECIMAL, so it can be an
// operand of exact arithmetic: `CAST(x AS DECIMAL(10,2)) * 2` is numeric in
// PostgreSQL and exact here. A BARE DECIMAL cast cannot — its (p,s) is the
// operand's, which this layer resolves per VALUE — so it declines and the
// expression stays where it was.

func (e *Cast) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	if e.columnDecOK {
		return e.columnDec, true
	}
	d, ok := e.decimalDestination()
	if !ok {
		return batch.DecimalType{}, false
	}
	if !d.params {
		return e.bareDecimalType(b)
	}
	return d.typ, true
}

// bareDecimalType is the type a BARE NUMERIC cast produces when its operand
// has one exactly: the operand's scale at the carrier's full width, (38,0)
// for an integer (castDecimalTarget's rule, per batch rather than per value).
// PostgreSQL's unconstrained numeric keeps the operand's value, so
// `CAST(14.0000000000000000001 AS NUMERIC) + n` and `CAST(i AS NUMERIC) * 0.1`
// are exact numeric there (#1386); a QUOTED literal operand is its spelling's
// DECIMAL, as a choice reads it (QuotedLitDecimalType). A float or text
// operand has no such type and declines — physical.castDeclaredDecimal draws
// the same line over the AST.
func (e *Cast) bareDecimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	if t, ok := quotedLitBareDecimal(e.Operand); ok {
		return t, true
	}
	if o, ok := decimalOperandOf(e.Operand, b); ok {
		if t, ok := o.decimalType(b); ok {
			return batch.DecimalType{Precision: batch.MaxDecimalPrecision, Scale: t.Scale}, true
		}
	}
	return batch.DecimalType{}, false
}

// quotedLitBareDecimal is a QUOTED literal's spelling as the bare NUMERIC
// cast over it produces it: its scale at the carrier's full width.
func quotedLitBareDecimal(op Expr) (batch.DecimalType, bool) {
	lit, ok := op.(*Lit)
	if !ok || lit.Text != "" {
		return batch.DecimalType{}, false
	}
	s, ok := lit.Val.(string)
	if !ok {
		return batch.DecimalType{}, false
	}
	t, ok := QuotedLitDecimalType(s)
	if !ok {
		return batch.DecimalType{}, false
	}
	return batch.DecimalType{Precision: batch.MaxDecimalPrecision, Scale: t.Scale}, true
}

func (e *Cast) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	if e.columnDecOK {
		// The outer column's own integer, range-checked by its type.
		return castDecimalValue(e.Eval(b, row), 0)
	}
	d, ok := e.decimalDestination()
	if !ok {
		return batch.Int128{}, false
	}
	if !d.params {
		// The cast's own box is the operand's value at bareDecimalType's
		// scale (castToDecimal); read it back.
		t, ok := e.bareDecimalType(b)
		if !ok {
			return batch.Int128{}, false
		}
		return castDecimalValue(e.Eval(b, row), t.Scale)
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

// columnIntegerDecimal is the fixed-point contribution of a correlated
// re-run's INTEGER or BIGINT outer value (Cast.Column): the whole range of the
// column's type at scale 0, exactly as that column contributes
// (vectorDecimalType). A cast the user wrote is an expression, not a column,
// and keeps its own rule — an integer cast is not a fixed-point operand.
func columnIntegerDecimal(typeName string) (batch.DecimalType, bool) {
	col, ok := ColumnOfCastName(typeName)
	if !ok {
		return batch.DecimalType{}, false
	}
	switch col.Type {
	case parquet.TypeInt32:
		return batch.DecimalType{Precision: batch.Int32DecimalDigits}, true
	case parquet.TypeInt64:
		return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
	}
	return batch.DecimalType{}, false
}

// IntegerCastDecimal is the fixed-point contribution of a CAST to an integer
// type: the int64 range at scale 0, DECIMAL(19,0), which is what every integer
// EXPRESSION contributes to exact arithmetic (integerBoxOperand; a scalar
// subquery's marked cast has contributed it since arc SS). PostgreSQL promotes
// the int2 / int4 / int8 the cast produces to numeric beside a numeric
// operand, so the cast is an integer operand of exact arithmetic wherever it
// sits (#1450): `CAST(i AS INTEGER) * 0.1` is 0.3 and `CAST(b AS BIGINT) *
// 10000000 * n - 3` keeps every digit past 2^53. The plan's mirror is
// physical.decimalArithOperand's CastNode arm, which reads this function.
func IntegerCastDecimal(typeName string) (batch.DecimalType, bool) {
	if !IsIntegerCastDest(typeName) {
		return batch.DecimalType{}, false
	}
	return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
}

// userIntegerCast reports whether e is an integer CAST a query wrote — not a
// correlated re-run's column stand-in (Column) and not one a scalar
// subquery's body marks (answer), which keep the rules arc SS gave them.
func userIntegerCast(e Expr) bool {
	c, ok := e.(*Cast)
	return ok && !c.Column && !c.answer && castIsInt(c)
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
// INT32, PORT, PROTOCOL and DATE share the int4 bound (#901, #911). INT32 is a second
// spelling of int4 and PostgreSQL raises `integer out of range` for the same
// magnitude; PORT and PROTOCOL are wadjet's own, and docs/data-types.md
// already states their rule — "a DATE, a PORT and a PROTOCOL are all stored in
// a signed 32-bit field, and a number with no room in one is 22003 integer out
// of range". The check belongs HERE as well as at the store, because a cast
// whose result is aggregated, compared or grouped may never reach a vector.
func castIntInRange(v int64, dest string) int64 {
	switch dest {
	case "port", "protocol":
		// The TYPE's own range, not the int4 carrier's. A PORT is 0..65535 and
		// a PROTOCOL 0..255, and the rule Derek settled on 2026-09-15 is that
		// the range is checked when a value ENTERS the type — by CAST or by
		// WRITE — and nowhere else. So `CAST(70000 AS PORT)` is 22003 naming
		// the value and the type, the same refusal and the same words the
		// writer's door has always produced (parquet.NetworkIntRangeError, one
		// check shared), while `port + 70000` stays plain int4 arithmetic and
		// may leave the range without error — PostgreSQL's `smallint + 1` rule,
		// and #901's position, both intact.
		//
		// Before this a cast held the CARRIER's range and minted a value no
		// PORT column could store: `CREATE TABLE p AS SELECT CAST(70000 AS
		// PORT)` persisted 70000 into a column the catalog declares PORT,
		// which `INSERT` refused with 22003 (review NT N2).
		typ := parquet.TypePort
		if dest == "protocol" {
			typ = parquet.TypeProtocol
		}
		if lo, hi := parquet.NetworkIntBounds(typ); v < lo || v > hi {
			panic(fatalEval{parquet.NetworkIntRangeError(typ, v)})
		}
	case "int", "integer", "int4", "int32", "date":
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

// castFloatToInt64 rounds a float to the int64 a cast produces, RAISING when
// the value has none.
//
// This check has to happen at the CONVERSION and not in castIntInRange above,
// which is why it is a separate function: by the time that one runs the value
// is already an int64, and Go's float-to-integer conversion for an
// out-of-range operand is implementation-defined — on amd64 it yields
// MinInt64. So `CAST(1e30 AS BIGINT)` came back as -9223372036854775808, a
// wrapped number wearing the right type, while `CAST(1e30 AS INTEGER)` raised
// correctly on the same tree: ONE destination family with two answers, which
// is this arc's headline defect, in the file it rewrote (review round 0, P2).
//
// PostgreSQL raises `bigint out of range` for it, measured, and the bound is
// its own: a float64 cannot represent 2^63-1 exactly, so the comparison is
// against the exact powers of two that bracket the range.
func castFloatToInt64(f float64, dest string) int64 {
	r := math.Round(f)
	if math.IsNaN(r) || r >= 9223372036854775808.0 || r < -9223372036854775808.0 {
		raiseIntegerOutOfRange(dest)
	}
	return int64(r)
}

// castFloatToInt64Even is castFloatToInt64 with PostgreSQL's rint() rounding —
// half TO EVEN, which is what a FLOAT source gets (#768). The range check is
// the same one and is shared rather than copied.
func castFloatToInt64Even(f float64, dest string) int64 {
	r := math.RoundToEven(f)
	if math.IsNaN(r) || r >= 9223372036854775808.0 || r < -9223372036854775808.0 {
		raiseIntegerOutOfRange(dest)
	}
	return int64(r)
}

// castExactSourceText is the operand's own numeric SOURCE TEXT, when it has
// one. Only a literal does: its box is whatever compileLit could fit, and past
// a double's significant digits that is not the number the query spelled
// (#1037, ADR-0012 item 6).
//
// A negated literal is folded into a literal with its own negated text at
// compile time, so the unary form arrives here already covered.
func castExactSourceText(operand Expr) (string, bool) {
	lit, ok := operand.(*Lit)
	if !ok || lit.Text == "" {
		return "", false
	}
	switch lit.Val.(type) {
	case float64, float32, int64, int32, int:
	default:
		return "", false
	}
	if _, ok := batch.DecimalTextType(lit.Text); !ok {
		return "", false
	}
	return lit.Text, true
}
