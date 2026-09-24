// SPDX-License-Identifier: MIT

// This file holds expr cast; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Cast wraps an expression with explicit type conversion.
type Cast struct {
	Operand  Expr
	DestType string // "int", "float", "string", "date", "timestamp"

	// boolSrc caches the OPERAND's declared type for a cast to BOOLEAN, which
	// selects the conversion rule (cast_bool.go). Zero means "not resolved
	// yet"; nothing else in this node needs it, and no other destination
	// reads it.
	boolSrc atomic.Int32
	// decDest caches the parsed DECIMAL destination, for the same reason:
	// `DECIMAL(10, 2)` is fixed for the query and re-parsing the type name
	// per row cost a string walk on every value (cast_decimal.go).
	decDest castDecimalState
	// strDest caches the parsed VARCHAR(n) / CHAR(n) length, for the same
	// reason again (cast_string_length.go, #838).
	strDest castStringState
}

func (e *Cast) Eval(b *batch.RecordBatch, row int) any {
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	// One normalization for both the temporal check and the switch: this
	// runs per row, and a WHERE over a typed date literal evaluates it once
	// per row of the scan.
	dest := strings.ToLower(e.DestType)
	if elem, ok := ArrayCastElement(dest); ok {
		return castToArray(v, elem)
	}
	// A VECTOR destination converts (pgvector's array_to_vector / vector_in)
	// and a CONTAINER operand is decided by the container table before any
	// scalar arm can read its box (cast_container.go, arc CW round 2).
	if dim, err, ok := VectorCastDim(dest); ok {
		if err != nil {
			panic(fatalEval{err})
		}
		return castToVector(v, dim)
	}
	if isContainerBox(v) {
		if r, ok := e.castContainerDest(b, row, v, dest); ok {
			return r
		}
	}
	if k := castTemporalKindLower(strings.TrimSpace(dest)); k != castNotTemporal {
		return castTemporal(b, row, e.Operand, v, k)
	}
	// A DECIMAL destination is resolved before the switch because its type
	// name CARRIES its parameters — `decimal(10, 2)` matches no case label,
	// and used to reach `default: return v`, which passed the value through
	// with the (p,s) silently ignored (ADR-0024 item 3, #555).
	if d, ok := e.decimalDestination(); ok {
		return e.castToDecimal(b, row, v, d)
	}
	// A length-carrying STRING destination has the SAME shape and had the
	// same defect: `varchar(4)` matches no case label either, so the whole
	// cast reached `default: return v` and returned six characters where
	// PostgreSQL returns four (#838).
	if n, ok := e.stringDestination(); ok {
		return truncateToChars(castStringRender(b, row, e.Operand, v), n)
	}
	// FLOAT(n), the third parameterized destination and the third one that
	// matched no case label: `float(1)` reached `default: return v` and
	// answered a double under a STRING declaration (#652). PostgreSQL
	// resolves it by WIDTH — float(1..24) is real, float(25..53) is double —
	// so the narrow half takes the REAL arm's rounding and its 22003 range
	// check rather than a second copy of them.
	if bits, err, ok := parquet.FloatTypePrecision(e.DestType); ok {
		if err != nil {
			panic(fatalEval{err})
		}
		if bits <= 24 {
			return e.castToReal(v)
		}
		if f, isText := castFloatText(v, "double precision", 64); isText {
			return f
		}
		return ToFloat64(v)
	}
	// A NETWORK destination is resolved before the switch for the same reason
	// a DECIMAL one is: its spellings (`macaddr`, `proto`, `ip`) are the
	// type's, not this switch's, and until #1092 not one of them had a case
	// label at all — every network cast reached `default: return v` and
	// published its operand unparsed under a network declaration.
	if nt, ok := networkCastType(dest); ok {
		return castToNetwork(b, row, e.Operand, v, nt)
	}
	if strings.TrimSpace(dest) == "interval" {
		return castToInterval(v)
	}
	switch dest {
	// Keep this label list and IsIntegerCastDest in step: that predicate tells
	// the DAG's gather materialization to build an INT64 vector for this
	// destination (#813), and a label here it does not know would put the same
	// query's answer in a float64 one.
	//
	// INT32, PORT and PROTOCOL are integer destinations this engine HAS and
	// this switch did not implement: all three fell to `default: return v` and
	// answered the operand unchanged under a STRING declaration, so
	// `3000000000::INT32` answered where PostgreSQL raises `integer out of
	// range`, and `3000000000::PORT` answered under a signed 32-bit carrier
	// (#901). castIntInRange carries their bound; a PORT/PROTOCOL vector's own
	// int4 guard is the second net (batch.IntegerRangeError). INT64 is the
	// same hole one spelling over and survived #901: physical.inferCastType
	// has read it as an integer destination all along — BIGINT's wadjet
	// spelling — so the projection allocated an INT64 vector while this switch
	// handed it the operand untouched, and `CAST('2.5' AS INT64)` died on the
	// #361 silent-write guard.
	//
	// A PROTOCOL destination also reads the IANA NAME, the type's own text
	// form (`CAST('udp' AS PROTOCOL)` is 17, #986); the NUMBER keeps int4's.
	case "int", "integer", "int4", "int32", "int64", "bigint", "int8", "signed",
		"smallint", "int2", "port", "protocol":
		// A string that does not read as a number is refused, not coerced to
		// 0: PostgreSQL raises 22P02 and ADR-0012 makes it the authority on
		// error-versus-not; the raise rides FatalEvalPanic (#347).
		//
		// A value that DOES read as a number follows PostgreSQL's other rule
		// (#373): a fractional cast to an integer type ROUNDS, half away from
		// zero, and TRUNC() is how a caller asks for truncation. An exact
		// DECIMAL is read on its own carrier, never through a double —
		// ParseFloat loses every digit past the sixteenth (ADR-0024 item 4).
		//
		// A TEXT operand is read by the DESTINATION TYPE'S INPUT FUNCTION and
		// by nothing else: PostgreSQL's two casts here are not the same cast,
		// `'2.5'::integer` being 22P02 where `numeric_col::integer` rounds to
		// 3 (measured on 17.11 for '2.5', '2.0', '26.7', '-0.4' and '1e3', at
		// integer, bigint and smallint alike). castDecimalToInt reads the BOX,
		// and a text box and a decimal box are the same Go string here, so
		// with it first every quoted fractional literal took the rounding cast
		// (#1141). castOperandDeclaresText decides from the EXPRESSION's own
		// declaration instead — before either reader, once, for every integer
		// destination rather than for PORT and PROTOCOL alone.
		if castOperandDeclaresText(e.Operand, b) {
			if s, ok := stringOperand(v); ok {
				return castTextToInt(s, dest)
			}
		}
		if i, ok := castDecimalToInt(v, dest); ok {
			return i
		}
		if s, ok := stringOperand(v); ok {
			// A box this arm could not decide from the expression, holding
			// text. It reads the SAME grammar a declared-text operand reads:
			// the destination's input function, one reader, so a value that
			// arrives through a MAP entry, a ROW field or a scalar subquery
			// cannot mean a different number than the same text written as a
			// literal.
			return castTextToInt(s, dest)
		}
		// EVERY source gets the destination's range, integers included:
		// `CAST(99999 AS SMALLINT)` answered 99999 because an integer box
		// returned before the check, and PostgreSQL raises `smallint out of
		// range` for it (#555 review).
		if i, ok := toInt64Safe(v); ok {
			return castIntInRange(i, dest)
		}
		// A FLOAT source rounds HALF TO EVEN, which is PostgreSQL's rint()
		// and C's default rounding mode: `-0.5::float8::int` is 0 there and
		// this engine answered -1, `0.5` is 0 and this answered 1, `2.5` is 2
		// and this answered 3. Measured live on 17 (#768).
		//
		// A CONSTANT operand does NOT: PostgreSQL types a bare `-0.5`
		// numeric, and its numeric-to-integer cast rounds HALF AWAY FROM ZERO
		// (`CAST(-0.5 AS int)` is -1 there, `CAST(2.5 AS int)` is 3). The two
		// sources round differently on the same server and so must these. The
		// operand's box cannot tell them apart — a bare numeric literal is a
		// float64 here, which is ADR-0024's recorded literal-typing deferral
		// — so the distinction is made from the EXPRESSION: a literal, or a
		// unary sign over one, is a constant. That is the same test
		// physical.isConstNumericLitNode makes for the same reason, and it
		// covers both spellings because `-0.5` parses as a UnaryOp and `0.5`
		// as a Lit, and covering only one made the two halves of one query
		// disagree about their own type (#668's note).
		if isConstNumericOperand(e.Operand) {
			return castIntInRange(castFloatToInt64(ToFloat64(v), dest), dest)
		}
		return castIntInRange(castFloatToInt64Even(ToFloat64(v), dest), dest)
	// FLOAT32 is the same gap one family over: it is this engine's own name
	// for float4 and matched no label, so `CAST(1e40 AS FLOAT32)` answered
	// 1e+40 as TEXT where `CAST(1e40 AS REAL)` raises 22003 (#901).
	case "real", "float4", "float32":
		// REAL is float4, a NARROWER type than the float64 every other
		// numeric box in this engine carries — and this arm used to sit
		// beside "float"/"double" and answer ToFloat64, so `CAST(x AS REAL)`
		// was a NO-OP. PostgreSQL types the result float4 and rounds the
		// value to it, which changes the answer of anything that compares it:
		//
		//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real) -> the row
		//	CAST(1.0/3 AS REAL)        ->  0.33333334, not 0.3333333333333333
		//
		// FLOAT is deliberately NOT here. PostgreSQL's bare `float` is
		// `double precision` (float(1..24) is real, float(25..53) is double,
		// and an unqualified FLOAT is the latter) — verified with pg_typeof —
		// so only the two spellings that really name float4 narrow.
		return e.castToReal(v)
	case "float", "double", "float8", "double precision", "float64":
		// Same hole, the wider destination: `CAST('abc' AS DOUBLE PRECISION)`
		// answered 0, a plausible measurement where PostgreSQL raises 22P02.
		if f, isText := castFloatText(v, "double precision", 64); isText {
			return f
		}
		return ToFloat64(v)
	case "bool", "boolean":
		// The conversion the operand's DECLARATION selects, not the one its
		// Go box suggests — see cast_bool.go for what each source type
		// answers and why the box cannot decide it.
		return e.castToBool(b, v)
	case "char", "varchar", "text", "string":
		// BYTES boxes as raw []byte; casting to text must produce PostgreSQL's default
		// bytea_output=hex form: backslash-x followed by LOWERCASE hex (ADR-0012 item 1).
		// Raw-string or Go slice rendering is wrong; hex is ASCII without embedded NUL,
		// so pgx and libpq/psql read the same value (#570).
		// LIKE deliberately differs: bytea ~~ is BYTEWISE, so kernel.likeTextRenderer
		// must continue matching raw bytes, not the hex text rendering.
		// See docs/internals/bytes-cast-text-versus-like.md for the design.
		return castStringRender(b, row, e.Operand, v)
	default:
		// An accepted destination this engine does not convert to hands the
		// operand's TEXT back under a text declaration (sql-reference, #652).
		// A container operand never reaches here: the container table above
		// decided it (cast_container.go).
		return v
	}
}

// castPortProtocolText reads a TEXT operand with PORT's or PROTOCOL's own
// input function: the IANA name, or a DECIMAL number held to the TYPE's range.
// It is the one reader the writer's doors use (parquet.DecimalIntegerText plus
// castIntInRange's bound), so a text this cast takes is a text that column can
// store.
func castPortProtocolText(s, dest string) any {
	if dest == "protocol" {
		if n, named := parquet.ProtocolNumberFromName(s); named {
			return int64(n)
		}
	}
	switch n, st := parquet.DecimalIntegerText(s); st {
	case parquet.NetTextOK:
		return castIntInRange(n, dest)
	case parquet.NetTextRange:
		raiseNumericOutOfRange("integer", s)
	}
	raiseInvalidTextRepresentation("integer", s)
	return nil
}

// castToReal narrows a value to float4, which is what `REAL`, `FLOAT4` and
// `FLOAT(1..24)` all name — one function so the three spellings cannot round
// differently (#652).
//
// REAL is a NARROWER type than the float64 every other numeric box in this
// engine carries, and this arm used to sit beside "float"/"double" and answer
// ToFloat64, so `CAST(x AS REAL)` was a NO-OP. PostgreSQL types the result
// float4 and rounds the value into it, which changes the answer of anything
// that compares it:
//
//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real) -> the row
//	CAST(1.0/3 AS REAL)        ->  0.33333334, not 0.3333333333333333
//
// Bare FLOAT is deliberately NOT here. PostgreSQL's unqualified `float` is
// `double precision` — verified with pg_typeof — so only the spellings that
// really name float4 narrow.
func (e *Cast) castToReal(v any) any {
	// TEXT is read by real's own input function, which REFUSES what it
	// cannot read rather than answering ToFloat64's zero (#839's sibling
	// hole: `CAST('abc' AS REAL)` answered 0).
	f, isText := castFloatText(v, "real", 32)
	if !isText {
		f = ToFloat64(v)
	}
	// PostgreSQL refuses a conversion that loses the value outright rather
	// than answering an infinity or a zero (float.c's overflow and underflow
	// checks, both SQLSTATE 22003). A value that is ALREADY infinite, or
	// already zero, is representable and passes through. kernel.Float32FitOf
	// is the one place that rule lives — the IN-list refusals read it too, so
	// `CAST(x AS REAL)` and `x IN (lit)` cannot disagree about what a real can
	// hold.
	if fit := kernel.Float32FitOf(f); fit != kernel.Float32Fits {
		raiseRealConversionError(e.Operand, f, fit)
	}
	return float32(f)
}

// castStringRender is the TEXT a CAST to the string family produces, for the
// unparameterized destinations and for VARCHAR(n) / CHAR(n) alike. It is one
// function because the two must not drift: `CAST(ts AS TEXT)` and
// `CAST(ts AS VARCHAR(4))` render the same instant, and the second is the
// first cut to four characters (#838). See the arm above for what each source
// family renders as and why.
func castStringRender(b *batch.RecordBatch, row int, operand Expr, v any) string {
	switch v.(type) {
	case []any, map[string]any:
		// A container's text is PostgreSQL's array_out / record_out — the
		// one renderer every door uses — under the operand's declaration,
		// which is what tells a TIMESTAMP element from a bigint one. Before
		// arc CW this fell to fmt.Sprint and `CAST(a AS TEXT)` was `[1 2 3]`.
		return batch.FormatPGText(v, containerOperandDecl(b, row, operand))
	case []float32:
		// A VECTOR's text is pgvector's vector_out, `[1,2,3]`.
		return batch.FormatPGText(v, nil)
	}
	text := boxedTextOperand(b, row, operand, v)
	if raw, ok := text.([]byte); ok {
		return `\x` + hex.EncodeToString(raw)
	}
	if s, ok := stringOperand(text); ok {
		return s
	}
	// A DOUBLE/REAL renders through the one float-text renderer (#1252,
	// review r5 P1) rather than fmt.Sprint's shortest %v, which switches to
	// exponent form once the exponent reaches the digit count:
	// `CAST(1234567.0 AS TEXT)` stored "1.234567e+06" where PostgreSQL's
	// float8out answers "1234567".
	switch fv := text.(type) {
	case float64:
		return batch.FormatFloat8Text(fv, 64)
	case float32:
		return batch.FormatFloat8Text(float64(fv), 32)
	}
	return fmt.Sprint(text)
}

// boxedTextOperand restores the column's display text so LIKE, CAST and
// scalar rendering agree with Vector.GetValue / likeTextRenderer.
// IPv4/MAC integer encodings, DATE day counts and widened FLOAT32 boxes
// must render as their declared values (#497, #521, ADR-0012).
// Resolve column references (including ROW fields) and temporal CASTs;
// other operand shapes pass v through unchanged.
// TestLikeAnswersTheSameAtBothSites sweeps flat types for rendering drift.
// See docs/internals/boxed-text-operand-rendering.md for the design.
func boxedTextOperand(b *batch.RecordBatch, row int, operand Expr, v any) any {
	// A CAST to a temporal type boxes its result exactly as the matching
	// COLUMN does (#340), so it needs the same undoing — and it did not get
	// it, because this resolver took only a bare column reference:
	// `CAST(CAST('1996-03-13 14:25:36' AS TIMESTAMP) AS TEXT)` answered
	// "826727136000" on the wire while the same cast over a COLUMN answered
	// the instant. FuncCall.formatTemporalArgs has had this arm since #273;
	// this is the same rule at the other text site (#544).
	if _, isCol := operand.(*ColRef); !isCol {
		// Every non-column temporal producer — the cast above, a clock
		// function, date arithmetic — boxes a unit producedTemporal names.
		if s, ok := renderTemporalBox(operand, b, v); ok {
			return s
		}
		// A bare DECIMAL LITERAL has the identical gap: Lit.Eval's float64
		// box loses the literal's own scale, so `CAST(2.50 AS TEXT)` read
		// "2.5" where PostgreSQL's numeric spelling is "2.50" — only
		// decimalType/evalDecimal carry the scale the literal was written
		// with (review r5 B1's "second spelling", #1252).
		if s, ok := decimalLitText(operand, b, row); ok {
			return s
		}
		return v
	}
	cr, ok := operand.(*ColRef)
	if !ok {
		return v
	}
	// A ROW FIELD PATH boxes exactly as a column of the field's type does
	// (ColRef.fieldValue), so it needs the same undoing — and gets it from
	// the same list, keyed on the FIELD's type (#568).
	switch cr.valueType() {
	case batch.TypeIPv4, batch.TypeMAC, batch.TypeDate, batch.TypeFloat32:
		dv, ok := cr.displayValue(b, row)
		if !ok {
			return v
		}
		return dv

	case batch.TypeTimestamp:
		// TIMESTAMP is the one type on this list whose display form
		// Vector.GetValue does NOT produce: it keeps the raw epoch-ms int64,
		// deliberately and with five consumers that need it (the GROUP BY
		// key, the aggregate and window spill row encoding, the window
		// comparator, the row map an UPDATE re-ingests) — see the comment on
		// that arm. So the rendering happens HERE, at the text site, from
		// the column's DECLARED type, which is the split FormatTimestamp's
		// own doc describes.
		//
		// Without it `CAST(c_ts AS STRING)` answered "1700000000000" and
		// `c_ts LIKE '2023%'` was false for 2023-11-14 (#544) — while the
		// SAME column projected over pgwire arrives as PostgreSQL's
		// `timestamp` text, because the send path converts under OID 1114
		// (#321). One connection, one column, two answers.
		//
		// displayValue is used for the read rather than Int64Data directly
		// because it goes through GetValue, which resolves a dictionary or
		// selection VIEW to its base row; the arms above rely on the same.
		dv, ok := cr.displayValue(b, row)
		if !ok {
			return v
		}
		ms, ok := dv.(int64)
		if !ok {
			return v // NULL (nil), or a box this column's type does not make
		}
		return batch.FormatTimestamp(ms)

	default:
		return v
	}
}

// stringOperand reports v's text when it is a string or byte slice — the two
// shapes a text column or literal reaches Cast.Eval in.
func stringOperand(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

// castToInterval is CAST(x AS INTERVAL): an INTERVAL passes through, TEXT is
// read by castToIntervalText, and anything else has no cast (PostgreSQL:
// 42846 `cannot cast type integer to interval`). The text used to fall
// through this switch unparsed, so `ts + CAST('1 day' AS INTERVAL)` —
// declared a TIMESTAMP shift — added the text's leading number as ONE
// MILLISECOND (arc VL round 4; round-3 review N2).
func castToInterval(v any) any {
	switch x := v.(type) {
	case IntervalValue:
		return x
	case string:
		return castToIntervalText(x)
	}
	panic(fatalEval{sqlerr.New("42846", "cannot cast type %s to interval", intervalSourceName(v))})
}

// intervalSourceName names a non-text operand's type for the 42846 sentence.
func intervalSourceName(v any) string {
	switch v.(type) {
	case int32:
		return "integer"
	case int64, int:
		return "bigint"
	case float32, float64:
		return "double precision"
	case bool:
		return "boolean"
	}
	return "numeric"
}
