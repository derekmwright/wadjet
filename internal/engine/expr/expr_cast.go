// This file holds expr cast; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
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
	switch dest {
	// Keep this label list and IsIntegerCastDest in step: that predicate is
	// what tells the DAG's gather materialization to build an INT64 vector
	// for this destination (#813), and a label here it does not know would
	// put the same query's answer in a float64 one.
	// INT32, PORT and PROTOCOL are here because they are integer destinations
	// this engine HAS and this switch did not implement: all three fell to
	// `default: return v` and answered the operand unchanged under a STRING
	// declaration, so `3000000000::INT32` answered 3000000000 where
	// PostgreSQL raises `integer out of range` for the same magnitude, and
	// `3000000000::PORT` answered it under a type whose whole carrier is a
	// signed 32-bit field (#901). castIntInRange carries their bound; PORT
	// and PROTOCOL then reach a PORT/PROTOCOL vector, whose own int4 guard is
	// the second net (batch.IntegerRangeError).
	case "int", "integer", "int4", "int32", "bigint", "int8", "signed", "smallint", "int2",
		"port", "protocol":
		// A string that does not read as a number is refused, not coerced to
		// 0: PostgreSQL raises 22P02 invalid_text_representation and ADR-0012
		// makes it the authority on error-versus-not. The per-row error
		// channel #340 lacked exists now — FatalEvalPanic, #347 — which is
		// what this raise rides.
		//
		// A value that DOES read as a number then follows PostgreSQL's other
		// rule (#373): a fractional cast to an integer type ROUNDS, half away
		// from zero. TRUNC() is how a caller asks for truncation. An
		// already-integral value passes through untouched.
		// An exact DECIMAL is read on its own carrier, not through a double:
		// strconv.ParseFloat loses every digit past the sixteenth, so a
		// DECIMAL(38,10) holding 493827160549382.7160549350 came back as the
		// nearest double's integer part, and a value past the destination's
		// range came back as whatever the float conversion produced instead
		// of the refusal PostgreSQL gives (ADR-0024 item 4).
		if i, ok := castDecimalToInt(v, dest); ok {
			return i
		}
		if s, ok := stringOperand(v); ok {
			typ := "integer"
			if dest == "bigint" || dest == "int8" || dest == "signed" {
				typ = "bigint"
			}
			// PostgreSQL's INTEGER input grammar FIRST, which is a strict
			// superset of Go's base-10 one: `'0x1A'::integer` is 26 there,
			// `'0o17'` 15, `'0b101'` 5, `'1_000'` 1000 and `'017'` decimal
			// seventeen. kernel.IntLitText is the one reader the comparison
			// kernels, the row path and the plan-time refusal already share,
			// so the CAST door cannot disagree with them about which strings
			// name an integer (#634).
			switch n, st := kernel.IntLitText(s); st {
			case kernel.NumConstOK:
				return castIntInRange(n, dest)
			case kernel.NumConstRange:
				raiseNumericOutOfRange(typ, s)
			}
			// Not an integer under that grammar. A FRACTIONAL string still
			// casts — PostgreSQL rounds `'26.7'::integer` to 27 — so the
			// float reader is the fallback, not the first try.
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				raiseInvalidTextRepresentation(typ, s)
			}
			return castIntInRange(castFloatToInt64(f, dest), dest)
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
	case "uuid":
		return castToUUID(v)
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
		return v
	}
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
	text := boxedTextOperand(b, row, operand, v)
	if raw, ok := text.([]byte); ok {
		return `\x` + hex.EncodeToString(raw)
	}
	if s, ok := stringOperand(text); ok {
		return s
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
	if c, ok := operand.(*Cast); ok {
		ms, isInt := v.(int64)
		if !isInt {
			return v
		}
		switch castTemporalKind(c.DestType) {
		case castToDateKind:
			return batch.FormatDate(int32(ms))
		case castToTimestampKind:
			return batch.FormatTimestamp(ms)
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
