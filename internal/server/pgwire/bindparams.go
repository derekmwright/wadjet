// SPDX-License-Identifier: MIT

package pgwire

// Extended-query parameter binding.
//
// Wadjet's planner has no bound-parameter path: a statement reaches it as SQL
// text. So Bind renders each parameter as a literal and substitutes it into
// the portal's SQL. That is only correct if the literal it writes has the type
// the parameter had — and it did not. Every parameter, of every type, was
// written as a single-quoted string, so `WHERE int_col = $1` bound with the
// integer 2 became `WHERE int_col = '2'`, which compares an integer column to
// a string and matches nothing. The query succeeded and returned no rows:
// a silent wrong answer, not an error (issue #305 item 3).
//
// The type is knowable. Parse carries the parameter OIDs the client declared,
// and Bind carries a format code per parameter. Together they say exactly how
// to read the bytes and how to write them back out as SQL.

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// PostgreSQL type OIDs this layer decodes. Values from the catalog's
// pg_type.oid; the same numbers pgTypeOID hands out in RowDescription.
const (
	oidUnknown     = 0
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidOID         = 26
	oidFloat4      = 700
	oidFloat8      = 701
	oidBPChar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTime        = 1083
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
	oidNumeric     = 1700
	oidUUID        = 2950
)

// pgEpoch is the origin PostgreSQL's binary date/time formats count from.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// numericOID reports whether a parameter of this type renders as a bare SQL
// numeric literal rather than a quoted string. Quoting one of these is the
// bug this file exists to fix: `int_col = '2'` matches nothing.
//
// numeric/decimal is included: it is arbitrary precision on the wire but its
// text form is a valid unquoted SQL number, and quoting it would compare a
// number to a string exactly as int4 did.
func numericOID(oid uint32) bool {
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidOID, oidFloat4, oidFloat8, oidNumeric:
		return true
	}
	return false
}

// integerOID reports the integer parameter types, whose text input is an
// integer's spelling only (PostgreSQL's int2in/int4in/int8in/oidin).
func integerOID(oid uint32) bool {
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidOID:
		return true
	}
	return false
}

// floatParamLiteral is a float parameter's text as a literal of its own type:
// `CAST('2.5' AS DOUBLE PRECISION)` (REAL for float4). The type is the
// parameter's, and a bare number is not one — it is a numeric literal here as
// in PostgreSQL.
func floatParamLiteral(text string, oid uint32) string {
	if oid == oidFloat4 {
		return "CAST(" + quoteLiteral(text) + " AS REAL)"
	}
	return "CAST(" + quoteLiteral(text) + " AS DOUBLE PRECISION)"
}

// quoteLiteral renders s as a single-quoted SQL string literal. Doubling the
// single quotes is the whole escape: this lexer reads ” inside a literal as
// one quote and treats a backslash as an ordinary character (the
// standard_conforming_strings=on behavior the server reports), so there is no
// backslash escape for a value to break out through.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// decodeByteaText reads PostgreSQL's TEXT representation of a bytea value
// into the bytes it denotes, the way byteain does:
//
//	\x48656c6c6f   hex form, the default bytea_output produces it
//	Hello\134\000  escape form: \\ is one backslash, \ooo one octal byte,
//	                and every other byte stands for itself
//
// A malformed spelling is an ERROR rather than a fallback to the raw text:
// the two forms are not ambiguous, and quietly binding the SPELLING of a
// value the client meant as bytes is how `WHERE b = $1` matches nothing.
func decodeByteaText(s string) ([]byte, error) {
	if strings.HasPrefix(s, `\x`) || strings.HasPrefix(s, `\X`) {
		raw, err := hex.DecodeString(s[2:])
		if err != nil {
			return nil, fmt.Errorf("bytea parameter is not valid hex: %w", err)
		}
		return raw, nil
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			out = append(out, s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == '\\' {
			out = append(out, '\\')
			i++
			continue
		}
		if i+3 < len(s) && isOctalDigit(s[i+1]) && isOctalDigit(s[i+2]) && isOctalDigit(s[i+3]) {
			v := (int(s[i+1]-'0') << 6) | (int(s[i+2]-'0') << 3) | int(s[i+3]-'0')
			if v > 0xFF {
				return nil, fmt.Errorf("bytea parameter has an octal escape past one byte: %q", s[i:i+4])
			}
			out = append(out, byte(v))
			i += 3
			continue
		}
		return nil, fmt.Errorf("bytea parameter has an invalid escape at offset %d", i)
	}
	return out, nil
}

func isOctalDigit(c byte) bool { return c >= '0' && c <= '7' }

// renderParam turns one Bind parameter into the SQL literal that stands in for
// it. raw is the parameter's bytes, binary reports the format code, and oid is
// what Parse declared for it (0 when the client left it to the server).
//
// An unparseable value for a numeric type falls back to a quoted literal
// rather than being spliced in bare: whatever it is, it is not a number, and
// a quoted literal can only ever be a wrong answer where bare text could be
// arbitrary SQL.
func renderParam(raw []byte, binaryFmt bool, oid uint32) (string, error) {
	if !binaryFmt {
		return renderTextParam(string(raw), oid)
	}
	return renderBinaryParam(raw, oid)
}

// renderTextParam handles the text format, where the bytes are PostgreSQL's
// own text representation of the value.
func renderTextParam(s string, oid uint32) (string, error) {
	switch {
	case oid == oidFloat4 || oid == oidFloat8:
		// A float parameter is a float wherever it lands. Spliced bare, its
		// text read as a NUMERIC literal (ADR-0024's literal rule), so a
		// float8 2.5 assigned to an integer column rounded half away from
		// zero where PostgreSQL rounds a float8 half to even (#1353:
		// a MERGE `SET n = $1` bound float8 2.5 stored 3, PostgreSQL 2).
		if _, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil && !errors.Is(err, strconv.ErrRange) {
			return quoteLiteral(s), nil
		}
		return floatParamLiteral(s, oid), nil
	case integerOID(oid):
		// int2in / int4in / int8in: an integer's spelling and nothing else.
		// A fraction or an exponent went out bare and was read as a
		// numeric literal, so `SET n = $1` (inferred int4) bound with the
		// text 2.5 stored 3 where PostgreSQL raises 22P02 at the
		// parameter's input function; quoted, the target's input rule
		// raises it here too.
		if _, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
			if oid == oidInt8 {
				// A bare integer that fits int4 is an int4 literal, so a
				// bigint parameter spliced bare declared integer (`SELECT
				// $1` answered OID 23 where PostgreSQL answers 20, #1410).
				// A bigint CAST is bigint whatever the value.
				return "CAST(" + strings.TrimSpace(s) + " AS BIGINT)", nil
			}
			return s, nil
		}
		return quoteLiteral(s), nil
	case numericOID(oid):
		// Confirm it really is a number before writing it unquoted. A range
		// error (1e400 overflowing to +Inf) still names a syntactically
		// valid number — ParseFloat's grammar accepted it and only
		// float64's exponent range could not hold it — and wadjet's DECIMAL
		// is not bound to float64, so the text itself, not the (unused)
		// parsed value, still splices as a bare literal here. Falling
		// through to quoteLiteral on ErrRange was the bug: it wrote a
		// numeric-shaped string as a quoted TEXT literal, comparing a
		// DECIMAL column to text for the one case (an out-of-range literal)
		// where the number really was a number. (Underflow does not take
		// this path: ParseFloat("1e-400") returns 0, nil — no ErrRange —
		// so it was already handled by the err == nil arm.)
		if _, err := strconv.ParseFloat(s, 64); err == nil || errors.Is(err, strconv.ErrRange) {
			if oid == oidNumeric {
				return numericParamLiteral(s), nil
			}
			return s, nil
		}
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return s, nil
		}
		return quoteLiteral(s), nil
	case oid == oidBytea:
		// PostgreSQL's TEXT input for bytea, byteain: either the hex form
		// `\x` + hex digits, or the historical escape form where a
		// backslash introduces `\\` for one backslash and `\ooo` for one
		// octal byte. Both denote BYTES, and what wadjet compares a BYTES
		// column against is the VALUE's bytes — so the literal written here
		// carries those bytes, not their spelling. Writing the spelling was
		// the defect: `WHERE b = $1` bound with the two bytes "hi" became
		// `WHERE b = '\x6869'`, a ten-character string against a two-byte
		// column, and matched nothing (#570).
		raw, err := decodeByteaText(s)
		if err != nil {
			return "", err
		}
		// Written back in byteain's HEX form rather than as the raw bytes.
		// Since #582 the engine reads a literal beside a BYTES column through
		// byteain too, so raw bytes carrying a backslash would be decoded a
		// SECOND time here — `\` would collapse to one byte and a lone
		// backslash would become a refusal. The hex form round-trips exactly,
		// whatever the bytes are, and is the spelling the server itself
		// produces.
		return `'\x` + hex.EncodeToString(raw) + `'`, nil
	case oid == oidBool:
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "t", "true", "y", "yes", "on", "1":
			return "true", nil
		case "f", "false", "n", "no", "off", "0":
			return "false", nil
		}
		return quoteLiteral(s), nil
	case oid == oidDate:
		return "CAST(" + quoteLiteral(s) + " AS DATE)", nil
	case oid == oidTimestamp:
		// A timestamp parameter is a TIMESTAMP wherever it lands. Spliced
		// as a bare quoted literal it was SQL's unknown, which a DATE
		// operand reads with the DATE input function — dropping the time
		// of day, so `d = $1` bound '1969-12-31 23:59:59.999' matched
		// 1969-12-31 where PostgreSQL matches nothing (#1426). Typed, it
		// reaches the DATE ↔ TIMESTAMP pair rule a TIMESTAMP literal does.
		return "CAST(" + quoteLiteral(s) + " AS TIMESTAMP)", nil
	case oid == oidTimestampTZ:
		return timestamptzParamLiteral(s), nil
	case oid == oidUUID:
		return "CAST(" + quoteLiteral(s) + " AS UUID)", nil
	default:
		// Text family and unknown: SQL's unknown literal, which the
		// position it lands in reads by its own input function — the
		// text-typed parameter's recorded divergence (ADR-0012
		// dml-assignment#r4): a text parameter beside an integer column
		// is read as an integer where PostgreSQL refuses the operator.
		return quoteLiteral(s), nil
	}
}

// numericParamLiteral is a numeric parameter's text as a NUMERIC value. A
// spelling with a fraction or an exponent is a numeric literal as it stands;
// an integer spelling is not — `7` is an integer literal, so `SELECT $1`
// declared int4 — and is written `7.`, the numeric literal of the same value
// (`SELECT 7.` is numeric 7 here as in PostgreSQL). A spelling of more
// significant digits than a double carries is written as a DECIMAL CAST of
// exactly its digits, because this engine reads such a literal as a double.
func numericParamLiteral(s string) string {
	t := strings.TrimSpace(s)
	t = strings.TrimPrefix(t, "+")
	digits, scale, plain := numericSpellingDigits(t)
	if !plain {
		return s
	}
	if digits > 15 {
		precision := digits
		if precision < scale {
			precision = scale
		}
		if precision <= 38 {
			return fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", quoteLiteral(t), precision, scale)
		}
		return t
	}
	if !strings.ContainsAny(t, ".eE") {
		return t + "."
	}
	return t
}

// numericSpellingDigits counts the significant digits and the fraction
// digits of a plain decimal spelling ([-]digits[.digits]); plain is false
// for an exponent or any other shape, which is left as the client wrote it.
func numericSpellingDigits(t string) (digits, scale int, plain bool) {
	t = strings.TrimPrefix(t, "-")
	if t == "" || strings.ContainsAny(t, "eE") {
		return 0, 0, false
	}
	intPart, frac, _ := strings.Cut(t, ".")
	for _, part := range []string{intPart, frac} {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return 0, 0, false
			}
		}
	}
	if intPart == "" && frac == "" {
		return 0, 0, false
	}
	intPart = strings.TrimLeft(intPart, "0")
	return len(intPart) + len(frac), len(frac), true
}

// timestamptzParamLiteral is a timestamptz parameter's text as the TIMESTAMP
// of the instant it names. This engine has no time zone type: an instant is a
// TIMESTAMP read at UTC, the session TimeZone the server reports. A spelling
// with no zone is that wall clock at UTC, as PostgreSQL reads it under
// TimeZone=UTC; a spelling with an offset names an instant, which is
// converted to UTC here — TIMESTAMP input DISCARDS an offset (PostgreSQL's
// timestamp rule), so splicing the text as it stands would have moved the
// instant by the offset.
//
// The offset is read by the ONE timestamp grammar (parquet.ParseTimestampZone:
// `Z`, `±hh`, `±hh:mm`, `±hhmm`, `±hh:mm:ss`, with or without a space before
// it). A zone NAME (`UTC`, `America/New_York`) is outside that grammar and
// is refused as the TIMESTAMP input refuses it (temporal catalog).
func timestamptzParamLiteral(s string) string {
	if wall, off, zoned, ok := parquet.ParseTimestampZone(s); ok && zoned {
		at := wall.Add(-time.Duration(off) * time.Second)
		return "CAST(" + quoteLiteral(at.Format("2006-01-02 15:04:05.999999999")) + " AS TIMESTAMP)"
	}
	return "CAST(" + quoteLiteral(s) + " AS TIMESTAMP)"
}

// paramNullLiteral is a NULL parameter of type oid: a NULL of that type, so a
// statement's Describe (which stands NULL in for every parameter) and a Bind
// of NULL declare what the bound value declares. A client that reads its rows
// by the statement's Describe (pgx's prepared statements, pgJDBC's
// server-prepared ones) decodes binary results by those declarations, so a
// stand-in that declares another type than the value is a wrong value, not a
// cosmetic one: `SELECT $1 + 1` with an untyped NULL standing in for an
// integer parameter described float8, the value 2 executed as an integer,
// and pgx decoded its eight bytes as the double 1.5e-323.
//
// An integer is NULLIF(0, 0), the int4 NULL — the type every non-negative
// int4 literal declares. (A NEGATIVE integer literal declares bigint in this
// engine; that residual is recorded in the arc notes.) Text and unknown stay
// the untyped NULL, which is SQL's unknown as their values are.
func paramNullLiteral(oid uint32) string {
	switch oid {
	case oidInt2, oidInt4:
		return "NULLIF(0, 0)"
	case oidBool:
		return "CAST(NULL AS BOOLEAN)"
	case oidInt8:
		return "CAST(NULL AS BIGINT)"
	case oidFloat4:
		return "CAST(NULL AS REAL)"
	case oidFloat8:
		return "CAST(NULL AS DOUBLE PRECISION)"
	case oidNumeric:
		// CAST(NULL AS NUMERIC) is a double here (an unconstrained NUMERIC
		// is carried as one); a numeric literal's NULLIF is numeric.
		return "NULLIF(0.0, 0.0)"
	case oidDate:
		return "CAST(NULL AS DATE)"
	case oidTimestamp, oidTimestampTZ:
		return "CAST(NULL AS TIMESTAMP)"
	case oidUUID:
		return "CAST(NULL AS UUID)"
	}
	return "NULL"
}

// binaryTimestampInstant reads a binary timestamp/timestamptz parameter —
// int64 microseconds since 2000-01-01 UTC — as the instant it names.
//
// Two things the obvious `pgEpoch.Add(time.Duration(micros) *
// time.Microsecond)` gets wrong (#1266's producer census): time.Duration
// holds nanoseconds, so the multiply wraps silently for any instant more than
// ~292 years from 2000 (before 1708, after 2292) and the parameter named a
// different year; and the literal it was rendered into dropped the fraction,
// so `12:30:45.5` bound as `12:30:45`. The seconds and the remainder are
// split with a FLOORED division here instead, and the caller keeps the
// fraction to the microsecond (the engine then floors it to its millisecond
// carrier, exactly as it does the same literal typed as text).
//
// PostgreSQL's `infinity` / `-infinity` are the int64 extremes on the wire.
// The engine's TIMESTAMP has no infinity, so they are refused rather than
// bound as the year 294247 they would otherwise decode to.
func binaryTimestampInstant(micros int64) (time.Time, error) {
	if micros == math.MaxInt64 || micros == math.MinInt64 {
		return time.Time{}, fmt.Errorf("timestamp parameter is infinity, which a TIMESTAMP cannot hold")
	}
	secs := micros / 1_000_000
	rem := micros % 1_000_000
	if rem < 0 {
		secs--
		rem += 1_000_000
	}
	return time.Unix(pgEpoch.Unix()+secs, rem*1000).UTC(), nil
}

// renderBinaryParam handles the binary format, where the bytes are
// PostgreSQL's network representation: big endian throughout, integers
// two's complement, floats IEEE 754, date/time counted from 2000-01-01 UTC.
//
// pgx sends binary by default, so this is the ordinary path for a Go client
// rather than an exotic one.
func renderBinaryParam(raw []byte, oid uint32) (string, error) {
	switch oid {
	case oidBool:
		if len(raw) != 1 {
			return "", fmt.Errorf("bool parameter has %d bytes, want 1", len(raw))
		}
		if raw[0] == 0 {
			return "false", nil
		}
		return "true", nil

	case oidInt2:
		if len(raw) != 2 {
			return "", fmt.Errorf("int2 parameter has %d bytes, want 2", len(raw))
		}
		return strconv.FormatInt(int64(int16(binary.BigEndian.Uint16(raw))), 10), nil

	case oidInt4:
		if len(raw) != 4 {
			return "", fmt.Errorf("int4 parameter has %d bytes, want 4", len(raw))
		}
		return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(raw))), 10), nil

	case oidOID:
		if len(raw) != 4 {
			return "", fmt.Errorf("oid parameter has %d bytes, want 4", len(raw))
		}
		return strconv.FormatUint(uint64(binary.BigEndian.Uint32(raw)), 10), nil

	case oidInt8:
		if len(raw) != 8 {
			return "", fmt.Errorf("int8 parameter has %d bytes, want 8", len(raw))
		}
		return "CAST(" + strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10) + " AS BIGINT)", nil

	case oidFloat4:
		if len(raw) != 4 {
			return "", fmt.Errorf("float4 parameter has %d bytes, want 4", len(raw))
		}
		return floatParamLiteral(strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), 'g', -1, 32), oidFloat4), nil

	case oidFloat8:
		if len(raw) != 8 {
			return "", fmt.Errorf("float8 parameter has %d bytes, want 8", len(raw))
		}
		return floatParamLiteral(strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(raw)), 'g', -1, 64), oidFloat8), nil

	case oidDate:
		if len(raw) != 4 {
			return "", fmt.Errorf("date parameter has %d bytes, want 4", len(raw))
		}
		days := int32(binary.BigEndian.Uint32(raw))
		return renderTextParam(pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02"), oidDate)

	case oidTimestamp, oidTimestampTZ:
		if len(raw) != 8 {
			return "", fmt.Errorf("timestamp parameter has %d bytes, want 8", len(raw))
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		t, err := binaryTimestampInstant(micros)
		if err != nil {
			return "", err
		}
		// The instant at UTC, which is what both types name here: a
		// timestamp's wall clock and a timestamptz's instant.
		return renderTextParam(t.Format("2006-01-02 15:04:05.999999"), oidTimestamp)

	case oidTime:
		if len(raw) != 8 {
			return "", fmt.Errorf("time parameter has %d bytes, want 8", len(raw))
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		return quoteLiteral(time.Time{}.Add(time.Duration(micros) * time.Microsecond).
			Format("15:04:05.999999")), nil

	case oidUUID:
		if len(raw) != 16 {
			return "", fmt.Errorf("uuid parameter has %d bytes, want 16", len(raw))
		}
		h := hex.EncodeToString(raw)
		return renderTextParam(h[0:8]+"-"+h[8:12]+"-"+h[12:16]+"-"+h[16:20]+"-"+h[20:], oidUUID)

	case oidBytea:
		// The binary form of a bytea parameter IS the value's bytes
		// (bytearecv), so they go straight into the literal. Rendering them
		// as `\x` + hex instead wrote a TEN-character string literal for a
		// two-byte value, which compared against a BYTES column matched
		// nothing — the silent-wrong-answer shape this whole file exists to
		// close, on the one type it had left open (#570).
		return quoteLiteral(string(raw)), nil

	case oidNumeric:
		text, err := renderBinaryNumeric(raw)
		if err != nil {
			return "", err
		}
		return renderTextParam(text, oidNumeric)

	case oidText, oidVarchar, oidBPChar:
		// PostgreSQL's binary form for these is the same bytes as the text
		// form.
		return renderTextParam(string(raw), oid)

	default:
		// An unknown type in binary format cannot be read. Saying so beats
		// quoting the raw bytes, which is how a silent wrong answer starts.
		return "", fmt.Errorf("parameter of type OID %d sent in binary format is not supported", oid)
	}
}

// PostgreSQL's sign field for binary `numeric` (numeric_recv /
// numeric_send in the backend). pgPositive/pgNegative are ordinary values;
// the other three name special values wadjet's DECIMAL cannot hold.
const (
	pgNumericSignPositive uint16 = 0x0000
	pgNumericSignNegative uint16 = 0x4000
	pgNumericSignNaN      uint16 = 0xC000
	pgNumericSignPosInf   uint16 = 0xD000
	pgNumericSignNegInf   uint16 = 0xF000
)

// renderBinaryNumeric decodes uint16 ndigits, int16 weight, uint16 sign/dscale
// and base-10000 digits into exact decimal text (#464).
// Only weight is signed; value is sum(digit[i]*10000^(weight-i)) with sign,
// while dscale controls displayed fractional digits.
// The text is the value's exact decimal spelling; renderBinaryParam passes it
// through renderTextParam so text and binary numeric literals share one path. appendBinaryNumeric/pgNumericDigits encode the
// other direction independently; their self-consistency cannot prove this decoder.
// See docs/internals/pgwire-binary-numeric-input.md for the design.
func renderBinaryNumeric(raw []byte) (string, error) {
	if len(raw) < 8 {
		return "", fmt.Errorf("numeric parameter has %d bytes, want at least 8", len(raw))
	}
	// ndigits is UNSIGNED on the wire (numeric_recv: `(uint16)
	// pq_getmsgint(...)`). Reading it as int16 rejected anything with the
	// high bit set as a "negative digit count" — including legitimate,
	// PostgreSQL-emitted values: (1e131071+1)::numeric, a number at the
	// documented max of 131072 digits before the decimal point, sends
	// ndigits=32768, which read as int16 is -32768.
	ndigits := int(binary.BigEndian.Uint16(raw[0:2]))
	weight := int(int16(binary.BigEndian.Uint16(raw[2:4])))
	sign := binary.BigEndian.Uint16(raw[4:6])
	dscale := int(binary.BigEndian.Uint16(raw[6:8]))

	switch sign {
	case pgNumericSignPositive, pgNumericSignNegative:
		// Ordinary value; handled below.
	case pgNumericSignNaN:
		return "", fmt.Errorf("numeric parameter is NaN, which wadjet DECIMAL cannot represent")
	case pgNumericSignPosInf, pgNumericSignNegInf:
		return "", fmt.Errorf("numeric parameter is infinite, which wadjet DECIMAL cannot represent")
	default:
		return "", fmt.Errorf("numeric parameter has an unrecognized sign %#04x", sign)
	}
	// ndigits is inherently >= 0 now that it is read unsigned; the real
	// well-formedness guard is that the client actually supplied that many
	// digit groups on the wire — same thing numeric_recv relies on (it
	// allocates ndigits digits and then reads that many uint16s from the
	// buffer, which errors on a short read).
	if want := 8 + 2*ndigits; len(raw) != want {
		return "", fmt.Errorf("numeric parameter has %d bytes, want %d for %d digits", len(raw), want, ndigits)
	}
	// dscale is the number of digits DISPLAYED after the decimal point, and
	// the loop below writes exactly that many characters. numeric_recv
	// bounds it to 14 bits (NUMERIC_DSCALE_MASK, `(value.dscale &
	// NUMERIC_DSCALE_MASK) != value.dscale` in the backend) — PostgreSQL's
	// own documented "16383 digits after the decimal point" maximum — and
	// rejects anything wider with "invalid scale in external numeric
	// value". Mirror that here: without it, dscale up to 65535 (the full
	// uint16 read above already allows, since it can never go negative)
	// writes up to 65535 fraction characters from a dscale field that costs
	// the client 2 wire bytes to set, four times PostgreSQL's own limit.
	//
	// weight gets no equivalent cap: it is already read as int16 above, so
	// its range is exactly [-32768, 32767] — PG_INT16_MAX, which
	// numeric_recv's own comment ("we allow any int16 for weight") confirms
	// is the type's whole legal range, and which is where PostgreSQL's
	// "131072 digits before the decimal point" limit comes from
	// ((32767+1) groups of 4 digits). The integer-part loop below can still
	// write up to 131072 characters from a weight field that costs 2 wire
	// bytes and one real digit group to set — that is PostgreSQL's own
	// documented maximum-precision NUMERIC, not a defect to cap further.
	const pgNumericDscaleMax = 0x3FFF // NUMERIC_DSCALE_MASK
	if dscale > pgNumericDscaleMax {
		return "", fmt.Errorf("numeric parameter has a display scale %d, want 0-%d", dscale, pgNumericDscaleMax)
	}

	digits := make([]int16, ndigits)
	for i := range digits {
		d := int16(binary.BigEndian.Uint16(raw[8+2*i : 10+2*i]))
		if d < 0 || d > 9999 {
			return "", fmt.Errorf("numeric parameter digit %d is %d, want 0-9999", i, d)
		}
		digits[i] = d
	}
	// digitAt reads a base-10000 digit by its position in the value, not its
	// index into the (trimmed) wire array: PostgreSQL omits leading and
	// trailing all-zero digit groups on the wire, so any position before
	// digit 0 or past the last one is an implicit zero group.
	digitAt := func(i int) int16 {
		if i < 0 || i >= ndigits {
			return 0
		}
		return digits[i]
	}

	var b strings.Builder
	if sign == pgNumericSignNegative && ndigits > 0 {
		// PostgreSQL never signs a zero value (ndigits == 0 always carries
		// the positive sign), so this stays unsigned rather than echoing a
		// stray negative sign on a zero this decoder was handed.
		b.WriteByte('-')
	}

	if ndigits == 0 || weight < 0 {
		b.WriteByte('0')
	} else {
		for i := 0; i <= weight; i++ {
			if i == 0 {
				b.WriteString(strconv.FormatInt(int64(digitAt(i)), 10))
			} else {
				fmt.Fprintf(&b, "%04d", digitAt(i))
			}
		}
	}

	if dscale > 0 {
		b.WriteByte('.')
		i := weight + 1
		remaining := dscale
		for remaining > 0 {
			group := fmt.Sprintf("%04d", digitAt(i))
			take := remaining
			if take > 4 {
				take = 4
			}
			b.WriteString(group[:take])
			remaining -= take
			i++
		}
	}

	return b.String(), nil
}

// paramRef is one $N placeholder: its byte range in the statement and the
// 1-based parameter number it names.
type paramRef struct {
	start, end int
	n          int
}

// scanParamRefs finds the $N placeholders in sql. It skips the two places a
// $N spelling is not a placeholder: inside a single-quoted string literal and
// inside a double-quoted identifier. (This dialect has no comments and no
// dollar quoting — a bare $ outside a literal is a lex error — so a $ found
// outside those two is a placeholder or nothing.)
//
// Substituting through one scan is also what keeps $1 from matching the first
// two characters of $10, which a per-parameter strings.Replace did: a ten
// parameter statement had its tenth placeholder half-rewritten.
func scanParamRefs(sql string) []paramRef {
	var refs []paramRef
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '\'':
			// Single-quoted literal; '' is an escaped quote, not the end.
			for i++; i < len(sql); i++ {
				if sql[i] != '\'' {
					continue
				}
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					continue
				}
				break
			}
		case '"':
			// Double-quoted identifier; "" is an escaped quote.
			for i++; i < len(sql); i++ {
				if sql[i] != '"' {
					continue
				}
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
					continue
				}
				break
			}
		case '$':
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if j == i+1 {
				continue // a lone $, not a placeholder
			}
			n, err := strconv.Atoi(sql[i+1 : j])
			if err != nil || n < 1 {
				i = j - 1
				continue
			}
			refs = append(refs, paramRef{start: i, end: j, n: n})
			i = j - 1
		}
	}
	return refs
}

// countParamPlaceholders returns the highest parameter number the statement
// refers to, which is how many parameters it takes. $1 may appear twice and
// $2 may not appear at all; the count is the maximum, not the occurrences.
func countParamPlaceholders(sql string) int {
	max := 0
	for _, r := range scanParamRefs(sql) {
		if r.n > max {
			max = r.n
		}
	}
	return max
}

// substituteStandIns replaces every $N placeholder with a stand-in and reports
// whether the statement had any. Describe answers a statement's result shape
// before Bind has supplied values, so it runs the statement with a NULL of each
// parameter's type standing in for it (paramNullLiteral) — and, in a LIMIT,
// OFFSET or FETCH count, where the count grammar reads a number and no NULL,
// with 0, which changes no column's type.
func substituteStandIns(sql string, oids []uint32) (string, bool) {
	refs := scanParamRefs(sql)
	if len(refs) == 0 {
		return sql, false
	}
	positions := paramPositions(sql, refs)
	i := 0
	return substituteRefs(sql, refs, func(r paramRef) (string, bool) {
		pos := positions[i]
		i++
		switch pos {
		case posCount:
			return "0", true
		case posWindowInt, posWindowDefault:
			// The window argument grammar reads a constant: NULL is one.
			return "NULL", true
		}
		var oid uint32
		if r.n <= len(oids) {
			oid = oids[r.n-1]
		}
		return paramNullLiteral(oid), true
	}), true
}

// untypedLiteral is a typed literal without its type's CAST: `CAST('2024-03-04'
// AS DATE)` is `'2024-03-04'`. It is the spelling for LAG / LEAD's default,
// which this engine evaluates only as a literal: a CAST there is stored as its
// text and refused at execution (a window-argument defect recorded in the arc
// notes), so the default takes the value's literal and is read by the column's
// type, as it was before parameters were typed.
func untypedLiteral(lit string) string {
	if !strings.HasPrefix(lit, "CAST(") || !strings.HasSuffix(lit, ")") {
		return lit
	}
	if i := strings.LastIndex(lit, " AS "); i > len("CAST(") {
		return lit[len("CAST("):i]
	}
	return lit
}

// substituteRefs replaces each placeholder in refs with lit(ref); a ref for
// which lit answers false is left as written.
func substituteRefs(sql string, refs []paramRef, lit func(paramRef) (string, bool)) string {
	var b strings.Builder
	b.Grow(len(sql))
	prev := 0
	for _, r := range refs {
		text, ok := lit(r)
		if !ok {
			continue
		}
		b.WriteString(sql[prev:r.start])
		b.WriteString(text)
		prev = r.end
	}
	b.WriteString(sql[prev:])
	return b.String()
}

// renderCountParam renders a parameter standing in a LIMIT, OFFSET or FETCH
// count or a TABLESAMPLE percentage. The count grammar reads a number token
// (or a float parameter's CAST, which it reads as its number), so an integer
// or numeric parameter is its number there rather than its typed literal; any
// other type renders as everywhere.
func renderCountParam(raw []byte, binaryFmt bool, oid uint32) (string, error) {
	if oid == oidNumeric {
		// A numeric count is the number as the client spelled it (an
		// integer spelling is the count; a fraction is the bare
		// spelling's refusal) — not the `7.` that types it numeric.
		text := string(raw)
		if binaryFmt {
			var err error
			if text, err = renderBinaryNumeric(raw); err != nil {
				return "", err
			}
		}
		if _, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err == nil {
			return strings.TrimSpace(text), nil
		}
		return quoteLiteral(text), nil
	}
	if !integerOID(oid) {
		return renderParam(raw, binaryFmt, oid)
	}
	if binaryFmt {
		switch {
		case oid == oidInt2 && len(raw) == 2:
			return strconv.FormatInt(int64(int16(binary.BigEndian.Uint16(raw))), 10), nil
		case oid == oidInt4 && len(raw) == 4:
			return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(raw))), 10), nil
		case oid == oidInt8 && len(raw) == 8:
			return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10), nil
		}
		return renderParam(raw, binaryFmt, oid)
	}
	t := strings.TrimSpace(string(raw))
	if _, err := strconv.ParseInt(t, 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		return t, nil
	}
	return quoteLiteral(string(raw)), nil
}
