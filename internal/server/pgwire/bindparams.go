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

// quoteLiteral renders s as a single-quoted SQL string literal. Doubling the
// single quotes is the whole escape: this lexer reads ” inside a literal as
// one quote and treats a backslash as an ordinary character (the
// standard_conforming_strings=on behavior the server reports), so there is no
// backslash escape for a value to break out through.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

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
			return s, nil
		}
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return s, nil
		}
		return quoteLiteral(s), nil
	case oid == oidBool:
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "t", "true", "y", "yes", "on", "1":
			return "true", nil
		case "f", "false", "n", "no", "off", "0":
			return "false", nil
		}
		return quoteLiteral(s), nil
	default:
		// Text family, temporal types, uuid, and unknown. A quoted literal is
		// what these are, and unknown-OID-plus-text is the case the old
		// unconditional quoting was already right about.
		return quoteLiteral(s), nil
	}
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
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10), nil

	case oidFloat4:
		if len(raw) != 4 {
			return "", fmt.Errorf("float4 parameter has %d bytes, want 4", len(raw))
		}
		return formatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), 32)

	case oidFloat8:
		if len(raw) != 8 {
			return "", fmt.Errorf("float8 parameter has %d bytes, want 8", len(raw))
		}
		return formatFloat(math.Float64frombits(binary.BigEndian.Uint64(raw)), 64)

	case oidDate:
		if len(raw) != 4 {
			return "", fmt.Errorf("date parameter has %d bytes, want 4", len(raw))
		}
		days := int32(binary.BigEndian.Uint32(raw))
		return quoteLiteral(pgEpoch.AddDate(0, 0, int(days)).Format("2006-01-02")), nil

	case oidTimestamp, oidTimestampTZ:
		if len(raw) != 8 {
			return "", fmt.Errorf("timestamp parameter has %d bytes, want 8", len(raw))
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		return quoteLiteral(pgEpoch.Add(time.Duration(micros) * time.Microsecond).
			Format("2006-01-02T15:04:05Z07:00")), nil

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
		return quoteLiteral(h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]), nil

	case oidBytea:
		return quoteLiteral(`\x` + hex.EncodeToString(raw)), nil

	case oidNumeric:
		return renderBinaryNumeric(raw)

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

// renderBinaryNumeric decodes PostgreSQL's binary `numeric` wire format —
//
//	uint16 ndigits, int16 weight, uint16 sign, uint16 dscale, int16 digits[ndigits]
//
// (numeric_recv in the backend reads ndigits with pq_getmsgint(..., uint16),
// same as sign and dscale; weight is the one signed field — "we allow any
// int16 for weight", per its own comment) — where the value is
// sum(digits[i] * 10000^(weight-i)) under sign and dscale
// is the number of fraction digits to DISPLAY — into the exact decimal TEXT
// PostgreSQL itself would print for the same value, then hands that text to
// renderTextParam so the bare-literal path (and its "confirm it's a number"
// fallback) stays the one place a numeric literal gets written, text or
// binary.
//
// pgx v5 sends this for any parameter whose declared OID is 1700, which
// paraminfer.go now infers for a placeholder compared against a DECIMAL
// column — so this is the ordinary path for a Go client, not an exotic one
// (#464). Before this, oidNumeric fell into the same arm as oidText and the
// digit-group bytes were read back as if they were ASCII: garbage that
// failed renderTextParam's number check and went out as a quoted string,
// comparing a DECIMAL column to text and matching nothing (or, past `>`,
// coercing in whatever direction that comparison's own fallback took).
//
// This is the read side of appendBinaryNumeric/pgNumericDigits (server.go),
// which encodes the same format for values wadjet SENDS. Neither calls the
// other, so a header-arithmetic mistake here would not be caught by that
// side agreeing with itself.
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

	return renderTextParam(b.String(), oidNumeric)
}

// formatFloat renders a float as an unquoted SQL numeric literal. Infinities
// and NaN have no unquoted spelling, so they go out quoted.
func formatFloat(v float64, bits int) (string, error) {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return quoteLiteral(strconv.FormatFloat(v, 'g', -1, bits)), nil
	}
	return strconv.FormatFloat(v, 'g', -1, bits), nil
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

// substituteParams replaces every $N in sql with literals[N-1]. A placeholder
// numbered past the literals it was given is left as written — the statement
// then fails to parse, which is the honest outcome for a Bind that supplied
// fewer parameters than the statement uses.
func substituteParams(sql string, literals []string) string {
	refs := scanParamRefs(sql)
	if len(refs) == 0 {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	prev := 0
	for _, r := range refs {
		if r.n > len(literals) {
			continue
		}
		b.WriteString(sql[prev:r.start])
		b.WriteString(literals[r.n-1])
		prev = r.end
	}
	b.WriteString(sql[prev:])
	return b.String()
}

// substituteNullParams replaces every $N placeholder with NULL and reports
// whether the statement had any. Describe answers a statement's result shape
// before Bind has supplied values, so it runs the statement with NULL standing
// in for each parameter.
func substituteNullParams(sql string) (string, bool) {
	refs := scanParamRefs(sql)
	if len(refs) == 0 {
		return sql, false
	}
	nulls := make([]string, countParamPlaceholders(sql))
	for i := range nulls {
		nulls[i] = "NULL"
	}
	return substituteParams(sql, nulls), true
}
