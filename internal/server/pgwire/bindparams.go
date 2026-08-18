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
		// Confirm it really is a number before writing it unquoted.
		if _, err := strconv.ParseFloat(s, 64); err == nil {
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

	case oidText, oidVarchar, oidBPChar, oidNumeric:
		// PostgreSQL's binary form for these is the same bytes as the text
		// form. numeric is the exception in principle — its binary form is a
		// digit-group structure — but no driver sends binary numeric without
		// declaring the OID, and if one does, the text below is at worst a
		// quoted literal rather than injected SQL.
		return renderTextParam(string(raw), oid)

	default:
		// An unknown type in binary format cannot be read. Saying so beats
		// quoting the raw bytes, which is how a silent wrong answer starts.
		return "", fmt.Errorf("parameter of type OID %d sent in binary format is not supported", oid)
	}
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
