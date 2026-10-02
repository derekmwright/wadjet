// SPDX-License-Identifier: MIT

package batch

import (
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// FormatPGJSON is PostgreSQL's to_json of val under its declared column: an
// ARRAY a JSON array, a ROW an object in its declared field order, a MAP an
// object keyed by each key's text, and each leaf as to_json writes it — a
// number or boolean bare, a TIMESTAMP in ISO 8601 with a `T`
// ("2024-01-01T01:00:00"), a non-finite float and every other scalar as the
// JSON string of its PostgreSQL text (FormatPGText). It is what
// `CAST(container AS JSON)` answers (arc CW round 2): PostgreSQL has no such
// cast and raises 42846, and the array's `{…}` text this engine answered
// instead is not JSON — json_array_length over it was NULL.
func FormatPGJSON(val any, col *parquet.Column) string {
	var b strings.Builder
	appendPGJSON(&b, val, col)
	return b.String()
}

func appendPGJSON(b *strings.Builder, val any, col *parquet.Column) {
	switch tv := val.(type) {
	case nil:
		b.WriteString("null")
		return
	case []any:
		if col != nil && col.Type == parquet.TypeMap {
			appendPGJSONMap(b, tv, col)
			return
		}
		var elem *parquet.Column
		if col != nil {
			elem = col.ElementType
		}
		b.WriteByte('[')
		for i, e := range tv {
			if i > 0 {
				b.WriteByte(',')
			}
			appendPGJSON(b, e, elem)
		}
		b.WriteByte(']')
		return
	case map[string]any:
		names, fields := compositeFieldOrder(tv, col)
		b.WriteByte('{')
		for i, n := range names {
			if i > 0 {
				b.WriteByte(',')
			}
			WriteJSONString(b, n)
			b.WriteByte(':')
			appendPGJSON(b, tv[n], fields[i])
		}
		b.WriteByte('}')
		return
	case bool:
		if tv {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
		return
	}
	if col != nil && col.Type == parquet.TypeTimestamp {
		if ms, ok := val.(int64); ok {
			WriteJSONString(b, strings.Replace(FormatTimestamp(ms), " ", "T", 1))
			return
		}
	}
	// A DATE's day count is a date under its declaration, whichever storage
	// width boxes it (FormatPGText's DATE arm): to_json writes it as a string.
	if col != nil && col.Type == parquet.TypeDate {
		switch val.(type) {
		case int32, int64:
			WriteJSONString(b, FormatPGText(val, col))
			return
		}
	}
	switch tv := val.(type) {
	case int64, int32, int:
		b.WriteString(FormatPGText(tv, col))
		return
	case float64:
		if !math.IsInf(tv, 0) && !math.IsNaN(tv) {
			b.WriteString(FormatPGFloat(tv, 64))
			return
		}
	case float32:
		if !math.IsInf(float64(tv), 0) && !math.IsNaN(float64(tv)) {
			b.WriteString(FormatPGFloat(float64(tv), 32))
			return
		}
	case string:
		// A numeric is written as its digits when they spell a JSON number
		// (its own scale: 2.25, -10.50, 0.0) and as a JSON string otherwise
		// (NaN), as datum_to_json writes one.
		if col != nil && col.Type == parquet.TypeDecimal && isJSONNumber(tv) {
			b.WriteString(tv)
			return
		}
	}
	WriteJSONString(b, FormatPGText(val, col))
}

// appendPGJSONMap writes a MAP's entries — one {key, value} ROW each, in the
// sorted-key order the engine stores them in — as a JSON object.
func appendPGJSONMap(b *strings.Builder, entries []any, col *parquet.Column) {
	keyName, valName := "key", "value"
	var keyCol, valCol *parquet.Column
	if col.ElementType != nil && len(col.ElementType.Fields) == 2 {
		keyName, valName = col.ElementType.Fields[0].Name, col.ElementType.Fields[1].Name
		keyCol, valCol = &col.ElementType.Fields[0], &col.ElementType.Fields[1]
	}
	b.WriteByte('{')
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		entry, _ := e.(map[string]any)
		WriteJSONString(b, FormatPGText(entry[keyName], keyCol))
		b.WriteByte(':')
		appendPGJSON(b, entry[valName], valCol)
	}
	b.WriteByte('}')
}

// WriteJSONString writes s as a JSON string literal the way PostgreSQL's
// escape_json does: `"` and `\` escaped, \b \f \n \r \t by name, every other
// control character below U+0020 as \u00XX, and everything else as itself —
// encoding/json's HTML escaping of `<`, `>`, `&` and its \u2028 / \u2029 are
// not PostgreSQL's.
func WriteJSONString(b *strings.Builder, s string) {
	const hexDigits = "0123456789abcdef"
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexDigits[c>>4])
				b.WriteByte(hexDigits[c&0xf])
				continue
			}
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
}

// isJSONNumber reports whether s is a JSON number literal (RFC 8259 §6):
// an optional minus, an integer part without a leading zero, an optional
// fraction and an optional exponent.
func isJSONNumber(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	digits := func() int {
		n := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			n++
		}
		return n
	}
	start := i
	if n := digits(); n == 0 || (n > 1 && s[start] == '0') {
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		if digits() == 0 {
			return false
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if digits() == 0 {
			return false
		}
	}
	return i == len(s)
}
