// SPDX-License-Identifier: MIT

package kernel

import (
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ByteaIn is PostgreSQL's `byteain`, the ONE reading of a text as bytea in this
// engine: every door a text becomes BYTES reads it here — a literal beside a
// BYTES column (#582), a literal assigned to one by INSERT, UPDATE, MERGE or
// COPY (#1501), `CAST(text AS BYTES)`, and a text-format bytea parameter. Four
// copies of this grammar lived in four packages before #1501 and no two agreed:
// the assignment door had none at all and stored the SPELLING, so a row written
// with `'\x6869'` did not match the same literal in a WHERE.
//
// byteain takes two forms, measured on PostgreSQL 17.11:
//
//	\x6869        the HEX form (hex_decode over the rest), which is what
//	              bytea_output produces; LOWERCASE x only — `\X6869` is the
//	              escape form, and 22P02
//	hi\000\\      the ESCAPE form: `\\` is one backslash, `\ooo` one byte
//	              (first digit 0-3), every other byte stands for itself, and a
//	              backslash followed by anything else is 22P02
//
// The error is PostgreSQL's own: 22023 for the hex form's two failures
// (`invalid hexadecimal data: odd number of digits`, `invalid hexadecimal
// digit: "z"`) and 22P02 `invalid input syntax for type bytea` for the escape
// form's.
func ByteaIn(s string) ([]byte, error) {
	if len(s) >= 2 && s[0] == '\\' && s[1] == 'x' {
		return ByteaHexDecode(s[2:])
	}
	return ByteaEscapeDecode(s)
}

// ByteaHexDecode is PostgreSQL's hex_decode: byteain's hex form after `\x`, and
// `decode(text, 'hex')`. Whitespace — space, tab, LF and CR, and nothing else —
// is skipped BETWEEN digit pairs only: `'\x68 69'` is two bytes, while
// `'\x6 869'` reads the space as the second digit of a pair and is 22023, and
// so is a vertical tab or a form feed anywhere (measured on 17.11).
func ByteaHexDecode(s string) ([]byte, error) {
	out := make([]byte, 0, len(s)/2)
	for i := 0; i < len(s); {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		}
		hi, ok := hexNibble(s[i])
		if !ok {
			return nil, invalidHexDigit(s[i:])
		}
		i++
		if i >= len(s) {
			return nil, sqlerr.New("22023", "invalid hexadecimal data: odd number of digits")
		}
		lo, ok := hexNibble(s[i])
		if !ok {
			return nil, invalidHexDigit(s[i:])
		}
		i++
		out = append(out, hi<<4|lo)
	}
	return out, nil
}

// ByteaEscapeDecode is byteain's escape form, and `decode(text, 'escape')`
// (PostgreSQL's esc_decode reads the same grammar and raises the same 22P02).
func ByteaEscapeDecode(s string) ([]byte, error) {
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
		if i+3 < len(s) && s[i+1] >= '0' && s[i+1] <= '3' && isOctalByte(s[i+2]) && isOctalByte(s[i+3]) {
			out = append(out, (s[i+1]-'0')<<6|(s[i+2]-'0')<<3|(s[i+3]-'0'))
			i += 3
			continue
		}
		return nil, sqlerr.New("22P02", "invalid input syntax for type bytea")
	}
	return out, nil
}

// invalidHexDigit names the offending CHARACTER, as PostgreSQL does (it reads
// one multibyte character there, not one byte).
func invalidHexDigit(rest string) error {
	_, n := utf8.DecodeRuneInString(rest)
	if n < 1 {
		n = 1
	}
	return sqlerr.New("22023", "invalid hexadecimal digit: %s", sqlerr.Quote(rest[:n]))
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func isOctalByte(c byte) bool { return c >= '0' && c <= '7' }

// ByteaLiteral is ByteaIn for a caller that only needs to know WHETHER the text
// names bytes: ok=false is text byteain refuses, and the caller that can raise
// asks ByteaIn for PostgreSQL's error.
func ByteaLiteral(s string) ([]byte, bool) {
	raw, err := ByteaIn(s)
	return raw, err == nil
}

// ByteaConstText is ByteaLiteral for a comparison CONSTANT, as the string the
// bytes-comparison kernels hold. A literal byteain refuses keeps its own
// spelling rather than becoming an error here: this is the arm every BYTES
// filter reads, the refusal belongs at the site that can raise it, and a
// silent empty constant would match nothing while `<>` matched everything.
func ByteaConstText(s string) string {
	if raw, ok := ByteaLiteral(s); ok {
		return string(raw)
	}
	return s
}
