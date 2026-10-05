// SPDX-License-Identifier: MIT

package expr

import (
	"errors"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An invalid temporal TEXT cast raises through fatalEval, never answers
// SQL NULL (ADR-0012 item 1; #836, #840, #367).
// Use parquet.ParseDateDays / ParseTimestampMillis for both value and error:
// 22008 for a well-formed date naming no day, 22007 for invalid syntax,
// with the parser's PostgreSQL-compatible message.
// See docs/internals/temporal-cast-refusal-classification.md for the design.

// castTemporalText reads TEXT into epoch days or epoch milliseconds using
// parquet.ParseDateDays / ParseTimestampMillis for BOTH value and refusal.
// The accept-set must agree with ingestion, storage and filter kernels (#836).
// Non-text boxes decline; unparseable non-text casts keep their NULL rather
// than mislabel a type-pair failure as 22007 (ADR-0012 divergence list).
// NULL never reaches here: Cast.Eval returns before conversion.
// See docs/internals/temporal-text-cast-shared-parser.md for the design.
func castTemporalText(src any, kind castTemporalKindT) (any, bool) {
	text, ok := stringOperand(src)
	if !ok {
		return nil, false
	}
	if kind == castToDateKind {
		days, err := parquet.ParseDateDays(text)
		if err != nil {
			panic(fatalEval{err})
		}
		return int64(days), true
	}
	ms, err := parquet.ParseTimestampMillis(text)
	if err != nil {
		panic(fatalEval{err})
	}
	return ms, true
}

// castFloatText reads TEXT as a value of a float destination, refusing what
// PostgreSQL refuses instead of answering ToFloat64's zero.
//
// `CAST('abc' AS DOUBLE PRECISION)` answered 0. That is worse than the NULLs
// #840 closed and worse than a wrong type: a client gets a NUMBER, and zero is
// a plausible measurement. PostgreSQL raises 22P02 for it and 22003 for a
// well-formed number the type cannot carry, and the two are different answers
// — one sends the reader hunting a typo, the other says the number was read
// correctly and does not fit (the distinction #646 already draws at the
// comparison sites, through these same two error types).
//
// The accept-set is PostgreSQL's, verified live on 17.11: surrounding
// whitespace is trimmed, and `inf` / `infinity` / `nan` are VALUES in every
// case spelling. strconv.ParseFloat takes those, but it is Go's literal
// grammar, not strtod, in two places that answer where PostgreSQL refuses,
// so the float destinations refuse them here (#1411 measured case B3):
//
//   - a `_` digit separator: '1_0' read as 10 and '1e1_0' as 1e10, where
//     float4in / float8in raise 22P02 (numeric and the integers take `_`
//     since PostgreSQL 16; real and double precision never did);
//   - an underflow: ParseFloat rounds a value below the type's smallest
//     subnormal to zero with no error, so '1e-46' as real and '1e-400' as
//     double precision answered 0, where PostgreSQL raises 22003 for a
//     nonzero input that underflows (a subnormal such as '1e-45' as real or
//     '1e-320' as double precision is a value, and answers).
//
// ok=false means v is not text and the caller's own conversion stands.
func castFloatText(v any, destType string, bitSize int) (float64, bool) {
	s, isText := stringOperand(v)
	if !isText {
		return 0, false
	}
	trimmed := strings.TrimSpace(s)
	floatDest := destType == "real" || destType == "double precision"
	if floatDest && strings.ContainsRune(trimmed, '_') {
		panic(fatalEval{&InvalidLiteralError{Input: s, DestType: destType}})
	}
	f, err := strconv.ParseFloat(trimmed, bitSize)
	if err == nil {
		if floatDest && f == 0 && floatTextNonzero(trimmed) {
			panic(fatalEval{&NumericRangeError{Input: trimmed, DestType: destType}})
		}
		return f, true
	}
	var ne *strconv.NumError
	if errors.As(err, &ne) && ne.Err == strconv.ErrRange {
		// PostgreSQL names the number it read, without the padding it
		// trimmed: `CAST(' 1e400 ' AS REAL)` is `"1e400" is out of range`.
		panic(fatalEval{&NumericRangeError{Input: trimmed, DestType: destType}})
	}
	panic(fatalEval{&InvalidLiteralError{Input: s, DestType: destType}})
}

// floatTextNonzero reports whether a float's text that ParseFloat accepted
// names a nonzero value: a nonzero digit before its exponent. A text that
// parsed to zero but says this underflowed.
//
// A hexadecimal mantissa (`0xAp-2000`, which strtod reads as PostgreSQL does)
// has letter digits and its exponent after `p`, so `e` is a digit there:
// `CAST('0xAp-2000' AS REAL)` answered 0 where PostgreSQL raises 22003.
func floatTextNonzero(s string) bool {
	s = strings.TrimLeft(s, "+-")
	hex := len(s) > 1 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')
	if hex {
		s = s[2:]
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == 'p' || c == 'P', !hex && (c == 'e' || c == 'E'):
			return false
		case c >= '1' && c <= '9', hex && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'):
			return true
		}
	}
	return false
}
