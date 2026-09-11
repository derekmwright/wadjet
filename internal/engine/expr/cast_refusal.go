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
// case spelling. strconv.ParseFloat takes exactly those, which is why the
// parse is not hand-rolled.
//
// ok=false means v is not text and the caller's own conversion stands.
func castFloatText(v any, destType string, bitSize int) (float64, bool) {
	s, isText := stringOperand(v)
	if !isText {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), bitSize)
	if err == nil {
		return f, true
	}
	var ne *strconv.NumError
	if errors.As(err, &ne) && ne.Err == strconv.ErrRange {
		panic(fatalEval{&NumericRangeError{Input: s, DestType: destType}})
	}
	panic(fatalEval{&InvalidLiteralError{Input: s, DestType: destType}})
}

// canonicalUUID reads PostgreSQL's UUID input syntax and renders its output
// form. Measured live on 17.11: the hyphenated 8-4-4-4-12 spelling in any
// case, the same 32 hex digits with no hyphens, and either wrapped in braces
// are all accepted; SURROUNDING WHITESPACE is not (`' 123e…000 '` is 22P02
// there), which is why this does not trim. The output is always lowercase and
// hyphenated.
func canonicalUUID(s string) (string, bool) {
	t := s
	if len(t) >= 2 && t[0] == '{' && t[len(t)-1] == '}' {
		t = t[1 : len(t)-1]
	}
	var hexDigits [32]byte
	n := 0
	for i := 0; i < len(t); i++ {
		ch := t[i]
		if ch == '-' {
			// A hyphen is accepted only at a group boundary of the canonical
			// spelling, which is what keeps `1-2-3-4-5` out.
			if n != 8 && n != 12 && n != 16 && n != 20 {
				return "", false
			}
			continue
		}
		if n == 32 {
			return "", false
		}
		switch {
		case ch >= '0' && ch <= '9', ch >= 'a' && ch <= 'f':
			hexDigits[n] = ch
		case ch >= 'A' && ch <= 'F':
			hexDigits[n] = ch + ('a' - 'A')
		default:
			return "", false
		}
		n++
	}
	if n != 32 {
		return "", false
	}
	var b strings.Builder
	b.Grow(36)
	for i, c := range hexDigits {
		if i == 8 || i == 12 || i == 16 || i == 20 {
			b.WriteByte('-')
		}
		b.WriteByte(c)
	}
	return b.String(), true
}

// castToUUID is `CAST(x AS UUID)`, which used to return its operand unchanged
// under a STRING declaration — a cast that changed neither the value nor the
// declared type, so a non-UUID went through as if it were one (#839).
//
// A UUID COLUMN already boxes as its canonical text (ColRef.Eval's string arm
// covers TypeUUID), so a cast over one is a no-op that now says so; text is
// canonicalized or refused with 22P02 and PostgreSQL's message. Anything else
// passes through, for the reason raiseTemporalCastRefusal's boundary gives:
// PostgreSQL answers a wrong TYPE PAIR at parse time with 42846, and minting a
// data-exception code for one would be a different divergence.
func castToUUID(v any) any {
	s, isText := stringOperand(v)
	if !isText {
		return v
	}
	if u, ok := canonicalUUID(s); ok {
		return u
	}
	panic(fatalEval{&InvalidLiteralError{Input: s, DestType: "uuid"}})
}
