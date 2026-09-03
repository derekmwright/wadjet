package expr

import (
	"errors"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The CAST refusals that used to be a NULL.
//
// ADR-0012 item 1 makes PostgreSQL the authority on error-versus-not, and
// protocol rule 8 states the consequence: when a value cannot be produced,
// the ERROR is the answer. A CAST that answers NULL for text naming no value
// of the destination type is indistinguishable, at every consumer, from a
// CAST over a NULL input — so `WHERE CAST(s AS DATE) IS NULL` counted 5000
// rows of unparseable text as if the column had been empty (#836, #840).
//
// The per-row error channel these ride is `fatalEval`, the same one the
// numeric casts have used since #367; #836 is the issue that noticed
// ADR-0012's residual text ("the CAST path has no per-row error channel for
// a temporal conversion") was contradicted by the tree.
//
// The classification is the parquet package's, not a second copy:
// `parquet.ParseDateDays` / `parquet.ParseTimestampMillis` already separate
// PostgreSQL's two temporal SQLSTATEs — 22008 (datetime_field_overflow) for
// a well-formed date naming no day, 22007 (invalid_datetime_format) for text
// that is not a date at all — and carry PostgreSQL's own message. Measured
// live on postgres:17.11:
//
//	CAST('not-a-date' AS DATE)              22007  invalid input syntax for type date: "not-a-date"
//	CAST('2020-02-30' AS DATE)              22008  date/time field value out of range: "2020-02-30"
//	CAST('not-a-ts' AS TIMESTAMP)           22007  invalid input syntax for type timestamp: "not-a-ts"
//	CAST('2020-02-30 12:00:00' AS TIMESTAMP) 22008 date/time field value out of range: "2020-02-30 12:00:00"

// raiseTemporalCastRefusal aborts a CAST to DATE / TIMESTAMP whose operand is
// TEXT that names no instant.
//
// The boundary is deliberate and is a claim the corpus attempts from both
// sides: only a TEXT operand raises. Every other Go box that fails to parse —
// a boolean, a container, a value type with no temporal reading at all — is
// a TYPE-PAIR failure, which PostgreSQL answers at PARSE time with 42846
// (`cannot cast type boolean to date`) and not with a data exception. Minting
// 22007 for those would put a data-exception code on a type error, so they
// keep the NULL they have and are recorded in ADR-0012's divergence list
// rather than given a code PostgreSQL does not use.
//
// A NULL operand never reaches here: Cast.Eval returns before the conversion.
func raiseTemporalCastRefusal(src any, kind castTemporalKindT) {
	text, ok := stringOperand(src)
	if !ok {
		return
	}
	if kind == castToDateKind {
		if _, err := parquet.ParseDateDays(text); err != nil {
			panic(fatalEval{err})
		}
		// ParseDateDays accepts text parseDateArg refused. The two accept-sets
		// are not identical and the classifier is the authority on the CODE
		// only, never on the VALUE — so a disagreement is still a refusal,
		// under the syntax class.
		panic(fatalEval{&parquet.DateParseError{Text: text}})
	}
	if _, err := parquet.ParseTimestampMillis(text); err != nil {
		panic(fatalEval{err})
	}
	panic(fatalEval{&parquet.TimestampParseError{Text: text}})
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
