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
