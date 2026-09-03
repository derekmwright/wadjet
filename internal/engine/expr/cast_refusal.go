package expr

import (
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
