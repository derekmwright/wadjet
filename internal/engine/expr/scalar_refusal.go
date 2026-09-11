package expr

import (
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Scalar domain failures raise through fatalEval (#855, #347, ADR-0012 item 1).
// Unknown DATE_TRUNC units and SPLIT_PART position zero raise 22023;
// WIDTH_BUCKET count <= 0 or equal bounds raise 2201G.
// CHR negative values raise 22023; NUL or codes above 1114111 raise 54000.
// Never emit NUL text: text DataRows cannot carry it and libpq truncates
// at NUL (#570). Keep each refusal's PostgreSQL message and SQLSTATE.
// See docs/internals/scalar-domain-error-table.md for the design.

// raiseUnitNotRecognized is PostgreSQL's refusal for a field name its
// timestamp functions do not know, SQLSTATE 22023 (invalid_parameter_value).
// The type name is part of the message on the server and is reproduced here.
func raiseUnitNotRecognized(unit string) {
	panic(fatalEval{sqlerr.New("22023",
		"unit %s not recognized for type timestamp without time zone", sqlerr.Quote(unit))})
}

// raiseWidthBucketCount is PostgreSQL's 2201G (invalid_argument_for_width_
// bucket_function) for a bucket count that is not positive.
func raiseWidthBucketCount() {
	panic(fatalEval{sqlerr.New("2201G", "count must be greater than zero")})
}

// raiseWidthBucketBounds is the same SQLSTATE for equal bounds, which leave
// the bucket width zero.
func raiseWidthBucketBounds() {
	panic(fatalEval{sqlerr.New("2201G", "lower bound cannot equal upper bound")})
}

// raiseFieldPositionZero is PostgreSQL's refusal for SPLIT_PART's zeroth
// field. Position is 1-based and NEGATIVE counts from the end (PG 14+), so
// zero names nothing at all.
func raiseFieldPositionZero() {
	panic(fatalEval{sqlerr.New("22023", "field position must not be zero")})
}

// raiseChrNotPositive and raiseChrNul are CHR's two refusals, and they carry
// DIFFERENT SQLSTATEs on the server: a negative code is 22023
// (invalid_parameter_value) and zero is 54000 (program_limit_exceeded), which
// is also the class a code past the encoding's range takes.
func raiseChrNotPositive() {
	panic(fatalEval{sqlerr.New("22023", "character number must be positive")})
}

func raiseChrNul() {
	panic(fatalEval{sqlerr.New("54000", "null character not permitted")})
}

// raiseNegativeSubstringLength is PostgreSQL's 22011 (substring_error) for
// SUBSTRING with a negative length. substrWindow's own doc recorded it as a
// refusal this layer could not make; the per-row channel has existed since
// #347 (#856).
func raiseNegativeSubstringLength() {
	panic(fatalEval{sqlerr.New("22011", "negative substring length not allowed")})
}

func raiseChrTooLarge(code int64) {
	panic(fatalEval{sqlerr.New("54000",
		"requested character too large for encoding: %d", code)})
}
