package expr

import (
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Four scalar functions manufactured a value where PostgreSQL raises (#855).
//
// The pattern is the same in all four: an argument outside the function's
// domain fell to `return nil` (SQL NULL) or to `return ""`, so a query that
// PostgreSQL refuses came back with a plausible answer and nothing downstream
// could tell it from a real one. ADR-0012 item 1 makes PostgreSQL the
// authority on error-versus-not, and the per-row channel that carries a
// refusal out of an evaluator has existed since #347 (fatal.go).
//
// PostgreSQL 17.11, measured live with VERBOSITY verbose:
//
//	DATE_TRUNC('bogus', ts)          22023  unit "bogus" not recognized for type
//	                                        timestamp without time zone
//	WIDTH_BUCKET(1,0,10,0)           2201G  count must be greater than zero
//	WIDTH_BUCKET(1,5,5,3)            2201G  lower bound cannot equal upper bound
//	SPLIT_PART('a,b,c', ',', 0)      22023  field position must not be zero
//	CHR(0)                           54000  null character not permitted
//	CHR(-1)                          22023  character number must be positive
//	CHR(1114112)                     54000  requested character too large for
//	                                        encoding: 1114112
//
// CHR(0) is the sharpest of them operationally: no text-format DataRow field
// can carry a NUL and libpq truncates at one, so the same query answered two
// lengths to two clients — #570's shape, reintroduced through a function.

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

func raiseChrTooLarge(code int64) {
	panic(fatalEval{sqlerr.New("54000",
		"requested character too large for encoding: %d", code)})
}
