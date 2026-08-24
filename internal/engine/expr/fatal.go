package expr

import "github.com/derekmwright/wadjet/internal/sqlerr"

// fatalEval carries a query ERROR out of an expression evaluator as a panic.
// It implements exec.FatalEvalPanic structurally (see exec.recoverFatalEval),
// the mechanism #347 introduced for MissingOuterColumnError: the pipeline
// drivers recover it and convert it back into the wrapped error, while every
// other panic re-raises untouched.
//
// This is the per-row error channel the expression layer historically lacked
// (the constraint behind #340's NULL-on-unparseable-date rule). ADR-0012 makes
// PostgreSQL the authority on error-versus-not, so evaluations PostgreSQL
// refuses — division by zero (22012), non-numeric text cast to an integer
// (22P02) — raise here instead of manufacturing a value.
type fatalEval struct{ err error }

func (f fatalEval) Error() string { return f.err.Error() }

// FatalEvalError satisfies exec.FatalEvalPanic.
func (f fatalEval) FatalEvalError() error { return f.err }

// raiseDivisionByZero aborts the query with SQLSTATE 22012, PostgreSQL's
// division_by_zero. Callers must have already established that the divisor is
// a genuine zero, not a NULL (a NULL divisor answers NULL, never an error).
func raiseDivisionByZero() {
	panic(fatalEval{sqlerr.New("22012", "division by zero")})
}

// raiseInvalidTextRepresentation aborts the query with SQLSTATE 22P02,
// PostgreSQL's invalid_text_representation, for a string that cannot be read
// as a value of the destination type.
func raiseInvalidTextRepresentation(destType, input string) {
	panic(fatalEval{sqlerr.New("22P02", "invalid input syntax for type %s: %q", destType, input)})
}

// invalidNumericLiteralError is raiseInvalidTextRepresentation's non-panicking
// sibling, for the one site that can know a numeric refusal is certain before
// any row is ever read: unary minus over a STRING literal that is not a
// number (#505). Compile() rejects it directly rather than deferring to a
// per-row panic, because the answer does not depend on the column it will
// eventually meet — `-'abc'` is refused whether or not a row ever reaches it,
// unlike `d = 'abc'`, where the same text could be a legitimate value against
// a non-DECIMAL column.
func invalidNumericLiteralError(input string) error {
	return sqlerr.New("22P02", "invalid input syntax for type numeric: %q", input)
}
