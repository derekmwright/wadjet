package expr

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

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

// raiseNoLikeOperator aborts the query with SQLSTATE 42883,
// PostgreSQL's undefined_function — the same code an unresolvable
// function name raises (UnknownFuncError.SQLState) — for LIKE against a
// container-shaped value (#522). PostgreSQL raises the identical code for
// the same reason against its own composite/array types (verified live:
// `ARRAY[1,2,3] LIKE 'x'` answers "operator does not exist: integer[] ~~
// unknown"): there is no `~~` operator for that type, not a value that
// fails to parse. kind names the closest PostgreSQL type
// (containerLikeKind resolves it).
func raiseNoLikeOperator(kind string) {
	panic(fatalEval{sqlerr.New("42883", "operator does not exist: %s ~~ unknown", kind)})
}

// InvalidLiteralError names a constant the compiler refused outright: a
// literal that cannot be read as a value of the type its context demands.
//
// It is a DISTINCT TYPE for exactly the reason UnknownFuncError is one. The
// physical planner's compile sites are forgiving by design — a projection
// whose AST will not compile falls back to copying an input column of the
// same name — and that fallback is right for every compile failure EXCEPT the
// ones that are the answer. A refused literal has no column to fall back to,
// so swallowing it turned `SELECT -'abc'` into `column "-'abc'" does not
// exist`, which sends the reader hunting a name-resolution bug for a
// perfectly well-diagnosed 22P02 (#505 review finding). Callers test for it
// with errors.As — IsCompileRefusal below — and propagate.
type InvalidLiteralError struct {
	Input    string // the literal's source text
	DestType string // the type it was being read as, e.g. "numeric"
}

func (e *InvalidLiteralError) Error() string {
	return fmt.Sprintf("invalid input syntax for type %s: %q", e.DestType, e.Input)
}

// SQLState returns PostgreSQL's invalid_text_representation code, the same one
// raiseInvalidTextRepresentation raises for the per-row version of this
// refusal. sqlerr.StateOf picks it up through the Coder interface.
func (e *InvalidLiteralError) SQLState() string { return "22P02" }

// invalidNumericLiteralError is raiseInvalidTextRepresentation's non-panicking
// sibling, for the one site that can know a numeric refusal is certain before
// any row is ever read: unary minus over a STRING literal that is not a
// number (#505). Compile() rejects it directly rather than deferring to a
// per-row panic, because the answer does not depend on the column it will
// eventually meet — `-'abc'` is refused whether or not a row ever reaches it,
// unlike `d = 'abc'`, where the same text could be a legitimate value against
// a non-DECIMAL column.
func invalidNumericLiteralError(input string) error {
	return &InvalidLiteralError{Input: input, DestType: "numeric"}
}

// IsInvalidLiteral reports whether err is, or wraps, an InvalidLiteralError.
func IsInvalidLiteral(err error) bool {
	var ile *InvalidLiteralError
	return errors.As(err, &ile)
}

// IsCompileRefusal reports whether err is a compile failure that the caller
// must PROPAGATE rather than fall back around.
//
// The physical planner has six sites that compile an AST and quietly keep
// going when it will not compile, because a failed compile usually means "this
// expression is really a reference to an aggregate's output column". Two
// classes of failure are never that, and both are the answer to the query:
// a name nothing implements (#341) and a literal that names no value of its
// type (#505). Naming them together here keeps the six sites from drifting
// apart as a third class arrives.
func IsCompileRefusal(err error) bool {
	return IsUnknownFunc(err) || IsInvalidLiteral(err)
}

// raiseRealConversionError aborts a CAST to REAL that cannot carry its value,
// with SQLSTATE 22003 and the message PostgreSQL gives for THIS operand.
//
// PostgreSQL has two, and which one it uses depends on what is being cast:
//
//	CAST(1e40 AS real)          -> "1000…000" is out of range for type real
//	CAST(1e40::float8 AS real)  -> value out of range: overflow
//
// The first is the numeric->real cast failing during constant folding, so it
// names the numeric's own digits; the second is float8->real at runtime, which
// has no literal to name. Wadjet gave the runtime text for both. A LITERAL
// operand takes the first form, rendered by kernel.RealOverflowText — the same
// renderer the IN-list refusal uses, so one query cannot produce two spellings
// of one refusal.
func raiseRealConversionError(operand Expr, f float64, fit kernel.Float32Fit) {
	if lit, ok := operand.(*Lit); ok {
		text := lit.Text
		if text == "" {
			text = toString(lit.Val)
		}
		panic(fatalEval{sqlerr.New("22003", "%q is out of range for type real",
			kernel.RealOverflowText(text))})
	}
	kind := "overflow"
	if fit == kernel.Float32Underflows {
		kind = "underflow"
	}
	panic(fatalEval{sqlerr.New("22003", "value out of range: %s", kind)})
}
