package batch

import (
	"fmt"
	"math"
)

// TypeMismatchError reports a write of a value a vector has nowhere to put:
// SetValue was handed a Go value whose type has no conversion into the
// vector's storage.
//
// Until #361 such a write VANISHED — the slot kept its zero value and was
// marked valid — which is the mechanism behind an entire bug family
// (#310, #327, #331, #333, #345, #353, #361, #371, #372): some declaration
// upstream picks the wrong vector type, and instead of an error the query
// answers 0 on every row. The write site is the one seam every one of those
// defects must cross, so it now panics with this typed value.
//
// The panic carries a query ERROR, not a crash: it implements the
// exec.FatalEvalPanic contract (Error + FatalEvalError), the same route the
// expression evaluator uses for a condition with no error return (#347).
// The pipeline drivers, the worker's task-level recover, the coordinator's
// and the embedded API's query entries all convert it back into an error —
// "a wrong type may cost a wrong answer, never the server" (#310) still
// holds, with the improvement that it now costs an ERROR instead of a wrong
// answer.
//
// The deliberate non-panics: a nil value is a NULL (WriteNullAt); STRING and
// BYTES destinations coerce any value through its string form, which is a
// documented rendering (group keys rely on it); and a PARSE failure of a
// value-level string (an unparseable IPv4, MAC, UUID) keeps its historical
// null-ish result — the type was right, the value was not.
type TypeMismatchError struct {
	Dst TypeID // the vector's type
	Val any    // the value that had nowhere to go
}

func (e *TypeMismatchError) Error() string {
	return fmt.Sprintf("batch: cannot store %T into %s vector (#361 silent-write guard)", e.Val, e.Dst)
}

// FatalEvalError implements the exec.FatalEvalPanic contract, so pipeline
// drivers convert the panic into a query error instead of a process exit.
func (e *TypeMismatchError) FatalEvalError() error { return e }

// mismatch raises the guard. Split out so every SetValue arm reads as one
// line and the panic allocates nothing until it actually fires.
func (v *Vector) mismatch(val any) {
	panic(&TypeMismatchError{Dst: v.Type, Val: val})
}

// IntegerRangeError reports a write of an integer VALUE that the vector's
// narrower integer storage has no room for — the sibling of the guard above,
// and the one #361's check cannot see: the Go type converts fine, so nothing
// is "mismatched"; it is the NUMBER that has nowhere to go.
//
// The mechanism this closes is not a typing mistake anywhere. ColRef.Eval
// widens an INT32 column to an int64 box on purpose (ADR-0012's recorded
// "every integer spelling is INT64" superset), so a per-row kernel over an
// int4 column computes in int64 and is RIGHT to: |−2147483648| is 2147483648,
// exactly what a bigint answer would be. The planner then declares the
// projection int4, because PostgreSQL's `abs(int4)` IS int4 — and the store
// narrowed 2147483648 back into an int32 and WRAPPED it to -2147483648. A
// different number wearing the right type: ADR-0012 item 9, and the class
// ADR-0024 forbids on every integer path.
//
// So the refusal belongs at the STORE and not inside ABS. Every kernel that
// computes an int4 result in int64 crosses this one seam, and a check here
// covers all of them at once, where a check inside ABS would leave the next
// such kernel for the next census to find. PostgreSQL's own SQLSTATE and its
// own wording, so a client sees the message it would see there.
type IntegerRangeError struct {
	Dst TypeID // the vector's type
	Val any    // the value with no room in it, for diagnosis
}

func (e *IntegerRangeError) Error() string { return "integer out of range" }

// SQLState is PostgreSQL's numeric_value_out_of_range.
func (e *IntegerRangeError) SQLState() string { return "22003" }

// FatalEvalError implements the exec.FatalEvalPanic contract, the same route
// TypeMismatchError takes: a query error, never a process exit.
func (e *IntegerRangeError) FatalEvalError() error { return e }

// int32OrRaise narrows an integer box into an int32 or refuses.
func (v *Vector) int32OrRaise(n int64) int32 {
	if n < math.MinInt32 || n > math.MaxInt32 {
		panic(&IntegerRangeError{Dst: v.Type, Val: n})
	}
	return int32(n)
}

// int32FromFloatOrRaise is the same guard for a float box. Go's float→int
// conversion is IMPLEMENTATION-DEFINED outside the destination's range and for
// a NaN, so the check has to happen HERE, on the float, and not on whatever
// int32 the conversion happened to produce.
func (v *Vector) int32FromFloatOrRaise(f float64) int32 {
	if math.IsNaN(f) || f < math.MinInt32 || f > math.MaxInt32 {
		panic(&IntegerRangeError{Dst: v.Type, Val: f})
	}
	return int32(f)
}
