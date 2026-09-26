// SPDX-License-Identifier: MIT

package batch

import (
	"errors"
	"fmt"
	"math"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DurationNanoser lets a value from a package this one cannot import supply
// its own DURATION carrier (arc CW round 6, B1): expr.IntervalValue is the
// one example today — an INTERVAL literal's element, which declares no
// column type anywhere else in this engine — implementing it rather than
// this package naming that type. false is the same refusal an unrecognized
// Go type gets from Vector.SetValue's TypeDuration arm: an opaque interval
// (a runtime CAST this engine's single-unit fields cannot hold) has no
// number to store and is loud, not a plausible zero.
type DurationNanoser interface{ DurationNanos() (int64, bool) }

// TypeMismatchError panics when a writer's Go type has no storage conversion
// (#361; #310, #327, #331, #333, #345, #353, #371, #372).
// It implements FatalEvalPanic (Error + FatalEvalError): drivers, worker recover,
// coordinator and embedded query entries return a query error, never kill the
// server or silently leave a valid zero slot (#347).
// Nil is NULL; STRING/BYTES render any SCALAR value, as group keys require;
// a container box into STRING/BYTES is refused (arc CW, ADR-0045 §2).
// Value-level parse failures for IPv4/MAC/UUID retain their null-ish result:
// the type was accepted, so these do not raise this mismatch guard.
// See docs/internals/batch-vector-type-mismatch-boundary.md for the design.
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

// ContainerShapeError panics when a container value reaches an ARRAY, MAP or
// ROW vector that was allocated without its shape — no element child, no
// field children. Such a vector has nowhere to put the value, and the old
// silent return left the slot NULL: a declaration seam that did not carry
// the container's shape answered a plausible NULL (arc CW, ADR-0045 §2).
type ContainerShapeError struct {
	Dst TypeID
	Val any
}

func (e *ContainerShapeError) Error() string {
	return fmt.Sprintf("batch: cannot store %T into a %s vector declared without its element or fields (ADR-0045)", e.Val, e.Dst)
}

// FatalEvalError implements the exec.FatalEvalPanic contract.
func (e *ContainerShapeError) FatalEvalError() error { return e }

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

// FloatRangeError reports a write of a FINITE float64 that has no float32 —
// the float sibling of IntegerRangeError, and the same mechanism: a kernel
// computes an int4 or a real result on the WIDER carrier (ADR-0012's recorded
// "every integer spelling is INT64" superset has a float twin), the planner
// declares the narrow PostgreSQL type because that IS the type
// (`pg_typeof(real + real)` is real), and the store is the one seam every such
// kernel crosses.
//
// Narrowing without the check is silent and in both directions: 1e39 becomes
// +Inf and 1e-60 becomes 0, where PostgreSQL 17.11 raises
// `value out of range: overflow` and `value out of range: underflow` — both
// measured, for `1e38::real * 10.0::real` and `1e-30::real * 1e-30::real`, and
// for a bare `1e-60::real` too.
//
// The operands are EXEMPT exactly as float_range.go's rule exempts them: an
// infinity or a NaN that ARRIVES is a value, so a non-finite source narrows to
// a non-finite float32 and is stored. Only a finite source with no float32 is
// an error, and only a non-zero source that becomes zero is an underflow.
type FloatRangeError struct {
	Val       float64 // the value with no float32, for diagnosis
	Underflow bool    // true for a non-zero value that narrows to zero
}

func (e *FloatRangeError) Error() string {
	if e.Underflow {
		return "value out of range: underflow"
	}
	return "value out of range: overflow"
}

// SQLState is PostgreSQL's numeric_value_out_of_range, the same class the
// integer guard and the float8 arithmetic rule raise.
func (e *FloatRangeError) SQLState() string { return "22003" }

// FatalEvalError implements the exec.FatalEvalPanic contract: a query error,
// never a process exit.
func (e *FloatRangeError) FatalEvalError() error { return e }

// VectorWidthError reports a write of a VECTOR value whose component count is
// not the column's declared dimension — the third member of this file's family
// and the one that is neither a wrong Go type nor a number out of range: the
// box is right and every component is storable, there is just the wrong NUMBER
// of them.
//
// Until #900 a short write copied what fit and left the remaining slots
// holding whatever the storage carried, which on a pooled batch is a PREVIOUS
// batch's components: `SetVector(0, []float32{1})` into a VECTOR(2) answered
// [1 0] on a fresh batch and [1 8] on a reused one. Same input, two answers,
// decided by pool history — and no error either way.
//
// Refusing rather than padding is the choice PostgreSQL's vector extension
// makes ('[1]'::vector(2) is an error), and it is the only one that keeps a
// declared width meaningful: a padded value is a DIFFERENT vector, and every
// distance function would then answer about a value nobody wrote.
type VectorWidthError struct {
	Dim int // the column's declared dimension
	Got int // components the writer supplied
}

// Error is pgvector's wording, so a client sees the message it would see there.
func (e *VectorWidthError) Error() string {
	return fmt.Sprintf("expected %d dimensions, not %d", e.Dim, e.Got)
}

// SQLState is PostgreSQL's data_exception, what pgvector raises for this.
func (e *VectorWidthError) SQLState() string { return "22000" }

// FatalEvalError implements the exec.FatalEvalPanic contract: a query error,
// never a process exit.
func (e *VectorWidthError) FatalEvalError() error { return e }

// raiseVectorWidth raises the guard. SetVector is one call per ROW and was
// small enough for the inliner before the check; the error's construction is
// what would push it over the budget, so this is NOT inlined and SetVector
// keeps a compare-and-branch where it used to have nothing.
//
//go:noinline
func (v *Vector) raiseVectorWidth(got int) {
	panic(&VectorWidthError{Dim: v.VectorDim, Got: got})
}

// int32OrRaise narrows an integer box into an int32 or refuses.
func (v *Vector) int32OrRaise(n int64) int32 {
	if n < math.MinInt32 || n > math.MaxInt32 {
		panic(&IntegerRangeError{Dst: v.Type, Val: n})
	}
	return int32(n)
}

// float32OrRaise narrows a float64 box into a float32 or refuses. See
// FloatRangeError for the rule and for why the operand exemptions are the
// whole of it.
//
//go:noinline
func raiseFloatRange(f float64, underflow bool) {
	panic(&FloatRangeError{Val: f, Underflow: underflow})
}

func float32OrRaise(f float64) float32 {
	r := float32(f)
	switch {
	case math.IsInf(float64(r), 0) && !math.IsInf(f, 0):
		raiseFloatRange(f, false)
	case r == 0 && f != 0:
		raiseFloatRange(f, true)
	}
	return r
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

// NetworkTextWriteError reports TEXT written into a PORT or PROTOCOL vector
// that names no value of that type — the third guard on this seam, beside
// TypeMismatchError (the Go type has nowhere to go) and IntegerRangeError (the
// type converts, the number does not fit). Here the type converts and the TEXT
// does not name a value.
//
// It exists because these two types are the only network pair whose vector
// carries a NUMBER, so the sibling arms' convention — IPv4/MAC/UUID leave a
// null-ish result for text they cannot parse — would put a silent ZERO in a
// PORT column, which reads as port 0, a real port. The five address types'
// zero is not a value anyone writes; 0 is. Loud, with the same sentence the
// writer's door and the CAST give for the same characters
// (parquet.NetworkTextError), and carrying the exec.FatalEvalPanic contract so
// a pipeline driver turns it into a query error rather than a process exit.
type NetworkTextWriteError struct {
	Dst TypeID
	Val string
	err error
}

func (e *NetworkTextWriteError) Error() string { return e.err.Error() }

// FatalEvalError implements the exec.FatalEvalPanic contract.
func (e *NetworkTextWriteError) FatalEvalError() error { return e.err }

// SQLState forwards the wrapped refusal's code — 22P02 for text that names no
// value of the type, 22003 for a number outside its range — so the wire reports
// what the writer's own door reports.
func (e *NetworkTextWriteError) SQLState() string {
	var c interface{ SQLState() string }
	if errors.As(e.err, &c) {
		return c.SQLState()
	}
	return "22P02"
}

// netIntFromTextOrRaise reads a PORT or PROTOCOL value out of its own TEXT
// form, or refuses with the same error the writer's door and the CAST raise for
// the same characters.
//
// It exists so a PORT or PROTOCOL vector reads text the way the IPV4, IPV6,
// CIDR, MAC and UUID vectors already do. A boxed value that reached SetValue as
// TEXT — the arm of a CASE or COALESCE that a query wrote as a literal — could
// not be stored at all before it, and `CASE WHEN … THEN c_port ELSE '80' END`
// failed the query where the same shape over the other five answers (#1137).
func (v *Vector) netIntFromTextOrRaise(s string) int32 {
	n, st, ok := parquet.NetworkTextValue(v.Type, s)
	if !ok || st != parquet.NetTextOK {
		panic(&NetworkTextWriteError{Dst: v.Type, Val: s, err: parquet.NetworkTextError(v.Type, s, st)})
	}
	out, _ := n.(int32)
	return out
}
