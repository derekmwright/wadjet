package expr

import (
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Integer-preserving arithmetic (+, -, *, %) for column operands.
//
// compileBinOp could only choose the int64 path when BOTH operands were
// compile-time int-native, and column types are unknown at compile time —
// so `ClientIP - 1` (any column arithmetic) fell to the float64 path and
// produced float64 values. That is both a semantics wart (SQL integer
// arithmetic yields integers; DuckDB agrees) and a performance tax:
// float-typed results force GROUP BY keys off the typed-int aggregation
// paths (ClickBench Q36's four ClientIP-derived keys profiled as
// Float64bits boxing inside generic-SoA key serialization).
//
// BinOpNumeric resolves its mode ONCE against the first batch, using the
// operands' actual column types: all-plain-integer (Int64/Int32) columns
// and integer literals → int64 arithmetic; anything else → exactly the
// old float64 behavior. Division stays float in compileBinOp (DuckDB
// semantics). Int64 overflow wraps (Go semantics), the same values the
// float path would have corrupted differently past 2^53.
var intArithToggle = optswitch.Register("int-arith", "WADJET_INT_ARITH",
	"integer-preserving +,-,*,%: int columns stay int64 instead of promoting to float64")

// IntArithOn exposes the toggle to the planner: projection output types
// may only declare Int64 for arithmetic when the runtime will actually
// take the integer path (see inferProjectionTypeCols).
func IntArithOn() bool { return intArithToggle.On() }

// numericOperand is what BinOpNumeric accepts: both typed getters, so the
// resolved mode can use either path.
type numericOperand interface {
	Expr
	Float64Expr
	Int64Expr
}

// intModer lets nested BinOpNumeric operands report their resolved mode.
type intModer interface {
	intMode(b *batch.RecordBatch) bool
}

// BinOpNumeric is the mode-resolved arithmetic node.
type BinOpNumeric struct {
	Left, Right numericOperand
	Op          string

	modeOnce sync.Once
	isInt    bool
	flt      *BinOpFloat64 // delegate for float mode (keeps its typed fast path)
}

// operandIsInt reports whether one operand is integer-preserving against
// this batch: a plain-integer column, an integer literal, or a nested
// int-mode arithmetic node. Timestamps/dates/network types keep the float
// path — their arithmetic semantics are handled elsewhere and unchanged.
func operandIsInt(e Expr, b *batch.RecordBatch) bool {
	switch v := e.(type) {
	case *ColRef:
		v.resolve(b)
		if v.idx < 0 {
			return false
		}
		switch v.typ {
		case batch.TypeInt64, batch.TypeInt32:
			return true
		}
		return false
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		}
		return false
	case intModer:
		return v.intMode(b)
	default:
		return isIntNative(e)
	}
}

func (e *BinOpNumeric) resolveMode(b *batch.RecordBatch) {
	e.modeOnce.Do(func() {
		e.isInt = operandIsInt(e.Left, b) && operandIsInt(e.Right, b)
		if !e.isInt {
			e.flt = &BinOpFloat64{Left: e.Left, Right: e.Right, Op: e.Op}
		}
	})
}

func (e *BinOpNumeric) intMode(b *batch.RecordBatch) bool {
	e.resolveMode(b)
	return e.isInt
}

func (e *BinOpNumeric) Eval(b *batch.RecordBatch, row int) any {
	e.resolveMode(b)
	if !e.isInt {
		return e.flt.Eval(b, row)
	}
	v, ok := e.EvalInt64(b, row)
	if !ok {
		return nil
	}
	return v
}

// EvalInt64 implements Int64Expr. Only meaningful in int mode; float mode
// reports not-ok so callers fall back to EvalFloat64/Eval.
func (e *BinOpNumeric) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	e.resolveMode(b)
	if !e.isInt {
		return 0, false
	}
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return 0, false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return 0, false
	}
	switch e.Op {
	case "+":
		return lv + rv, true
	case "-":
		return lv - rv, true
	case "*":
		return lv * rv, true
	case "%":
		if rv == 0 {
			return 0, false
		}
		return lv % rv, true
	}
	return 0, false
}

// EvalFloat64 implements Float64Expr for consumers on the float protocol.
func (e *BinOpNumeric) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	e.resolveMode(b)
	if e.isInt {
		v, ok := e.EvalInt64(b, row)
		return float64(v), ok
	}
	return e.flt.EvalFloat64(b, row)
}
