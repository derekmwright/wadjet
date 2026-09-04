package expr

import (
	"math"
	"sync"
	"sync/atomic"

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
// old float64 behavior. Int64 overflow is a query ERROR — PostgreSQL's
// `bigint out of range`, 22003 (#637) — never the wrapped number Go's
// operators answer: a wrapped total is a different number wearing the right
// type, and nothing downstream can see that it is wrong.
//
// Division over integer operands truncates toward zero — PostgreSQL
// semantics (#369, ADR-0012; the original float-`/` pin followed DuckDB,
// which ADR-0012 overturns). Unlike the +,-,*,% typing, that truncation is
// SEMANTICS and must not ride this kill switch: with the switch off the
// operands evaluate through the float delegate and the quotient is
// truncated there (divTrunc), so both settings answer 3 for 7/2.
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

	// modeReady publishes every field below it: set last under modeMu, read
	// first (and alone) by the evaluators. The mode cannot move to compile
	// time — it is read off the operands' resolved COLUMN types, which do
	// not exist until a batch arrives — so the row loop pays a guard either
	// way. This one is an acquire load the compiler inlines into the three
	// Eval entry points; sync.Once.Do costs a call plus a closure build at
	// the same place and showed up as its own frame in the SF100 worker
	// profile (2026-08-21).
	modeReady atomic.Bool
	modeMu    sync.Mutex
	isInt     bool
	// isDec and dec are the EXACT fixed-point mode (ADR-0024 item 3, #555):
	// at least one DECIMAL operand and no float one, computed on the Int128
	// carrier at the result type batch.DecimalResultType names. It is
	// resolved here rather than at compile time for the same reason isInt is
	// — the operands' declarations do not exist until a batch arrives — and
	// it is checked FIRST, because an integer operand beside a DECIMAL one is
	// DECIMAL(19,0) in the result-type rule and must not take the int path.
	// See binop_decimal.go.
	isDec  bool
	dec    decMode
	opCode arithOp       // e.Op as an opcode: no per-row string compare
	flt    *BinOpFloat64 // delegate for float mode (keeps its typed fast path)
	// divTrunc marks a `/` whose operands are integer-typed while the node
	// resolved to float mode (the int-arith kill switch is off): the float
	// quotient is truncated toward zero so integer-division SEMANTICS hold
	// on both switch settings. See the type comment.
	divTrunc bool
	// A column operand of a temporal (or string) type makes `col ± col` and
	// `col ± n` candidates for DATE arithmetic rather than for reading both
	// sides as numbers. Resolved with the mode, from the same column types,
	// and nil for every other operand pair — see BinOp.dateArith (#340).
	dateNode *BinOp
}

// operandIsInt reports whether one operand is integer-preserving against
// this batch: a plain-integer column, an integer literal, a nested int-mode
// arithmetic node, or any of the value PRODUCERS int_domain.go enumerates —
// a CAST to an integer type, a polymorphic function over integer arguments,
// a choice construct whose branches are all integer (#849).
// Timestamps/dates/network types keep the float path — their arithmetic
// semantics are handled elsewhere and unchanged.
//
// The concrete node cases come BEFORE the intModer interface case on purpose:
// *BinOp implements intMode too now, and a type switch takes the first case
// that matches.
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
	case *Cast:
		return castIsInt(v)
	case *Case:
		return caseIsInt(v, b)
	case *Coalesce:
		return coalesceIsInt(v, b)
	case *decimalScalarFn:
		return decimalScalarFnIsInt(v, b)
	case *numericFuncCall:
		return funcCallIsInt(v.FuncCall, b)
	case *FuncCall:
		return funcCallIsInt(v, b)
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && operandIsInt(v.Operand, b)
	case intModer:
		return v.intMode(b)
	default:
		return isIntNative(e)
	}
}

// temporalColOperand reports whether an operand is a column whose declared
// type can carry a date: a DATE or TIMESTAMP column, or a text column holding
// date strings — which is how the TPC-H fixtures (and plenty of real catalogs)
// spell a date. Nothing else can be one: a resolved integer column is a number
// in its own right, and every non-column operand already decides its own form.
func temporalColOperand(e Expr, b *batch.RecordBatch) bool {
	cr, ok := e.(*ColRef)
	if !ok {
		return false
	}
	cr.resolve(b)
	if cr.idx < 0 {
		return false
	}
	// valueType: a ROW FIELD PATH of a date-carrying type is one too, and
	// typ names the CONTAINER (#568).
	switch cr.valueType() {
	case batch.TypeDate, batch.TypeTimestamp, batch.TypeString, batch.TypeBytes:
		return true
	}
	return false
}

func (e *BinOpNumeric) resolveMode(b *batch.RecordBatch) {
	if !e.modeReady.Load() {
		e.resolveModeSlow(b)
	}
}

// resolveModeSlow runs exactly once per node. The kill switch is read here,
// with the operand types, so both halves of the decision are taken at the
// same point in the lifecycle they always were.
func (e *BinOpNumeric) resolveModeSlow(b *batch.RecordBatch) {
	e.modeMu.Lock()
	defer e.modeMu.Unlock()
	if e.modeReady.Load() {
		return
	}
	// The DECIMAL question is asked first and answers for the whole node: a
	// DECIMAL beside an integer is numeric in PostgreSQL, so `d * 2` must not
	// be caught by the int mode below, and `d * f` must not be caught by this
	// one — a float operand does not implement decimalOperand at all, which
	// is what makes that fall through to float mode (ADR-0024 item 2).
	e.dec, e.isDec = resolveDecimalMode(e.Op, e.Left, e.Right, b)
	e.isInt = !e.isDec && intArithToggle.On() && operandIsInt(e.Left, b) && operandIsInt(e.Right, b)
	if !e.isInt && !e.isDec {
		e.flt = &BinOpFloat64{Left: e.Left, Right: e.Right, Op: e.Op}
		e.divTrunc = e.Op == "/" &&
			operandIsIntStructural(e.Left, b) && operandIsIntStructural(e.Right, b)
	}
	if !e.isDec && (e.Op == "+" || e.Op == "-") &&
		(temporalColOperand(e.Left, b) || temporalColOperand(e.Right, b)) {
		e.dateNode = &BinOp{Left: e.Left, Right: e.Right, Op: e.Op}
	}
	e.opCode = resolveArithOp(e.Op)
	e.modeReady.Store(true)
}

func (e *BinOpNumeric) intMode(b *batch.RecordBatch) bool {
	e.resolveMode(b)
	return e.isInt
}

func (e *BinOpNumeric) Eval(b *batch.RecordBatch, row int) any {
	e.resolveMode(b)
	if e.isDec {
		// The exact answer, boxed the way a DECIMAL COLUMN's value is boxed —
		// its rendered text — so a computed decimal and a stored one reach
		// every consumer of a boxed value in the same shape (binop_decimal.go).
		return e.evalDecimalText(b, row)
	}
	// A temporal column operand: ask date arithmetic first. It declines for
	// anything that is not a date (a text column of non-dates, a timestamp
	// difference, a fractional shift) and the numeric answer below stands
	// unchanged — so `l_receiptdate - l_shipdate` becomes the day count it
	// always meant instead of NULL, and nothing else moves (#340).
	if e.dateNode != nil {
		lv := e.Left.Eval(b, row)
		rv := e.Right.Eval(b, row)
		if lv == nil || rv == nil {
			return nil
		}
		if res, ok := e.dateNode.dateArith(b, row, lv, rv); ok {
			return res
		}
	}
	if !e.isInt {
		// The float delegate's own typed path, with the divTrunc rule
		// spelled out rather than routed back through EvalFloat64 — that
		// round trip re-ran the resolution guard for every row.
		v, ok := e.flt.EvalFloat64(b, row)
		if !ok {
			return nil
		}
		if e.divTrunc {
			v = math.Trunc(v)
		}
		return v
	}
	v, ok := e.intArith(b, row)
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
		// Decimal mode reports not-ok too. A DECIMAL result is not an int64
		// and answering one would truncate the fraction silently — the caller
		// falls back to Eval, which hands it the exact value.
		return 0, false
	}
	return e.intArith(b, row)
}

// intArith is the int64 kernel. Every caller has already resolved the mode
// and checked it: routing them back through EvalInt64 made a resolution
// guard fire twice per row (three times for a truncating division).
func (e *BinOpNumeric) intArith(b *batch.RecordBatch, row int) (int64, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return 0, false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return 0, false
	}
	// Checked: an integer result with no int64 is 22003, PostgreSQL's
	// `bigint out of range`, and never the wrapped number this node's own
	// doc comment used to promise (#637 — int_overflow.go).
	switch e.opCode {
	case arithAdd:
		return addInt64Checked(lv, rv), true
	case arithSub:
		return subInt64Checked(lv, rv), true
	case arithMul:
		return mulInt64Checked(lv, rv), true
	case arithDiv:
		// Integer division truncates toward zero (#369, PostgreSQL
		// semantics per ADR-0012). A GENUINE zero divisor raises 22012 —
		// NULL divisors already exited above with ok=false, and the
		// FatalEvalPanic channel (#347) carries the error from any depth,
		// so "this layer has no error channel" stopped being true when
		// that mechanism landed (#367 uses it for the literal 1/0).
		return divInt64Checked(lv, rv), true
	case arithMod:
		return modInt64Checked(lv, rv), true
	}
	return 0, false
}

// EvalFloat64 implements Float64Expr for consumers on the float protocol.
func (e *BinOpNumeric) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	e.resolveMode(b)
	if e.isInt {
		v, ok := e.intArith(b, row)
		return float64(v), ok
	}
	if e.isDec {
		// A consumer that asked for a float gets one, narrowed from the exact
		// value rather than computed in float: the digits past a double are
		// lost either way, but the ones a double CAN hold are the right ones,
		// and every consumer that can take the exact answer (Eval, the
		// vectorized DECIMAL writer) already has.
		v, ok := e.evalDecimal(b, row)
		if !ok {
			return 0, false
		}
		return v.ToFloat64(e.dec.out.Scale), true
	}
	v, ok := e.flt.EvalFloat64(b, row)
	if ok && e.divTrunc {
		v = math.Trunc(v)
	}
	return v, ok
}

// operandIsIntStructural is operandIsInt with nested arithmetic judged by
// its OPERAND types rather than its resolved mode, so the answer does not
// depend on the int-arith kill switch. It exists only to decide division
// truncation (divTrunc), which is semantics rather than optimization: the
// kill switch may move values between int64 and float64 representations,
// but never between 3 and 3.5.
func operandIsIntStructural(e Expr, b *batch.RecordBatch) bool {
	if bn, ok := e.(*BinOpNumeric); ok {
		return operandIsIntStructural(bn.Left, b) && operandIsIntStructural(bn.Right, b)
	}
	return operandIsInt(e, b)
}
