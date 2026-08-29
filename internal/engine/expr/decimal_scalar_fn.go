package expr

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// The scalar math functions that answer in their argument's OWN domain —
// abs/ceil/floor/round/trunc/sign/mod — computed exactly over a DECIMAL
// (ADR-0024 items 2 and 3, #668).
//
// PostgreSQL answers all seven in `numeric` over a numeric argument. Wadjet
// declared every one of them RetFloat64 and computed through ToFloat64, whose
// default arm parses a DECIMAL column's rendered TEXT with fmt.Sscanf — so the
// value made a round trip through a double before any rounding happened, and
// on the paths where that parse fails it produced 0 for every row. ROUND over a
// DECIMAL was the visible one; the whole family shares the cause.
//
// The transcendental functions — sqrt/exp/ln/log/power — stay float64. That is
// a DELIBERATE, recorded divergence of the class ADR-0012 item 9 already
// carries for STDDEV/VARIANCE/CORR/MEDIAN: PostgreSQL answers them in numeric,
// and doing the same here means building an exact fixed-point tower, not
// widening an accumulator.
//
// Shape: a typed node that replaces the FuncCall and carries it as a
// FALLBACK, the way ColShapeLen does for the length family. The argument's
// type is not known until a batch arrives, so the node resolves per batch and
// delegates every non-DECIMAL argument to the fallback — semantics for every
// other type are unchanged, bit for bit.

// decimalScalarFn is one of the seven functions, over a DECIMAL argument.
type decimalScalarFn struct {
	name string
	op   batch.DecimalScalarOp
	// arg is the value; digits is round/trunc's second argument, and modArg
	// is mod's. Exactly one of the two is ever set.
	arg      Expr
	digits   Expr
	modArg   Expr
	fallback *FuncCall

	// mode is resolved once against the first batch: the argument's (p,s),
	// the result's, and whether the exact path applies at all.
	ready    atomic.Bool
	mu       sync.Mutex
	on       bool
	in       batch.DecimalType
	out      batch.DecimalType
	digitsN  int
	modMode  decMode
	isModDec bool
}

// decimalScalarOps names the one-argument (and round/trunc's optional-second)
// functions this node handles, and the batch op each maps to.
//
// `truncate` is wadjet's registered spelling and `trunc` is PostgreSQL's; both
// are accepted so a query written either way gets the same exact answer.
var decimalScalarOps = map[string]batch.DecimalScalarOp{
	"abs":      batch.DecimalScalarAbs,
	"ceil":     batch.DecimalScalarCeil,
	"ceiling":  batch.DecimalScalarCeil,
	"floor":    batch.DecimalScalarFloor,
	"round":    batch.DecimalScalarRound,
	"trunc":    batch.DecimalScalarTrunc,
	"truncate": batch.DecimalScalarTrunc,
	"sign":     batch.DecimalScalarSign,
}

// DecimalScalarFnOp reports the batch-level op a scalar math function maps to,
// for the planner's declared-type layer. ok=false for a name with no exact
// fixed-point form.
func DecimalScalarFnOp(name string) (batch.DecimalScalarOp, bool) {
	op, ok := decimalScalarOps[strings.ToLower(strings.TrimSpace(name))]
	return op, ok
}

// IsDecimalScalarFn reports whether a function answers in its argument's own
// domain and so has an exact DECIMAL form — the seven of ADR-0024 item 3, mod
// included. The planner asks so its declaration and this node agree about
// which names take the exact path.
func IsDecimalScalarFn(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	if _, ok := decimalScalarOps[n]; ok {
		return true
	}
	return n == "mod"
}

// newDecimalScalarFn wraps a call in the exact node when the name is one of
// the seven and the argument count matches. It returns nil for everything
// else, and the caller keeps the FuncCall it built.
func newDecimalScalarFn(fc *FuncCall) *decimalScalarFn {
	name := strings.ToLower(fc.Name)
	if name == "mod" {
		if len(fc.Args) != 2 {
			return nil
		}
		return &decimalScalarFn{name: name, arg: fc.Args[0], modArg: fc.Args[1], fallback: fc}
	}
	op, ok := decimalScalarOps[name]
	if !ok || len(fc.Args) < 1 || len(fc.Args) > 2 {
		return nil
	}
	// A second argument is round/trunc's digit count and nothing else has
	// one; a two-argument abs is not a call this node can answer.
	if len(fc.Args) == 2 && op != batch.DecimalScalarRound && op != batch.DecimalScalarTrunc {
		return nil
	}
	n := &decimalScalarFn{name: name, op: op, arg: fc.Args[0], fallback: fc}
	if len(fc.Args) == 2 {
		n.digits = fc.Args[1]
	}
	return n
}

// resolve settles the mode once per node, against the first batch that can
// answer it — the same lifecycle BinOpNumeric's mode has, and for the same
// reason: the argument's declaration does not exist until a batch arrives.
func (e *decimalScalarFn) resolve(b *batch.RecordBatch) bool {
	if e.ready.Load() {
		return e.on
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ready.Load() {
		return e.on
	}
	e.on = e.resolveMode(b)
	e.ready.Store(true)
	return e.on
}

func (e *decimalScalarFn) resolveMode(b *batch.RecordBatch) bool {
	if !decimalScalarArg(e.arg, b) {
		return false
	}
	o, ok := e.arg.(decimalOperand)
	if !ok {
		return false
	}
	in, ok := o.decimalType(b)
	if !ok {
		return false
	}
	e.in = in
	if e.modArg != nil {
		// mod(x, y) is the `%` operator spelled as a call, so it takes the
		// same result-type rule and the same kernel — one rule, not two.
		m, ok := resolveDecimalMode("%", e.arg, e.modArg, b)
		if !ok {
			return false
		}
		e.modMode, e.isModDec = m, true
		e.out = m.out
		return true
	}
	// round/trunc's digit count must be a CONSTANT: the result's SCALE is a
	// function of it, and a type that changed per row is not a type. A
	// non-constant second argument therefore declines to the float fallback,
	// which is also what the planner's declaration does with it.
	if e.digits != nil {
		n, ok := constIntOperand(e.digits)
		if !ok {
			return false
		}
		e.digitsN = n
	}
	out, ok := batch.DecimalScalarType(e.op, in, e.digitsN)
	if !ok {
		return false
	}
	e.out = out
	return true
}

// decimalScalarArg reports whether an argument makes this call exact.
//
// It is operandIsDecimalTyped with a BARE NUMERIC LITERAL excluded, and the
// exclusion is deliberate: `SELECT 1.5` declares FLOAT64 in this engine
// (physical.nodeDeclaredType's Lit arm), so `ROUND(0.5)` answering an exact
// DECIMAL would make a constant-folded expression change type depending on
// which function wrapped it. PostgreSQL types both as numeric, and closing
// that gap is a change to the LITERAL's own declaration — every projection,
// comparison and set-operation arm that carries one — not to this family.
// A literal INSIDE an expression still counts: `ROUND(d + 0.5, 1)` is exact,
// because its argument is arithmetic over a real DECIMAL.
//
// Unary ± over a literal is a literal too. `ROUND(-0.5)` parses as a UnaryOp
// and `ROUND(0.5)` as a Lit, and letting only one of them take this path made
// the two halves of one query disagree about their own type — which is how the
// oracle found it: `SELECT ROUND(0.5), ROUND(-0.5)` came back float 1 beside
// decimal -1.
func decimalScalarArg(e Expr, b *batch.RecordBatch) bool {
	if isConstNumericLit(e) {
		return false
	}
	return operandIsDecimalTyped(e, b)
}

// isConstNumericLit reports whether an operand is a numeric CONSTANT — a
// literal, or unary ± over one. physical.isConstNumericLitNode makes the same
// test over the AST.
func isConstNumericLit(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		return true
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && isConstNumericLit(v.Operand)
	}
	return false
}

// constIntOperand reads a compile-time integer literal. Only a literal
// qualifies: `ROUND(d, n)` over a COLUMN n has a different scale on every row,
// which is not a type a vector can be built from.
func constIntOperand(e Expr) (int, bool) {
	switch v := e.(type) {
	case *UnaryOp:
		// `ROUND(d, -2)` parses as unary minus over a literal, and a negative
		// digit count is a real request: PostgreSQL's round(1234.56, -2) is
		// 1200. physical.constIntArg makes the same allowance.
		n, ok := constIntOperand(v.Operand)
		if !ok {
			return 0, false
		}
		switch v.Op {
		case "-":
			return -n, true
		case "+":
			return n, true
		}
		return 0, false
	case *Lit:
		switch n := v.Val.(type) {
		case int64:
			return int(n), true
		case int32:
			return int(n), true
		case int:
			return n, true
		}
	}
	return 0, false
}

// Eval answers the exact value boxed as its rendered text — the box a DECIMAL
// COLUMN produces — or delegates to the float FuncCall for a non-DECIMAL
// argument.
func (e *decimalScalarFn) Eval(b *batch.RecordBatch, row int) any {
	if !e.resolve(b) {
		return e.fallback.Eval(b, row)
	}
	v, ok := e.evalDecimal(b, row)
	if !ok {
		return nil
	}
	return v.FormatDecimal(e.out.Scale)
}

// decimalType lets the result be an operand of exact arithmetic:
// `ROUND(d, 1) * 2` is numeric in PostgreSQL and exact here.
func (e *decimalScalarFn) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	if !e.resolve(b) {
		return batch.DecimalType{}, false
	}
	return e.out, true
}

func (e *decimalScalarFn) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	if !e.resolve(b) {
		return batch.Int128{}, false
	}
	lv, ok := e.arg.(decimalOperand).evalDecimal(b, row)
	if !ok {
		return batch.Int128{}, false
	}
	if e.isModDec {
		rv, ok := e.modArg.(decimalOperand).evalDecimal(b, row)
		if !ok {
			return batch.Int128{}, false
		}
		return decApplyChecked(e.modMode, lv, rv)
	}
	v, st := batch.DecimalScalar(e.op, lv, e.in.Scale, e.digitsN, e.out.Precision, e.out.Scale)
	if st != batch.DecimalOK {
		raiseDecimalStatus(st, e.out.Precision, e.out.Scale)
	}
	return v, true
}

func (e *decimalScalarFn) decimalVec(_ *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	// No materialized column of its own; a caller reads it per row through
	// evalDecimal, unboxed.
	return kernel.DecimalOperandVec{}, false
}

// EvalVec is the vectorized arm: the whole batch through the typed kernel,
// writing carriers straight into the DECIMAL output vector. It falls back to
// the FuncCall's own vec path for a non-DECIMAL argument, so nothing else
// changes shape.
func (e *decimalScalarFn) EvalDecimalVec(b *batch.RecordBatch, out *batch.Vector, n int) bool {
	if !e.resolve(b) || !decimalVecWritable(out, n) {
		return false
	}
	outS := out.DecimalData.Scale
	outP := e.out.Precision
	if outS != e.out.Scale {
		// The declaration this node resolved does not describe the vector it
		// is writing into. Keep the vector's scale — it is what the value is
		// stored at — and fall back to the carrier's bound, the same
		// direction of safety BinOpNumeric.EvalDecimalVec takes.
		outP = batch.MaxDecimalPrecision
	}
	if e.isModDec {
		// mod is a two-operand op and shares the arithmetic kernel.
		lv, lok := e.arg.(decimalOperand).decimalVec(b)
		rv, rok := e.modArg.(decimalOperand).decimalVec(b)
		if lok && rok {
			markColumnarNulls(e.arg, e.modArg, b, out, n)
			f := kernel.DecimalArithVec(kernel.DecimalOpMod, out.DecimalData.Data, lv, rv, outP, outS, n, &out.Nulls)
			if !f.Fine() {
				raiseDecimalStatus(f.Status, outP, outS)
			}
			return true
		}
		e.evalDecimalRows(b, out, n)
		return true
	}
	src, ok := e.arg.(decimalOperand).decimalVec(b)
	if !ok || src.Data == nil {
		e.evalDecimalRows(b, out, n)
		return true
	}
	markColumnarNulls(e.arg, e.arg, b, out, n)
	f := kernel.DecimalScalarVec(e.op, out.DecimalData.Data, src.Data, src.Scale,
		e.digitsN, outP, outS, n, &out.Nulls)
	if !f.Fine() {
		raiseDecimalStatus(f.Status, outP, outS)
	}
	return true
}

// evalDecimalRows is the per-row arm for an argument with no columnar form —
// nested arithmetic, an integer column. Still unboxed: the value goes straight
// into the carrier slice.
func (e *decimalScalarFn) evalDecimalRows(b *batch.RecordBatch, out *batch.Vector, n int) {
	for i := 0; i < n && i < len(out.DecimalData.Data); i++ {
		v, ok := e.evalDecimal(b, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Nulls.SetValid(i)
		out.DecimalData.Data[i] = v
	}
}
