// This file holds expr arith; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Arithmetic ---

// BinOp is a binary arithmetic expression (generic, uses ToFloat64).
type BinOp struct {
	Left, Right Expr
	Op          string // +, -, *, /, %
	// dec is the EXACT fixed-point arm (ADR-0024 item 3, #555). This node is
	// where operands with no typed protocol meet — a negated column, a CAST —
	// and a DECIMAL is one of them, because its box is text and nothing about
	// it satisfies Float64Expr for compileBinOp to see. Resolved once, against
	// the first batch. See binop_decimal.go.
	dec decArm
	// ints is the INTEGER arm, resolved the same way and for the same reason:
	// this node is also where a CAST, a function and a choice construct meet,
	// and an integer expression under one of those keeps its domain and its
	// 22003 rather than being read through a double (#849, int_domain.go).
	ints intArm
}

func (e *BinOp) Eval(b *batch.RecordBatch, row int) any {
	// Exact fixed-point arithmetic, ahead of the ToFloat64 pair below: `-a + b`
	// over two DECIMAL columns is a numeric expression in PostgreSQL and an
	// exact one here, where reading both boxes as doubles loses every digit
	// past the sixteenth.
	if v, ok := e.binOpDecimalBox(b, row); ok {
		return v
	}
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return nil
	}

	// Date ± interval arithmetic. Subtraction is not commutative, so only the
	// date-on-the-left form takes an interval on the right; `interval + date`
	// is the one reversed shape that means anything.
	if e.Op == "+" || e.Op == "-" {
		if iv, ok := rv.(IntervalValue); ok {
			if dv, ok := temporalOperand(b, row, e.Left, lv); ok {
				return intervalShift(dv, iv, e.Op == "-")
			}
		}
		if iv, ok := lv.(IntervalValue); ok && e.Op == "+" {
			if dv, ok := temporalOperand(b, row, e.Right, rv); ok {
				return intervalShift(dv, iv, false)
			}
		}
		// date - date and date ± n. Declines for everything that is not
		// date arithmetic, leaving the numeric path below untouched (#340).
		if res, ok := e.dateArith(b, row, lv, rv); ok {
			return res
		}
	}

	// The INTEGER domain, ahead of the ToFloat64 pair below and for the same
	// reason the decimal arm is: this node is where a CAST, a function and a
	// choice construct meet, and PostgreSQL types every one of those integer
	// when its operands are. Reading them through a double answered
	// 9.223399706970886e+24 for a product the server refuses with 22003, and
	// answered the non-overflowing shapes under float8 where the server
	// declares bigint (#849, ADR-0024 item 2). `/` is not here: its own arm
	// below has carried the integer rule since #369.
	if e.intMode(b) {
		if v, ok := e.intArith(lv, rv); ok {
			return v
		}
	}
	lf := ToFloat64(lv)
	rf := ToFloat64(rv)
	switch e.Op {
	case "+":
		return lf + rf
	case "-":
		return lf - rf
	case "*":
		return lf * rf
	case "/":
		// int / int is INTEGER division, truncating toward zero (#369,
		// PostgreSQL semantics per ADR-0012). This generic node is where
		// operands without a typed protocol arrive — a CAST, a negated
		// column — so the rule must hold here too, not only in the typed
		// kernels. A float on either side keeps float division.
		if li, lok := toInt64Safe(lv); lok {
			if ri, rok := toInt64Safe(rv); rok {
				if ri == 0 {
					return nil
				}
				// The CHECKED divide, like every other integer arm: this node
				// is where operands with no typed protocol arrive, so
				// MinInt64 / -1 reached it too and wrapped back to MinInt64
				// where PostgreSQL raises `bigint out of range` (#637, #555
				// review).
				return divInt64Checked(li, ri)
			}
		}
		if rf == 0 {
			// Both operands are non-NULL here (checked above), so this is a
			// genuine zero divisor: PostgreSQL refuses it and so do we (#367).
			raiseDivisionByZero()
		}
		return lf / rf
	case "%":
		if rf == 0 {
			raiseDivisionByZero()
		}
		// math.Mod — see BinOpFloat64's arithMod arm for why truncating to
		// integers first was both wrong and a crash (#555 review, N1).
		return math.Mod(lf, rf)
	default:
		return nil
	}
}

// BinOpFloat64 is a typed binary op that operates on float64 without boxing.
// Uses a pre-resolved arithOp opcode for the hot EvalFloat64 path to avoid
// per-row string comparison on the Op field. The opcode is resolved lazily
// so external callers can construct BinOpFloat64 directly with only Op
// populated.
//
// opReady is a double-checked atomic flag rather than sync.Once: Once.Do
// builds a closure and loads the done flag on EVERY row, and that closure
// keeps resolveOpCode too big to inline. This form is small enough that the
// compiler inlines resolveOpCode straight into EvalFloat64 (verified with
// -gcflags='-m'), the same guard BinOpNumeric.resolveMode and ColRef.resolve
// already use. opReady publishes opCode: set last under opMu, read first
// (and alone) by EvalFloat64 — concurrent pipeline workers share one
// *BinOpFloat64 through a captured closure, same as those two.
type BinOpFloat64 struct {
	Left, Right Float64Expr
	Op          string
	opCode      arithOp
	opReady     atomic.Bool
	opMu        sync.Mutex
	vecBuf      []float64 // scratch buffer for vectorized evaluation
}

func (e *BinOpFloat64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalFloat64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpFloat64) resolveOpCode() {
	if !e.opReady.Load() {
		e.resolveOpCodeSlow()
	}
}

// resolveOpCodeSlow runs exactly once per node.
func (e *BinOpFloat64) resolveOpCodeSlow() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.opReady.Load() {
		return
	}
	e.opCode = resolveArithOp(e.Op)
	e.opReady.Store(true)
}

func (e *BinOpFloat64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	lf, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return 0, false
	}
	rf, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return 0, false
	}
	e.resolveOpCode()
	switch e.opCode {
	case arithAdd:
		return lf + rf, true
	case arithSub:
		return lf - rf, true
	case arithMul:
		return lf * rf, true
	case arithDiv:
		if rf == 0 {
			// A NULL divisor returned above already; a zero here is genuine.
			raiseDivisionByZero()
		}
		return lf / rf, true
	case arithMod:
		if rf == 0 {
			raiseDivisionByZero()
		}
		// math.Mod, not `int64(lf) % int64(rf)`: truncating both operands to
		// integers first answered 1 for `7.9 % 3.0` where every engine with
		// the operator answers 1.9, and — worse — it PANICKED with an integer
		// divide by zero for a divisor whose integer part is 0, which is
		// every `x % 0.5` (#555 review, N1). PostgreSQL has no `%` over
		// double precision at all, so this pair is a superset either way; a
		// superset that crashes the query is not one.
		return math.Mod(lf, rf), true
	default:
		return 0, false
	}
}

// CloneVec creates a deep copy of the BinOpFloat64 tree with fresh scratch
// buffers. Required for parallel pipeline execution where multiple workers
// must not share mutable vecBuf state. Stateless leaf nodes (ColRef, Literal)
// are shared; only BinOpFloat64 nodes (which own vecBuf) are cloned.
func (e *BinOpFloat64) CloneVec() *BinOpFloat64 {
	clone := &BinOpFloat64{
		Op:     e.Op,
		opCode: e.opCode,
		// vecBuf intentionally nil — each clone allocates on first use
	}
	if child, ok := e.Left.(*BinOpFloat64); ok {
		clone.Left = child.CloneVec()
	} else {
		clone.Left = e.Left
	}
	if child, ok := e.Right.(*BinOpFloat64); ok {
		clone.Right = child.CloneVec()
	} else {
		clone.Right = e.Right
	}
	return clone
}

// EvalFloat64Vec evaluates left and right operands in bulk, then applies the
// arithmetic op in a tight loop. Eliminates ~5 function calls per row.
func (e *BinOpFloat64) EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool {
	e.resolveOpCode()

	// Fused (column op constant) fast path: one typed convert+op loop, no
	// constant-vector fill and no separate column→float64 conversion pass.
	// ClickBench Q30 (90 SUM(col + k) expressions over one int16 column)
	// spent 20% of CPU materializing constant vectors and 25% re-converting
	// the same column per expression; this path removes both.
	if cr, ok := e.Left.(*ColRef); ok {
		if lit, ok2 := e.Right.(*Lit); ok2 && lit.Val != nil {
			if done, hasNull := fusedColConstFloat64(b, cr, ToFloat64(lit.Val), e.opCode, false, dst, n); done {
				return hasNull
			}
		}
	}
	if lit, ok := e.Left.(*Lit); ok && lit.Val != nil {
		if cr, ok2 := e.Right.(*ColRef); ok2 {
			if done, hasNull := fusedColConstFloat64(b, cr, ToFloat64(lit.Val), e.opCode, true, dst, n); done {
				return hasNull
			}
		}
	}

	// Check if children support vectorized evaluation
	leftVec, leftOK := e.Left.(VecFloat64Expr)
	rightVec, rightOK := e.Right.(VecFloat64Expr)
	if !leftOK || !rightOK {
		// Fallback to per-row evaluation
		hasNull := false
		for i := 0; i < n; i++ {
			v, ok := e.EvalFloat64(b, i)
			dst[i] = v
			if !ok {
				hasNull = true
			}
		}
		return hasNull
	}

	// Evaluate right into dst, left into scratch buffer.
	// Reuse scratch slice across calls; grow only if needed.
	rightNull := rightVec.EvalFloat64Vec(b, dst, n)
	if cap(e.vecBuf) < n {
		e.vecBuf = make([]float64, n)
	}
	tmp := e.vecBuf[:n]
	leftNull := leftVec.EvalFloat64Vec(b, tmp, n)

	// Apply op in tight loop (compiler can auto-vectorize these)
	switch e.opCode {
	case arithAdd:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] + dst[i]
		}
	case arithSub:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] - dst[i]
		}
	case arithMul:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] * dst[i]
		}
	case arithDiv:
		// A zero divisor slot is either a NULL row (whose placeholder is 0)
		// or a genuine zero. This loop cannot tell the two apart, so it
		// reports "has nulls" and lets the caller's per-row pass decide:
		// EvalFloat64 answers NULL for the NULL row and raises 22012
		// (division by zero) for the genuine one. Before this, a zero
		// divisor silently produced 0 (#367).
		sawZero := false
		for i := 0; i < n; i++ {
			if dst[i] != 0 {
				dst[i] = tmp[i] / dst[i]
			} else {
				sawZero = true
			}
		}
		if sawZero {
			return true
		}
	case arithMod:
		sawZero := false
		for i := 0; i < n; i++ {
			if dst[i] != 0 {
				dst[i] = float64(int64(tmp[i]) % int64(dst[i]))
			} else {
				sawZero = true
			}
		}
		if sawZero {
			return true
		}
	}

	return leftNull || rightNull
}

// fusedColConstFloat64 computes dst = col op c (or c op col when constFirst)
// in a single typed loop. Returns done=false when the column type or op has
// no fused kernel — the caller falls through to the generic two-pass path.
// Division keeps the fused path only when the CONSTANT is the (non-zero)
// divisor; per-element zero-divisor handling stays on the generic path.
func fusedColConstFloat64(b *batch.RecordBatch, cr *ColRef, c float64, op arithOp, constFirst bool, dst []float64, n int) (bool, bool) {
	switch op {
	case arithAdd, arithSub, arithMul:
	case arithDiv:
		if constFirst || c == 0 {
			return false, false
		}
	default:
		return false, false
	}
	cr.resolve(b)
	if cr.idx < 0 || cr.idx >= len(b.Columns) {
		return false, false
	}
	v := b.Columns[cr.idx]

	apply := func(get func(i int) float64) {
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = get(i) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = get(i) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - get(i)
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = get(i) * c
			}
		case op == arithDiv:
			for i := 0; i < n; i++ {
				dst[i] = get(i) / c
			}
		}
	}

	// Monomorphic loops per storage type: the closure indirection above
	// would defeat the point, so each case runs its own tight loop.
	switch cr.typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		src := v.Int32Data
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - float64(src[i])
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) * c
			}
		default:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) / c
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		src := v.Int64Data
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - float64(src[i])
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) * c
			}
		default:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) / c
			}
		}
	case batch.TypeFloat64:
		apply(func(i int) float64 { return v.Float64Data[i] })
	case batch.TypeFloat32:
		apply(func(i int) float64 { return float64(v.Float32Data[i]) })
	default:
		return false, false
	}
	return true, v.Nulls.HasNulls()
}

// BinOpInt64 is a typed binary op that operates on int64 without boxing.
// opCode is resolved lazily through the same double-checked atomic.Bool
// guard as BinOpFloat64 — see that type for why sync.Once doesn't fit the
// inliner and this does.
type BinOpInt64 struct {
	Left, Right Int64Expr
	Op          string
	opCode      arithOp
	opReady     atomic.Bool
	opMu        sync.Mutex
}

func (e *BinOpInt64) resolveOpCode() {
	if !e.opReady.Load() {
		e.resolveOpCodeSlow()
	}
}

// resolveOpCodeSlow runs exactly once per node.
func (e *BinOpInt64) resolveOpCodeSlow() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.opReady.Load() {
		return
	}
	e.opCode = resolveArithOp(e.Op)
	e.opReady.Store(true)
}

func (e *BinOpInt64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalInt64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpInt64) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return 0, false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return 0, false
	}
	e.resolveOpCode()
	// Checked: an integer result with no int64 is 22003, PostgreSQL's
	// `bigint out of range`, and never the wrapped number Go's operators
	// answer (#637 — int_overflow.go).
	switch e.opCode {
	case arithAdd:
		return addInt64Checked(lv, rv), true
	case arithSub:
		return subInt64Checked(lv, rv), true
	case arithMul:
		return mulInt64Checked(lv, rv), true
	case arithDiv:
		// Operands are non-NULL here, so a zero divisor is a genuine one.
		return divInt64Checked(lv, rv), true
	case arithMod:
		return modInt64Checked(lv, rv), true
	default:
		return 0, false
	}
}

// EvalFloat64 allows BinOpInt64 to be used as Float64Expr (int→float promotion).
func (e *BinOpInt64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	v, ok := e.EvalInt64(b, row)
	return float64(v), ok
}

// UnaryOp is a unary arithmetic expression (negation).
type UnaryOp struct {
	Operand Expr
	Op      string // -, +
}

func (e *UnaryOp) Eval(b *batch.RecordBatch, row int) any {
	// A DECIMAL operand negates EXACTLY, on the carrier, and boxes as its
	// rendered text — the same box a DECIMAL column produces. Asked first,
	// because the numeric arms below reach a decimal only through
	// ToFloat64 of that text (ADR-0024, #555).
	if v, ok := e.unaryDecimalBox(b, row); ok {
		return v
	}
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	// Negating an integer stays an integer (SQL and Go agree), which is what
	// keeps `-int_col / 2` on the integer-division path (#369).
	switch e.Op {
	case "-":
		if i, ok := toInt64Safe(v); ok {
			if i == math.MinInt64 {
				// |MinInt64| has no int64 — two's complement has one more
				// negative value than positive — so this WRAPPED to itself and
				// `-bigint_min` answered a NEGATIVE number under a right type.
				// PostgreSQL raises `bigint out of range`, measured. Same
				// refusal as absKeepsDomain's, one operator over (review round
				// 0, P2/P3).
				raiseBigintOutOfRange()
			}
			return -i
		}
		return -ToFloat64(v)
	case "+":
		if i, ok := toInt64Safe(v); ok {
			return i
		}
		return ToFloat64(v)
	default:
		return v
	}
}
