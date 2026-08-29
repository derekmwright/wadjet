package kernel

import "github.com/derekmwright/wadjet/internal/engine/batch"

// Vectorized exact fixed-point arithmetic: ADR-0024 item 3's rules applied to
// a whole batch at once.
//
// The element operations are batch.DecimalAddAt / SubAt / MulAt / DivAt /
// ModAt, which already fold in item 4's declared-precision bound. What this
// file adds is the SHAPE resolution: which side is a column and which a
// broadcast constant is a property of the EXPRESSION, not of the row, so it is
// decided once per batch and the row loop then reads two slices — or a slice
// and a register — with no type switch inside it (ADR-0002's typed-kernel
// rule, and the reason a per-row `if operand is a constant` does not appear
// below).
//
// Everything here is allocation-free: the results are written into a
// caller-owned slice, which is the projection's DECIMAL vector, and the
// operands are read from theirs. The exact big.Int fallbacks inside the batch
// primitives are the one exception and are taken only when an INTERMEDIATE
// overflows 128 bits.

// DecimalOp names the fixed-point operator a kernel applies. It is an opcode
// rather than the operator's source text so the row loop never compares a
// string; the text is resolved once, where the shape is.
type DecimalOp uint8

const (
	DecimalOpAdd DecimalOp = iota
	DecimalOpSub
	DecimalOpMul
	DecimalOpDiv
	DecimalOpMod
)

// String names the operator as SQL spells it, for the messages the wiring
// sites build.
func (o DecimalOp) String() string {
	switch o {
	case DecimalOpAdd:
		return "+"
	case DecimalOpSub:
		return "-"
	case DecimalOpMul:
		return "*"
	case DecimalOpDiv:
		return "/"
	case DecimalOpMod:
		return "%"
	}
	return "?"
}

// DecimalOpOf maps an operator's SQL text to its opcode. ok=false for an
// operator with no fixed-point rule, which the caller must route elsewhere
// rather than default into one of these.
func DecimalOpOf(op string) (DecimalOp, bool) {
	switch op {
	case "+":
		return DecimalOpAdd, true
	case "-":
		return DecimalOpSub, true
	case "*":
		return DecimalOpMul, true
	case "/":
		return DecimalOpDiv, true
	case "%":
		return DecimalOpMod, true
	}
	return 0, false
}

// DecimalOperandVec is one side of a vectorized fixed-point operation: a
// COLUMN of unscaled carriers, or a single CONSTANT broadcast over the batch.
//
// Data == nil is what makes it a constant, and it is the only discriminant —
// there is no second flag that could disagree with it. Scale is the operand's
// OWN declared scale either way: a constant carries the scale its literal text
// resolved at, which is the spelling the user wrote (ADR-0024 item 3), never
// the output's.
type DecimalOperandVec struct {
	Data  []batch.Int128
	Const batch.Int128
	Scale int
}

// DecimalArithFault is where a vectorized fixed-point operation stopped and
// why. Status DecimalOK means it ran to the end and Row is meaningless.
//
// The row travels with the status because the SQLSTATE alone does not locate
// the value: a caller reporting 22003 for `a * b` over 2048 rows wants to name
// the row whose product had no place in the declared type, and re-scanning to
// find it would run the multiply twice.
type DecimalArithFault struct {
	Status batch.DecimalStatus
	Row    int
}

// Fine reports whether the operation produced every row it was asked for.
func (f DecimalArithFault) Fine() bool { return f.Status == batch.DecimalOK }

// DecimalArithVec applies op elementwise over rows [0, n), writing the exact
// unscaled result at the declared DECIMAL(outP, outS) into out.
//
// nulls, when non-nil, is the OUTPUT's null mask, already carrying the
// combined nullity of the two operands. A null row is SKIPPED: its carriers
// are whatever the vector happened to hold, and running the operation over
// them would raise 22012 for a NULL divisor — an error where SQL says NULL.
// This kernel writes VALUES and never nullity; the caller owns the mask for
// exactly that reason.
//
// It stops at the FIRST row with no answer and reports it. The output is
// partial then and the caller must not use it: the query is over at that
// point, because ADR-0024 item 4 makes a value with no carrier an error rather
// than a number.
func DecimalArithVec(op DecimalOp, out []batch.Int128, l, r DecimalOperandVec, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	if n > len(out) {
		n = len(out)
	}
	if l.Data != nil && n > len(l.Data) {
		n = len(l.Data)
	}
	if r.Data != nil && n > len(r.Data) {
		n = len(r.Data)
	}
	// Both facts that the row loop would otherwise re-ask are settled here:
	// whether any row is null at all, and which of the three operand shapes
	// this expression has.
	if nulls != nil && !nulls.HasNulls() {
		nulls = nil
	}
	switch {
	case l.Data != nil && r.Data != nil:
		return decArithColCol(op, out, l, r, outP, outS, n, nulls)
	case l.Data != nil:
		return decArithColConst(op, out, l, r, outP, outS, n, nulls)
	case r.Data != nil:
		return decArithConstCol(op, out, l, r, outP, outS, n, nulls)
	}
	return decArithConstConst(op, out, l, r, outP, outS, n, nulls)
}

func decArithColCol(op DecimalOp, out []batch.Int128, l, r DecimalOperandVec, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	ld, rd := l.Data[:n], r.Data[:n]
	ls, rs := l.Scale, r.Scale
	for i := 0; i < n; i++ {
		if nulls != nil && nulls.IsNullFast(i) {
			continue
		}
		v, st := decApply(op, ld[i], ls, rd[i], rs, outP, outS)
		if st != batch.DecimalOK {
			return DecimalArithFault{Status: st, Row: i}
		}
		out[i] = v
	}
	return DecimalArithFault{}
}

func decArithColConst(op DecimalOp, out []batch.Int128, l, r DecimalOperandVec, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	ld := l.Data[:n]
	ls, rs, rv := l.Scale, r.Scale, r.Const
	for i := 0; i < n; i++ {
		if nulls != nil && nulls.IsNullFast(i) {
			continue
		}
		v, st := decApply(op, ld[i], ls, rv, rs, outP, outS)
		if st != batch.DecimalOK {
			return DecimalArithFault{Status: st, Row: i}
		}
		out[i] = v
	}
	return DecimalArithFault{}
}

func decArithConstCol(op DecimalOp, out []batch.Int128, l, r DecimalOperandVec, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	rd := r.Data[:n]
	ls, rs, lv := l.Scale, r.Scale, l.Const
	for i := 0; i < n; i++ {
		if nulls != nil && nulls.IsNullFast(i) {
			continue
		}
		v, st := decApply(op, lv, ls, rd[i], rs, outP, outS)
		if st != batch.DecimalOK {
			return DecimalArithFault{Status: st, Row: i}
		}
		out[i] = v
	}
	return DecimalArithFault{}
}

// decArithConstConst is two constants: one value, computed once and broadcast.
// It is here so the shape switch is total — a folding planner would never emit
// this shape and nothing depends on its speed.
func decArithConstConst(op DecimalOp, out []batch.Int128, l, r DecimalOperandVec, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	v, st := decApply(op, l.Const, l.Scale, r.Const, r.Scale, outP, outS)
	if st != batch.DecimalOK {
		return DecimalArithFault{Status: st, Row: 0}
	}
	for i := 0; i < n; i++ {
		if nulls != nil && nulls.IsNullFast(i) {
			continue
		}
		out[i] = v
	}
	return DecimalArithFault{}
}

// decApply is the single element operation, with ADR-0024 item 4's
// declared-precision bound folded in by the ...At wrappers.
//
// The op switch is five branches the predictor settles on the first row, and
// the work behind it — a 256-bit product, a rescale, a half-away-from-zero
// rounding decision — is one to two orders of magnitude larger. Hoisting the
// switch out of the loops above would cost five copies of each of them and buy
// nothing the benchmark can see.
func decApply(op DecimalOp, a batch.Int128, aScale int, b batch.Int128, bScale, outP, outS int) (batch.Int128, batch.DecimalStatus) {
	switch op {
	case DecimalOpAdd:
		return batch.DecimalAddAt(a, aScale, b, bScale, outP, outS)
	case DecimalOpSub:
		return batch.DecimalSubAt(a, aScale, b, bScale, outP, outS)
	case DecimalOpMul:
		return batch.DecimalMulAt(a, aScale, b, bScale, outP, outS)
	case DecimalOpDiv:
		return batch.DecimalDivAt(a, aScale, b, bScale, outP, outS)
	case DecimalOpMod:
		return batch.DecimalModAt(a, aScale, b, bScale, outP, outS)
	}
	return batch.Int128{}, batch.DecimalInvalidScale
}

// DecimalScalarVec applies a one-argument scalar math function elementwise —
// abs/ceil/floor/round/trunc/sign over a DECIMAL column — writing the exact
// result at the declared DECIMAL(outP, outS) into out.
//
// It is the execution half of batch.DecimalScalarType, and the two must agree:
// the type says how many digits the answer keeps and this produces exactly
// those digits. Rounding is half away from zero throughout (batch.Rescale),
// which is PostgreSQL's numeric rounding.
//
// nulls has DecimalArithVec's meaning and the same reason.
// digits is round/trunc's second argument, ignored by the other ops.
func DecimalScalarVec(op batch.DecimalScalarOp, out []batch.Int128, in []batch.Int128, inScale, digits, outP, outS, n int, nulls *batch.Bitmap) DecimalArithFault {
	if n > len(out) {
		n = len(out)
	}
	if n > len(in) {
		n = len(in)
	}
	if nulls != nil && !nulls.HasNulls() {
		nulls = nil
	}
	for i := 0; i < n; i++ {
		if nulls != nil && nulls.IsNullFast(i) {
			continue
		}
		v, st := batch.DecimalScalar(op, in[i], inScale, digits, outP, outS)
		if st != batch.DecimalOK {
			return DecimalArithFault{Status: st, Row: i}
		}
		out[i] = v
	}
	return DecimalArithFault{}
}
