package expr

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DECIMAL mode of BinOpNumeric: `+ - * / %` computed EXACTLY on the Int128
// carrier instead of through float64 (ADR-0024 items 3 and 4, #555).
//
// Before this, every arithmetic expression with a DECIMAL operand resolved
// float mode — `operandIsInt` accepts only INT32/INT64 — so `d_2 - d_4` over
// 12.75 and 12.7500 answered -9.999999999976694e-05 where the exact difference
// is 0, and `d / d` answered 1 where PostgreSQL answers 0.99999215690465.
//
// The mode is resolved once per node against the first batch, beside the int
// and float ones and for the same reason: a column's type does not exist until
// a batch arrives. What it needs beyond the type is the operands' (p,s), and
// that comes from two places — the vector carries the SCALE and the batch
// SCHEMA carries the precision.
//
// Two execution paths, both exact and both required to agree (the two-path
// rule of ADR-0018 §3):
//
//   - the BOXED path (Eval), which every row-at-a-time consumer takes and
//     which the stage DAG takes for every projection. It answers the value's
//     rendered TEXT, exactly as a DECIMAL COLUMN's box is — so every consumer
//     that already knows how to read a decimal box (the comparison layer, the
//     group-key encoder, Vector.SetValueChecked) reads a computed decimal the
//     same way, with no new box type to teach them.
//   - the VECTORIZED path (EvalDecimalVec), which writes unscaled carriers
//     straight into the projection's DECIMAL vector with no boxing and no
//     allocation.

// decimalOperand is an operand that can produce EXACT fixed-point values: a
// DECIMAL or integer column, a numeric literal, or nested decimal arithmetic.
//
// It is deliberately not "anything numeric". A FLOAT operand makes the whole
// expression float8 — PostgreSQL's rule, because float8 is the preferred type
// of the numeric category (ADR-0024 item 2) — so a float operand must NOT
// implement this, or `d * f` would answer an exact decimal where PostgreSQL
// answers a double.
type decimalOperand interface {
	// decimalType reports the (p,s) this operand's values carry in this
	// batch. ok=false means it produces no fixed-point value and the caller
	// must not take the decimal mode.
	decimalType(b *batch.RecordBatch) (batch.DecimalType, bool)
	// evalDecimal reads one row's exact unscaled value at decimalType's
	// scale. ok=false is SQL NULL, never a zero.
	evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool)
	// decimalVec exposes the operand as a whole column, or as a constant
	// broadcast over the batch, for the vectorized kernel. ok=false means it
	// has no columnar form and the caller falls back to evalDecimal per row —
	// still unboxed, just not a slice walk.
	decimalVec(b *batch.RecordBatch) (kernel.DecimalOperandVec, bool)
}

// DecimalVecExpr is an expression that can write EXACT fixed-point results
// into a DECIMAL output vector for a whole batch at once.
//
// It is separate from VecExpr because exec.Project's DECIMAL arm runs ahead of
// every vectorized path — the checked per-row writer is the only route with an
// error channel, and no other vec kernel writes DecimalData. A distinct
// interface lets the one kernel that does write it skip the box without
// changing that ordering for anything else.
type DecimalVecExpr interface {
	EvalDecimalVec(b *batch.RecordBatch, out *batch.Vector, n int)
}

// --- ColRef -----------------------------------------------------------------

// colRefDecimalType reads a column's fixed-point declaration out of the batch.
//
// The SCALE comes from the vector, which is the only place it is certain: a
// DECIMAL vector's values ARE unscaled integers at that scale, so reading them
// at any other one is the #533 misreading. The PRECISION comes from the batch
// SCHEMA, which is where a parquet.Column's declaration travels.
//
// A schema that carries no precision falls back to the carrier's full width.
// That is the honest answer rather than a guess — 38 is what an Int128 holds,
// so it bounds nothing the carrier does not already bound — and it is safe in
// the one direction that matters: it can only ADMIT a value a narrower
// declaration would have refused, never produce a different number. The scale,
// which is what would change a value, never falls back.
func colRefDecimalType(v *batch.Vector, declaredPrecision int) batch.DecimalType {
	p := batch.MaxDecimalPrecision
	if declaredPrecision > 0 {
		p = declaredPrecision
	}
	s := v.DecimalData.Scale
	if p < s {
		p = s
	}
	return batch.DecimalType{Precision: p, Scale: s}
}

func (e *ColRef) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return batch.DecimalType{}, false
	}
	// A ROW FIELD PATH is typed from the FIELD, not the container: `rw.d + 1`
	// and `d + 1` over the same value are two spellings of one question and
	// must answer the same type (#568's rule). The field's scale is on its
	// child vector and its precision on the container's schema entry, which
	// is the same pair colRefDecimalType reads one level up.
	if e.structField != "" {
		fv, _, ok := e.fieldVector(b, 0)
		if !ok {
			return batch.DecimalType{}, false
		}
		return vectorDecimalType(fv, e.fieldSchemaPrecision(b))
	}
	return vectorDecimalType(b.Columns[e.idx], colRefSchemaPrecision(b.Schema, e.idx))
}

// vectorDecimalType is a vector's fixed-point contribution: its own (p,s) for
// a DECIMAL, and the whole integer range at scale 0 for an integer.
func vectorDecimalType(v *batch.Vector, precision int) (batch.DecimalType, bool) {
	switch v.Type {
	case batch.TypeDecimal:
		return colRefDecimalType(v, precision), true
	case batch.TypeInt32:
		return batch.DecimalType{Precision: batch.Int32DecimalDigits}, true
	case batch.TypeInt64:
		return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
	}
	return batch.DecimalType{}, false
}

// colRefSchemaPrecision is a plain column's declared precision, 0 when the
// batch's schema does not carry one.
func colRefSchemaPrecision(schema []parquet.Column, idx int) int {
	if idx < 0 || idx >= len(schema) {
		return 0
	}
	return schema[idx].Precision
}

// fieldSchemaPrecision is a ROW field's declared precision, read off the
// container's schema entry — the only place a field's declaration exists in a
// batch, since the child VECTOR carries the scale and nothing else.
func (e *ColRef) fieldSchemaPrecision(b *batch.RecordBatch) int {
	if e.idx < 0 || e.idx >= len(b.Schema) {
		return 0
	}
	f, ok := b.Schema[e.idx].Field(e.structField)
	if !ok {
		return 0
	}
	return f.Precision
}

func (e *ColRef) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return batch.Int128{}, false
	}
	v, r := b.Columns[e.idx], row
	if e.structField != "" {
		var ok bool
		if v, r, ok = e.fieldVector(b, row); !ok {
			return batch.Int128{}, false
		}
	}
	if v.Nulls.IsNullFast(r) {
		return batch.Int128{}, false
	}
	switch v.Type {
	case batch.TypeDecimal:
		return v.DecimalData.Data[r], true
	case batch.TypeInt32:
		return batch.Int128From(int64(v.Int32Data[r])), true
	case batch.TypeInt64:
		return batch.Int128From(v.Int64Data[r]), true
	}
	return batch.Int128{}, false
}

func (e *ColRef) decimalVec(b *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) || e.structField != "" {
		return kernel.DecimalOperandVec{}, false
	}
	v := b.Columns[e.idx]
	if v.Type != batch.TypeDecimal {
		// An INTEGER column has no []Int128 to hand over. Materializing one
		// would cost a per-batch buffer that has to be cloned for every
		// parallel worker; the per-row arm reads Int64Data directly and is
		// unboxed either way.
		return kernel.DecimalOperandVec{}, false
	}
	return kernel.DecimalOperandVec{Data: v.DecimalData.Data, Scale: v.DecimalData.Scale}, true
}

// --- Lit --------------------------------------------------------------------

// litDecimal resolves a numeric literal into its own fixed-point type and
// value — ADR-0024 item 3's "a numeric literal's (p,s) is its spelling".
//
// The carrier is the literal's verbatim TEXT, never the float64 box the
// compiler built for arithmetic: that box has already lost every digit past a
// double's ~16 (ADR-0012 item 6), which is the loss `d = 493827160549382.716…`
// taught this codebase once already.
//
// A literal whose spelling has no DECIMAL type at all — 40 fraction digits, or
// an exponent past the carrier — reports false, and the expression stays on
// the float path it was on before. That is the same answer as today rather
// than a truncation.
func litDecimal(text string) (batch.DecimalType, batch.Int128, bool) {
	t, ok := batch.DecimalTextType(text)
	if !ok {
		return batch.DecimalType{}, batch.Int128{}, false
	}
	d, ok := batch.DecimalTextAt(text, t.Scale)
	if !ok || d.Residual != 0 || d.Sat != 0 {
		return batch.DecimalType{}, batch.Int128{}, false
	}
	return t, d.Unscaled, true
}

func (e *Lit) decimalType(_ *batch.RecordBatch) (batch.DecimalType, bool) {
	t, _, ok := e.decimalValue()
	return t, ok
}

func (e *Lit) evalDecimal(_ *batch.RecordBatch, _ int) (batch.Int128, bool) {
	_, v, ok := e.decimalValue()
	return v, ok
}

func (e *Lit) decimalVec(_ *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	t, v, ok := e.decimalValue()
	if !ok {
		return kernel.DecimalOperandVec{}, false
	}
	return kernel.DecimalOperandVec{Const: v, Scale: t.Scale}, true
}

// decimalValue reads the literal's exact fixed-point form. Only a NUMERIC
// literal has one: Lit.Text is set by compileLit for exactly that shape and is
// empty for every other kind, so it is both the carrier and the test.
func (e *Lit) decimalValue() (batch.DecimalType, batch.Int128, bool) {
	if e.Text == "" {
		return batch.DecimalType{}, batch.Int128{}, false
	}
	return litDecimal(e.Text)
}

// --- BinOpNumeric's decimal mode --------------------------------------------

// decMode is everything the decimal arm of BinOpNumeric resolved from the
// first batch. It is published as a whole, under BinOpNumeric's modeReady, so
// the row loops read immutable fields.
type decMode struct {
	op   kernel.DecimalOp
	l, r batch.DecimalType
	out  batch.DecimalType
	// text is the operator as SQL spells it, kept for the error messages so a
	// 22003 can name the operation that produced the value.
	text string
}

// resolveDecimalMode decides whether this node computes in exact fixed point,
// and at what type.
//
// Both operands must produce fixed-point values and at least one must be a
// genuine DECIMAL. `int + int` stays integer — PostgreSQL's rule and the whole
// of #636's truncating division — so an all-integer pair declines here and
// falls through to the int mode below it.
//
// A FLOAT operand does not implement decimalOperand at all, so `d * f` never
// reaches this and resolves float mode, which is what PostgreSQL answers.
func resolveDecimalMode(op string, left, right Expr, b *batch.RecordBatch) (decMode, bool) {
	code, ok := kernel.DecimalOpOf(op)
	if !ok {
		return decMode{}, false
	}
	lo, lok := left.(decimalOperand)
	ro, rok := right.(decimalOperand)
	if !lok || !rok {
		return decMode{}, false
	}
	lt, lok := lo.decimalType(b)
	rt, rok := ro.decimalType(b)
	if !lok || !rok {
		return decMode{}, false
	}
	if !operandIsDecimalTyped(left, b) && !operandIsDecimalTyped(right, b) {
		return decMode{}, false
	}
	p, s, ok := batch.DecimalResultType(op, lt.Precision, lt.Scale, rt.Precision, rt.Scale)
	if !ok {
		return decMode{}, false
	}
	return decMode{
		op:   code,
		l:    lt,
		r:    rt,
		out:  batch.DecimalType{Precision: p, Scale: s},
		text: op,
	}, true
}

// operandIsDecimalTyped reports whether an operand is a DECIMAL rather than an
// integer wearing a fixed-point type. It is what keeps `i64 + i64` on the
// integer path: both operands answer decimalType (an integer IS DECIMAL(19,0)
// for a result-type computation), and only this tells the two shapes apart.
func operandIsDecimalTyped(e Expr, b *batch.RecordBatch) bool {
	switch v := e.(type) {
	case *ColRef:
		v.resolve(b)
		if v.idx < 0 || v.idx >= len(b.Columns) {
			return false
		}
		if v.structField != "" {
			return v.fieldTyp == batch.TypeDecimal
		}
		return v.typ == batch.TypeDecimal
	case *Lit:
		// A numeric literal with a FRACTION is a decimal the user wrote:
		// `i64 * 1.5` is numeric in PostgreSQL, not integer. A whole-number
		// literal is not, so `i64 * 2` stays integer.
		t, _, ok := v.decimalValue()
		return ok && t.Scale > 0
	case *BinOpNumeric:
		v.resolveMode(b)
		return v.isDec
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && operandIsDecimalTyped(v.Operand, b)
	case *Cast:
		// A cast that NAMES a (p,s) produces an exact DECIMAL; a bare one's
		// type is the operand's, resolved per value, so it is not a
		// declaration this layer can compute an arithmetic result from.
		return castIsExactDecimal(v)
	}
	return false
}

// decimalType lets a nested BinOpNumeric be an operand of another: `(a+b)*c`
// computes the inner sum at its own result type and the outer product from
// that, which is the same nesting PostgreSQL's numeric does.
func (e *BinOpNumeric) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	e.resolveMode(b)
	if !e.isDec {
		return batch.DecimalType{}, false
	}
	return e.dec.out, true
}

func (e *BinOpNumeric) decimalVec(_ *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	// A nested node has no materialized column of its own. The caller's
	// per-row arm reads it through evalDecimal, unboxed.
	return kernel.DecimalOperandVec{}, false
}

// evalDecimal computes one row exactly, raising rather than answering when the
// value has no place in the declared type (ADR-0024 item 4).
func (e *BinOpNumeric) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	e.resolveMode(b)
	lo, _ := e.Left.(decimalOperand)
	ro, _ := e.Right.(decimalOperand)
	lv, lok := lo.evalDecimal(b, row)
	if !lok {
		return batch.Int128{}, false
	}
	rv, rok := ro.evalDecimal(b, row)
	if !rok {
		return batch.Int128{}, false
	}
	v, st := decApplyChecked(e.dec, lv, rv)
	return v, st
}

// decApplyChecked runs one element operation and turns a non-OK status into
// the query error it names, rather than returning a value nobody may see.
func decApplyChecked(m decMode, a, bb batch.Int128) (batch.Int128, bool) {
	var v batch.Int128
	var st batch.DecimalStatus
	switch m.op {
	case kernel.DecimalOpAdd:
		v, st = batch.DecimalAddAt(a, m.l.Scale, bb, m.r.Scale, m.out.Precision, m.out.Scale)
	case kernel.DecimalOpSub:
		v, st = batch.DecimalSubAt(a, m.l.Scale, bb, m.r.Scale, m.out.Precision, m.out.Scale)
	case kernel.DecimalOpMul:
		v, st = batch.DecimalMulAt(a, m.l.Scale, bb, m.r.Scale, m.out.Precision, m.out.Scale)
	case kernel.DecimalOpDiv:
		v, st = batch.DecimalDivAt(a, m.l.Scale, bb, m.r.Scale, m.out.Precision, m.out.Scale)
	case kernel.DecimalOpMod:
		v, st = batch.DecimalModAt(a, m.l.Scale, bb, m.r.Scale, m.out.Precision, m.out.Scale)
	}
	if st != batch.DecimalOK {
		raiseDecimalStatus(st, m.out.Precision, m.out.Scale)
	}
	return v, true
}

// evalDecimalText is the BOXED answer: the exact value rendered the way a
// DECIMAL COLUMN's box is rendered (Vector.GetValue → FormatDecimal), so that
// every consumer of a boxed value reads a computed decimal exactly as it reads
// a stored one. A box of a new shape would have to be taught to the comparison
// layer, the group-key encoder, the sort comparator and the checked vector
// writer separately, and each one that missed it would be a silent wrong
// answer of the class ADR-0024 exists to close.
func (e *BinOpNumeric) evalDecimalText(b *batch.RecordBatch, row int) any {
	v, ok := e.evalDecimal(b, row)
	if !ok {
		return nil
	}
	return v.FormatDecimal(e.dec.out.Scale)
}

// EvalDecimalVec computes the whole batch into a DECIMAL output vector.
//
// The output's SCALE is read from the vector rather than from this node's own
// resolved type. They are the same number whenever the planner and the runtime
// resolved the same operand declarations, which is the ordinary case — and
// where they are not, the vector's scale is the one the value must be stored
// at, so computing at it rounds ONCE, in the right place, instead of rounding
// here and rounding again on the way in.
//
// The precision bound is the vector's own scale plus the carrier's width: a
// batch.Vector carries no precision (DecimalColumn is Data plus Scale), so the
// declared bound this node resolved is applied through the checked element
// path only when the two scales agree. That is the same direction of safety
// colRefDecimalType takes — a wider bound can only ADMIT a value, never change
// one — and the boxed path, which the stage DAG always takes, still applies
// the declared bound in full.
func (e *BinOpNumeric) EvalDecimalVec(b *batch.RecordBatch, out *batch.Vector, n int) {
	e.resolveMode(b)
	if !e.isDec || out == nil || out.Type != batch.TypeDecimal {
		return
	}
	outS := out.DecimalData.Scale
	outP := e.dec.out.Precision
	if outS != e.dec.out.Scale {
		// The declaration this node resolved does not describe the vector it
		// is writing into. Keep the vector's scale (it is what the value is
		// stored at) and fall back to the carrier's bound, so the mismatch
		// cannot turn into a refusal of a value the column can hold.
		outP = batch.MaxDecimalPrecision
	}
	lv, lok := e.Left.(decimalOperand).decimalVec(b)
	rv, rok := e.Right.(decimalOperand).decimalVec(b)
	if lok && rok {
		markColumnarNulls(e.Left, e.Right, b, out, n)
		f := kernel.DecimalArithVec(e.dec.op, out.DecimalData.Data, lv, rv, outP, outS, n, &out.Nulls)
		if !f.Fine() {
			raiseDecimalStatus(f.Status, outP, outS)
		}
		return
	}
	// One operand has no columnar form — an integer column, or nested
	// arithmetic. Still unboxed: the values come out of evalDecimal and go
	// straight into the carrier slice.
	lo := e.Left.(decimalOperand)
	ro := e.Right.(decimalOperand)
	m := e.dec
	m.out = batch.DecimalType{Precision: outP, Scale: outS}
	for i := 0; i < n && i < len(out.DecimalData.Data); i++ {
		a, aok := lo.evalDecimal(b, i)
		if !aok {
			out.Nulls.SetNull(i)
			continue
		}
		c, cok := ro.evalDecimal(b, i)
		if !cok {
			out.Nulls.SetNull(i)
			continue
		}
		v, _ := decApplyChecked(m, a, c)
		out.DecimalData.Data[i] = v
	}
}

// markColumnarNulls writes the output's nullity — the union of the operands'
// — before any value is computed, so the kernel can SKIP a null row rather
// than divide by the zero carrier a null leaves behind in the vector.
//
// It handles exactly the two operand shapes decimalVec answers for: a plain
// column, whose nullity is its mask, and a numeric literal, which is a value
// and never NULL. Anything else has no columnar form, so the caller never
// reaches here with one.
func markColumnarNulls(left, right Expr, b *batch.RecordBatch, out *batch.Vector, n int) {
	lm := columnNullMask(left, b)
	rm := columnNullMask(right, b)
	if lm == nil && rm == nil {
		for i := 0; i < n; i++ {
			out.Nulls.SetValid(i)
		}
		return
	}
	for i := 0; i < n; i++ {
		if (lm != nil && lm.IsNullFast(i)) || (rm != nil && rm.IsNullFast(i)) {
			out.Nulls.SetNull(i)
		} else {
			out.Nulls.SetValid(i)
		}
	}
}

// columnNullMask is a column operand's null bitmap, or nil for an operand that
// never produces one (a literal).
func columnNullMask(e Expr, b *batch.RecordBatch) *batch.Bitmap {
	c, ok := e.(*ColRef)
	if !ok {
		return nil
	}
	c.resolve(b)
	if c.idx < 0 || c.idx >= len(b.Columns) {
		return nil
	}
	v := b.Columns[c.idx]
	if !v.Nulls.HasNulls() {
		return nil
	}
	return &v.Nulls
}

// --- The generic BinOp's decimal arm ----------------------------------------

// decArm is the lazily-resolved decimal mode, shared by the two arithmetic
// nodes that can meet a DECIMAL operand.
//
// BinOpNumeric gets it because it is the node every `column op something`
// compiles to. The GENERIC BinOp gets it because it is where operands with no
// typed protocol arrive — a negated column, a CAST — and a DECIMAL is exactly
// such an operand: its box is text, so nothing about it satisfies Float64Expr
// in a way compileBinOp can see. Without this arm `-a + b` fell through to
// `ToFloat64(lv)` and answered a float where the planner had already declared
// (and allocated) an exact DECIMAL vector, which the checked store then
// refused — loud, but not the answer.
type decArm struct {
	ready atomic.Bool
	mu    sync.Mutex
	on    bool
	mode  decMode
}

// resolve settles the mode once per node against the first batch that can
// answer it, the way BinOpNumeric.resolveMode does and for the same reason: an
// operand's declaration does not exist until a batch arrives.
func (a *decArm) resolve(op string, left, right Expr, b *batch.RecordBatch) (decMode, bool) {
	if a.ready.Load() {
		return a.mode, a.on
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ready.Load() {
		return a.mode, a.on
	}
	a.mode, a.on = resolveDecimalMode(op, left, right, b)
	a.ready.Store(true)
	return a.mode, a.on
}

// evalDecimalBox computes one row exactly and boxes it as a DECIMAL column's
// value is boxed — its rendered text. ok=false means this pair is not decimal
// arithmetic and the caller's own arms answer.
func (a *decArm) evalDecimalBox(op string, left, right Expr, b *batch.RecordBatch, row int) (any, bool) {
	m, on := a.resolve(op, left, right, b)
	if !on {
		return nil, false
	}
	lv, lok := left.(decimalOperand).evalDecimal(b, row)
	if !lok {
		return nil, true // NULL, and this arm still owns the answer
	}
	rv, rok := right.(decimalOperand).evalDecimal(b, row)
	if !rok {
		return nil, true
	}
	v, _ := decApplyChecked(m, lv, rv)
	return v.FormatDecimal(m.out.Scale), true
}

// binOpDecimalBox is the generic BinOp's exact arm. It runs ahead of the
// ToFloat64 pair below it, and declines for every operand pair that is not
// fixed-point — which is all of them but the ones ADR-0024 item 3 names.
func (e *BinOp) binOpDecimalBox(b *batch.RecordBatch, row int) (any, bool) {
	return e.dec.evalDecimalBox(e.Op, e.Left, e.Right, b, row)
}

// --- UnaryOp ----------------------------------------------------------------

// Unary ± over a DECIMAL is exact and moves no digit, so -d is a value the
// same column holds and keeps its own (p,s). Before this it went through
// ToFloat64 of the rendered text: `SELECT -d` answered a float64 declared
// STRING, and `-d * 2` left the exact path entirely.

func (e *UnaryOp) decimalType(b *batch.RecordBatch) (batch.DecimalType, bool) {
	if e.Op != "-" && e.Op != "+" {
		return batch.DecimalType{}, false
	}
	o, ok := e.Operand.(decimalOperand)
	if !ok {
		return batch.DecimalType{}, false
	}
	return o.decimalType(b)
}

func (e *UnaryOp) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	o, ok := e.Operand.(decimalOperand)
	if !ok {
		return batch.Int128{}, false
	}
	v, ok := o.evalDecimal(b, row)
	if !ok {
		return batch.Int128{}, false
	}
	if e.Op != "-" {
		return v, true
	}
	neg := v.Neg()
	if !v.IsZero() && neg.IsNegative() == v.IsNegative() {
		// -2^127 negates to itself: its magnitude has no Int128, so there is
		// no value to answer with (ADR-0024 item 4).
		raiseNumericFieldOverflow(0, 0)
	}
	return neg, true
}

func (e *UnaryOp) decimalVec(_ *batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	// No materialized column of its own; a caller reads it per row through
	// evalDecimal, unboxed.
	return kernel.DecimalOperandVec{}, false
}

// unaryDecimalBox is UnaryOp.Eval's exact arm — the negated value rendered the
// way a DECIMAL column's box is, so it reaches every boxed consumer in the
// shape they already read. ok=false means the operand is not a genuine DECIMAL
// and Eval's numeric arms answer as they always did.
func (e *UnaryOp) unaryDecimalBox(b *batch.RecordBatch, row int) (any, bool) {
	if e.Op != "-" && e.Op != "+" {
		return nil, false
	}
	if !operandIsDecimalTyped(e.Operand, b) {
		return nil, false
	}
	t, ok := e.decimalType(b)
	if !ok {
		return nil, false
	}
	v, ok := e.evalDecimal(b, row)
	if !ok {
		return nil, true // NULL, and the decimal arm still owns the answer
	}
	return v.FormatDecimal(t.Scale), true
}

// --- Errors -----------------------------------------------------------------

// raiseDecimalStatus turns a batch.DecimalStatus into the query error
// PostgreSQL raises for it, through the per-row panic channel (#347).
//
// The two conditions are two SQLSTATEs and PostgreSQL keeps them apart:
// `1/0` is 22012 division_by_zero for both `/` and `%`, and a value with no
// place in the declared type is 22003 numeric_value_out_of_range. The 22003
// text is PostgreSQL's own — "numeric field overflow", with the DETAIL line
// naming the bound — so a client's error string matches what it would get from
// postgres for the same overflow.
func raiseDecimalStatus(st batch.DecimalStatus, p, s int) {
	switch st {
	case batch.DecimalDivByZero:
		raiseDivisionByZero()
	case batch.DecimalOverflow:
		raiseNumericFieldOverflow(p, s)
	}
	panic(fatalEval{sqlerr.New("22003", "numeric result is not representable: %s", st)})
}

// raiseNumericFieldOverflow is PostgreSQL's numeric field overflow, message and
// DETAIL both:
//
//	ERROR:  numeric field overflow
//	DETAIL:  A field with precision 5, scale 2 must round to an absolute value less than 10^3.
func raiseNumericFieldOverflow(p, s int) {
	panic(fatalEval{numericFieldOverflow(p, s)})
}

// raiseIntegerOutOfRange is PostgreSQL's refusal for a numeric that has no
// value in the integer type being cast to — `integer out of range` for int4
// and `bigint out of range` for int8, both SQLSTATE 22003.
//
// It is what a wrapped or float-narrowed answer used to be: the generic cast
// arm read the value through strconv.ParseFloat and then int64(math.Round(f)),
// which is undefined past 2^63 and silently truncating below it.
func raiseIntegerOutOfRange(dest string) {
	name := "bigint"
	switch dest {
	case "int", "integer":
		name = "integer"
	}
	panic(fatalEval{sqlerr.New("22003", "%s out of range", name)})
}

// nonFiniteDecimalError is ADR-0024 item 6's refusal: NaN and the infinities
// are values PostgreSQL's numeric holds and an Int128 has no bit pattern for,
// so they are refused as a VALUE with the SQLSTATE that says the range is the
// problem. A recorded divergence, not a silent substitution.
func nonFiniteDecimalError(text string) error {
	return sqlerr.New("22003",
		"%q has no DECIMAL value: a DECIMAL is a 128-bit unscaled integer with no "+
			"representation for NaN or the infinities, which PostgreSQL's numeric does "+
			"hold — a recorded divergence (ADR-0024 item 6)", strings.TrimSpace(text))
}

// numericFieldOverflow builds the error without raising it, for the sites that
// have an error channel of their own.
func numericFieldOverflow(p, s int) error {
	if p <= 0 || p > batch.MaxDecimalPrecision {
		// No declared bound to name: the value simply has no Int128, which is
		// the carrier's limit rather than the type's (ADR-0024 item 1).
		return sqlerr.New("22003",
			"numeric field overflow: the value has no exact DECIMAL at scale %d — "+
				"a DECIMAL is a 128-bit unscaled integer and this needs more than %d digits "+
				"(ADR-0024 item 1)", s, batch.MaxDecimalPrecision)
	}
	return sqlerr.New("22003",
		"numeric field overflow: a field with precision %d, scale %d must round to an "+
			"absolute value less than 10^%d", p, s, p-s)
}
