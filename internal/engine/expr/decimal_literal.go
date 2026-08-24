package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// decimalLitCmp binds a bare column reference to the numeric literals it is
// compared against, so that when the column turns out to be a DECIMAL the
// comparison is answered in the column's own domain — the unscaled integer at
// the column's scale — instead of through float64 on both sides.
//
// Two separate losses live on that float64 path, and this closes both (#452):
//
//   - the LITERAL: a float64 holds ~15-16 significant decimal digits, so
//     `= 493827160549382.7160549350` became `= 493827160549382.6875` and
//     matched nothing, while `>` gained the row it should have excluded.
//   - the COLUMN: ColRef.Eval boxes a DECIMAL as its rendered text, and
//     compare() has no numeric reading of text against a float64, so it fell
//     through to a LEXICOGRAPHIC comparison — "1339815.97" against
//     "1.33981597e+06" — which is not the same order and not the same
//     equality.
//
// The binding is decided at COMPILE time (the operand shapes) and applied per
// BATCH (the column's type and scale), because a column's type is not known
// until a batch arrives. Anything that is not a materialized DECIMAL column
// falls through to the generic path untouched, which is what keeps every
// other type answering exactly as before.
type decimalLitCmp struct {
	col  *ColRef
	lits []*kernel.DecimalLiteral // one per literal operand, in operand order
	flip bool                     // the literal was the LEFT operand

	// notDecimal caches a settled "this column is not DECIMAL" answer. A
	// column's type cannot change across the batches of one query, so the
	// first batch that resolves the bound column to anything but DECIMAL
	// settles it for every row of every later batch too. vector() checks
	// this FIRST so the common case — every non-DECIMAL type, on the
	// row-at-a-time path — costs one atomic load instead of a
	// ColRef.resolve() call plus three field checks per row (measured
	// +5.4% Cmp / +13% In on a FLOAT64 column before this cache existed).
	//
	// Cmp/In/Between are shared across parallel pipeline workers evaluating
	// the same batch concurrently (see ColRef.resolved's doc comment), so
	// this needs the same atomic publish that ColRef itself uses — a plain
	// bool would be a data race. Concurrent writers can only ever agree
	// (the answer is a pure function of the column's fixed type), so a
	// losing racer's store is redundant, not wrong.
	notDecimal atomic.Bool
}

// numericLit reports the exact source text of a constant operand that a
// DECIMAL column can be compared against.
//
// A numeric literal contributes its verbatim source Text: the float64 box the
// compiler built for arithmetic has already lost the digits past a double
// (ADR-0012 item 6). A STRING literal contributes its own text, because
// PostgreSQL types an unquoted-type literal from the other operand — `d =
// '12.75'` is a numeric comparison there, not a string one — and because a
// string that is NOT a number must reach the comparison to be REFUSED there
// rather than read as zero (#463). Whether the column is a DECIMAL at all is
// still decided per batch by vector(); nothing here changes what a string
// literal means against a string column.
func numericLit(e Expr) (*kernel.DecimalLiteral, bool) {
	lit, ok := e.(*Lit)
	if !ok {
		return nil, false
	}
	// Text is set for a NUMERIC literal and empty for every other kind, so it
	// is both the exact carrier and the test for "this operand is a number
	// the user wrote" — including one no Go numeric box can hold, which
	// compileLit boxes as its own text.
	if lit.Text != "" {
		return kernel.NewDecimalLiteral(lit.Text), true
	}
	if s, ok := lit.Val.(string); ok {
		return kernel.NewDecimalLiteral(s), true
	}
	return nil, false
}

// bareCol returns the operand as a plain column reference — not a ROW field
// access, which reads a boxed value out of a container rather than a column.
func bareCol(e Expr) (*ColRef, bool) {
	col, ok := e.(*ColRef)
	if !ok || col.structField != "" {
		return nil, false
	}
	return col, true
}

// bindDecimalCmp binds `col op lit` or `lit op col`, in either operand order.
func bindDecimalCmp(left, right Expr) *decimalLitCmp {
	if col, ok := bareCol(left); ok {
		if lit, ok := numericLit(right); ok {
			return &decimalLitCmp{col: col, lits: []*kernel.DecimalLiteral{lit}}
		}
		return nil
	}
	if col, ok := bareCol(right); ok {
		if lit, ok := numericLit(left); ok {
			return &decimalLitCmp{col: col, lits: []*kernel.DecimalLiteral{lit}, flip: true}
		}
	}
	return nil
}

// bindDecimalList binds `col IN (lit, ...)` / `col BETWEEN lit AND lit`. It
// binds only when EVERY operand is a numeric literal: a mixed list would need
// two comparison rules inside one predicate, and one rule per predicate is
// the property that keeps the paths from disagreeing.
func bindDecimalList(col Expr, values []Expr) *decimalLitCmp {
	c, ok := bareCol(col)
	if !ok || len(values) == 0 {
		return nil
	}
	lits := make([]*kernel.DecimalLiteral, len(values))
	for i, v := range values {
		lit, ok := numericLit(v)
		if !ok {
			return nil
		}
		lits[i] = lit
	}
	return &decimalLitCmp{col: c, lits: lits}
}

// vector returns the batch's DECIMAL column for this binding, or nil when the
// exact path does not apply: an unresolved name, a column of any other type,
// or a VIEW whose values live in a base vector through an index (the decimal
// kernels read DecimalData directly, as every other kernel does).
func (d *decimalLitCmp) vector(b *batch.RecordBatch) *batch.Vector {
	if d.notDecimal.Load() {
		return nil
	}
	d.col.resolve(b)
	if d.col.idx < 0 || d.col.idx >= len(b.Columns) || d.col.typ != batch.TypeDecimal {
		d.notDecimal.Store(true)
		return nil
	}
	v := b.Columns[d.col.idx]
	if v.Base != nil || len(v.DecimalData.Data) == 0 {
		return nil
	}
	return v
}

// order compares the row's value against literal i, as -1, 0 or +1, with the
// operand order the expression was written in.
//
// A literal that is not a number ABORTS the query here rather than answering.
// The column is a DECIMAL by the time this runs, so PostgreSQL's answer to
// `d = 'abc'` is a 22P02, and the alternative — the old silent reading of an
// unparseable constant as the value zero — matched every row holding zero
// (#463). The kernel path raises the same error from decimalConstError, so
// both paths refuse the same query.
func (d *decimalLitCmp) order(vec *batch.Vector, row, i int) int {
	lit := d.lits[i]
	if !lit.Numeric() {
		raiseInvalidTextRepresentation("numeric", lit.Text())
	}
	c := lit.Order(vec, row)
	if d.flip {
		return -c
	}
	return c
}

// cmpOrder turns an ordering into the answer for one comparison operator.
func cmpOrder(c int, op CmpOp) bool {
	switch op {
	case CmpEq:
		return c == 0
	case CmpNe:
		return c != 0
	case CmpLt:
		return c < 0
	case CmpLe:
		return c <= 0
	case CmpGt:
		return c > 0
	case CmpGe:
		return c >= 0
	}
	return false
}

// negateLitText flips the sign of a numeric literal's source text, so folding
// a unary minus into the literal keeps the exact text alongside the box.
func negateLitText(text string) string {
	switch {
	case text == "":
		return ""
	case text[0] == '-':
		return text[1:]
	case text[0] == '+':
		return "-" + text[1:]
	default:
		return "-" + text
	}
}
