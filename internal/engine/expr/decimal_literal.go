package expr

import (
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
}

// numericLit reports the exact source text of a numeric literal operand.
func numericLit(e Expr) (*kernel.DecimalLiteral, bool) {
	lit, ok := e.(*Lit)
	if !ok || lit.Text == "" || !isNumeric(lit.Val) {
		return nil, false
	}
	return kernel.NewDecimalLiteral(lit.Text), true
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
	d.col.resolve(b)
	if d.col.idx < 0 || d.col.idx >= len(b.Columns) || d.col.typ != batch.TypeDecimal {
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
func (d *decimalLitCmp) order(vec *batch.Vector, row, i int) int {
	c := d.lits[i].Order(vec, row)
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
