package kernel

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// DecimalLiteral keeps source numeric text EXACTLY, never narrowed to float64
// before meeting a DECIMAL column (#452).
// Resolve by column scale plus discarded-digit residual using decimalLiteralAt,
// identically to compareFilterDecimal (#394), and cache for row-at-a-time use.
// Publish the entire resolved value through an atomic pointer; losing racers
// may resolve the same value again. One literal may meet different scales.
// See docs/internals/kernel-exact-decimal-literal-cache.md for the design.
type DecimalLiteral struct {
	text     string
	resolved atomic.Pointer[decimalLiteralScaled]
}

// decimalLiteralScaled is one literal at one scale. Scale is part of the value
// because nothing promises a literal only ever meets one column.
type decimalLiteralScaled struct {
	scale int
	sd    batch.ScaledDecimal
}

// NewDecimalLiteral binds literal text — plain or exponent form — for
// comparison against DECIMAL columns. The text is kept VERBATIM: the exponent
// is folded into the scaling exactly when the literal is resolved at a
// column's scale, never expanded through a float64 first (#463).
func NewDecimalLiteral(text string) *DecimalLiteral {
	return &DecimalLiteral{text: text}
}

// Numeric reports whether the literal's text names a number at all. A false
// here is a query error at the comparison — PostgreSQL raises "invalid input
// syntax for type numeric" rather than reading the text as zero (#463).
func (d *DecimalLiteral) Numeric() bool { return isDecimalText(d.text) }

// Text is the literal's source text, verbatim.
func (d *DecimalLiteral) Text() string { return d.text }

func (d *DecimalLiteral) at(scale int) *decimalLiteralScaled {
	if r := d.resolved.Load(); r != nil && r.scale == scale {
		return r
	}
	r := &decimalLiteralScaled{scale: scale, sd: decimalLiteralAt(d.text, scale)}
	d.resolved.Store(r)
	return r
}

// Order returns -1, 0 or +1 as vec[row] is less than, equal to, or greater
// than the literal — exactly, including for a literal with more fractional
// digits than the column's scale (which equals no stored value but still has
// a place in the order) and for one wider than the carrier itself (which
// orders above or below every value the column can hold).
//
// The caller owns the null check: a NULL row has no value to order.
func (d *DecimalLiteral) Order(vec *batch.Vector, row int) int {
	return d.at(vec.DecimalData.Scale).sd.Order(vec.DecimalData.Data[row])
}

// OrderAt is Order against a value already read out of a column at `scale`.
func (d *DecimalLiteral) OrderAt(cell batch.Int128, scale int) int {
	return d.at(scale).sd.Order(cell)
}

// Compare answers `vec[row] <op> literal`.
func (d *DecimalLiteral) Compare(vec *batch.Vector, row int, op CompareOp) bool {
	return applyCompareOp(d.Order(vec, row), op)
}
