package kernel

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// DecimalLiteral is a numeric literal held as the EXACT text it was written
// with, ready to be compared against a DECIMAL column in that column's own
// domain.
//
// It exists because a literal is not a float64. `compileLit` used to turn
// every numeric literal that is not an int64 into one, and a float64 carries
// ~15-16 significant decimal digits where a DECIMAL(38,10) carries 38: the
// literal a user typed was silently replaced by the nearest double before it
// ever met the column, so `= 493827160549382.7160549350` matched nothing and
// `>` gained a row (#452). Text is the only lossless carrier the whole way
// from the parser to a kernel, which is why the filter kernels already take
// their DECIMAL constant that way (compareFilterDecimal).
//
// The resolution — text, at the column's scale, plus the residual of any
// digits the scale cannot hold — is the SAME one compareFilterDecimal
// performs, through the same decimalLiteralAt: one comparison rule for one
// predicate, per #394. What this type adds is the cache, for the
// row-at-a-time paths that would otherwise re-parse per row.
//
// Safe for concurrent use: a resolved literal is published whole, through an
// atomic pointer, and a losing racer merely re-resolves to the same value.
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
