package expr

import (
	"strconv"
	"strings"
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

// decimalColCmp binds two BARE COLUMN operands so that when both turn out to
// be DECIMALs the comparison happens on the unscaled Int128s, at whatever
// scales the two columns declare.
//
// The boxed path cannot do this one. Both operands reach it as their RENDERED
// TEXT, and compare()'s two-strings fast path answers first — lexicographically,
// where "10.001" sorts below "2.0002" — so nothing about the BOX distinguishes
// two DECIMAL columns from two ordinary string columns. Only the DECLARATION
// does, which is ADR-0012 item 8's rule and the reason this is a binding
// rather than another branch in compare(). Two DECIMALs of different SCALE are
// the case that matters even for equality: "1.50" and "1.5000" are the same
// number and different text (#477).
type decimalColCmp struct {
	left, right *ColRef
	// notDecimal caches a settled "this pair is not two DECIMAL columns".
	// See decimalLitCmp.notDecimal for why it is atomic.
	notDecimal atomic.Bool
}

// bindDecimalCols binds `col op col`. Nil for every other operand shape.
func bindDecimalCols(left, right Expr) *decimalColCmp {
	l, ok := bareCol(left)
	if !ok {
		return nil
	}
	r, ok := bareCol(right)
	if !ok {
		return nil
	}
	return &decimalColCmp{left: l, right: r}
}

// vectors returns the batch's two DECIMAL columns for this binding, or nil
// when the exact path does not apply.
func (d *decimalColCmp) vectors(b *batch.RecordBatch) (*batch.Vector, *batch.Vector) {
	if d.notDecimal.Load() {
		return nil, nil
	}
	lv := decimalVectorOf(d.left, b)
	rv := decimalVectorOf(d.right, b)
	if lv == nil || rv == nil {
		// Only a resolved NON-DECIMAL type is permanent; an empty column in
		// one batch says nothing about the next.
		if (lv == nil && d.left.typ != batch.TypeDecimal) ||
			(rv == nil && d.right.typ != batch.TypeDecimal) {
			d.notDecimal.Store(true)
		}
		return nil, nil
	}
	return lv, rv
}

// decimalVectorOf resolves one bound column to its DECIMAL vector, or nil for
// any other type, an unresolved name, or a VIEW whose values live in a base
// vector through an index (the decimal kernels read DecimalData directly).
func decimalVectorOf(col *ColRef, b *batch.RecordBatch) *batch.Vector {
	col.resolve(b)
	if col.idx < 0 || col.idx >= len(b.Columns) || col.typ != batch.TypeDecimal {
		return nil
	}
	v := b.Columns[col.idx]
	if v.Base != nil || len(v.DecimalData.Data) == 0 {
		return nil
	}
	return v
}

// decimalTextOrder orders a NUMBER against a DECIMAL value that reached the
// boxed path as its rendered text, returning -1, 0 or +1 for num against the
// text, and ok=false when the text is not a number (an ordinary string
// column's value, which must keep comparing as a string).
//
// Two different rules, both PostgreSQL's:
//
//   - Against an INTEGER the comparison is EXACT. batch.DecimalTextAt at
//     scale 0 truncates the text to its integer part and reports the sign of
//     what it dropped, and ScaledDecimal.Order settles the tie with that
//     residual — so 3 vs "3.0000" is equal, 3 vs "3.0001" is less, and
//     nothing is rounded on either side. It saturates too, so a text value
//     wider than Int128 still orders (#462).
//   - Against a FLOAT the comparison is a float64 one, because that is what
//     PostgreSQL does with `numeric <op> double precision`: it casts the
//     numeric. Verified on live PostgreSQL — `9007199254740993::numeric =
//     9007199254740992::float8` is TRUE.
func decimalTextOrder(num any, text string) (int, bool) {
	switch v := num.(type) {
	case int64:
		return decimalTextOrderInt(v, text)
	case int:
		return decimalTextOrderInt(int64(v), text)
	case int32:
		return decimalTextOrderInt(int64(v), text)
	case float64:
		return decimalTextOrderFloat(v, text)
	case float32:
		return decimalTextOrderFloat(float64(v), text)
	}
	return 0, false
}

func decimalTextOrderInt(v int64, text string) (int, bool) {
	sd, ok := batch.DecimalTextAt(text, 0)
	if !ok {
		return 0, false
	}
	return sd.Order(batch.Int128From(v)), true
}

func decimalTextOrderFloat(v float64, text string) (int, bool) {
	// The numeric SHAPE is settled by the same parser the exact arm uses, so
	// the two agree about which strings are numbers at all: strconv.ParseFloat
	// alone would accept "NaN" and "Inf", which no DECIMAL column renders.
	if _, ok := batch.DecimalTextAt(text, 0); !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil {
		return 0, false
	}
	return kernel.CompareFloat64(v, f), true
}

// litText reports a numeric literal operand's exact source text, and "" for
// every other operand. Text is set only for a numeric literal (compileLit), so
// it doubles as the test for "this operand is a number the user wrote".
func litText(e Expr) string {
	if l, ok := e.(*Lit); ok {
		return l.Text
	}
	return ""
}

// compareWithText is compare() for a site that knows its operands' literal
// TEXTS.
//
// It exists because the boxed comparison cannot be exact on its own where one
// side is a DECIMAL — the column arrives as its rendered text and the literal
// as a float64 that has already lost the digits past a double, so the best
// compare() can do for the pair is a float comparison. That is right for a
// genuine FLOAT column and wrong for a numeric literal, which PostgreSQL types
// as numeric and compares at full precision.
//
// `CASE d WHEN lit`, `d IS DISTINCT FROM lit` and `GREATEST/LEAST(d, lit)` are
// the sites: they compare through the boxed path, and #452's binding — which
// covers `col op lit`, IN and BETWEEN — never reached them (#465).
func compareWithText(a, b any, aText, bText string, op CmpOp) bool {
	if c, ok := exactTextOrder(a, b, aText, bText); ok {
		return cmpOrder(c, op)
	}
	return compare(a, b, op)
}

// exactTextOrder orders a value BOXED AS TEXT against a literal whose exact
// text is known, as the two exact decimals they are.
//
// It applies only when exactly one side is a text box and the other carries a
// literal's text. Two text boxes are two STRING values as far as anything here
// can tell — nothing in a box says "this came from a DECIMAL column", which is
// what decimalColCmp's declaration binding is for — and two numeric boxes are
// already compared by their own types' rules.
func exactTextOrder(a, b any, aText, bText string) (int, bool) {
	if as, ok := a.(string); ok {
		if bText == "" {
			return 0, false
		}
		return batch.CompareDecimalTexts(as, bText)
	}
	if bs, ok := b.(string); ok {
		if aText == "" {
			return 0, false
		}
		return batch.CompareDecimalTexts(aText, bs)
	}
	return 0, false
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
