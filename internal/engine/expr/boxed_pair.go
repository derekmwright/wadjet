package expr

import (
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// This file is the declaration-driven half of the boxed comparison path.
//
// ADR-0012 item 8's rule is that a boxed value's comparison order follows the
// COLUMN'S DECLARATION, not the Go type its box happens to be. Two shapes make
// that rule impossible to follow from the box alone, because both arrive as a
// plain Go `string`:
//
//   - a DECIMAL column, which Vector.GetValue renders as its decimal TEXT, and
//   - a STRING column, whose value may look exactly like that text.
//
// `expr.decimalColCmp` already answers one of those pairs from the two
// columns' declarations — but it is bound in `NewCmp` alone, so the SAME two
// DECIMAL columns at a BOXED site (a simple `CASE d1 WHEN d2`, `d1 IS DISTINCT
// FROM d2`, `GREATEST(d1, d2)`) fell through `compare()`'s two-rendered-strings
// path and compared LEXICOGRAPHICALLY, where "10.001" sorts below "2.0002"
// (#506).
//
// A boxedPair is armed from the operand EXPRESSIONS at construction, resolves
// each operand's DECLARED kind on the first batch that can answer it, and then
// applies one rule per kind pair. Nothing here reads a value's box to decide
// which RULE applies — only to decide whether the rule's text arm or its
// numeric arm is the one holding this row's value.

// boxKind is what an operand's declaration says its values are, independent of
// the Go box a particular row produces.
type boxKind int32

const (
	// boxUnknown: no declaration-driven reading applies. The pair falls
	// through to compare(), which is what it always did.
	boxUnknown boxKind = iota
	// boxDecimal: values from here are DECIMALs. A DECIMAL boxes as its
	// rendered text, so a string box from this operand is decimal text — the
	// distinction the sniff could not make.
	boxDecimal
	// boxNumber: a non-DECIMAL number, which never boxes as a string.
	boxNumber
	// boxText: a genuine text value, which compares AS TEXT — bytewise,
	// wadjet's collation (ADR-0012 item 5) — whatever its digits look like.
	// No rule below reads one yet; classifying it keeps a STRING column from
	// being mistaken for a DECIMAL's rendering by any rule that does.
	boxText
)

// classifyOperand reports an operand's declared kind and whether that answer
// is SETTLED — safe to cache for the rest of the query.
//
// Unsettled means "this batch cannot answer": a column name that resolves in
// no batch yet says nothing about the next one, so the caller must ask again.
// Every settled answer is a pure function of a declared type, which does not
// change across the batches of one query.
func classifyOperand(e Expr, b *batch.RecordBatch) (boxKind, bool) {
	switch v := e.(type) {
	case *ColRef:
		if v.structField != "" {
			// A ROW field access reads a boxed value out of a container; the
			// container's declaration does not type the field here.
			return boxUnknown, true
		}
		v.resolve(b)
		if v.idx < 0 || v.idx >= len(b.Columns) {
			return boxUnknown, false
		}
		switch v.typ {
		case batch.TypeDecimal:
			return boxDecimal, true
		case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64:
			return boxNumber, true
		case batch.TypeString:
			return boxText, true
		default:
			// Every other type either boxes as a number of its own (PORT,
			// DURATION), as a formatted string with its own comparison rule
			// (DATE, CIDR, UUID), or as a container. None of them is a
			// DECIMAL-vs-number pair, and none of them wants the text rule
			// applied to a numeric literal, so they keep compare()'s
			// behaviour exactly.
			return boxUnknown, true
		}
	case *Lit:
		// Text is set only for a NUMERIC literal (compileLit), so it is both
		// the exact carrier and the test for "the user wrote a number here".
		if v.Text != "" {
			return boxNumber, true
		}
		switch v.Val.(type) {
		case string:
			return boxText, true
		case int64, int32, int, float64, float32:
			return boxNumber, true
		}
		return boxUnknown, true
	case *FuncCall:
		// GREATEST/LEAST answer with ONE OF THEIR ARGUMENTS, so their kind is
		// the join of the arguments' kinds. No other function is transparent
		// this way; the rest declare their own result type.
		switch strings.ToLower(v.Name) {
		case "greatest", "least":
			return joinOperandKinds(v.Args, b)
		}
		return boxUnknown, true
	case *Case:
		// A CASE answers with one of its RESULTS (the operand and the WHEN
		// conditions only steer), so those are what decide its kind.
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return joinOperandKinds(arms, b)
	case *Coalesce:
		return joinOperandKinds(v.Args, b)
	}
	return boxUnknown, true
}

// joinOperandKinds is classifyOperand over a set of alternatives that one
// value is chosen from.
//
// The join keeps DECIMAL over a plain number: an expression that answers
// either a DECIMAL or an integer still only ever produces a STRING box when
// the DECIMAL wins, so "a string from here is decimal text" stays true, which
// is the only claim the kind makes. Any other disagreement — text against a
// number — leaves the box ambiguous again and yields boxUnknown.
func joinOperandKinds(args []Expr, b *batch.RecordBatch) (boxKind, bool) {
	kind, have, settled := boxUnknown, false, true
	for _, a := range args {
		if a == nil {
			continue
		}
		k, s := classifyOperand(a, b)
		settled = settled && s
		if !have {
			kind, have = k, true
			continue
		}
		switch {
		case k == kind:
		case (k == boxDecimal && kind == boxNumber) || (k == boxNumber && kind == boxDecimal):
			kind = boxDecimal
		default:
			return boxUnknown, settled
		}
	}
	if !have {
		return boxUnknown, true
	}
	return kind, settled
}

// boxOperand caches one operand's settled kind.
//
// The atomic is the same publish decimalLitCmp.notDecimal uses and for the
// same reason: these nodes are shared across parallel pipeline workers
// evaluating one batch, and concurrent writers can only ever agree, because
// the answer is a pure function of the operand's fixed declaration.
type boxOperand struct {
	expr Expr
	// kind holds int32(k)+1 once settled; 0 means "not settled yet".
	kind atomic.Int32
}

func (o *boxOperand) resolve(b *batch.RecordBatch) boxKind {
	if v := o.kind.Load(); v != 0 {
		return boxKind(v - 1)
	}
	k, settled := classifyOperand(o.expr, b)
	if settled {
		o.kind.Store(int32(k) + 1)
	}
	return k
}

// boxedPair binds two operands of a boxed comparison — a Cmp's generic arm, a
// simple CASE's operand against one WHEN, IS DISTINCT FROM's two sides, or one
// (best-so-far, candidate) pair of GREATEST/LEAST — together with their
// literal source texts.
//
// It is the one place that decides HOW two boxes compare, from what they
// declare. Sites build one per operand pair at construction time; resolution
// happens on the first batch that can answer it and is then a single atomic
// load per row.
type boxedPair struct {
	left, right  boxOperand
	lText, rText string
	// disarmed settles "no declaration-driven rule can ever apply to this
	// pair", which is every comparison between two ordinary columns of the
	// same family. It turns the whole check into one atomic load, keeping the
	// generic path's per-row cost where it was before this binding existed.
	disarmed atomic.Bool
}

func newBoxedPair(left, right Expr) *boxedPair {
	return &boxedPair{
		left:  boxOperand{expr: left},
		right: boxOperand{expr: right},
		lText: litText(left), rText: litText(right),
	}
}

// pairApplies reports whether any rule below can fire for this KIND pair. It
// depends only on the declarations, so a false answer is permanent.
func pairApplies(lk, rk boxKind, lText, rText string) bool {
	switch {
	case lk == boxDecimal && (rk == boxDecimal || rk == boxNumber):
		return true
	case rk == boxDecimal && lk == boxNumber:
		return true
	}
	return false
}

// order compares two boxed values under the rule their DECLARATIONS select,
// returning -1, 0 or +1, or ok=false when no such rule applies and the caller
// must fall through to compare().
//
// The rules, each PostgreSQL's:
//
//   - DECIMAL against DECIMAL: the two exact decimals, at whatever scales the
//     columns declare — "1.50" and "1.5000" are one number (#477, #506).
//   - DECIMAL against a numeric LITERAL: the literal's exact source text
//     against the column's, because PostgreSQL types an unsuffixed decimal
//     literal as `numeric` and compares it at full precision (#452, #465).
//   - DECIMAL against a non-DECIMAL number: exact against an integer, float64
//     against a float, which is what `numeric <op> double precision` does —
//     it casts the numeric (#476).
func (p *boxedPair) order(b *batch.RecordBatch, lv, rv any) (int, bool) {
	if p == nil || p.disarmed.Load() {
		return 0, false
	}
	lk := p.left.resolve(b)
	rk := p.right.resolve(b)
	if !pairApplies(lk, rk, p.lText, p.rText) {
		// Only a SETTLED pair may disarm: an operand that no batch has
		// resolved yet is still going to answer on a later one.
		if p.left.kind.Load() != 0 && p.right.kind.Load() != 0 {
			p.disarmed.Store(true)
		}
		return 0, false
	}
	return orderByKinds(lk, rk, lv, rv, p.lText, p.rText)
}

// orderByKinds is order's rule table, with the kinds already resolved. It is
// separate because GREATEST/LEAST compare (best-so-far, candidate) pairs whose
// OPERANDS move between iterations: those sites resolve one boxOperand per
// ARGUMENT and assemble the pair per comparison, instead of holding a
// boxedPair for every ordered pair of arguments.
func orderByKinds(lk, rk boxKind, lv, rv any, lText, rText string) (int, bool) {
	ls, lIsStr := lv.(string)
	rs, rIsStr := rv.(string)
	_ = rIsStr
	switch {
	case lk == boxDecimal && rk == boxDecimal:
		if lIsStr && rIsStr {
			return batch.CompareDecimalTexts(ls, rs)
		}
	case lk == boxDecimal && lIsStr:
		if rText != "" {
			return batch.CompareDecimalTexts(ls, rText)
		}
		if c, ok := decimalTextOrder(rv, ls); ok {
			return -c, true
		}
	case rk == boxDecimal && rIsStr:
		if lText != "" {
			return batch.CompareDecimalTexts(lText, rs)
		}
		if c, ok := decimalTextOrder(lv, rs); ok {
			return c, true
		}
	}
	return 0, false
}

// compare answers one comparison operator for this pair, falling through to
// compareWithText — the literal-side carry-through this binding is the
// column-side counterpart of — when no declaration-driven rule applies.
// A nil pair is an unbound node — a Cmp assembled as a struct literal rather
// than through NewCmp, the same shape that leaves dec and decCols nil — and
// falls all the way through to compare(), which is what such a node did before
// any of these bindings existed.
func (p *boxedPair) compare(b *batch.RecordBatch, lv, rv any, op CmpOp) bool {
	if c, ok := p.order(b, lv, rv); ok {
		return cmpOrder(c, op)
	}
	if p == nil {
		return compare(lv, rv, op)
	}
	return compareWithText(lv, rv, p.lText, p.rText, op)
}
