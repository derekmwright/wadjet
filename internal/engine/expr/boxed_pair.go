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
// The other direction was worse. `compare()` used to tell the two apart by
// SNIFFING: any string operand that PARSED as a number was read numerically
// against the other side. That made a genuine STRING column compare
// NUMERICALLY on the row path — `WHERE s = 1.5` found the row holding "1.50" —
// while the vectorized kernel compared the same predicate as text, so one
// query had two answers depending on which path it took (#504). The sniff is
// gone: a STRING column is classified from its declaration and compares as
// text on both paths.
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
	boxText
	// boxQuoted: a QUOTED string literal, which is a different thing from a
	// text value. PostgreSQL types such a literal as `unknown` and resolves
	// it FROM THE OTHER OPERAND, so `k > '2'` over a BIGINT column is an
	// integer comparison and `d = '12.75'` over a DECIMAL one is an exact
	// numeric comparison — while `s > '2'` over a text column is the text
	// comparison it looks like. Collapsing this into boxText is what dropped
	// PostgreSQL's rule for the first two (#504 review, B1).
	boxQuoted
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
			return boxQuoted, true
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
// is the only claim the kind makes. A QUOTED literal alternative contributes
// nothing and takes the others' type, the way PostgreSQL resolves an
// unknown-typed literal from its context — `COALESCE(d, 'text')` is a numeric
// expression there, not an ambiguous one. Any other disagreement leaves the
// box ambiguous again and yields boxUnknown.
//
// A NULL alternative is SKIPPED outright. `COALESCE(d, NULL)` is a DECIMAL
// expression, and reading the NULL literal as its own kind poisoned the join
// to boxUnknown — which is how a DECIMAL column wrapped in a COALESCE started
// comparing as rendered text (#504 review, B2). NULL never reaches a
// comparison anyway: every caller short-circuits a nil operand first.
func joinOperandKinds(args []Expr, b *batch.RecordBatch) (boxKind, bool) {
	kind, have, settled := boxUnknown, false, true
	for _, a := range args {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok && lit.Val == nil {
			continue
		}
		k, s := classifyOperand(a, b)
		settled = settled && s
		if k == boxQuoted {
			continue // unknown-typed: takes whatever the others declare
		}
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
		lText: operandLitText(left), rText: operandLitText(right),
	}
}

// operandLitText is a LITERAL operand's source text, for either spelling a
// user can write a value in: the verbatim digits of a numeric literal
// (`Lit.Text`, set by compileLit) or the contents of a quoted string. Empty
// for every operand that is not a literal.
//
// One field for both because the rule that reads it is the same one —
// PostgreSQL compares a `numeric` column against `12.75` and against `'12.75'`
// identically — and the operand's KIND is what tells the two apart where it
// matters (a quoted literal is unknown-typed and takes the column's type; a
// numeric literal is already `numeric` and so makes a TEXT column's
// comparison a text one, per ADR-0012 item 5).
func operandLitText(e Expr) string {
	lit, ok := e.(*Lit)
	if !ok {
		return ""
	}
	if lit.Text != "" {
		return lit.Text
	}
	if s, ok := lit.Val.(string); ok {
		return s
	}
	return ""
}

// pairApplies reports whether any rule below can fire for this KIND pair. It
// depends only on the declarations, so a false answer is permanent.
func pairApplies(lk, rk boxKind, lText, rText string) bool {
	switch {
	// A PROVEN DECIMAL operand applies against ANY other kind. It used to
	// require the other side to be a number as well, which left a DECIMAL
	// column comparing as RENDERED TEXT against every operand this file
	// cannot classify — a scalar subquery, arithmetic, a CAST (#504 review,
	// B2). Nothing is lost by widening it: orderByKinds' decimalTextOrder
	// returns ok=false for a box that is not a number, so a genuine STRING
	// column on the other side still falls through to the lexical comparison
	// #504 settled.
	case lk == boxDecimal || rk == boxDecimal:
		return true
	// A TEXT column against a NUMERIC literal: the text comparison (#504).
	case lk == boxText && rk == boxNumber && rText != "":
		return true
	case rk == boxText && lk == boxNumber && lText != "":
		return true
	// A NUMBER column against a QUOTED literal: the numeric comparison,
	// because PostgreSQL types the unknown literal from the column (B1).
	case lk == boxNumber && rk == boxQuoted && rText != "":
		return true
	case rk == boxNumber && lk == boxQuoted && lText != "":
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
//   - TEXT against a numeric LITERAL: the literal's source TEXT against the
//     column's value, bytewise. PostgreSQL refuses this pair outright —
//     verified live, `WHERE s = 1.5` over a text column is 42883 "operator
//     does not exist: text = numeric" — but that is an OVERLOAD RESOLUTION
//     failure, and wadjet has one generic comparison operator with no
//     overload set to fail resolution against, exactly the situation
//     ADR-0012 item 5 already records for unary minus over a quoted string.
//     So the pair gets the STRING column's own rule instead of a reading of
//     its digits, which is also what the vectorized kernel answers (#504).
//   - A NUMBER against a QUOTED literal: the NUMBER's rule, because
//     PostgreSQL types an unknown-typed literal from the operand it meets.
//     `k > '2'` over a BIGINT column is `k > 2` there, not a text comparison
//     and not a comparison against zero.
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
	case lk == boxText && lIsStr && rText != "":
		return strings.Compare(ls, rText), true
	case rk == boxText && rIsStr && lText != "":
		return strings.Compare(lText, rs), true
	// A NUMBER column against a QUOTED literal. PostgreSQL types an
	// unknown-typed literal from the operand it meets, so `k > '2'` over a
	// BIGINT column is the integer comparison `k > 2` — exact against an
	// integer, float64 against a float, which is what decimalTextOrder
	// already states. A literal that names no number answers ok=false here
	// and falls through; refusing it is #536's rule, not this one's.
	case lk == boxNumber && rk == boxQuoted:
		if c, ok := decimalTextOrder(lv, rText); ok {
			return c, true
		}
	case rk == boxNumber && lk == boxQuoted:
		if c, ok := decimalTextOrder(rv, lText); ok {
			return -c, true
		}
	}
	return 0, false
}

// compare answers one comparison operator for this pair, falling through to
// compare() when the operands' declarations select no rule.
//
// That fallthrough is the whole reason the sniff could be deleted. An operand
// whose declaration nothing here can read — a CAST, a scalar subquery, an
// ordinary function's result — is compared by compare()'s own type rules,
// which is the documented stance for a value with no declaration to consult
// (ADR-0012 item 8's `compareAny` fallback). What it must NOT do is guess a
// declaration from the box, because that guess is what disagreed with the
// kernel.
//
// A nil pair is an unbound node — a Cmp assembled as a struct literal rather
// than through NewCmp, the same shape that leaves dec and decCols nil.
func (p *boxedPair) compare(b *batch.RecordBatch, lv, rv any, op CmpOp) bool {
	if c, ok := p.order(b, lv, rv); ok {
		return cmpOrder(c, op)
	}
	return compare(lv, rv, op)
}
