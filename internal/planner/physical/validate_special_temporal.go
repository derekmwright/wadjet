// SPDX-License-Identifier: MIT

package physical

import (
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// FoldSpecialTemporalWords resolves PostgreSQL's special date/time input words
// where a quoted literal meets a DATE or TIMESTAMP operand whose type the
// caller can state at plan time — the one site PostgreSQL itself coerces the
// constant — rewriting the comparison IN PLACE (#1512 round 2):
//
//   - 'now' / 'today' / 'tomorrow' / 'yesterday' become the literal text of
//     the statement clock's instant / day (expr.StatementClock, the clock
//     CURRENT_TIMESTAMP and CURRENT_DATE read), as PostgreSQL resolves them
//     when it parses the constant.
//   - 'infinity' / '-infinity' have no value in the millisecond / day
//     carriers, so the comparison is FOLDED: every finite x is below
//     'infinity' and above '-infinity', so `x < 'infinity'` is `x = x` (true,
//     NULL for a NULL x) and `x = 'infinity'` is `x <> x` (false, NULL for a
//     NULL x); IS [NOT] DISTINCT FROM folds to its NULL-safe twin. BETWEEN
//     folds per bound, an IN / NOT IN list drops an infinite member (or, when
//     every member is infinite, becomes the NULL-keeping twin over x itself).
//
// 'epoch' needs no fold: the grammar reads it (parquet.parseTemporalText).
// typeOf states an operand's declared type; a word beside an operand it cannot
// type is left alone and the per-evaluation path refuses it 22007 (temporal
// r2 / r25).
//
// Any OTHER quoted literal beside an operand typeOf types is read by that
// type's input function here too, and its refusal (22007 / 22008 / 22009) is
// returned: PostgreSQL coerces the constant while it analyses the statement,
// so the refusal does not wait for a row reaching the comparison — through a
// derived table, a CTE, a set operation, a scalar subquery or an expression
// as much as for a stored column. Reports whether anything was rewritten.
func FoldSpecialTemporalWords(n plansql.Node, typeOf func(plansql.Node) (parquet.TypeID, bool), now time.Time) (bool, error) {
	f := &specialFold{typeOf: typeOf, now: now}
	f.walk(n)
	return f.changed, f.err
}

type specialFold struct {
	typeOf  func(plansql.Node) (parquet.TypeID, bool)
	now     time.Time
	changed bool
	err     error // the first refusal of a literal beside a typed operand
}

// refuse records the input function's refusal of a quoted literal n beside an
// operand of type typ (nil for a literal the type reads).
func (f *specialFold) refuse(n plansql.Node, typ parquet.TypeID) {
	if f.err != nil {
		return
	}
	lit, ok := unwrapParens(n).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return
	}
	f.err = expr.RefuseTemporalLiteral(batch.TypeID(typ), lit.Value)
}

// temporalOperand is typeOf narrowed to DATE / TIMESTAMP.
func (f *specialFold) temporalOperand(n plansql.Node) (parquet.TypeID, bool) {
	if n == nil {
		return 0, false
	}
	t, ok := f.typeOf(n)
	if !ok || (t != parquet.TypeDate && t != parquet.TypeTimestamp) {
		return 0, false
	}
	return t, true
}

// word reads a quoted special word; a clock word is substituted in place and
// reported as SpecialNone (nothing left to fold).
func (f *specialFold) word(n plansql.Node, typ parquet.TypeID) expr.SpecialTemporalWord {
	lit, ok := unwrapParens(n).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return expr.SpecialNone
	}
	w := expr.ReadSpecialTemporalWord(lit.Value)
	if text, ok := expr.SpecialTemporalText(w, batch.TypeID(typ), f.now); ok {
		lit.Value = text
		f.changed = true
		return expr.SpecialNone
	}
	return w
}

// infiniteTruth: does a finite x satisfy `x op w`? op is as written with x on
// the left.
func infiniteTruth(op string, w expr.SpecialTemporalWord) (bool, bool) {
	above := w == expr.SpecialInfinity // the word is above every finite x
	switch op {
	case "=":
		return false, true
	case "!=", "<>":
		return true, true
	case "<", "<=":
		return above, true
	case ">", ">=":
		return !above, true
	}
	return false, false
}

func flipCmp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return op
}

func (f *specialFold) cmp(c *plansql.CmpExpr) {
	x, other, op := c.Left, c.Right, c.Op
	typ, ok := f.temporalOperand(x)
	if !ok {
		x, other, op = c.Right, c.Left, flipCmp(c.Op)
		if typ, ok = f.temporalOperand(x); !ok {
			return
		}
	}
	w := f.word(other, typ)
	if w != expr.SpecialInfinity && w != expr.SpecialNegInfinity {
		f.refuse(other, typ)
		return
	}
	switch op {
	case "is distinct from":
		c.Left, c.Op, c.Right = x, "is not distinct from", x
	case "is not distinct from":
		c.Left, c.Op, c.Right = x, "is distinct from", x
	default:
		truth, known := infiniteTruth(op, w)
		if !known {
			return
		}
		c.Left, c.Right = x, x
		c.Op = "<>"
		if truth {
			c.Op = "="
		}
	}
	f.changed = true
}

func (f *specialFold) between(b *plansql.BetweenExpr) {
	typ, ok := f.temporalOperand(b.Left)
	if !ok {
		return
	}
	lo, hi := f.word(b.Low, typ), f.word(b.High, typ)
	if lo == expr.SpecialNone {
		f.refuse(b.Low, typ)
	}
	if hi == expr.SpecialNone {
		f.refuse(b.High, typ)
	}
	unsat := lo == expr.SpecialInfinity || hi == expr.SpecialNegInfinity
	switch {
	case unsat:
		// x >= +inf or x <= -inf: false for every finite x, NULL for NULL —
		// NOT (x BETWEEN x AND x).
		b.Low, b.High, b.Not = b.Left, b.Left, !b.Not
	default:
		if lo == expr.SpecialNegInfinity {
			b.Low = b.Left
		}
		if hi == expr.SpecialInfinity {
			b.High = b.Left
		}
		if lo == expr.SpecialNone && hi == expr.SpecialNone {
			return
		}
	}
	f.changed = true
}

func (f *specialFold) in(e *plansql.InExpr) {
	typ, ok := f.temporalOperand(e.Left)
	if !ok || len(e.Values) == 0 {
		return
	}
	kept := e.Values[:0:0]
	dropped := false
	for _, v := range e.Values {
		if _, isSub := v.(*plansql.SubqueryNode); isSub {
			return
		}
		switch f.word(v, typ) {
		case expr.SpecialInfinity, expr.SpecialNegInfinity:
			// x = ±infinity is false for every finite x: the member
			// contributes nothing to IN, and nothing to NOT IN.
			dropped = true
			continue
		}
		f.refuse(v, typ)
		kept = append(kept, v)
	}
	if !dropped {
		return
	}
	if len(kept) == 0 {
		// x IN (±inf) is false (NULL for NULL): NOT (x IN (x)); x NOT IN
		// (±inf) is true (NULL for NULL): x IN (x).
		kept = []plansql.Node{e.Left}
		e.Not = !e.Not
	}
	e.Values = kept
	f.changed = true
}

func (f *specialFold) walk(n plansql.Node) {
	if n == nil {
		return
	}
	each := func(ns []plansql.Node) {
		for _, x := range ns {
			f.walk(x)
		}
	}
	switch t := n.(type) {
	case *plansql.CmpExpr:
		f.cmp(t)
		f.walk(t.Left)
		f.walk(t.Right)
	case *plansql.BetweenExpr:
		f.between(t)
		f.walk(t.Left)
		f.walk(t.Low)
		f.walk(t.High)
	case *plansql.InExpr:
		f.in(t)
		f.walk(t.Left)
		each(t.Values)
	case *plansql.ParenNode:
		f.walk(t.Inner)
	case *plansql.BinaryOp:
		f.walk(t.Left)
		f.walk(t.Right)
	case *plansql.UnaryOp:
		f.walk(t.Inner)
	case *plansql.AndNode:
		f.walk(t.Left)
		f.walk(t.Right)
	case *plansql.OrNode:
		f.walk(t.Left)
		f.walk(t.Right)
	case *plansql.NotNode:
		f.walk(t.Inner)
	case *plansql.IsExpr:
		f.walk(t.Left)
	case *plansql.LikeExpr:
		f.walk(t.Left)
		f.walk(t.Pattern)
	case *plansql.AnyAllExpr:
		f.walk(t.Left)
		each(t.Values)
	case *plansql.CastNode:
		f.walk(t.Inner)
	case *plansql.FuncCallNode:
		each(t.Args)
	case *plansql.CaseNode:
		f.walk(t.Subject)
		for _, w := range t.Whens {
			f.walk(w.Cond)
			f.walk(w.Result)
		}
		f.walk(t.Else)
	case *plansql.ArrayLitNode:
		each(t.Elements)
	case *plansql.TupleNode:
		each(t.Elements)
	}
}
