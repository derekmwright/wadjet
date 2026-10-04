// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// RefuseTemporalTextBesideTypedOperand reads every quoted literal compared
// with (=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM), bounding (BETWEEN) or
// listed against (IN / NOT IN) an operand whose DATE or TIMESTAMP type typeOf
// states, with that type's input function, and returns the first refusal
// (22007 / 22008 / 22009) — the coercion PostgreSQL performs while it
// analyses the statement, so the refusal does not wait for a row reaching the
// comparison: through a derived table, a CTE, a set operation, a scalar
// subquery or an expression as much as for a stored column (#1512).
//
// The comparison coerces its text exactly as CAST does: a text CAST refuses
// is refused here. That includes PostgreSQL's special input words other than
// 'epoch' (which the grammar reads) — 'infinity', 'now', 'today', … — which
// this engine's temporal input does not read (ADR-0012 temporal r2 / r25).
// Nothing is rewritten.
func RefuseTemporalTextBesideTypedOperand(n plansql.Node, typeOf func(plansql.Node) (parquet.TypeID, bool)) error {
	r := &temporalTextRefusal{typeOf: typeOf}
	r.walk(n)
	return r.err
}

type temporalTextRefusal struct {
	typeOf func(plansql.Node) (parquet.TypeID, bool)
	err    error // the first refusal of a literal beside a typed operand
}

// refuse records the input function's refusal of a quoted literal n beside an
// operand of type typ.
func (r *temporalTextRefusal) refuse(n plansql.Node, typ parquet.TypeID) {
	if r.err != nil {
		return
	}
	lit, ok := unwrapParens(n).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return
	}
	r.err = expr.RefuseTemporalLiteral(batch.TypeID(typ), lit.Value)
}

// temporalOperand is typeOf narrowed to DATE / TIMESTAMP.
func (r *temporalTextRefusal) temporalOperand(n plansql.Node) (parquet.TypeID, bool) {
	if n == nil {
		return 0, false
	}
	t, ok := r.typeOf(n)
	if !ok || (t != parquet.TypeDate && t != parquet.TypeTimestamp) {
		return 0, false
	}
	return t, true
}

func (r *temporalTextRefusal) walk(n plansql.Node) {
	if n == nil || r.err != nil {
		return
	}
	each := func(ns []plansql.Node) {
		for _, x := range ns {
			r.walk(x)
		}
	}
	switch t := n.(type) {
	case *plansql.CmpExpr:
		if typ, ok := r.temporalOperand(t.Left); ok {
			r.refuse(t.Right, typ)
		} else if typ, ok := r.temporalOperand(t.Right); ok {
			r.refuse(t.Left, typ)
		}
		r.walk(t.Left)
		r.walk(t.Right)
	case *plansql.BetweenExpr:
		if typ, ok := r.temporalOperand(t.Left); ok {
			r.refuse(t.Low, typ)
			r.refuse(t.High, typ)
		}
		r.walk(t.Left)
		r.walk(t.Low)
		r.walk(t.High)
	case *plansql.InExpr:
		if typ, ok := r.temporalOperand(t.Left); ok {
			for _, v := range t.Values {
				r.refuse(v, typ)
			}
		}
		r.walk(t.Left)
		each(t.Values)
	case *plansql.ParenNode:
		r.walk(t.Inner)
	case *plansql.BinaryOp:
		r.walk(t.Left)
		r.walk(t.Right)
	case *plansql.UnaryOp:
		r.walk(t.Inner)
	case *plansql.AndNode:
		r.walk(t.Left)
		r.walk(t.Right)
	case *plansql.OrNode:
		r.walk(t.Left)
		r.walk(t.Right)
	case *plansql.NotNode:
		r.walk(t.Inner)
	case *plansql.IsExpr:
		r.walk(t.Left)
	case *plansql.LikeExpr:
		r.walk(t.Left)
		r.walk(t.Pattern)
	case *plansql.AnyAllExpr:
		r.walk(t.Left)
		each(t.Values)
	case *plansql.CastNode:
		r.walk(t.Inner)
	case *plansql.FuncCallNode:
		each(t.Args)
	case *plansql.CaseNode:
		r.walk(t.Subject)
		for _, w := range t.Whens {
			r.walk(w.Cond)
			r.walk(w.Result)
		}
		r.walk(t.Else)
	case *plansql.ArrayLitNode:
		each(t.Elements)
	case *plansql.TupleNode:
		each(t.Elements)
	}
}
