package expr

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Row-value comparison and the quantified comparisons (`= ANY`, `<> ALL`,
// `= SOME`).
//
// Both spellings PARSED and neither COMPILED. `compileWithCtx`'s `default:`
// arm returned `&Lit{Val: node.String()}` — the expression's own SQL text as a
// string constant — for every node type it had no case for, and it had no case
// for `*plansql.AnyAllExpr` or `*plansql.TupleNode`. Two different silent
// wrong answers came out of that one line (#710):
//
//	WHERE id = ANY(ARRAY[1,2])   the whole predicate became the STRING
//	                             "id = ANY (ARRAY[1, 2])", so the filter's
//	                             bool assertion failed and NO row matched.
//	WHERE (id, n) = (1, 10)      each OPERAND became a string, and a real
//	                             comparison compared "(id, n)" against
//	                             "(1, 10)" byte-wise — a genuine false.
//
// Six spellings answered zero rows on the ordinary query path, and DML
// inherited every one of them: `DELETE … WHERE id = ANY(ARRAY[1,2])` reported
// `DELETE 0` where PostgreSQL deletes two rows.
//
// It is #610 recurring at the same `default:`, for the node types that fix did
// not enumerate — which is why the arm now FAILS instead of guessing.

// evalBool3 evaluates a compiled boolean expression in three-valued logic.
func evalBool3(e Expr, b *batch.RecordBatch, row int) (val, null bool) {
	if bn, ok := e.(BoolNullExpr); ok {
		return bn.EvalBoolNull(b, row)
	}
	v := e.Eval(b, row)
	if v == nil {
		return false, true
	}
	bv, ok := v.(bool)
	return ok && bv, false
}

// Quantified is `left <op> ANY|SOME|ALL (v1, v2, …)`.
//
// The candidate list is fixed at COMPILE time — a value list, or the elements
// of an `ARRAY[…]` literal — so each candidate is an ordinary comparison and
// the quantifier is the fold over them. `= ANY (subquery)` and `<> ALL
// (subquery)` do not come here: they are `IN` and `NOT IN`, and the compiler
// routes them to InSubquery, which already knows how to stream a subquery's
// rows.
//
// PostgreSQL's three-valued fold, which is why this is not a plain OR/AND
// over the comparisons:
//
//	ANY  TRUE if any comparison is TRUE; else NULL if any is NULL; else FALSE
//	ALL  FALSE if any comparison is FALSE; else NULL if any is NULL; else TRUE
//
// An empty candidate list is FALSE for ANY and TRUE for ALL, which is
// PostgreSQL's answer for the empty array.
type Quantified struct {
	Cmps []Expr // one comparison of the left operand against each candidate
	All  bool   // ALL, rather than ANY/SOME
}

func (e *Quantified) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Quantified) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *Quantified) EvalBoolNull(b *batch.RecordBatch, row int) (val, null bool) {
	sawNull := false
	for _, c := range e.Cmps {
		v, isNull := evalBool3(c, b, row)
		if isNull {
			sawNull = true
			continue
		}
		if e.All && !v {
			return false, false
		}
		if !e.All && v {
			return true, false
		}
	}
	if sawNull {
		return false, true
	}
	return e.All, false
}

// RowCmp compares two row values field by field, as PostgreSQL does.
//
// `=` and `<>` compare every field; the ordering operators stop at the first
// field pair that is not equal and answer from it, which is what makes
// `(1, 2) < (1, 3)` true and `(2, 0) < (1, 9)` false. A NULL anywhere the
// comparison has to LOOK makes the whole thing NULL — for `=` that is any
// field, for `<` only the fields up to and including the deciding one.
type RowCmp struct {
	// eq[i] is the equality of field i, cmp[i] the requested operator on it.
	// Both are built by the ordinary comparison compiler, so a row comparison
	// resolves its operand types exactly as the scalar one does.
	eq  []Expr
	cmp []Expr
	op  CmpOp
}

func (e *RowCmp) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *RowCmp) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *RowCmp) EvalBoolNull(b *batch.RecordBatch, row int) (val, null bool) {
	switch e.op {
	case CmpEq, CmpNe:
		sawNull := false
		for _, c := range e.eq {
			v, isNull := evalBool3(c, b, row)
			if isNull {
				sawNull = true
				continue
			}
			if !v {
				return e.op == CmpNe, false
			}
		}
		if sawNull {
			return false, true
		}
		return e.op == CmpEq, false
	}

	// Ordering: the first field pair that is not equal decides.
	for i := range e.eq {
		v, isNull := evalBool3(e.eq[i], b, row)
		if isNull {
			return false, true
		}
		if v {
			continue
		}
		return evalBool3(e.cmp[i], b, row)
	}
	// Every field equal.
	return e.op == CmpLe || e.op == CmpGe, false
}

// compileRowCmp builds a row comparison from two tuples.
func compileRowCmp(left, right *plansql.TupleNode, op CmpOp, ctx *compileContext) (Expr, error) {
	if len(left.Elements) != len(right.Elements) {
		// PostgreSQL: 42601, "unequal number of entries in row expressions".
		return nil, sqlerr.New("42601",
			"unequal number of entries in row expressions: %d and %d",
			len(left.Elements), len(right.Elements))
	}
	if len(left.Elements) == 0 {
		return nil, sqlerr.New("42601", "row expression has no entries")
	}
	out := &RowCmp{op: op}
	for i := range left.Elements {
		l, err := compileWithCtx(left.Elements[i], ctx)
		if err != nil {
			return nil, err
		}
		r, err := compileWithCtx(right.Elements[i], ctx)
		if err != nil {
			return nil, err
		}
		out.eq = append(out.eq, compileCmp(l, r, CmpEq))
		out.cmp = append(out.cmp, compileCmp(l, r, op))
	}
	return out, nil
}

// asTuple reports whether a node is a row value of more than one field.
// A one-field parenthesized value is a scalar, exactly as in PostgreSQL.
func asTuple(n plansql.Node) (*plansql.TupleNode, bool) {
	t, ok := n.(*plansql.TupleNode)
	if !ok || len(t.Elements) < 2 {
		return nil, false
	}
	return t, true
}

// compileQuantified builds `left op ANY|ALL|SOME (…)`.
func compileQuantified(n *plansql.AnyAllExpr, ctx *compileContext) (Expr, error) {
	op, ok := cmpOpFromSQL(n.Op)
	if !ok {
		return nil, sqlerr.New("0A000", "operator %q is not supported with %s", n.Op, n.Modifier)
	}
	all := n.Modifier == "ALL"

	left, err := compileWithCtx(n.Left, ctx)
	if err != nil {
		return nil, err
	}

	// A subquery on the right is IN / NOT IN, and only for the two spellings
	// that mean them. Anything else over a subquery — `> ANY (SELECT …)` —
	// needs the subquery's rows compared with an ordering operator, which the
	// runner does not do; refusing it is honest, and the refusal is what makes
	// the boundary visible.
	if len(n.Values) == 1 {
		if sq, ok := n.Values[0].(*plansql.SubqueryNode); ok {
			if ctx.runner == nil {
				return nil, fmt.Errorf("%s subquery requires a SubqueryRunner", n.Modifier)
			}
			switch {
			case op == CmpEq && !all:
				return &InSubquery{Expr: left, SQL: sq.SQL, Runner: ctx.runner,
					Budget: ctx.budget, SetBound: ctx.setRowBound}, nil
			case op == CmpNe && all:
				return &InSubquery{Expr: left, SQL: sq.SQL, Runner: ctx.runner, Not: true,
					Budget: ctx.budget, SetBound: ctx.setRowBound}, nil
			}
			return nil, sqlerr.New("0A000",
				"%s %s (subquery) is not supported; only `= ANY` and `<> ALL` over a subquery are", n.Op, n.Modifier)
		}
	}

	// The candidates: an ARRAY literal's elements, or the value list itself.
	candidates := n.Values
	if len(n.Values) == 1 {
		if arr, ok := n.Values[0].(*plansql.ArrayLitNode); ok {
			candidates = arr.Elements
		}
	}
	out := &Quantified{All: all}
	for _, c := range candidates {
		switch c.(type) {
		case *plansql.SubqueryNode, *plansql.ArrayLitNode:
			return nil, sqlerr.New("0A000",
				"%s %s over %s is not supported", n.Op, n.Modifier, c.String())
		}
		compiled, err := compileWithCtx(c, ctx)
		if err != nil {
			return nil, err
		}
		out.Cmps = append(out.Cmps, compileCmp(left, compiled, op))
	}
	return out, nil
}

// cmpOpFromSQL maps an operator's SQL spelling to its CmpOp.
func cmpOpFromSQL(op string) (CmpOp, bool) {
	switch op {
	case "=":
		return CmpEq, true
	case "!=", "<>":
		return CmpNe, true
	case "<":
		return CmpLt, true
	case "<=":
		return CmpLe, true
	case ">":
		return CmpGt, true
	case ">=":
		return CmpGe, true
	}
	return CmpEq, false
}
