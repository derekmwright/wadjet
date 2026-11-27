// SPDX-License-Identifier: MIT

package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ConstantEvalError is the error a CONSTANT expression raises when it is
// evaluated, or nil: PostgreSQL folds constants when it plans, so the
// statement `SELECT INTERVAL '1 day' * 1e300` is its 22008 (and an interval
// literal past the range its 22015) before anything else is decided. The
// planner's interval rule (#1713d) asks this before it refuses a constant
// interval's publication with 0A000, so PostgreSQL's own refusal keeps its
// code. A node that reads a column, a subquery or a non-constant function is
// not folded (nil).
func ConstantEvalError(node plansql.Node) (err error) {
	if !foldableConstant(node) {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(fatalEval)
			if !ok || isTimestampDifferenceRefusal(fe.err) {
				// The timestamp-difference guard is this engine's refusal,
				// not one PostgreSQL raises while folding: the caller's own
				// diagnostic stands (#1713d round 8).
				err = nil
				return
			}
			err = fe.err
		}
	}()
	e, cerr := Compile(node)
	if cerr != nil {
		if sqlerr.StateOf(cerr) != "" {
			return cerr
		}
		return nil
	}
	e.Eval(batch.NewRecordBatch(nil, 1), 0)
	return nil
}

// foldableConstant reports an expression of literals, casts, operators and
// the choice functions only.
func foldableConstant(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.Lit, *plansql.IntervalLit:
		return true
	case *plansql.ParenNode:
		return foldableConstant(n.Inner)
	case *plansql.CastNode:
		return !n.Column && foldableConstant(n.Inner)
	case *plansql.UnaryOp:
		return foldableConstant(n.Inner)
	case *plansql.BinaryOp:
		return foldableConstant(n.Left) && foldableConstant(n.Right)
	case *plansql.CmpExpr:
		return foldableConstant(n.Left) && foldableConstant(n.Right)
	case *plansql.CaseNode:
		if n.Subject != nil && !foldableConstant(n.Subject) {
			return false
		}
		if n.Else != nil && !foldableConstant(n.Else) {
			return false
		}
		for _, w := range n.Whens {
			if !foldableConstant(w.Cond) || !foldableConstant(w.Result) {
				return false
			}
		}
		return true
	case *plansql.FuncCallNode:
		switch strings.ToLower(n.Name) {
		case "coalesce", "nullif", "greatest", "least":
			for _, a := range n.Args {
				if !foldableConstant(a) {
					return false
				}
			}
			return true
		}
	}
	return false
}
