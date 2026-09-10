// This file holds expr logical; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Logical operators ---
//
// The connectives implement Kleene's strong three-valued logic on the
// EvalBoolNull protocol. Their EvalBool forms keep the original two-valued
// short-circuits: collapsing UNKNOWN to false BEFORE the connective gives
// the same filter answer as collapsing after it for AND and OR (min/max
// logic), so the hot path pays nothing — NOT is the one operator where the
// two orders disagree, and it evaluates on the three-valued protocol.

// And is a logical AND.
type And struct {
	Left, Right Expr
}

func (e *And) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *And) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) && toBool(e.Right, b, row)
}

// EvalBoolNull: FALSE AND anything is FALSE; otherwise a NULL operand makes
// it UNKNOWN. Short-circuits on a FALSE left operand.
func (e *And) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lnull := evalBoolNull(e.Left, b, row)
	if !lnull && !lv {
		return false, false
	}
	rv, rnull := evalBoolNull(e.Right, b, row)
	if !rnull && !rv {
		return false, false
	}
	if lnull || rnull {
		return false, true
	}
	return true, false
}

// Or is a logical OR.
type Or struct {
	Left, Right Expr
}

func (e *Or) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Or) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) || toBool(e.Right, b, row)
}

// EvalBoolNull: TRUE OR anything is TRUE; otherwise a NULL operand makes it
// UNKNOWN. Short-circuits on a TRUE left operand.
func (e *Or) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lnull := evalBoolNull(e.Left, b, row)
	if !lnull && lv {
		return true, false
	}
	rv, rnull := evalBoolNull(e.Right, b, row)
	if !rnull && rv {
		return true, false
	}
	if lnull || rnull {
		return false, true
	}
	return false, false
}

// Not is a logical NOT.
type Not struct {
	Operand Expr
}

func (e *Not) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBool: NOT must see the third value — collapsing first turned
// NOT (UNKNOWN) into true and admitted rows SQL excludes, which was the
// dangerous half of #370 (`1 NOT IN (2, NULL)` answering true).
func (e *Not) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: NOT UNKNOWN stays UNKNOWN.
func (e *Not) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	v, null := evalBoolNull(e.Operand, b, row)
	if null {
		return false, true
	}
	return !v, false
}
