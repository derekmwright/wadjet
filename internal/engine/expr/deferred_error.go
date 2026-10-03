// SPDX-License-Identifier: MIT

package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// DeferredError is a subquery's coded failure raised when it is EVALUATED
// (plansql.DeferredErrorNode): the coordinator ran the subquery at plan time,
// the run raised, and the failure stands where the subquery's answer would
// have. Evaluating it for a row raises the failure through the channel every
// evaluation refusal uses (fatalEval); a row that never evaluates it — under
// a WHEN no row satisfies, behind an AND whose left operand is false — never
// raises it, which is PostgreSQL's rule for an uncorrelated sublink (an
// InitPlan runs on its first reference).
//
// Every connective that holds one evaluates its operands per row and
// short-circuits (And, Or, Case, Coalesce), so the raise is as lazy as the
// evaluator; no batch-level kernel evaluates an arm for rows its condition
// did not select.
type DeferredError struct {
	State   string
	Message string
}

func (e *DeferredError) raise() {
	panic(fatalEval{sqlerr.New(e.State, "%s", e.Message)})
}

func (e *DeferredError) Eval(_ *batch.RecordBatch, _ int) any {
	e.raise()
	return nil
}

func (e *DeferredError) EvalBool(_ *batch.RecordBatch, _ int) bool {
	e.raise()
	return false
}

func (e *DeferredError) EvalBoolNull(_ *batch.RecordBatch, _ int) (bool, bool) {
	e.raise()
	return false, false
}
