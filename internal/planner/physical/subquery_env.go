// SPDX-License-Identifier: MIT

package physical

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// EnsureMemoryTracker initializes the per-query tracker through getSpillManager,
// so SubqueryEnv's budget charges IN-subquery membership on compiling-only doors.
// SubqueryEnv supplies the runner, inner FROM resolver and scalar declaration/
// budget options to CompileWithScopeResolver alongside the caller's outer scope.
// This preserves correlated DML predicates and one subquery implementation.
// Its stored context governs later executions, once uncorrelated or per outer row.
// ADR-0031, #688; ADR-0006, #531.
// See docs/internals/compiled-predicate-subquery-environment.md for the design.
func (p *Planner) EnsureMemoryTracker() {
	p.getSpillManager()
}

// ReleaseSubqueryEnv is the teardown of a compiling-only door, the one a
// physical plan's Cleanup is for every other statement: it returns every
// IN-subquery membership set compiled under this planner's budget and removes
// the statement's spill scope. A DML predicate's environment calls Plan never,
// so without it those charges would outlive the statement — permanently, on
// an engine instance's pool (ADR-0006's 2026-11-24 amendment). Idempotent.
func (p *Planner) ReleaseSubqueryEnv() {
	r := p.resources()
	r.releaseSubqueryCharges()
	r.releaseSpillScope()
}

func (p *Planner) SubqueryEnv(ctx context.Context) (expr.SubqueryRunner, plansql.TableColumns, []expr.CompileOption) {
	p.PlanCtx = ctx
	return p.subqueryRunner, p.SubqueryInnerColumns(),
		[]expr.CompileOption{p.subqueryDeclOption(), p.subqueryBudgetOption(), p.clockOption()}
}
