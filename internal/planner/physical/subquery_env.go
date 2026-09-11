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

func (p *Planner) SubqueryEnv(ctx context.Context) (expr.SubqueryRunner, plansql.TableColumns, []expr.CompileOption) {
	p.planCtx = ctx
	return p.subqueryRunner, p.subqueryInnerColumns(),
		[]expr.CompileOption{p.subqueryDeclOption(), p.subqueryBudgetOption()}
}
