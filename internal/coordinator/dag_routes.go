// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"errors"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
)

// localRoute runs a plan the stage DAG refused on the coordinator-local
// single-process pipeline, from the statement's bound plan L.
type localRoute func(queryID string, lp *statementPlan, start time.Time, refusal error) (*SQLResult, error)

// runRoute runs route on the statement's bound plan (ADR-0050 §1.3 item 4):
// the L the fast path left unconsumed, a fresh one when the fast path
// physically planned it, and L's own error when L could not be built. The
// incarnation check runs at the route's entry, before it obtains or plans L
// (§3).
func (c *Coordinator) runRoute(local *localPlans, queryID string, start time.Time, refusal error, route localRoute) (*SQLResult, error) {
	if err := checkTransition(local.ctx, transitionRouteEntry); err != nil {
		return nil, err
	}
	lp, err := local.forRoute()
	if err != nil {
		if final, ok := finalPlanningError(local.ctx, err); ok {
			return nil, final
		}
		return nil, err
	}
	return route(queryID, lp, start, refusal)
}

// dagRefusalRoute is the route for a plan the stage planner refused with one
// of the sentinels the single-process pipeline answers, or nil. The order is
// the order the checks have always been asked in.
func (c *Coordinator) dagRefusalRoute(err error) localRoute {
	switch {
	// A per-row correlated subquery has no distributed lowering — the
	// planner refuses it with a typed error rather than letting the
	// scalar-deferral path answer 0 silently (#359). Route it onto the
	// coordinator-local single-process pipeline, which owns
	// correlated-subquery semantics; any failure THERE is the query's
	// outcome (no DAG plan exists to fall back to).
	case errors.Is(err, dagplan.ErrCorrelatedSubqueryDistributed):
		return c.runCorrelatedLocal
	// Same shape one construct over: a DISTINCT the DAG has no stage
	// for and the post-gather dedup cannot see. The refusal exists so
	// the DISTINCT is not silently DROPPED (#466) — but the query has
	// an answer, and the single-process pipeline applies a Distinct
	// wherever it sits, so route it there rather than erroring.
	// And a null-aware anti join whose build the plan cannot show every
	// task sees whole. A partitioned build splits NOT IN's three-valued
	// fact and the query silently answers its NOT EXISTS twin (#507), so
	// the invariant refuses rather than dispatch — and the single-process
	// pipeline, whose one join holds the whole build by construction,
	// answers it (#539).
	case errors.Is(err, dagplan.ErrNullAwareAntiBuildNotReplicated):
		return c.runNullAwareAntiLocal
	case errors.Is(err, dagplan.ErrDistinctDistributed):
		return c.runDistinctLocal
	// And once more for GROUPING SETS / ROLLUP / CUBE, which the DAG has no
	// Stage field, no wire tag and no worker read for — so it answered
	// their union as a plain GROUP BY and dropped every super-aggregate
	// row, silently (#778). The single-process HashAggregate is the only
	// operator in the process that knows what a grouping set is.
	case errors.Is(err, dagplan.ErrGroupingSetsDistributed):
		return c.runGroupingSetsLocal
	// And a GROUP BY key that needs its resolution name and its published
	// name to be two different things, which one `Stage.GroupByCols` entry
	// cannot be. The single-process pipeline carries them separately —
	// a hidden slot and exec.HashAggregate.GroupByOutNames (ADR-0026 §2) —
	// so the query has an answer here (#736).
	case errors.Is(err, dagplan.ErrGroupKeyDistributed):
		return c.runGroupKeyLocal
	// And once more for an IN-subquery the planner could not materialize
	// into a literal set — too many rows, or a value with no literal
	// spelling. The single-process pipeline resolves the set once through
	// expr.InSubquery and caches it, so the query has an answer here even
	// though the DAG has no stage for the predicate (#524).
	case errors.Is(err, dagplan.ErrInSubqueryDistributed):
		return c.runInSubqueryLocal
	// And a subquery in the SELECT list, whose only distributed lowering
	// was to ship the SQL text to a worker with no SubqueryRunner and
	// fail every task (#659). The single-process pipeline compiles it
	// against a real runner.
	case errors.Is(err, dagplan.ErrScalarSubqueryProjectionDistributed):
		return c.runScalarProjectionLocal
	// And a SELECT list no stage computed. The DAG would hand the client
	// the producer's raw columns under their source names; the
	// single-process pipeline runs the Project as a real operator
	// (#656 F2).
	case errors.Is(err, dagplan.ErrUnreachableGatherOutput):
		return c.runUnreachableOutputLocal
	// And a star over a derived block whose projection no stage could be
	// made to publish. Arc K3 materializes that projection onto the stage
	// for every block it can state (#984); this is the residue — a
	// computed item with no plan-time type, a producer that cannot carry a
	// projection — where the star would otherwise publish the STREAM. One
	// spelling failed loudly under ADR-0010 and the other silently dropped
	// a column PostgreSQL sends; the single-process pipeline runs the
	// block's Project as a real operator.
	case errors.Is(err, dagplan.ErrLateralProjectionDistributed):
		return c.runLateralProjectionLocal
	// And a SELECT that reads no table at all. Its `dual` stage has no
	// dependencies and no scan files, so the dispatcher could not build
	// task inputs for it and every table-less SELECT FAILED on the DAG
	// (#806). There is nothing to distribute — the answer is one row —
	// and the dual stage's own comment already says it runs here.
	case errors.Is(err, dagplan.ErrTableLessSelectDistributed):
		return c.runTableLessLocal
	// And a stage the dispatcher could not build task inputs for. #806
	// was one producer of that shape and #812 is another — a scalar
	// subquery over a CTE in a WHERE clause, whose substitution leaves a
	// stage naming neither a dependency nor a table. The query answers
	// here, where the CTE is a real operator, instead of failing three
	// task attempts later with an internal message and no SQLSTATE.
	case errors.Is(err, dagplan.ErrUnbuildableStageDistributed):
		return c.runUnbuildableStageLocal
	// And a join residual whose stage re-spelling would read both of the
	// sides it compares from ONE arm (arc DC): emitted, EXISTS
	// answered no rows and NOT EXISTS every row. The single-process
	// pipeline binds each leaf through its own arm.
	case errors.Is(err, dagplan.ErrResidualSidesMergedDistributed):
		return c.runResidualSidesLocal
	// And a window over a policed scan that feeds a join (arc LT): the
	// four DAG doors answered the STORED column's pairing under the mask.
	// The single-process pipeline answers the mask on every door.
	case errors.Is(err, dagplan.ErrPolicedWindowUnderJoinDistributed):
		return c.runPolicedWindowLocal
	// And a window above a LATERAL join (arc LT): the DAG binds
	// its key to the outer occurrence; the single-process pipeline
	// answers PostgreSQL's rows.
	case errors.Is(err, dagplan.ErrWindowOverLateralDistributed):
		return c.runWindowOverLateralLocal
	// And a correlated LATERAL whose arm shares a column name with another
	// relation (arc JP): the DAG re-spells the body's names onto
	// its scan stream and a lost qualifier binds the other relation's
	// column. The single-process pipeline runs the body as written.
	case errors.Is(err, dagplan.ErrLateralIdentityDistributed):
		return c.runLateralIdentityLocal
	// And an OUTER join whose filter reads a derived table's computed column (#1678
	// line j): the re-spelled filter is right, but an empty partition shapes
	// its side from a declared schema that disagrees with the stream and the
	// gather refuses the files (ADR-0010). The single-process join holds
	// whole inputs.
	case errors.Is(err, dagplan.ErrOuterJoinDerivedFilterDistributed):
		return c.runOuterJoinDerivedFilterLocal
	}
	return nil
}
