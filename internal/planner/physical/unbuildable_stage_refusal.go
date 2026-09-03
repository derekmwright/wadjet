package physical

import (
	"errors"
	"fmt"
)

// ErrUnbuildableStageDistributed marks a plan the stage DAG refuses because
// one of its stages has nothing to read.
//
// `buildTaskInputsForStage`'s default arm needs a stage to name either a
// DEPENDENCY or its own SCAN FILES. A stage with neither cannot be dispatched
// and the query fails with
//
//	stage sort-4 has no dependencies and no ScanFiles
//
// which is an INTERNAL message about the planner's own output, reaching a
// client with no SQLSTATE, for a query the single-process pipeline answers.
//
// #806 fixed one producer of that shape — a `dual` stage for a table-less
// SELECT — by refusing the PLAN and routing local. This is the same refusal
// asked of the FINISHED stage list rather than of one node kind, because the
// second producer is not a node kind at all: a scalar subquery over a CTE in a
// WHERE clause (#812). `substituteScalarDependencies` rewrites the predicate's
// dependency, the CTE's own stages are emitted for the outer query, and the
// stage the substitution leaves behind names neither — measured:
//
//	WITH c AS (SELECT id, d92 AS v FROM zzp)
//	SELECT c.id FROM c WHERE c.id < (SELECT COUNT(*) FROM c)
//
// fails on both DAG arms while the BASE-TABLE and DERIVED-TABLE spellings of
// the same query answer. Asking the question of the stage list catches both
// producers and any third, which is the point: the condition is a property of
// the plan the dispatcher is handed, not of the SQL that produced it.
//
// The refusal is a HANDOFF and not the query's outcome. `Coordinator.ExecuteSQL`
// matches it and answers on the coordinator-local single-process pipeline,
// exactly as it does for the seven other constructs the DAG has no stage for.
// It costs the query its distribution, which is a performance cliff and not a
// wrong answer — the same trade every refusal here makes — and it is strictly
// better than the failure it replaces.
//
// It is deliberately NOT a repair of `substituteScalarDependencies`. Making
// that pass attach the CTE's stages is the real fix and it is #812's own; this
// makes the query ANSWER in the meantime, and the gate asserts the ROUTING
// COUNTER beside the rows so the day the pass is repaired is visible.
var ErrUnbuildableStageDistributed = errors.New(
	"a stage has no dependencies and no scan files")

// refuseUnbuildableStages returns a typed refusal for the first stage the
// dispatcher could not build task inputs for.
//
// A SCAN stage is exempt: it reads its own table and its files are resolved at
// dispatch from the catalog rather than carried here. Everything else must
// name a dependency.
func refuseUnbuildableStages(stages []Stage) error {
	for i := range stages {
		s := &stages[i]
		if s.Type == StageScan || len(s.Dependencies) > 0 || len(s.ScanFiles) > 0 {
			continue
		}
		if s.TableName != "" {
			// A stage that names its own relation resolves files at dispatch.
			continue
		}
		return fmt.Errorf("%w: stage %s (%s) names neither a dependency nor a"+
			" table, so the dispatcher cannot build task inputs for it",
			ErrUnbuildableStageDistributed, s.ID, s.Type)
	}
	return nil
}
