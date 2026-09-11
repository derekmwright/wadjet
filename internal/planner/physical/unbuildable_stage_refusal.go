package physical

import (
	"errors"
	"fmt"
)

// ErrUnbuildableStageDistributed hands a finished plan with an unreadable stage
// to Coordinator.ExecuteSQL's local pipeline, sacrificing distribution.
// Check the final stage list, not a SQL/node shape: dispatcher inputs require
// a dependency or scan files (including dispatch-resolved relations).
// This covers dual stages (#806) and stranded scalar-CTE dependencies (#812).
// It does not repair substituteScalarDependencies; that needs to attach the
// CTE's stages. The gate asserts the routing counter as well as rows.
// See docs/internals/unbuildable-stage-distributed-handoff.md for the design.
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
