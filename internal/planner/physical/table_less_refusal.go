package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrTableLessSelectDistributed marks a plan the stage DAG refuses because it
// reads from no table at all.
//
// `SELECT CONCAT('a', NULL, 'b')`, `SELECT 1`, `SELECT current_schema()` —
// every SELECT with no FROM — becomes a `logical.NodeDual`, and `walkStages`
// emits a Stage of type "dual" with `Tasks: 1`, no dependencies and no
// ScanFiles. Nothing downstream can run that: `buildTaskInputsForStage`'s
// default arm requires a dependency and fails the dispatch with
//
//	stage dual-0 has no dependencies and no ScanFiles
//
// so the query FAILS on the DAG (#806). It is not rare — pgwire's synthetic
// answers cover the introspection shapes a BI client sends, but anything past
// that list reaches the engine, and every table-less SELECT a user writes does.
//
// The refusal is a HANDOFF, not the query's outcome: `Coordinator.ExecuteSQL`
// matches this error and answers on the coordinator-local single-process
// pipeline, exactly as it does for six other constructs the DAG has no stage
// for. The `dual` stage's own comment already says "runs locally on
// coordinator"; this is what makes that true.
//
// There is nothing to distribute here — a table-less SELECT is one row — so
// routing costs nothing for the shape the issue is about. It DOES cost
// something for a plan where a dual sits beside real scans, `SELECT 1 UNION
// ALL SELECT c FROM big` being the shape: the whole query then runs in the
// coordinator's process. That is a performance cliff, not a wrong answer, and
// it is the same trade every other refusal here makes. Making the DAG execute
// a dual stage — a single-row source operator, its fragment builder and a
// wire tag — removes it; that is a feature, not this refusal.
var ErrTableLessSelectDistributed = errors.New(
	"a table-less SELECT has no distributed stage")

// refuseTableLessSelect returns a typed refusal for the first Dual node
// anywhere in the plan.
func refuseTableLessSelect(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeDual {
		return fmt.Errorf("%w: a SELECT with no FROM clause emits a `dual` stage"+
			" with no dependencies and no scan files, which the dispatcher cannot"+
			" build task inputs for",
			ErrTableLessSelectDistributed)
	}
	for _, child := range n.Children {
		if err := refuseTableLessSelect(child); err != nil {
			return err
		}
	}
	return nil
}
