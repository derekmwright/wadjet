package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrTableLessSelectDistributed hands plans containing a Dual to
// Coordinator.ExecuteSQL's local pipeline: the DAG cannot dispatch a dual stage
// with no dependencies or scan files (#806).
// The refusal routes the whole query, including mixed dual/real-scan plans such
// as SELECT 1 UNION ALL SELECT c FROM big; those lose distribution.
// Removing that cost requires a distributed single-row source, fragment builder
// and wire tag.
// See docs/internals/table-less-distributed-handoff.md for the design.
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
