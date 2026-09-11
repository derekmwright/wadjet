package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrGroupingSetsDistributed unconditionally refuses GROUPING SETS/ROLLUP/CUBE,
// including a single set: Stage/OpSpec carry no sets and workers cannot restore
// metadata lost when GroupByCols takes the union of terms. The local operator
// inserts each row per set, with set-prefixed keys and NULL out-of-set columns.
// Coordinator.ExecuteSQL hands off locally like ErrDistinctDistributed (#308).
// Remove the refusal when Stage carries sets, not via an equivalence heuristic.
// See docs/internals/distributed-grouping-sets-refusal.md for the design.
var ErrGroupingSetsDistributed = errors.New(
	"GROUPING SETS / ROLLUP / CUBE has no distributed stage")

// refuseGroupingSets returns a typed refusal for the first Aggregate carrying
// grouping-set metadata anywhere in the plan.
func refuseGroupingSets(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeAggregate && len(n.GroupingSets) > 0 {
		return fmt.Errorf("%w: an aggregate over %d grouping set(s) of %v"+
			" would run on the stage DAG as a plain GROUP BY over their union,"+
			" which answers different rows",
			ErrGroupingSetsDistributed, len(n.GroupingSets), n.GroupBy)
	}
	// GROUPING(...) rides on the sets, so the clause above catches every plan
	// the builder produces. This second arm is the BOUNDARY claim made
	// checkable: a GroupingCalls-bearing aggregate that somehow reached the
	// DAG without its sets would answer a bitmask nothing computed — the
	// silent-wrong-number outcome this issue exists to avoid — so it refuses
	// on its own terms rather than relying on the first arm's invariant
	// (#804).
	if n.Type == logical.NodeAggregate && len(n.GroupingCalls) > 0 {
		return fmt.Errorf("%w: an aggregate carrying %d GROUPING() call(s) has"+
			" no distributed stage that can compute the bitmask",
			ErrGroupingSetsDistributed, len(n.GroupingCalls))
	}
	for _, child := range n.Children {
		if err := refuseGroupingSets(child); err != nil {
			return err
		}
	}
	return nil
}
