package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrGroupingSetsDistributed marks a plan the stage DAG refuses because it
// carries GROUPING SETS / ROLLUP / CUBE, which the DAG has no representation
// for at all.
//
// `logical.buildGroupingSets` emits ONE Aggregate whose GroupBy is the UNION of
// every set's terms, with the sets themselves as node metadata. The
// single-process builder reads that metadata into `exec.HashAggregate.
// GroupingSets`, which inserts each row once per set under a set-prefixed key
// and leaves the out-of-set columns NULL. `walkStages` reads it nowhere:
// `Stage` has no field for it, `distributed.OpSpec` has no wire tag for it, and
// no worker sets `hashAgg.GroupingSets`. The information is destroyed where the
// stage's `GroupByCols` are copied, and is unreconstructible below that.
//
// So the DAG ran the union of the terms as a PLAIN GROUP BY and returned it as
// the answer. Measured against PostgreSQL 17 over `collslot`:
//
//	GROUP BY GROUPING SETS ((g), (h))  PG 7 rows, DAG 12 — the CROSS PRODUCT
//	GROUP BY ROLLUP (g)                PG 4 rows, DAG 3 — no grand total
//
// Silently, and for PLAIN column keys, which is wider than the filing said.
//
// Refusing is the #308 position, and this is a HANDOFF rather than the query's
// outcome: Coordinator.ExecuteSQL matches this error and answers on the
// coordinator-local single-process pipeline, exactly as it does for
// ErrDistinctDistributed — the same class of defect one construct over, where
// walkStages drops a construct it has no stage for.
//
// The refusal is deliberately UNCONDITIONAL rather than "only where the sets
// differ from the union". A single-set GROUPING SETS is a plain GROUP BY and
// would be safe to run on the DAG, but a predicate that has to be exactly right
// about which shapes are equivalent is the kind that drifts; and no such query
// is written by hand. When a Stage learns to carry the sets this refusal goes
// away with it.
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
	for _, child := range n.Children {
		if err := refuseGroupingSets(child); err != nil {
			return err
		}
	}
	return nil
}
