package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrScalarSubqueryProjectionDistributed refuses SELECT-list subqueries that
// remain without distributed lowering (#659): workers have no SubqueryRunner.
// The coordinator answers through its local pipeline, as for correlation (#359),
// unstageable DISTINCT (#466) and unmaterializable IN sets (#524).
// The refusal applies to base tables and dimensions as well as CTEs; it excludes
// WHERE/HAVING subqueries handled by scalar producer deferral.
// See docs/internals/unlowered-scalar-projection-refusal.md for the design.
var ErrScalarSubqueryProjectionDistributed = errors.New(
	"scalar subquery in a SELECT-list item has no distributed lowering")

// refuseScalarSubqueryProjections returns ErrScalarSubqueryProjectionDistributed
// when a Project carries a subquery in an item the SELECT-list lowering did
// NOT rewrite into a producer stage.
//
// `lowered` is the set of PROJECTION NODES the lowering handled, keyed by the
// item's own address (scalar_projection_lowering.go). Keying it by the item's
// rendered TEXT was the first cut and it is a latent wrong answer: the walk
// visits EVERY Project in the plan, so two items that happen to spell the same
// expression — one in a derived table and one in the outer SELECT — shared a
// verdict, and one of them would be exempted for the other's sake. An address
// names one node.
//
// Everything else still refuses: an item the resolver's walk does not descend
// into (a CASE arm, a function argument), a correlated subquery, and every
// projection position the attach pass declines.
func refuseScalarSubqueryProjections(root *logical.Node, lowered map[*logical.Projection]bool) error {
	var found error
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil || found != nil {
			return
		}
		if n.Type == logical.NodeProject {
			for i := range n.Projections {
				p := &n.Projections[i]
				if p.ASTExpr == nil || lowered[p] {
					continue
				}
				visitExprSubqueries(p.ASTExpr, func(sql, construct string) {
					if found != nil {
						return
					}
					name := p.Alias
					if name == "" {
						name = p.Expr
					}
					found = fmt.Errorf("%w: SELECT-list item %q contains %s"+
						" (the DAG's scalar-producer machinery covers predicates only);"+
						" the coordinator runs this query single-process",
						ErrScalarSubqueryProjectionDistributed, name, construct)
				})
				if found != nil {
					return
				}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	return found
}
