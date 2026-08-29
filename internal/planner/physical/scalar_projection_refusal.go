package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrScalarSubqueryProjectionDistributed marks a plan the stage DAG refuses
// because a SELECT-LIST item contains a subquery.
//
// The DAG lowers a scalar subquery in a PREDICATE: walkStages replaces it
// with a `:scalar_N` placeholder, emits a producer stage for it, records the
// edge in Stage.ScalarDependencies, and the coordinator substitutes the
// producer's value into the filter text before dispatch
// (resolveFilterSubqueries → emitScalarProducerStages →
// substituteScalarDependencies). There is no such machinery for a
// PROJECTION: attachScanSelectProjections attaches the SELECT list verbatim,
// and the worker's expression compiler has no SubqueryRunner, so every task
// failed three times with
//
//	compile projection "(SELECT MAX(v) FROM c)": subqueries require a SubqueryRunner
//
// for a query PostgreSQL answers and the single-process pipeline answers
// (#659). Loud, but the query HAS an answer and one engine in this process
// can compute it — so the planner refuses BEFORE stage generation and the
// coordinator routes it onto its local pipeline, exactly as it does for a
// correlated subquery (#359), an unstageable DISTINCT (#466) and an
// unmaterializable IN set (#524).
//
// The refusal is not CTE-specific: the same failure reproduces for a subquery
// over a base table or a dimension. What it does NOT cover is a subquery in a
// WHERE or a HAVING, which the deferral machinery above really does lower —
// those keep running on the DAG.
var ErrScalarSubqueryProjectionDistributed = errors.New(
	"scalar subquery in a SELECT-list item has no distributed lowering")

// refuseScalarSubqueryProjections returns ErrScalarSubqueryProjectionDistributed
// when any Project in the plan carries a subquery in one of its items.
func refuseScalarSubqueryProjections(root *logical.Node) error {
	var found error
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil || found != nil {
			return
		}
		if n.Type == logical.NodeProject {
			for i := range n.Projections {
				p := &n.Projections[i]
				if p.ASTExpr == nil {
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
