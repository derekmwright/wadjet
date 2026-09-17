// SPDX-License-Identifier: MIT

// The two unexpanded-star refusals, which BOTH planners make: the local entry
// raises them before it builds a pipeline and the stage planner before it
// emits a DAG. The file was called dag_refusals.go, which named one caller of
// two and put a DAG word in the MIT package; the refusals themselves are about
// a star the expander could not expand, not about stages (LS review round 2,
// P1/P2). Governed by ADR-0026 and ADR-0034.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseUnexpandedStarBesideItems refuses any unexpanded qualified star,
// including a star alone (#979), with the planner's common refusal sentence
// (docs/sql-reference.md; ADR-0012). logical.ExpandStarProjections runs first
// for base tables and derived tables/CTEs with known SELECT-list columns.
// Leave DeferredColumnAliases alone: deferColumnAliasesOverStar created
// that wrapper, and RefuseUnappliedColumnAliasLists provides the more
// specific refusal about its list (#958).
func refuseUnexpandedStarBesideItems(node *logical.Node) error {
	if node == nil || node.Type != logical.NodeProject || len(node.Projections) == 0 {
		return nil
	}
	if len(node.DeferredColumnAliases) > 0 {
		return nil
	}
	for _, pr := range logical.VisibleProjections(node.Projections) {
		name := strings.TrimSpace(pr.Expr)
		if name == "" {
			name = strings.TrimSpace(pr.Column)
		}
		if name != "*" && !strings.HasSuffix(name, ".*") {
			continue
		}
		return sqlerr.New("0A000",
			"column %q does not exist in the input schema: a `%s` expands only from a "+
				"relation whose column list is known — a base table, or a derived table "+
				"or CTE whose own SELECT list names its columns — and this one's is not; "+
				"name the columns",
			name, name)
	}
	return nil
}

// refuseUnexpandedStarAnywhere is refuseUnexpandedStarBesideItems over a whole
// plan, for the DISTRIBUTED entry: the stage planner does not go through
// buildProject, and the rule is the plan's, not one path's.
func refuseUnexpandedStarAnywhere(node *logical.Node) error {
	if node == nil {
		return nil
	}
	if err := refuseUnexpandedStarBesideItems(node); err != nil {
		return err
	}
	for _, child := range node.Children {
		if err := refuseUnexpandedStarAnywhere(child); err != nil {
			return err
		}
	}
	return nil
}
