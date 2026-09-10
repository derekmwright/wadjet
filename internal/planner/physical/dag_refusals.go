// This file holds dag refusals for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// PlanDistributed generates a stage DAG for distributed execution.
// Returns stages with dependency ordering suitable for coordinator dispatch.
// refuseUnexpandedStarBesideItems refuses a QUALIFIED star in a SELECT list
// that could not be expanded.
//
// Every consumer below resolves an output column by name, and `d.*` is not
// one: the single-process arms failed with `column "d.*" does not exist in the
// input schema` and the DAG published a column whose NAME and VALUE were both
// the string `*`. One sentence on every arm, and the shapes that CAN be
// expanded — a base table, a derived table or a CTE whose own SELECT list
// names its columns — are expanded before this runs
// (logical.ExpandStarProjections).
//
// It used to require a SECOND select item, because a star ALONE built no
// Project at all and so could not reach it. One does now (#979), and without
// this the LATERAL's own star — the shape the expansion deliberately declines —
// escaped to the executor's generic `42000 operator execute: column "s.*" does
// not exist in the input schema` instead of the planner's one sentence, which
// is what `docs/sql-reference.md` and ADR-0012 describe.
//
// A node carrying a DEFERRED column-alias list is left alone: its star is the
// wrapper `deferColumnAliasesOverStar` made, and
// `RefuseUnappliedColumnAliasLists` refuses it with a sentence about the LIST,
// which is the more specific answer for that shape (#958).
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
