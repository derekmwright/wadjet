// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// RefuseContestedLiftedRefs checks lifted columns after annotation supplies
// the enclosing schema. A shared name would bind the wrong output column
// on the single-process pipeline, so it refuses every join spelling there.
// With outerJoinsOnly, the DAG retains its supported inner/comma spellings
// and refuses outer joins. See ADR-0021 §1s and ADR-0026 §8j.
func RefuseContestedLiftedRefs(n *Node, outerJoinsOnly bool) error {
	if n == nil {
		return nil
	}
	if n.Type == NodeJoin && len(n.Children) == 2 && len(n.StarLiftedRefCols) > 0 &&
		(!outerJoinsOnly || !lateralDualInnerJoin(n.JoinType)) {
		probe, build := n.Children[0], n.Children[1]
		if build != nil && !build.LateralSubtree && probe != nil && probe.LateralSubtree {
			probe, build = build, probe
		}
		outer := map[string]bool{}
		for _, e := range emittedColumns(probe) {
			outer[strings.ToLower(stripQualifier(e.name))] = true
		}
		for _, slot := range n.StarLiftedRefCols {
			bare := strings.ToLower(stripQualifier(strings.TrimSpace(slot)))
			if outer[bare] {
				return sqlerr.New("0A000",
					"LATERAL body's correlated predicate names the inner column %s, which the "+
						"enclosing relation also publishes: the predicate is evaluated over the "+
						"join's output, where the two columns cannot be told apart, and PostgreSQL "+
						"evaluates the body per outer row, which this engine does not do for this "+
						"shape. Correlate on an equality, or alias the body's column and compare "+
						"the alias in the enclosing WHERE",
					sqlerr.Quote(bare))
			}
		}
	}
	for _, c := range n.Children {
		if err := RefuseContestedLiftedRefs(c, outerJoinsOnly); err != nil {
			return err
		}
	}
	return nil
}

// RefuseDeclinedLiftedRefs is the single-process pipeline's refusal for a
// lateral whose lifted predicate declined under a bare enclosing star
// (Node.LiftedRefDeclinedUnderStar): that pipeline evaluates the predicate
// above the join over a column the body did not publish and answered a
// NULL-padded row per outer row for PostgreSQL's rows (arc L1 round 4, arc LT
// round 2). Called from physical.Plan only; the stage DAG answers this shape.
func RefuseDeclinedLiftedRefs(n *Node) error {
	if n == nil {
		return nil
	}
	if n.LiftedRefDeclinedUnderStar {
		return sqlerr.New("0A000",
			"LATERAL body's correlated predicate is not an equality on an inner column and the "+
				"enclosing query writes a star over this join: the predicate is evaluated over the "+
				"body's OUTPUT, which would have to publish the column it names, and a bare star "+
				"would publish that column too. PostgreSQL evaluates the body per outer row, which "+
				"this engine does not do for this shape on the single-process path. Name the columns "+
				"instead of a star, or correlate on an equality")
	}
	for _, c := range n.Children {
		if err := RefuseDeclinedLiftedRefs(c); err != nil {
			return err
		}
	}
	return nil
}
