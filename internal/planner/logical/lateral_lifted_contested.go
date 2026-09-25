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
		// A lateral WITH an alias has its lifted predicate spelled through
		// that alias (`s.id <> o.id`, qualifyLiftedRefsByLateralAlias), and
		// the join emits the body's colliding column under exactly that
		// name — so on the single-process pipeline the two columns ARE told
		// apart and there is nothing to refuse (arc JP round 3, B7: `i.k =
		// o.k AND i.id <> o.id` answers PostgreSQL's rows, JOIN, LEFT and
		// comma). The stage DAG keeps the refusal; it routes such a plan
		// single-process first (dagplan.ErrLateralIdentityDistributed).
		aliased := !outerJoinsOnly && build != nil && build.LateralSubtree && build.DerivedAlias != ""
		outer := map[string]bool{}
		for _, e := range emittedColumns(probe) {
			outer[strings.ToLower(stripQualifier(e.name))] = true
		}
		for _, slot := range n.StarLiftedRefCols {
			if aliased {
				break
			}
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
	// A BARE star left unexpanded over a lateral join publishes the join's
	// stream, and a join that EMITS a lifted slot (an expression-keyed
	// correlation, `i.k = o.k + 1`) would show it. The star is normally
	// expanded to the arms' lists, which hide it (joinArmColumns); where it
	// could not be — a list naming one column twice, say — it is refused
	// here, after expansion, rather than before it for every such star (arc
	// JP round 4, B5).
	if n.Type == NodeProject && len(n.Children) == 1 && hasBareStarItem(n) && streamEmitsLiftedSlot(n.Children[0]) {
		return sqlerr.New("0A000",
			"a bare `SELECT *` over a LATERAL whose correlated equality has an EXPRESSION on its "+
				"outer side could not be expanded into the relations' own column lists, and the "+
				"join's output carries the body's key column the equality is evaluated against. "+
				"Name the columns, or select `<lateral alias>.*` for the lateral's own list")
	}
	for _, c := range n.Children {
		if err := RefuseDeclinedLiftedRefs(c); err != nil {
			return err
		}
	}
	return nil
}

// hasBareStarItem reports whether a Project still carries an unexpanded bare
// `*` item.
func hasBareStarItem(n *Node) bool {
	for _, p := range n.Projections {
		if isStarProjection(p) && starQualifier(p) == "" {
			return true
		}
	}
	return false
}

// streamEmitsLiftedSlot reports whether the stream a bare star over n
// publishes comes from a join chain one of whose joins emits a lifted slot.
func streamEmitsLiftedSlot(n *Node) bool {
	for n != nil {
		switch n.Type {
		case NodeFilter, NodeSort, NodeLimit, NodeDistinct:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		case NodeJoin:
			if len(n.StarLiftedRefCols) > 0 {
				return true
			}
			for _, c := range n.Children {
				if streamEmitsLiftedSlot(c) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}
