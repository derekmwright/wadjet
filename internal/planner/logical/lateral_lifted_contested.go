// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A LIFTED PREDICATE'S COLUMN THAT THE ENCLOSING RELATION ALSO PUBLISHES IS
// REFUSED AFTER ANNOTATION — #1130, ADR-0021 §1s.
//
// `publishLiftedRefs` materializes, under its OWN name, every inner column a
// non-equality correlated predicate names, so the predicate can be evaluated
// above the join. Where the ENCLOSING relation publishes that name too the
// join's output carries two columns called `id`, and the lifted filter's
// `i.id < o.id` binds the outer one on the single-process arms — a NULL pad
// per outer row for PostgreSQL's rows (#1130). The lowering declines that
// shape when it can SEE the collision, but for a BASE outer relation the
// column list is not known at build time: the catalog annotator supplies it
// later, in Optimize. So the check runs here, on the annotated plan, from
// the same two doors every other post-optimize refusal takes
// (physical.Plan and dagplan.Plan), which is what makes it one disposition
// on every arm — the DAG arms answer the shape today, the single arms do
// not, and a refusal is a property of the plan.
//
// The mechanism that would close it instead of refusing it is ADR-0026 §8j's
// corollary 2 for the LATERAL producer: the filter's reference translated to
// the body's carrier inside the occurrence the qualifier names.
//
// outerJoinsOnly is the stage DAG's door (round-2 review, B5): the DAG
// evaluates an INNER or comma lateral's lifted predicate at the join off the
// scan's own stream and answers PostgreSQL's rows (both fixtures), while its
// LEFT spelling there is not one answer — it padded every outer row NULL on
// the shuffled shape in one run and routed to this refusal in the next
// (`r2_gates3.log` / `r2_gates4.log`; right on arc L1's fixture, wrong on
// arc LT's) — so the DAG refuses the OUTER join uniformly, and the
// single-process pipeline, wrong for every spelling, refuses them all.
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
