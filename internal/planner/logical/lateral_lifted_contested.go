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
func RefuseContestedLiftedRefs(n *Node) error {
	if n == nil {
		return nil
	}
	if n.Type == NodeJoin && len(n.Children) == 2 && len(n.StarLiftedRefCols) > 0 {
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
		if err := RefuseContestedLiftedRefs(c); err != nil {
			return err
		}
	}
	return nil
}
