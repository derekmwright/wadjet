// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// projectionPublishingName is physical.PlanContext.ProjectionForName widened to an item the SELECT
// list wrote with NO ALIAS, which publishes under its own bare column name:
// `SELECT u.g, u.x FROM (…) u` publishes `g` and `x`.
//
// Only the CLASS walk asks it, and the split is deliberate rather than a
// second copy of the rule. The other walks resolve a NAME to its SOURCE, and
// for an unaliased qualified item those two are the same string — `u.x`
// resolves to `u.x`, a self-rename that stops the walk one Project short of
// the answer. Measured: widening physical.PlanContext.ProjectionForName itself made
// `SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x … ) u ORDER BY u.x`
// refuse its whole plan, because the sort key stopped resolving to the column
// the aggregate emits. The CLASS question has no such fixpoint: it wants the
// item, and an unaliased item is one.
//
// Without it, `renameIsAggregateOutput` answered "not an aggregate output" for
// a name whose value IS one two derived tables above the aggregate, and the
// gather's duplicate-name pairing then took the first column of the name —
// the group KEY (#785, ADR-0026 §3a).
func projectionPublishingName(projs []logical.Projection, name, bare string) *logical.Projection {
	if p := localPlanFacts.ProjectionForName(projs, name, bare); p != nil {
		return p
	}
	for _, want := range []string{name, bare} {
		if want == "" {
			continue
		}
		for i := range projs {
			if projs[i].Alias != "" || projs[i].Column == "" {
				continue
			}
			if strings.EqualFold(projs[i].Column, want) {
				return &projs[i]
			}
		}
	}
	return nil
}
