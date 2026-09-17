// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// syntheticSlotRefs lists the hidden slots (`__win_N`, `__agg_N`) an
// expression reads, in first-seen order, and reports whether the walk
// understood the WHOLE expression.
//
// It shares `physical.RewriteColRefs` rather than growing a fourth private walk, for
// the reason ADR-0025 gives about the three that existed: a walk that does not
// descend into a node kind is not a no-op, it silently reports fewer
// references than the expression has. Here that would be a pass-through list
// missing a slot the gather is about to read, which answers NULL — so the
// `complete` flag is a REFUSAL condition at the caller, not advice.
func syntheticSlotRefs(n plansql.Node) (slots []string, complete bool) {
	seen := map[string]bool{}
	_, _, complete = localPlanFacts.RewriteColRefs(n, func(c *plansql.ColRef) (plansql.Node, bool) {
		name := c.Column
		if !strings.HasPrefix(name, string(plansql.SlotWindowOutput)) &&
			!strings.HasPrefix(name, string(plansql.SlotNestedAgg)) {
			return nil, false
		}
		if !seen[strings.ToLower(name)] {
			seen[strings.ToLower(name)] = true
			slots = append(slots, name)
		}
		return nil, false
	})
	return slots, complete
}
