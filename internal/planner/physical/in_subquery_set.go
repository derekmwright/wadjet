// SPDX-License-Identifier: MIT

package physical

import (
	"os"
	"strconv"
)

// When semi/anti lowering declines LIMIT/OFFSET, ungrouped aggregates or
// computed items (#482, #516, #524), resolveSubqueryAST materializes an
// UNCORRELATED IN subquery once on the coordinator as a literal set. Execute
// it AS WRITTEN, preserving LIMIT/OFFSET/ORDER BY and NOT IN's three-valued
// rule (#370, #507). Cap at InlinedInSetRowCap; require exact text round trips
// for every value (integer/float/string/bool/NULL), never approximate.
// Crossing either bound is a typed refusal routed to local execution, like
// correlated subqueries and unstageable DISTINCT (#359, #466).
// See docs/internals/in-subquery-literal-sets.md for the design.

// inlinedInSetRowCap bounds the set an IN-subquery may be materialized into.
//
// The number is a plan-text budget, not a memory one: every row becomes a
// literal in a filter expression that is serialized into each task, so the
// cost is paid per task and shows up in dispatch size. Ten thousand keeps a
// bounded subquery (the #482 LIMIT shapes this exists for) comfortably inside
// it while refusing an unbounded one early enough to route local before the
// coordinator has read a large result into memory.
//
// WADJET_IN_SET_MAX overrides it; 0 disables materialization entirely, which
// makes every declined IN-subquery take the local route (the kill switch for
// this path). Read per call rather than at init so a test can exercise the
// refusal without a fixture large enough to cross the real bound.
const defaultInlinedInSetRows = 10000

// MaxInlinedInSetRows is the bound, for the doors that materialize a subquery
// result WITHOUT going through this file's inlining. The DML doors are those:
// they hand `expr.InSubquery` a runner and it builds its membership map from
// whatever the runner returns, so the bound this package applies to the
// planner's own inlining reaches nothing there and a write statement could
// pull an unbounded relation into coordinator memory.
//
// The number means the same thing at both sites — how many rows a subquery
// result may become a set of — even though what it protects differs (plan
// text here, a hash map there). One knob, one meaning; two would be two.
func MaxInlinedInSetRows() int { return inlinedInSetRowCap() }

// inlinedInSetRowCap is the resolved cap. Both planners read it — the local
// one to bound the plan text it writes, the distributed one to bound the hash
// map a fragment builds — which is why it is exported from the MIT side
// rather than moved (LS review round 2, P2).
func inlinedInSetRowCap() int {
	if v := os.Getenv("WADJET_IN_SET_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return defaultInlinedInSetRows
}
