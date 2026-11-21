// SPDX-License-Identifier: MIT

package logical

import "strings"

// decorrelatedScalarSlots is the set of `__scalar_N` slots a subtree's
// decorrelated scalar joins publish: the output of the Aggregate each such
// LEFT join builds against, lowercased.
//
// pushColumnNeeds partitions a join's needs by what each side publishes, and
// an aggregate's outputs are deliberately not claimed there
// (collectMintedNames). One decorrelated scalar subquery never meets that
// partition with its own slot: the slot is the build side of the topmost
// join. Two in one Filter stack two LEFT joins, and the upper join's
// partition sent the LOWER join's slot to neither side; the lower join's
// NeededColumns then lacked it, its probe OutputFilter dropped the column,
// and the Filter's reference to it read NULL on the single-process path —
// `o.p = (SELECT max(c) …) AND 0 <= (SELECT max(c) …)` kept no row where
// PostgreSQL 17.11 keeps the matched one (#1701). This set is consulted only
// for a need neither side claims, so a need that was routed before is routed
// as it was.
//
// The walk stops at a Project or an Aggregate: a slot below one reaches the
// join only if that node republishes it, which collectMintedNames already
// claims. A semi or anti join publishes its probe side only.
func decorrelatedScalarSlots(n *Node) map[string]bool {
	var out map[string]bool
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil {
			return
		}
		switch cur.Type {
		case NodeProject, NodeAggregate:
			return
		case NodeJoin:
			if len(cur.Children) == 2 {
				jt := strings.ToLower(cur.JoinType)
				if jt == "semi" || jt == "anti" {
					walk(cur.Children[0])
					return
				}
				if agg := cur.Children[1]; cur.ScalarDecorrelated && agg != nil && agg.Type == NodeAggregate {
					for _, a := range agg.AggExprs {
						if a.OutputCol == "" {
							continue
						}
						if out == nil {
							out = map[string]bool{}
						}
						out[strings.ToLower(a.OutputCol)] = true
					}
				}
			}
		}
		for _, child := range cur.Children {
			walk(child)
		}
	}
	walk(n)
	return out
}
