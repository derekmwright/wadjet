// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import "github.com/derekmwright/wadjet/internal/planner/logical"

// limitPushdownSafe reports whether a LIMIT may be applied independently by
// each task under it.
//
// It may when every node between the LIMIT and its scans passes rows through
// one at a time: Project and Filter qualify (a filtered task simply reaches n
// later, or never), and a scan is the base case. Anything that derives rows
// from more than one input row — join, aggregate, distinct, sort, window, set
// operation — does not: bounding its INPUT changes its OUTPUT, which would
// silently produce wrong answers rather than merely fewer rows.
//
// Multiple scans under a UNION ALL are fine: each bounds itself, and the
// coordinator trims the union to n.
func limitPushdownSafe(node *logical.Node) bool {
	if node == nil {
		return false
	}
	sawScan := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return false
		}
		switch n.Type {
		case logical.NodeScan:
			// A table function's row count is not bounded by its input, but
			// stopping early still yields a prefix of what it would produce.
			sawScan = true
			return true
		case logical.NodeProject, logical.NodeFilter, logical.NodeLimit:
			// A nested LIMIT is at most as permissive as this one.
		default:
			return false
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return false
	}
	return sawScan
}
