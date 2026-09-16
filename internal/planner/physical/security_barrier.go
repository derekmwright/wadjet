// SPDX-License-Identifier: MIT

// Whether a subtree carries a security barrier. It was called
// security_stage_order.go, from when the answer ordered stage emission; the
// question it answers is about the logical subtree (ADR-0037).
package physical

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// subtreeHasSecurityBarrier reports whether a security projection stands
// anywhere below this node. It is the guard on scan-level filter pushdown: a
// predicate above a barrier must not be evaluated against the file.
func subtreeHasSecurityBarrier(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeProject && n.SecurityBarrier {
		return true
	}
	for _, c := range n.Children {
		if subtreeHasSecurityBarrier(c) {
			return true
		}
	}
	return false
}
