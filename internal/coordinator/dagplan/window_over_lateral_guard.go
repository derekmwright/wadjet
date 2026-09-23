// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrWindowOverLateralDistributed hands a plan whose WINDOW sits above a
// dependent (LATERAL) join back to the coordinator, which runs it on the
// single-process pipeline (Coordinator.runWindowOverLateralLocal).
//
// MEASURED (arc LT round 2, B4, ADR-0026 §8j's LATERAL-producer residue): a
// window keyed on the OUTER relation over a lateral join — `ROW_NUMBER() OVER
// (PARTITION BY o.id ORDER BY s.m)` above `lat_ord o JOIN LATERAL (…) s` —
// answers `1,1,1 | 1,1,2 | 2,2,1 | 2,2,2` on the three DAG arms for
// PostgreSQL's `1,1,1 | 1,2,2 | 2,3,1 | 2,4,2`, and `SUM(s.m) OVER (PARTITION
// BY o.customer)` answers NULL, while the single-process arms answer
// PostgreSQL's rows; the bounded spelling refused there before arc LT and its
// unbounded twin was pinned wrong. A decorrelated body's Project emits no
// stage, so the DAG's join publishes the body's inner-scan spelling and the
// window's key binds the outer occurrence. Until the LATERAL producer carries
// its identity through the stage plan (filed `distributed`), the shape runs
// single-process, which is PostgreSQL's answer.
var ErrWindowOverLateralDistributed = errors.New(
	"a window above a LATERAL join is not evaluable on the distributed path")

// refuseWindowOverDependentJoin refuses a logical plan in which a Window node
// has a dependent join anywhere below it.
func refuseWindowOverDependentJoin(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeWindow && hasDependentJoinBelow(n) {
		return fmt.Errorf("%w (window over %d expression(s))", ErrWindowOverLateralDistributed, len(n.WindowExprs))
	}
	for _, c := range n.Children {
		if err := refuseWindowOverDependentJoin(c); err != nil {
			return err
		}
	}
	return nil
}

func hasDependentJoinBelow(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if dependentJoinNode(n) {
		return true
	}
	for _, c := range n.Children {
		if hasDependentJoinBelow(c) {
			return true
		}
	}
	return false
}
