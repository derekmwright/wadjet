// SPDX-License-Identifier: MIT

package expr

import "sync"

// The planner's interval lineage (planner/physical/validate_interval_lineage.go,
// #1680c) forwards an interval slot's declaration across each relational
// boundary. A TEST may turn the forwarding off at one boundary to prove
// that the lineage tests and the corpus's negative cells depend on it (the
// negative control). The seam lives here, beside the timestamp-difference
// seam, so the AGPL packages' tests reach it without widening what they see
// of the MIT planner (tools/licensecheck holds that surface to a budget).

// The boundaries a test may disable.
const (
	LineageDerived  = "derived"  // a derived table's (and a join's) published column
	LineageCTE      = "cte"      // a CTE reference's column
	LineageSubquery = "subquery" // a scalar subquery's slot
	LineageSetOp    = "setop"    // a set operation's reconciled position
	LineageStar     = "star"     // a star's published columns
	LineageAll      = "all"      // every boundary
)

var (
	lineageMu  sync.RWMutex
	lineageOff = map[string]bool{}
)

// DisableIntervalLineageForTest turns lineage forwarding off at boundary
// (or at every boundary, LineageAll) until restore is called.
func DisableIntervalLineageForTest(boundary string) (restore func()) {
	lineageMu.Lock()
	prev := lineageOff[boundary]
	lineageOff[boundary] = true
	lineageMu.Unlock()
	return func() {
		lineageMu.Lock()
		lineageOff[boundary] = prev
		lineageMu.Unlock()
	}
}

// IntervalLineageDisabledAt reports whether a test has turned forwarding off
// at boundary.
func IntervalLineageDisabledAt(boundary string) bool {
	lineageMu.RLock()
	defer lineageMu.RUnlock()
	return lineageOff[boundary] || lineageOff[LineageAll]
}
