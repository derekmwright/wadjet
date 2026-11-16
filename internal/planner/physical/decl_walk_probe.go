// SPDX-License-Identifier: MIT

package physical

import "sync/atomic"

// declWalkProbe, when set, is told every uncached computation of a
// declaration walk (a memo miss: the walk's real unit of work, since a hit is
// one map lookup and a miss is the node's whole declaration). It is the seam
// the planning-bound gate reads (#1688): a wall-clock ratio of the walk sat
// inside its own noise, a count of the walk's misses does not.
var declWalkProbe atomic.Pointer[func(kind string)]

// SetDeclWalkProbe installs f as the declaration-walk probe and returns the
// function that restores the previous one. A nil f removes the probe. Tests
// only: the probe sees every walk of the process, whichever goroutine runs
// it. Unset, a miss costs one atomic load.
func SetDeclWalkProbe(f func(kind string)) (restore func()) {
	var next *func(string)
	if f != nil {
		next = &f
	}
	prev := declWalkProbe.Swap(next)
	return func() { declWalkProbe.Store(prev) }
}
