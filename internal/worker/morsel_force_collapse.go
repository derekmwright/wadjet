package worker

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Forced morsel collapse is TEST ONLY: pressure is a condition, not a reliable
// gate trigger (ADR-0027; #788, #564).
// The Nth stop check of every parallel breaker takes the SAME collapsed latch,
// clone merge and serial continuation as real pressure.
// Read WADJET_TEST_FORCE_MORSEL_COLLAPSE_EVERY once or set it from Go;
// production never arms it. Unset costs one atomic load per consumed morsel.
// TestExecuteFragment_MorselParallel_AggCollapseUnresolvedPrimary must observe
// an actual collapse, not merely successful rows.
// See docs/internals/worker-forced-morsel-collapse-gate.md for the design.
var forceMorselCollapseEvery atomic.Int64

func init() {
	if v := os.Getenv("WADJET_TEST_FORCE_MORSEL_COLLAPSE_EVERY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			forceMorselCollapseEvery.Store(n)
		}
	}
}

// ForceMorselCollapseEvery arms (n > 0) or disarms (n <= 0) forced collapse for
// every morsel-parallel breaker in this process, returning the previous setting
// so a test can restore it. TEST ONLY.
func ForceMorselCollapseEvery(n int64) int64 {
	return forceMorselCollapseEvery.Swap(n)
}

// ForcedMorselCollapses counts collapses taken because of the knob rather than
// because of memory pressure. A gate asserts it moved: a forcing knob that
// silently stopped engaging turns the gate it arms into a no-op.
var ForcedMorselCollapses atomic.Int64

// forcedCollapseDue reports whether this stop check is the Nth since the knob
// was armed, and therefore owes a collapse. seq is the caller's own per-breaker
// counter, so two concurrent fragments do not share a phase.
func forcedCollapseDue(seq *atomic.Int64) bool {
	n := forceMorselCollapseEvery.Load()
	if n <= 0 {
		return false
	}
	if seq.Add(1)%n != 0 {
		return false
	}
	ForcedMorselCollapses.Add(1)
	return true
}
