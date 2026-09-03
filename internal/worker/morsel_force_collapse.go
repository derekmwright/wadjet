package worker

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Deterministic morsel-collapse forcing — TEST ONLY.
//
// The morsel-parallel breaker collapses to serial when the shared spill
// manager says the fragment is under memory pressure
// (`e.sharedSpill.ShouldSpillFor(memory.SpillCheap)` in runBreakerConsumeParallel).
// That is a CONDITION, not a plan shape, and it is decided partly by a
// process-wide Go-heap gauge whose verdict is cached for 100ms across the whole
// process (memory.heapPressureExceeded) and partly by a race: the stop check
// runs only after a morsel has been consumed, so a run whose four consumers
// drain the whole fixture before the tracker crosses its 40% mark records ZERO
// collapses however hard the machine is working.
//
// A gate whose trigger is a condition cannot be relied on to fire (ADR-0027,
// and the same lesson #788 taught four investigation rounds running).
// TestExecuteFragment_MorselParallel_AggCollapseUnresolvedPrimary asserts what
// happens to aggregate state ACROSS a collapse, and it opens by requiring at
// least one collapse to have happened — a precondition that was failing under a
// loaded parallel suite while nothing was wrong (#564).
//
// With the knob armed, the Nth stop check of every morsel-parallel breaker
// collapses regardless of pressure, and it takes exactly the branch pressure
// would have taken: the same `collapsed` latch, the same serial continuation,
// the same clone merge. A gate observes the production path, not a simulation.
//
// It is read once from WADJET_TEST_FORCE_MORSEL_COLLAPSE_EVERY so an
// end-to-end gate can arm it, and is settable from Go for worker-level gates.
// It is never set on any production path: unset, the cost is one relaxed atomic
// load per consumed morsel, beside a batch of work.
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
