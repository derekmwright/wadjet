package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Deterministic drain forcing — TEST ONLY.
//
// Every spill defect in a HashAggregate is condition-triggered: it needs a
// drain to land on a particular batch (the one that migrated the key path, the
// one that carried a NULL key, the one whose group is wholly inside the drained
// run). Under a real memory budget WHICH batch drains is decided by tracker
// pressure, so a gate written against the budget alone reproduces the defect
// some of the time and passes for the wrong reason the rest of it — five
// replications of the 512 KiB arm of #782's DECIMAL twin produced the wrong
// answer twice.
//
// ForceAggDrainEvery makes the drain deterministic: with N set, every Nth
// HashAggregate.Consume takes the same drain branch memory pressure would have
// taken, and the drain-productivity gate (#325) is bypassed for those drains
// because there is no pressure for it to measure. The aggregate still needs a
// spill directory — a forced drain writes real run files through the real
// writer, so what a gate observes is the production path, not a simulation.
//
// It is read from WADJET_TEST_FORCE_AGG_DRAIN_EVERY once per process so an
// end-to-end gate at the SQL layer can arm it, and settable from Go for exec
// level gates (ForceAggDrainEvery / ResetForcedDrains). It is never set on any
// production path: the only cost when unset is one relaxed atomic load per
// Consume, next to a batch of work.
var forceAggDrainEvery atomic.Int64

func init() {
	if v := os.Getenv("WADJET_TEST_FORCE_AGG_DRAIN_EVERY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			forceAggDrainEvery.Store(n)
		}
	}
}

// ForceAggDrainEvery arms (n > 0) or disarms (n <= 0) forced draining for every
// HashAggregate in this process, and returns the previous setting so a test can
// restore it. TEST ONLY.
func ForceAggDrainEvery(n int64) int64 {
	return forceAggDrainEvery.Swap(n)
}

// ForcedAggDrains counts drains taken because of the forcing knob rather than
// because of memory pressure. A gate asserts it moved: a forcing knob that
// silently failed to engage turns the gate it arms into a no-op.
var ForcedAggDrains atomic.Int64

// forcedDrainDue reports whether this Consume is the Nth since the knob was
// armed, and therefore owes a drain. Caller holds h.mu.
func (h *HashAggregate) forcedDrainDue() bool {
	n := forceAggDrainEvery.Load()
	if n <= 0 {
		return false
	}
	if h.Spill == nil || h.Spill.SpillDir() == "" {
		return false
	}
	h.forcedDrainSeq++
	return h.forcedDrainSeq%n == 0
}
