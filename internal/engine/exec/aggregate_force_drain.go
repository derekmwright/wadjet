package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// TEST ONLY: every Nth HashAggregate.Consume takes the production drain branch,
// bypassing the drain-productivity gate (#325). A spill directory is still
// required: forced drains write real run files through the real writer.
// Pressure alone cannot reliably select the batch a defect needs (#782).
// Read WADJET_TEST_FORCE_AGG_DRAIN_EVERY once per process or set from Go
// (ForceAggDrainEvery / ResetForcedDrains), including SQL-level gates.
// Never set in production; unset cost is one atomic load per Consume.
// See docs/internals/aggregate-drain-forcing.md for the design.
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
