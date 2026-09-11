package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// TEST ONLY: every Nth Window.Consume with buffered batches writes a real run,
// bypassing pressure and minSortRunBytes. Read
// WADJET_TEST_FORCE_WINDOW_SPILL_EVERY once per process or set from Go;
// never set in production; unset cost is one atomic load per Consume.
// Only exec gates driving ONE operator until #864 closes and the guard is
// understood: forcing bypasses TrackingOnlyView's ShouldSpillFor protection
// against orphaned clone runs. Window is not in wireCloneSinkSpill's switch,
// but that does not authorize whole-query forcing.
// See docs/internals/window-spill-forcing-boundary.md for the design.
var forceWindowSpillEvery atomic.Int64

func init() {
	if v := os.Getenv("WADJET_TEST_FORCE_WINDOW_SPILL_EVERY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			forceWindowSpillEvery.Store(n)
		}
	}
}

// ForceWindowSpillEvery arms (n > 0) or disarms (n <= 0) forced spilling for
// every Window in this process, and returns the previous setting so a test can
// restore it. TEST ONLY.
func ForceWindowSpillEvery(n int64) int64 {
	return forceWindowSpillEvery.Swap(n)
}

// ForcedWindowSpills counts runs written because of the forcing knob rather
// than because of memory pressure. A gate asserts it moved: a forcing knob that
// silently failed to engage turns the gate it arms into a no-op.
var ForcedWindowSpills atomic.Int64

// forcedSpillDue reports whether this Consume is the Nth since the knob was
// armed, and therefore owes a run. Caller holds w.mu.
func (w *Window) forcedSpillDue() bool {
	n := forceWindowSpillEvery.Load()
	if n <= 0 {
		return false
	}
	w.consumesSinceSpill++
	if w.consumesSinceSpill < n {
		return false
	}
	w.consumesSinceSpill = 0
	return true
}
