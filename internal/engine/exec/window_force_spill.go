package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Deterministic window-spill forcing — TEST ONLY.
//
// The Window's spill trigger is the Sort's, line for line:
// SpillManager.ShouldSpillFor(SpillCheap), which is `tracker.Used() > 40% of
// the budget` — a reading of the WHOLE query's memory, not of the window's. In
// the type-matrix sweep's 36 window cells that reading is dominated by how many
// row-group slabs the SCAN is holding at the instant the window checks, so the
// family engaged 22 to 23 of 36 on a 24-core box and 0 of 36 on one core with
// one scan worker. Same random variable, same reason, as the sort family's
// (sort_force_spill.go carries the per-check measurement).
//
// ForceWindowSpillEvery makes it deterministic: with N set, every Nth
// Window.Consume that holds buffered batches writes a run through the real
// writer, and the minSortRunBytes floor — a merge-economy heuristic sized
// against real pressure — does not apply to it, because there is no pressure
// for it to be economising against.
//
// It is read from WADJET_TEST_FORCE_WINDOW_SPILL_EVERY once per process and
// settable from Go. It is never set on any production path: the only cost when
// unset is one relaxed atomic load per Consume, next to a batch of work.
//
// # The same #864 warning as the sort's knob
//
// Arming it around a whole QUERY bypasses ShouldSpillFor, and ShouldSpillFor is
// where the production guard lives: a morsel clone gets a
// memory.SpillManager.TrackingOnlyView whose ShouldSpillFor answers false, so a
// clone never writes runs that the merge could orphan. The sort's knob armed at
// the SQL layer drops 44% to 78% of the rows for exactly that reason (#864).
// exec.Window is not in wireCloneSinkSpill's switch today, so it has no clone
// arm to be wrong about — but the knob still belongs to exec-level gates that
// drive ONE operator until #864 closes and the guard is understood end to end.
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
