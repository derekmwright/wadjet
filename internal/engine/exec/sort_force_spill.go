package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// TEST ONLY: every Nth Sort.Consume with buffered batches writes a real run,
// bypassing pressure and minSortRunBytes (ADR-0027 decision 6).
// Read WADJET_TEST_FORCE_SORT_SPILL_EVERY once per process or set from Go;
// never set in production; unset cost is one atomic load per Consume.
// Use only exec gates driving ONE operator until #864 is fixed, never whole
// SQL queries: forcing bypasses TrackingOnlyView's ShouldSpillFor guard and
// morsel-clone runs can be orphaned at merge, dropping rows (#790).
// Ordinary whole-query memory pressure varies with scan read-ahead, so it
// cannot guarantee spill engagement (ADR-0013).
// See docs/internals/sort-spill-forcing-boundary.md for the design.
var forceSortSpillEvery atomic.Int64

func init() {
	if v := os.Getenv("WADJET_TEST_FORCE_SORT_SPILL_EVERY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			forceSortSpillEvery.Store(n)
		}
	}
}

// ForceSortSpillEvery arms (n > 0) or disarms (n <= 0) forced spilling for
// every Sort in this process, and returns the previous setting so a test can
// restore it. TEST ONLY.
func ForceSortSpillEvery(n int64) int64 {
	return forceSortSpillEvery.Swap(n)
}

// ForcedSortSpills counts runs written because of the forcing knob rather than
// because of memory pressure. A gate asserts it moved: a forcing knob that
// silently failed to engage turns the gate it arms into a no-op, which is the
// failure mode ForcedAggDrains exists to catch on the aggregate's side.
var ForcedSortSpills atomic.Int64

// forcedSpillDue reports whether this Consume is the Nth since the knob was
// armed, and therefore owes a run. Caller holds s.mu.
func (s *Sort) forcedSpillDue() bool {
	n := forceSortSpillEvery.Load()
	if n <= 0 {
		return false
	}
	s.consumesSinceSpill++
	if s.consumesSinceSpill < n {
		return false
	}
	s.consumesSinceSpill = 0
	return true
}
