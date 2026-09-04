package exec

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Deterministic sort-spill forcing — TEST ONLY.
//
// A Sort spills when SpillManager.ShouldSpillFor(SpillCheap) is true, which is
// `tracker.Used() > 40% of the budget` — a reading of the WHOLE query's memory,
// not of the sort's own. In the type-matrix sweep's ORDER BY cells that reading
// is dominated by how many row-group slabs the SCAN happens to be holding when
// the sort checks, and the sort itself is nowhere near the line:
//
//	293 spill checks per run, in every configuration measured
//	median used   165,152 bytes      threshold  209,715 bytes (40% of 512 KiB)
//	checks above the threshold:  13 of 293 (2 cores)   8 of 293 (12 cores)
//
// So whether an ORDER BY cell spills is decided by a transient in the scan's
// read-ahead — ADR-0013's class of legal nondeterminism — and it varied 7 to 14
// of 18 cells across six runs of the SAME code, reaching 0 of 18 on a 2-vCPU CI
// runner. A per-family "at least one cell spilled" assertion over that variable
// is a coin toss, and CLAUDE.md already names the shape: "a gate whose trigger
// is a CONDITION cannot be relied on to fire".
//
// ForceSortSpillEvery makes it deterministic, the way ForceAggDrainEvery
// already does for the aggregate's drain (ADR-0027 decision 6): with N set,
// every Nth Sort.Consume that holds buffered batches writes a run through the
// real writer, and the minSortRunBytes floor — a size heuristic, not a
// correctness rule — does not apply to it, because there is no memory pressure
// for that floor to be economising against. What a gate then observes is the
// production spill/merge path on every run and every core count.
//
// It is read from WADJET_TEST_FORCE_SORT_SPILL_EVERY once per process so an
// end-to-end gate at the SQL layer can arm it, and settable from Go for exec
// level gates. It is never set on any production path: the only cost when unset
// is one relaxed atomic load per Consume, next to a batch of work.
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
