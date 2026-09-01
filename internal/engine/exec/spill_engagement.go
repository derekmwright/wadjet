package exec

import "sync/atomic"

// Spill-engagement counters and the small-run forcing seam — TEST-ONLY
// controls, production-visible counters.
//
// Why they exist. A gate that compares a budgeted run against an unbudgeted
// one proves nothing unless the budgeted run actually SPILLED, and the repo has
// already been bitten by exactly that: the container group-key gate "claimed to
// gate all four under a 1 KiB budget and measured ZERO drain writes … it
// compared two in-memory runs and would have passed with the whole drain path
// deleted" (container_group_key_test.go). The spill sweep repeated the mistake
// on a larger scale — 54 of its 173 cells never reached a sort or window run,
// and not one cell in the whole file ever wrote a raw-row spill file, so
// memory.SpillManager.SpillRows (the encoder #632 IS) was never invoked by it.
//
// Two things were needed to close that, and they are the two halves of this
// file.
//
// THE COUNTERS make engagement assertable. Each is bumped where the operator
// records a spill it cannot take back — the run file it appends, the drain that
// produced paths — so a family of cells that stopped spilling FAILS its gate
// instead of passing quietly.
//
// THE FORCING SEAM makes engagement reachable. minSortRunBytes and
// spillFileTargetBytes are 64 MiB floors, deliberately: below them a pressured
// operator writes single-batch runs and the merge fan-in grows with the input
// (#325's shape for the aggregate). No budget makes a 1.2 MB fixture cross
// them, so a SQL-level gate cannot reach those paths at all by turning the
// budget down — the floors are structural, not a tuning question. exec's own
// tests already lower the two package vars directly; ForceSmallSpillRuns is the
// same thing exported, so a gate in another package can do it too and put it
// back.
//
// Neither half is on any production path: the counters are relaxed atomic adds
// next to file I/O, and the seam is only reachable from a test that calls it.

var (
	// AggregatePartialDrains counts HashAggregate drains that produced at
	// least one partial-state run file — the external-merge spill path.
	AggregatePartialDrains atomic.Int64
	// RawRowSpillFiles counts HashAggregate raw-row buffers flushed to disk
	// through memory.SpillManager.SpillRows — the legacy path, and the one
	// #632's encoder lives on.
	RawRowSpillFiles atomic.Int64
	// SortRunsWritten counts sorted columnar runs Sort wrote to disk.
	SortRunsWritten atomic.Int64
	// WindowRunsWritten counts run files Window wrote to disk, columnar runs
	// and the legacy row-oriented spill together.
	WindowRunsWritten atomic.Int64
)

// ForceSmallSpillRuns lowers the sort/window run floor and the raw-row buffer's
// flush target to n bytes and returns a function restoring both. TEST ONLY, and
// NOT safe for parallel tests in the same process — the two knobs are package
// vars, as they already were for exec's own tests.
//
// n of a few kilobytes is the useful range: large enough that a run holds more
// than one row, small enough that a fixture of a megabyte or two crosses it
// several times.
func ForceSmallSpillRuns(n int64) func() {
	prevSort, prevRaw := minSortRunBytes, spillFileTargetBytes
	minSortRunBytes, spillFileTargetBytes = n, n
	return func() { minSortRunBytes, spillFileTargetBytes = prevSort, prevRaw }
}
