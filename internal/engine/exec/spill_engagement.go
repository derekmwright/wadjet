package exec

import "sync/atomic"

// Spill counters are production-visible; small-run forcing is TEST ONLY.
// Increment at irreversible engagement (appended run files or produced drain
// paths); gates must assert engagement rather than compare two in-memory runs.
// Sort/window and raw-row paths have 64 MiB run floors: lowering a fixture's
// budget cannot cross them. ForceSmallSpillRuns temporarily lowers both floors
// for SQL-level gates; restore them afterward. The floors prevent single-batch
// runs and input-sized merge fan-in (#325). Raw-row coverage must exercise
// SpillManager.SpillRows, not only partial-state drains (#632).
// Counters add atomically beside I/O; forcing is called only by tests.
// See docs/internals/spill-engagement-and-small-run-seam.md for the design.

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
	// JoinUnroutedProbeFlatBuilds counts spill-eligible HashJoin builds that
	// took the FLAT path because their probe does not route by the partition
	// key — a cross join, whose every probe row reads every build row
	// (probeRoutesByPartition, #832).
	//
	// It is an engagement counter for a decision rather than for a file: the
	// cells that need it are ones where spilling is not merely absent but
	// IMPOSSIBLE, so "the operator wrote a run" cannot be their evidence and
	// "the operator reached the decision that keeps it readable" is. Without
	// it a computed-key cell would compare two in-memory runs and pass with
	// the fix deleted, which is the anti-pattern ADR-0027 decision 5 exists
	// for.
	JoinUnroutedProbeFlatBuilds atomic.Int64
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
