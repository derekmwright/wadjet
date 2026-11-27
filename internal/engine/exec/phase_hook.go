// SPDX-License-Identifier: MIT

package exec

import "sync/atomic"

// PhaseSite names a point inside an operator phase that mutates state the
// relief registry can reach (Inspect, EstimateRelief, SpillSome,
// DrainOnHeapPressure) while the operator is still registered: a clone merge,
// or a hash join's build tail.
type PhaseSite string

const (
	// PhaseSortMerge is inside Sort.MergeSink, after the clone's batches have
	// moved and before their accounting has.
	PhaseSortMerge PhaseSite = "Sort.MergeSink"
	// PhaseAggMerge is inside HashAggregate.MergeSink's state merge, after the
	// clone's run lists have moved and before its group state has.
	PhaseAggMerge PhaseSite = "HashAggregate.MergeSink"
	// PhaseJoinBuildTail is inside a partitioned HashJoin build after the last
	// accumulator froze and before the matched bitmaps and the bloom filter
	// are built from the index.
	PhaseJoinBuildTail PhaseSite = "HashJoin.buildTail"
	// PhaseJoinBuildEnd is in a registered HashJoin's Build after the build
	// itself returned (on every path) and before Build's deferred close takes
	// mu to end relief eligibility and unregister. No lock is held there: it
	// is the window in which a relief call selected during the build can
	// still take mu (#1706, line 1706o).
	PhaseJoinBuildEnd PhaseSite = "HashJoin.buildEnd"

	// The compact builds (a key-only semi/anti build, serial or parallel, and
	// the distinct-pair NE build) report their synchronized points with a
	// *CompactBuildPoint, so a test can enumerate what they hold against what
	// they charged (#1706, line 1706p). None of them is registered for relief.
	//
	// PhaseKeyOnlyLocals is in the parallel key-only build after every worker
	// stopped and before the merge: every local table is live.
	PhaseKeyOnlyLocals PhaseSite = "HashJoin.keyOnlyLocals"
	// PhaseKeyOnlyMerged is after the parallel merge and before the bloom.
	PhaseKeyOnlyMerged PhaseSite = "HashJoin.keyOnlyMerged"
	// PhaseCompactArrival is after one arrival batch of a serial key-only or
	// NE build, under the join's mu.
	PhaseCompactArrival PhaseSite = "HashJoin.compactArrival"
	// PhaseCompactDone is at the end of a compact build on any route, after
	// the bloom and any final accounting.
	PhaseCompactDone PhaseSite = "HashJoin.compactDone"
)

// CompactBuildPoint is what a compact build hands the phase hook: the join,
// the route that ran ("parallel key-only", "serial key-only", "ne") and, on
// the parallel route, the per-worker local tables that are still referenced.
type CompactBuildPoint struct {
	Join   *HashJoin
	Route  string
	Locals []*localKeyBuild
}

// phaseHook runs at a PhaseSite with the operator in that phase. nil in
// production.
var phaseHook atomic.Pointer[func(PhaseSite, any)]

// SetPhaseHook installs f to run at every PhaseSite and returns the restore.
// TEST-ONLY: with memory.SetReliefCallHook it lands a relief call the registry
// selected at a chosen point inside the victim's phase on every run, instead
// of sampling the scheduler (#1706, line 1706n). f runs on the operator's own
// goroutine, holding whatever lock the phase holds there: it must not call the
// operator's locking methods.
func SetPhaseHook(f func(PhaseSite, any)) func() {
	var p *func(PhaseSite, any)
	if f != nil {
		p = &f
	}
	prev := phaseHook.Swap(p)
	return func() { phaseHook.Store(prev) }
}

func runPhaseHook(site PhaseSite, op any) {
	if f := phaseHook.Load(); f != nil {
		(*f)(site, op)
	}
}
