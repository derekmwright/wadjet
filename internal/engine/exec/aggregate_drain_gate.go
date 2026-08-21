package exec

import (
	"fmt"
	"time"
)

// Drain productivity gate (#325).
//
// SpillManager.ShouldSpillFor answers a question about the WHOLE tracker —
// "is the shared budget (or the process heap) over threshold?" — not about
// this operator. A HashAggregate that owns almost none of the pressured
// bytes therefore sees the signal on every batch and, before this gate,
// answered each one with a drain: whole-table for the packed/string/generic
// key modes, a partition slice for the int-keyed one. Neither relieves
// pressure it did not cause, so the signal is still true on the next batch
// and the operator drains again.
//
// The SF100 report in #325 is that loop at scale: `GROUP BY l_partkey,
// l_suppkey` over three years of lineitem, 23,153 backpressure pauses
// totalling 1,158 s, 24 GB of agg-spill-*.bin, and a stage watchdog that
// eventually blamed a worker crash that never happened. Draining once per
// batch also makes each run file batch-sized, so the k-way merge's fan-in
// grows with the input rather than with the state.
//
// The gate is a floor on NEW state since the last drain. Its two properties
// are what break the loop:
//
//   - A drain only runs when this operator has accumulated enough of its own
//     state that writing it out actually returns something. Foreign pressure
//     alone can no longer trigger one.
//   - It doubles as hysteresis. A whole-table drain resets the footprint to
//     ~0, so the next drain waits for a floor's worth of regrowth; a partial
//     partition drain leaves array capacity in place (the SoA arrays keep
//     their len/cap and reconcileGroupMemory only ratchets upward), so the
//     floor is measured against that retained footprint and successive
//     drains are spaced by real growth rather than by batch arrival.
//
// In-memory state stays bounded by (post-drain footprint + floor), and the
// run count by (total state / floor) instead of one run per batch.

// drainFloorDivisor sets the floor as budget/drainFloorDivisor. 8 keeps the
// floor comfortably under the 40% SpillCheap trigger — an aggregate that is
// genuinely the memory hog crosses 40% of budget long before the gate can
// hold it back, so this never delays a drain that would have relieved real
// pressure. It only suppresses the drains that would have freed nothing.
//
// A var, not a const, so 0 restores the pre-#325 "drain whenever asked"
// behavior for A/B measurement of the gate.
var drainFloorDivisor int64 = 8

// minDrainBytes is the floor of new group state required before another
// drain is worth its I/O. Returns 0 when the budget is unknown (tests and
// embedded uses with no tracker), which disables the gate and preserves the
// pre-#325 "always drain when asked" behavior.
func (h *HashAggregate) minDrainBytes() int64 {
	if h.Spill == nil {
		return 0
	}
	t := h.Spill.Tracker()
	if t == nil {
		return 0
	}
	// SpillBudget, not Tracker().Budget(): a #318 degraded-retry view
	// carries a deliberately reduced budget, and the gate's floor must
	// shrink with it or the gate would hold back the early drains the
	// degradation exists to force.
	budget := h.Spill.SpillBudget()
	if budget <= 0 || drainFloorDivisor <= 0 {
		return 0
	}
	return budget / drainFloorDivisor
}

// drainIsProductive reports whether a drain would release enough of this
// operator's own state to be worth the run file it writes. Caller holds
// h.mu and must have reconciled group memory first, so trackedGroupMem is
// current.
func (h *HashAggregate) drainIsProductive() bool {
	floor := h.minDrainBytes()
	if floor <= 0 {
		return true
	}
	return h.trackedGroupMem-h.lastDrainFootprint >= floor
}

// Non-convergence detection (#325, point 3).
//
// Past the gate an aggregate can still fail to converge: if each drain frees
// less than the floor that admitted it, the footprint ratchets upward while
// drain I/O eats the wall clock, and the operator will never finish however
// long it is given. That is a real failure and it should be reported as one,
// with the counters that identify it — not left to a coordinator watchdog to
// misreport 10 minutes later as "likely worker crash, deadlock, or lost
// result publish".
//
// Both conditions must hold, over a window long enough to be more than a
// transient:
//
//   - drain I/O dominates the wall clock (drainStallRatio of the window), and
//   - the average drain frees less than the floor that admitted it, i.e. the
//     drains are churning rather than reclaiming.
//
// The second condition is what keeps a small-budget-but-healthy aggregate
// out of the error: those drain often, but each one frees far more than
// their tiny floor, so they are making progress and are left alone.
var (
	// drainStallMinCycles is the number of drains before the check can fire.
	drainStallMinCycles = 32
	// drainStallMinWindow is the minimum observation window.
	drainStallMinWindow = 30 * time.Second
	// drainStallRatio is the fraction of the window spent in drain I/O that
	// counts as "not making progress".
	drainStallRatio = 0.9
)

// checkDrainProgress returns a non-convergence error when this aggregate is
// spending its wall clock on drains that no longer reclaim anything. Caller
// holds h.mu.
func (h *HashAggregate) checkDrainProgress() error {
	if h.drainCount < drainStallMinCycles || h.firstDrainAt.IsZero() {
		return nil
	}
	window := time.Since(h.firstDrainAt)
	if window < drainStallMinWindow {
		return nil
	}
	if float64(h.drainNanos) < drainStallRatio*float64(window) {
		return nil
	}
	floor := h.minDrainBytes()
	avgFreed := h.drainFreedBytes / int64(h.drainCount)
	if floor <= 0 || avgFreed >= floor {
		return nil
	}
	var used, budget int64
	if t := h.Spill.Tracker(); t != nil {
		used, budget = t.Used(), t.Budget()
	}
	const mib = 1 << 20
	return fmt.Errorf("aggregate could not make progress within its memory budget: "+
		"%d spill cycles over %s spent %s (%.0f%%) writing partial state and freed only %d MiB "+
		"(%d MiB per cycle, below the %d MiB that admitted them); group state %d MiB, "+
		"tracker used %d MiB of %d MiB budget",
		h.drainCount, window.Round(time.Second), time.Duration(h.drainNanos).Round(time.Second),
		100*float64(h.drainNanos)/float64(window), h.drainFreedBytes/mib,
		avgFreed/mib, floor/mib, h.trackedGroupMem/mib, used/mib, budget/mib)
}

// noteDrain records what a completed drain cost and what it returned, and
// rebases the gate on the post-drain footprint. Every drain — self-triggered
// or cooperative — must go through it, or the gate measures growth against a
// stale baseline. Caller holds h.mu.
func (h *HashAggregate) noteDrain(before int64, start time.Time) {
	if h.firstDrainAt.IsZero() {
		h.firstDrainAt = start
	}
	h.drainCount++
	h.drainNanos += time.Since(start).Nanoseconds()
	if freed := before - h.trackedGroupMem; freed > 0 {
		h.drainFreedBytes += freed
	}
	// Rebase on the MEASURED footprint, not on trackedGroupMem. The two
	// differ in both drain modes, and in both the tracked value is the
	// misleading one:
	//
	//   - A whole-table drain zeroes trackedGroupMem but immediately
	//     reallocates empty 4096-slot index and accumulator arrays; the next
	//     reconcileGroupMemory charges those hundreds of KB back as if they
	//     were new group state, which would re-open the gate on the very
	//     next batch.
	//   - A partial partition drain decrements trackedGroupMem by the
	//     drained share, but the SoA arrays keep their len/cap, so
	//     reconcileGroupMemory — which only ever ratchets upward — restores
	//     the same bytes on the next batch.
	//
	// groupMemoryUsage is what reconcileGroupMemory itself will compare
	// against, so anchoring here means the gate measures real growth in both
	// modes.
	h.lastDrainFootprint = h.groupMemoryUsage()
}

// drainAndAccount runs a self-triggered drain, records it, and reports
// non-convergence when the drains stop reclaiming. Caller holds h.mu.
func (h *HashAggregate) drainAndAccount(target int64) error {
	before := h.trackedGroupMem
	start := time.Now()
	if err := h.spillPartialState(target); err != nil {
		return err
	}
	h.noteDrain(before, start)
	return h.checkDrainProgress()
}
