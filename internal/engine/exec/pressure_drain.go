package exec

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// Heap backpressure asks dominant live-state holders to spill rather than sleep
// (#326; ADR-0006). Only the dominant share of tracked bytes answers; small
// holders still sleep for transient decode garbage to be reclaimed.
// HashAggregate must pass the #325 productivity gate. If that gate declines,
// neither drain nor sleep: waiting cannot reclaim the operator's live state.
// WADJET_PRESSURE_DRAIN=0 restores sleep-on-pressure everywhere.
// See docs/internals/heap-pressure-drain-response.md for the design.

// pressureDrainDisabled gates the whole mechanism. A var (initialized from
// the environment) so tests can A/B both arms in-process.
var pressureDrainDisabled = os.Getenv("WADJET_PRESSURE_DRAIN") == "0"

// pressureDrainDominanceNum/Den: an operator holds the "dominant share"
// when own ≥ used/2 of the shared tracker's bytes.
const (
	pressureDrainDominanceNum = 1
	pressureDrainDominanceDen = 2
)

// heapPauseCount counts 50 ms sleeps taken by PauseOrDrainOnHeapBackpressure;
// heapDrainCount counts valve firings a breaker answered by draining (or by
// declining to sleep over live state). Process-wide, monotonic; the #326
// before/after evidence reads them.
var (
	heapPauseCount atomic.Int64
	heapDrainCount atomic.Int64
)

// PressureDrainer is implemented by pipeline breakers that can answer heap
// backpressure by draining their own state to disk instead of having their
// feed loop sleep.
type PressureDrainer interface {
	// DrainOnHeapPressure is called between batches when the heap
	// backpressure valve fires. It returns handled=true when the operator
	// either drained state or holds the dominant live share (in which case
	// sleeping reclaims nothing and the caller should skip its pause);
	// handled=false means the operator holds too little for draining to
	// relieve the pressure, and the caller should sleep as before.
	DrainOnHeapPressure(ctx context.Context) (handled bool, err error)
}

// TryPressureDrain gives sink the chance to answer heap pressure by
// draining its own state. It does NOT check the valve — callers do that
// first. handled=true means the caller must skip its sleep.
func TryPressureDrain(ctx context.Context, sink Sink) (bool, error) {
	if pressureDrainDisabled {
		return false, nil
	}
	d, ok := sink.(PressureDrainer)
	if !ok {
		return false, nil
	}
	handled, err := d.DrainOnHeapPressure(ctx)
	if err != nil {
		return true, err
	}
	if handled {
		heapDrainCount.Add(1)
		return true, ctx.Err()
	}
	return false, nil
}

// PauseOrDrainOnHeapBackpressure is the sink-aware valve response for a
// consume loop feeding a pipeline breaker: when backpressure fires, a
// spill-capable breaker holding the dominant tracked share drains instead
// of sleeping; every other sink keeps the 50 ms GC catch-up pause.
func PauseOrDrainOnHeapBackpressure(ctx context.Context, sink Sink) error {
	if !memory.HeapBackpressureActive() {
		return nil
	}
	if handled, err := TryPressureDrain(ctx, sink); handled || err != nil {
		return err
	}
	heapPauseCount.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(memory.HeapBackpressurePauseDuration):
	}
	return nil
}

// pauseOrDrainUnless is PauseOrDrainOnHeapBackpressure with the drain-phase
// exemption flag (drain-phase pipelines pass true and never pause).
func pauseOrDrainUnless(ctx context.Context, exempt bool, sink Sink) error {
	if exempt {
		return ctx.Err()
	}
	return PauseOrDrainOnHeapBackpressure(ctx, sink)
}

// dominantLiveShare reports whether an operator owning `own` tracked bytes
// out of `used` total plausibly caused the current heap pressure. The
// second gate — own must be a material fraction of the valve threshold —
// keeps a breaker that dominates a tiny tracked pool from suppressing the
// GC catch-up sleep when the real pressure is untracked transient heap.
// With no GOMEMLIMIT the threshold gate is moot (the real valve never
// fires without one; only the test seam can force it).
func dominantLiveShare(own, used int64) bool {
	if own <= 0 || used <= 0 {
		return false
	}
	if own*pressureDrainDominanceDen < used*pressureDrainDominanceNum {
		return false
	}
	if thr := memory.HeapBackpressureThresholdBytes(); thr > 0 && own < thr/8 {
		return false
	}
	return true
}

// DrainOnHeapPressure implements PressureDrainer for HashAggregate. The
// drain routes through drainAndAccount, so the #325 productivity gate and
// non-convergence detection apply exactly as they do to a self-triggered
// spill; when the gate refuses, the aggregate reports handled (its state
// is live — sleeping cannot reclaim it) without writing a run.
func (h *HashAggregate) DrainOnHeapPressure(ctx context.Context) (bool, error) {
	if h.Spill == nil || h.Spill.IsTrackingOnly() || h.Spill.SpillDir() == "" {
		return false, nil
	}
	t := h.Spill.Tracker()
	if t == nil {
		return false, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.canUseExternalMerge() {
		return false, nil
	}
	h.reconcileGroupMemory()
	if !dominantLiveShare(h.trackedGroupMem, t.Used()) {
		return false, nil
	}
	if !h.drainIsProductive() {
		return true, nil
	}
	return true, h.drainAndAccount(h.selfSpillReliefTarget())
}

// DrainOnHeapPressure implements PressureDrainer for Sort. Draining flushes
// the buffered batches to a sorted run — the same operation Sort's own
// ShouldSpillFor trigger performs — bounded below by minSortRunBytes so
// run files stay merge-worthy.
func (s *Sort) DrainOnHeapPressure(ctx context.Context) (bool, error) {
	if s.Spill == nil || s.Spill.IsTrackingOnly() || s.Spill.SpillDir() == "" {
		return false, nil
	}
	t := s.Spill.Tracker()
	if t == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized {
		return false, nil
	}
	if !dominantLiveShare(s.trackedMem, t.Used()) {
		return false, nil
	}
	if s.trackedMem < minSortRunBytes {
		return true, nil // dominant but under the run floor: live state, sleep frees nothing
	}
	_, err := s.flushSpillLocked()
	return true, err
}

// DrainOnHeapPressure implements PressureDrainer for Window; same shape as
// Sort (columnar runs, minSortRunBytes floor).
func (w *Window) DrainOnHeapPressure(ctx context.Context) (bool, error) {
	if w.Spill == nil || w.Spill.IsTrackingOnly() || w.Spill.SpillDir() == "" {
		return false, nil
	}
	t := w.Spill.Tracker()
	if t == nil {
		return false, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return false, nil
	}
	if !dominantLiveShare(w.trackedMem, t.Used()) {
		return false, nil
	}
	if w.trackedMem < minSortRunBytes {
		return true, nil
	}
	_, err := w.flushSpillLocked()
	return true, err
}
