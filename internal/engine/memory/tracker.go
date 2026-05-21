// Package memory provides memory tracking and budget enforcement for the query engine.
package memory

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ErrMemoryExceeded is returned when a memory reservation exceeds the budget.
var ErrMemoryExceeded = fmt.Errorf("memory budget exceeded")

// Tracker tracks memory usage with a configurable budget.
// Hierarchical: a query tracker can have child operator trackers.
//
// Budget (hard cap) is checked by Reserve and fails the allocation when
// exceeded. Share (soft cap) is checked by SpillManager.ShouldSpillForTaskShare
// and triggers cooperative spill on this task before its allocations
// reach the hard cap. Share is intended to express a task's fair slice
// of a shared pool (e.g., sharedBudget / maxConcurrent) so heavy
// operators can't hog more than their portion of cumulative cluster
// memory even when other concurrent tasks happen to be idle. Share = 0
// disables the soft cap (legacy behavior).
type Tracker struct {
	name   string
	budget int64
	share  int64 // soft cap for ShouldSpillForTaskShare; 0 = disabled
	used   atomic.Int64
	peak   atomic.Int64
	parent *Tracker
}

// NewTracker creates a root memory tracker with the given budget in bytes.
func NewTracker(name string, budget int64) *Tracker {
	return &Tracker{
		name:   name,
		budget: budget,
	}
}

// Child creates a child tracker that shares the parent's budget.
func (t *Tracker) Child(name string) *Tracker {
	return &Tracker{
		name:   name,
		budget: t.budget,
		parent: t,
	}
}

// Reserve attempts to allocate n bytes. Returns ErrMemoryExceeded if the budget would be exceeded.
func (t *Tracker) Reserve(n int64) error {
	newUsed := t.used.Add(n)
	if t.budget > 0 && newUsed > t.budget {
		t.used.Add(-n) // rollback
		return fmt.Errorf("%s: %w (used=%d, requested=%d, budget=%d)",
			t.name, ErrMemoryExceeded, newUsed-n, n, t.budget)
	}

	if t.parent != nil {
		if err := t.parent.Reserve(n); err != nil {
			t.used.Add(-n) // rollback
			return err
		}
	}

	t.bumpPeak(newUsed)
	return nil
}

// Release frees n bytes of previously reserved memory.
func (t *Tracker) Release(n int64) {
	t.used.Add(-n)
	if t.parent != nil {
		t.parent.Release(n)
	}
}

// Used returns the current memory usage in bytes.
func (t *Tracker) Used() int64 {
	return t.used.Load()
}

// Budget returns the configured budget in bytes.
func (t *Tracker) Budget() int64 {
	return t.budget
}

// Share returns the soft cap (cooperative spill threshold base) for
// this tracker, or 0 when disabled. Used by SpillManager to drive
// per-task spill on the operator's own contribution rather than on
// cumulative shared-pool pressure.
func (t *Tracker) Share() int64 {
	return t.share
}

// SetShare configures the soft cap. Pass 0 to disable. Safe to call
// before any allocations have been Reserved against this tracker —
// changing share mid-flight is allowed but won't retroactively trigger
// spill on already-reserved bytes.
func (t *Tracker) SetShare(n int64) {
	t.share = n
}

// Name returns the tracker name.
func (t *Tracker) Name() string {
	return t.name
}

// ForceReserve adds n bytes without checking or rolling back on over-budget.
// Used by operators that need to track usage for spill detection without failing.
func (t *Tracker) ForceReserve(n int64) {
	newUsed := t.used.Add(n)
	if t.parent != nil {
		t.parent.ForceReserve(n)
	}
	t.bumpPeak(newUsed)
}

// Peak returns the highest value ever observed by used. Used by per-task
// observability hooks to surface peak per-tracker footprint at task end.
func (t *Tracker) Peak() int64 {
	return t.peak.Load()
}

// bumpPeak updates peak with newUsed via lock-free CAS. Cheaper than holding
// a mutex on the hot Reserve/ForceReserve path; contention is rare because
// peak only moves up.
func (t *Tracker) bumpPeak(newUsed int64) {
	for {
		cur := t.peak.Load()
		if newUsed <= cur {
			return
		}
		if t.peak.CompareAndSwap(cur, newUsed) {
			return
		}
	}
}

// ReserveBlocking attempts to reserve n bytes, retrying every pollInterval
// until success or ctx cancellation. Used as an admission gate so callers
// can wait for the budget to free instead of failing immediately on a
// transient over-budget condition.
//
// The polling design is intentional: the existing tracker is lock-free
// (atomic counters), and adding a sync.Cond would require holding a mutex
// in every Release path on the hot batch loop. Polling at 100ms keeps
// admission overhead near zero (one Reserve attempt + one timer goroutine
// per blocked task) while bounding wait latency to one poll interval.
//
// Returns ctx.Err() on cancellation or any non-budget error from Reserve.
func (t *Tracker) ReserveBlocking(ctx context.Context, n int64, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	for {
		err := t.Reserve(n)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrMemoryExceeded) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// Reset resets the usage counter. Does not reset peak — peak is a high-water
// mark for the tracker's lifetime; callers that want a fresh peak should
// construct a new tracker.
func (t *Tracker) Reset() {
	t.used.Store(0)
}
