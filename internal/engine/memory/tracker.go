// Package memory provides memory tracking and budget enforcement for the query engine.
package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrMemoryExceeded is returned when a memory reservation exceeds the budget.
var ErrMemoryExceeded = fmt.Errorf("memory budget exceeded")

// Tracker tracks memory usage with a configurable budget.
// Hierarchical: a query tracker can have child operator trackers.
type Tracker struct {
	name   string
	budget int64
	used   atomic.Int64
	peak   atomic.Int64
	parent *Tracker

	// published holds per-instance externally-reported owned-byte counters,
	// keyed by instanceID -> *atomic.Int64. Operators publish their current owned
	// footprint here; the spill manager reads it wait-free. A sync.Map (rather
	// than a struct-wide mutex) preserves the lock-free invariant documented on
	// ReserveBlocking — reads are an atomic.Load, writes touch one per-instance
	// counter created once via LoadOrStore.
	published sync.Map // map[uint64]*atomic.Int64
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

// Transfer moves n bytes of accounting from one tracker to another. When `from`
// and `to` share a parent, the bubbled release and reserve cancel at the common
// ancestor, so the parent's total is conserved; when they live in different
// subtrees the bytes move between subtrees, which is the intended semantics.
//
// Concurrency: like the rest of Tracker, this is lock-free. It is two
// independent atomic ops on `used` (not transactional across the pair), matching
// Reserve's Add-then-rollback non-atomicity. A concurrent reader may briefly
// observe the in-between state, but both endpoints converge to a consistent sum,
// which is what the conserved-sum property test asserts under -race.
//
// If the subtraction would drive `from.used` below zero — an accounting bug,
// bytes released that were never reserved — the source is clamped back to 0 and
// a WARN is emitted, rather than letting `used` go negative as bare Release does.
// The destination still receives the full n.
//
// The receiver is unused; Transfer is a method only so callers can write
// tracker.Transfer(from, to, n) on any convenient tracker handle.
func (t *Tracker) Transfer(from, to *Tracker, n int64) {
	if n <= 0 || from == nil || to == nil || from == to {
		return
	}

	newFrom := from.used.Add(-n)
	if newFrom < 0 {
		slog.Warn("memory tracker transfer underflow (accounting gap)",
			"from", from.name,
			"to", to.name,
			"requested", n,
			"resulting_used", newFrom,
		)
		from.used.Add(-newFrom) // -newFrom > 0, lands the counter back at 0
	}
	if from.parent != nil {
		from.parent.Release(n)
	}

	newTo := to.used.Add(n)
	if to.parent != nil {
		to.parent.ForceReserve(n)
	}
	to.bumpPeak(newTo)
}

// Used returns the current memory usage in bytes.
func (t *Tracker) Used() int64 {
	return t.used.Load()
}

// Budget returns the configured budget in bytes.
func (t *Tracker) Budget() int64 {
	return t.budget
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

// PublishOwned records the externally-reported owned-byte total for the given
// instance. It is an idempotent overwrite (Store of the latest total), not
// additive — callers publish their current owned footprint and readers see the
// latest. Lock-free on the common path: the *atomic.Int64 is created once per
// instance via LoadOrStore, then updated in place.
func (t *Tracker) PublishOwned(instanceID uint64, total int64) {
	v, _ := t.published.LoadOrStore(instanceID, new(atomic.Int64))
	v.(*atomic.Int64).Store(total)
}

// OwnedFor returns the published owned bytes for one instance, wait-free.
// Returns 0 if the instance has never published.
func (t *Tracker) OwnedFor(instanceID uint64) int64 {
	if v, ok := t.published.Load(instanceID); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// OwnedSnapshot returns a copy of instanceID -> published owned bytes. Wait-free
// per entry (atomic.Load); the walk itself uses sync.Map's lock-free Range.
func (t *Tracker) OwnedSnapshot() map[uint64]int64 {
	out := make(map[uint64]int64)
	t.published.Range(func(k, v any) bool {
		out[k.(uint64)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// OwnedTotal returns the sum of all published owned-byte counters across
// instances. Wait-free per entry.
func (t *Tracker) OwnedTotal() int64 {
	var sum int64
	t.published.Range(func(_, v any) bool {
		sum += v.(*atomic.Int64).Load()
		return true
	})
	return sum
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
