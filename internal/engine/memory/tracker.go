// Package memory provides memory tracking and budget enforcement for the query engine.
package memory

import (
	"fmt"
	"sync/atomic"
)

// ErrMemoryExceeded is returned when a memory reservation exceeds the budget.
var ErrMemoryExceeded = fmt.Errorf("memory budget exceeded")

// Tracker tracks memory usage with a configurable budget.
// Hierarchical: a query tracker can have child operator trackers.
type Tracker struct {
	name     string
	budget   int64
	used     atomic.Int64
	parent   *Tracker
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

// Name returns the tracker name.
func (t *Tracker) Name() string {
	return t.name
}

// Reset resets the usage counter.
func (t *Tracker) Reset() {
	t.used.Store(0)
}
