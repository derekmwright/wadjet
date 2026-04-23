package worker

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestSetMemoryBudget_SharedPool verifies that SetMemoryBudget creates a
// single shared Tracker + SpillManager that all tasks on the executor will
// consult, rather than the prior per-task newSpillManager pattern. This is
// the worker-level memory pool that matches Trino's MemoryPool / Spark's
// ExecutionMemoryPool: N concurrent tasks Reserve against one budget and
// spill cooperatively under pool pressure.
func TestSetMemoryBudget_SharedPool(t *testing.T) {
	store := objstore.NewMemStore()
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)

	// Zero budget: no shared pool (backward compat — unlimited / no spill).
	if executor.sharedTracker != nil || executor.sharedSpill != nil {
		t.Fatal("fresh executor should have no shared pool")
	}

	executor.SetMemoryBudget(1<<30, t.TempDir()) // 1 GB

	if executor.sharedTracker == nil {
		t.Fatal("expected shared tracker after SetMemoryBudget")
	}
	if executor.sharedSpill == nil {
		t.Fatal("expected shared spill manager after SetMemoryBudget")
	}
	if executor.sharedTracker.Budget() != 1<<30 {
		t.Errorf("shared tracker budget: got %d, want %d", executor.sharedTracker.Budget(), int64(1<<30))
	}
}

// TestSharedPool_CumulativeReservations verifies that two concurrent
// allocations via the shared tracker accumulate into one used counter
// (the core property that makes pool-based spill work: one task's
// reservations are visible to the other, so ShouldSpill fires when the
// SUM crosses threshold, not when any individual task's local count does).
func TestSharedPool_CumulativeReservations(t *testing.T) {
	store := objstore.NewMemStore()
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetMemoryBudget(100, t.TempDir()) // tiny budget to force threshold

	tracker := executor.sharedTracker
	if err := tracker.Reserve(40); err != nil {
		t.Fatalf("task-A reserve 40: %v", err)
	}
	if err := tracker.Reserve(30); err != nil {
		t.Fatalf("task-B reserve 30: %v", err)
	}

	// Sum is 70/100 — 70%, above the 50% spill trigger (per spill.go).
	if !executor.sharedSpill.ShouldSpill() {
		t.Fatalf("ShouldSpill should fire: used=%d budget=%d (70%% > 50%% default)",
			tracker.Used(), tracker.Budget())
	}

	tracker.Release(40)
	if executor.sharedSpill.ShouldSpill() {
		t.Errorf("ShouldSpill should no longer fire after release: used=%d", tracker.Used())
	}
	tracker.Release(30)
}
