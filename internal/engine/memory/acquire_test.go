package memory

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestReserveOrForce_CleanUnderBudget(t *testing.T) {
	tr := NewTracker("test", 1000)
	forced := ReserveOrForce(context.Background(), tr, nil, 400, time.Second, ForceUnattributed)
	if forced {
		t.Fatal("expected clean reservation under budget")
	}
	if got := tr.Used(); got != 400 {
		t.Fatalf("used = %d, want 400", got)
	}
}

func TestReserveOrForce_NilTrackerOrZeroBytes(t *testing.T) {
	if ReserveOrForce(context.Background(), nil, nil, 100, 0, ForceUnattributed) {
		t.Fatal("nil tracker must be a no-op, not forced")
	}
	tr := NewTracker("test", 10)
	if ReserveOrForce(context.Background(), tr, nil, 0, 0, ForceUnattributed) {
		t.Fatal("zero bytes must be a no-op")
	}
	if tr.Used() != 0 {
		t.Fatalf("used = %d, want 0", tr.Used())
	}
}

func TestReserveOrForce_ForcedPastBudget(t *testing.T) {
	tr := NewTracker("test", 100)
	if err := tr.Reserve(90); err != nil {
		t.Fatal(err)
	}
	forced := ReserveOrForce(context.Background(), tr, nil, 50, 10*time.Millisecond, ForceUnattributed)
	if !forced {
		t.Fatal("expected forced fallback past budget")
	}
	// Accounting stays honest: the bytes are charged even though over budget.
	if got := tr.Used(); got != 140 {
		t.Fatalf("used = %d, want 140", got)
	}
}

func TestReserveOrForce_SucceedsAfterConcurrentRelease(t *testing.T) {
	tr := NewTracker("test", 100)
	if err := tr.Reserve(90); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		tr.Release(90)
	}()
	forced := ReserveOrForce(context.Background(), tr, nil, 50, 2*time.Second, ForceUnattributed)
	if forced {
		t.Fatal("expected clean reservation once budget freed")
	}
	if got := tr.Used(); got != 50 {
		t.Fatalf("used = %d, want 50", got)
	}
}

// reliefFakeOp frees tracker bytes when asked to spill, so a blocked
// reservation can succeed after RequestRelief.
type reliefFakeOp struct {
	mu      sync.Mutex
	tracker *Tracker
	owned   int64
}

func (f *reliefFakeOp) Inspect() OperatorFootprint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return OperatorFootprint{
		OwnedBytes:     f.owned,
		RetainedBytes:  f.owned,
		SpillableBytes: f.owned,
		State:          OpActive,
		InstanceID:     1,
		Name:           "relief-fake",
	}
}

func (f *reliefFakeOp) EstimateRelief(target int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if target > 0 && target < f.owned {
		return target
	}
	return f.owned
}

func (f *reliefFakeOp) SpillSome(want int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.owned
	if want > 0 && want < n {
		n = want
	}
	f.owned -= n
	f.tracker.Release(n)
	return n, nil
}

func TestReserveOrForce_ReliefUnblocksReservation(t *testing.T) {
	tr := NewTracker("test", 100)
	sm, err := NewSpillManager(t.TempDir(), tr)
	if err != nil {
		t.Fatal(err)
	}
	// An operator owns 90 of the 100-byte budget.
	if err := tr.Reserve(90); err != nil {
		t.Fatal(err)
	}
	op := &reliefFakeOp{tracker: tr, owned: 90}
	unregister := sm.RegisterAccounted(op)
	defer unregister()

	forced := ReserveOrForce(context.Background(), tr, sm, 50, 2*time.Second, ForceUnattributed)
	if forced {
		t.Fatal("expected relief to unblock a clean reservation")
	}
	if got := tr.Used(); got > 100 {
		t.Fatalf("used = %d, want <= budget 100 after relief", got)
	}
}
