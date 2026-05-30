package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTrackerName(t *testing.T) {
	tr := NewTracker("my-tracker", 1024)
	if tr.Name() != "my-tracker" {
		t.Fatalf("expected 'my-tracker', got %q", tr.Name())
	}
}

func TestTrackerBudget(t *testing.T) {
	tr := NewTracker("test", 4096)
	if tr.Budget() != 4096 {
		t.Fatalf("expected budget=4096, got %d", tr.Budget())
	}
}

func TestTrackerReset(t *testing.T) {
	tr := NewTracker("test", 1024)
	tr.Reserve(512)
	if tr.Used() != 512 {
		t.Fatalf("expected used=512, got %d", tr.Used())
	}
	tr.Reset()
	if tr.Used() != 0 {
		t.Fatalf("expected used=0 after reset, got %d", tr.Used())
	}
}

func TestTrackerReserveExactBudget(t *testing.T) {
	tr := NewTracker("test", 1024)
	if err := tr.Reserve(1024); err != nil {
		t.Fatalf("exact budget reservation should succeed: %v", err)
	}
	if tr.Used() != 1024 {
		t.Fatalf("expected used=1024, got %d", tr.Used())
	}
	// One more byte should fail
	if err := tr.Reserve(1); !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("reservation beyond budget should fail")
	}
}

func TestTrackerReserveZero(t *testing.T) {
	tr := NewTracker("test", 1024)
	if err := tr.Reserve(0); err != nil {
		t.Fatalf("zero reservation should succeed: %v", err)
	}
	if tr.Used() != 0 {
		t.Fatalf("expected used=0, got %d", tr.Used())
	}
}

func TestTrackerChildName(t *testing.T) {
	parent := NewTracker("parent", 1024)
	child := parent.Child("child-op")
	if child.Name() != "child-op" {
		t.Fatalf("expected child name 'child-op', got %q", child.Name())
	}
}

func TestTrackerChildBudgetShared(t *testing.T) {
	parent := NewTracker("query", 1024)
	child1 := parent.Child("op1")
	child2 := parent.Child("op2")

	if err := child1.Reserve(600); err != nil {
		t.Fatal(err)
	}
	// child2 trying to reserve 600 should fail (parent has 600 used, budget 1024)
	if err := child2.Reserve(600); !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("expected ErrMemoryExceeded for shared budget")
	}
	// After child2 fails, child2 usage should be 0
	if child2.Used() != 0 {
		t.Fatalf("child2 used should be 0 after failed reserve, got %d", child2.Used())
	}
	// Parent should still show 600
	if parent.Used() != 600 {
		t.Fatalf("parent used should be 600, got %d", parent.Used())
	}
}

func TestTrackerChildReleasePropagation(t *testing.T) {
	parent := NewTracker("query", 2048)
	child := parent.Child("sort")

	child.Reserve(1000)
	if parent.Used() != 1000 {
		t.Fatalf("parent should reflect child usage")
	}

	child.Release(500)
	if parent.Used() != 500 {
		t.Fatalf("parent should reflect child release, got %d", parent.Used())
	}
	if child.Used() != 500 {
		t.Fatalf("child should show 500, got %d", child.Used())
	}
}

func TestTrackerGrandchild(t *testing.T) {
	root := NewTracker("root", 1024)
	child := root.Child("query")
	grandchild := child.Child("operator")

	if err := grandchild.Reserve(512); err != nil {
		t.Fatal(err)
	}
	if root.Used() != 512 {
		t.Fatal("root should reflect grandchild usage")
	}
	if child.Used() != 512 {
		t.Fatal("child should reflect grandchild usage")
	}

	grandchild.Release(512)
	if root.Used() != 0 {
		t.Fatal("root should be 0 after release")
	}
}

func TestTrackerConcurrentReserveRelease(t *testing.T) {
	tr := NewTracker("concurrent", 1_000_000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tr.Reserve(100)
				tr.Release(100)
			}
		}()
	}
	wg.Wait()
	if tr.Used() != 0 {
		t.Fatalf("expected 0 after balanced reserve/release, got %d", tr.Used())
	}
}

func TestTrackerNegativeBudget(t *testing.T) {
	// Negative budget should behave like unlimited (budget <= 0)
	tr := NewTracker("test", -1)
	if err := tr.Reserve(1 << 30); err != nil {
		t.Fatalf("negative budget should act unlimited: %v", err)
	}
}

func TestTracker_Transfer_ConcurrentSumInvariant(t *testing.T) {
	parent := NewTracker("parent", 0) // 0 = unlimited
	a := parent.Child("a")
	b := parent.Child("b")

	const start = 1_000_000
	a.ForceReserve(start) // all bytes start on a (bubbles to parent)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				parent.Transfer(a, b, 100)
				parent.Transfer(b, a, 100)
			}
		}()
	}
	wg.Wait()

	// All bytes back on a; b empty; parent total conserved.
	if a.Used() != start {
		t.Fatalf("a.Used = %d, want %d", a.Used(), start)
	}
	if b.Used() != 0 {
		t.Fatalf("b.Used = %d, want 0", b.Used())
	}
	if parent.Used() != start {
		t.Fatalf("parent.Used = %d, want %d (sum not conserved)", parent.Used(), start)
	}
}

func TestTracker_Transfer_UnderflowClamps(t *testing.T) {
	a := NewTracker("a", 0)
	b := NewTracker("b", 0)
	// a holds nothing; transferring drives a below zero → clamp to 0.
	a.Transfer(a, b, 500)
	if a.Used() != 0 {
		t.Fatalf("underflow source should clamp to 0, got %d", a.Used())
	}
	if b.Used() != 500 {
		t.Fatalf("dest should still receive 500, got %d", b.Used())
	}
}

func TestTracker_PublishOwned_SnapshotAndTotal(t *testing.T) {
	tr := NewTracker("pub", 0)
	tr.PublishOwned(1, 100)
	tr.PublishOwned(2, 250)
	tr.PublishOwned(1, 175) // overwrite, not additive

	if got := tr.OwnedFor(1); got != 175 {
		t.Fatalf("OwnedFor(1) = %d, want 175", got)
	}
	if got := tr.OwnedFor(99); got != 0 {
		t.Fatalf("OwnedFor(unknown) = %d, want 0", got)
	}
	if got := tr.OwnedTotal(); got != 175+250 {
		t.Fatalf("OwnedTotal = %d, want %d", got, 175+250)
	}
	snap := tr.OwnedSnapshot()
	if snap[1] != 175 || snap[2] != 250 {
		t.Fatalf("snapshot = %v", snap)
	}
}

func TestTracker_PublishOwned_ConcurrentNoTear(t *testing.T) {
	tr := NewTracker("pub", 0)
	var wg sync.WaitGroup
	for id := uint64(0); id < 50; id++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tr.PublishOwned(id, int64(id)*1000)
				_ = tr.OwnedFor(id)
			}
		}(id)
	}
	wg.Wait()
	for id := uint64(0); id < 50; id++ {
		if got := tr.OwnedFor(id); got != int64(id)*1000 {
			t.Fatalf("instance %d: got %d, want %d", id, got, int64(id)*1000)
		}
	}
}

func TestTracker_Transfer_Noops(t *testing.T) {
	a := NewTracker("a", 0)
	b := NewTracker("b", 0)
	a.ForceReserve(100)
	a.Transfer(a, b, 0)   // n <= 0: no-op
	a.Transfer(a, a, 50)  // from == to: no-op
	a.Transfer(nil, b, 1) // nil from: no-op
	a.Transfer(a, nil, 1) // nil to: no-op
	if a.Used() != 100 {
		t.Fatalf("a.Used = %d, want 100 (no-ops must not move bytes)", a.Used())
	}
	if b.Used() != 0 {
		t.Fatalf("b.Used = %d, want 0", b.Used())
	}
}

func TestTrackerErrorMessage(t *testing.T) {
	tr := NewTracker("my-query", 100)
	tr.Reserve(50)
	err := tr.Reserve(200)
	if err == nil {
		t.Fatal("expected error")
	}
	errMsg := err.Error()
	if !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("expected ErrMemoryExceeded")
	}
	// Check that error message contains useful info
	if !containsStr(errMsg, "my-query") {
		t.Error("error should contain tracker name")
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && findSubstr(s, substr)
}

func findSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestReserveBlocking_ImmediateSuccess confirms the fast path: when the
// budget has room, ReserveBlocking returns immediately.
func TestReserveBlocking_ImmediateSuccess(t *testing.T) {
	tr := NewTracker("admit", 1000)
	ctx := context.Background()
	start := time.Now()
	if err := tr.ReserveBlocking(ctx, 500, 100*time.Millisecond); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("immediate success should be near-instant, took %v", elapsed)
	}
	if tr.Used() != 500 {
		t.Errorf("expected used=500, got %d", tr.Used())
	}
}

// TestReserveBlocking_WaitsForRelease confirms a blocked reservation
// completes once another caller releases bytes. This is the admission-gate
// pattern: in-flight tasks fill the pool, a new task waits, an in-flight
// task finishes and releases, the new task admits.
func TestReserveBlocking_WaitsForRelease(t *testing.T) {
	tr := NewTracker("admit", 1000)
	if err := tr.Reserve(800); err != nil {
		t.Fatalf("initial reserve: %v", err)
	}

	// Goroutine waits for 700 bytes (would need 1500 used; only 200 free).
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- tr.ReserveBlocking(ctx, 700, 25*time.Millisecond)
	}()

	// Should still be blocked after one poll interval.
	select {
	case err := <-done:
		t.Fatalf("ReserveBlocking should still be blocked, got err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release enough for the waiter to admit.
	tr.Release(600) // 800-600=200 remains, plus 700 incoming = 900 ≤ 1000

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected admission after release, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReserveBlocking should have admitted after release")
	}
	if tr.Used() != 900 {
		t.Errorf("expected used=900 (200+700), got %d", tr.Used())
	}
}

// TestReserveBlocking_RespectsContextCancel confirms that a blocked
// reservation returns ctx.Err promptly when cancelled — important so a
// worker shutdown isn't delayed by a stuck task waiting for a perpetually
// full pool.
func TestReserveBlocking_RespectsContextCancel(t *testing.T) {
	tr := NewTracker("admit", 1000)
	if err := tr.Reserve(1000); err != nil {
		t.Fatalf("initial reserve: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ReserveBlocking(ctx, 100, 25*time.Millisecond)
	}()

	time.Sleep(75 * time.Millisecond) // ensure goroutine entered the wait
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Errorf("expected ctx.Err on cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReserveBlocking should have returned promptly on cancel")
	}
	// Used should be unchanged — no partial reservation should leak.
	if tr.Used() != 1000 {
		t.Errorf("expected used=1000 unchanged, got %d", tr.Used())
	}
}
