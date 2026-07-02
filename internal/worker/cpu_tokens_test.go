package worker

import (
	"sync"
	"testing"
)

func TestCPUTokens_TryAcquireSemantics(t *testing.T) {
	tok := newCPUTokens(4)

	if got := tok.TryAcquire(0); got != 0 {
		t.Errorf("TryAcquire(0) = %d, want 0", got)
	}
	if got := tok.TryAcquire(-1); got != 0 {
		t.Errorf("TryAcquire(-1) = %d, want 0", got)
	}
	// Partial grant when the ask exceeds what's left.
	if got := tok.TryAcquire(3); got != 3 {
		t.Fatalf("TryAcquire(3) = %d, want 3", got)
	}
	if got := tok.TryAcquire(3); got != 1 {
		t.Fatalf("TryAcquire(3) after 3 held = %d, want 1 (partial)", got)
	}
	// Exhausted: non-blocking zero, not a wait.
	if got := tok.TryAcquire(1); got != 0 {
		t.Fatalf("TryAcquire(1) at capacity = %d, want 0", got)
	}
	tok.Release(4)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse after full release = %d, want 0", got)
	}
	if got := tok.TryAcquire(10); got != 4 {
		t.Fatalf("TryAcquire(10) on empty pool = %d, want capacity 4", got)
	}
}

func TestCPUTokens_ZeroCapacityAndNil(t *testing.T) {
	tok := newCPUTokens(0)
	if got := tok.TryAcquire(5); got != 0 {
		t.Errorf("zero-capacity TryAcquire = %d, want 0", got)
	}
	neg := newCPUTokens(-3)
	if got := neg.TryAcquire(1); got != 0 {
		t.Errorf("negative-capacity TryAcquire = %d, want 0", got)
	}
	var nilTok *cpuTokens
	if got := nilTok.TryAcquire(1); got != 0 {
		t.Errorf("nil TryAcquire = %d, want 0", got)
	}
	nilTok.Release(1) // must not panic
	if nilTok.InUse() != 0 || nilTok.Capacity() != 0 {
		t.Error("nil accessors should report 0")
	}
}

// TestCPUTokens_ConcurrentNeverExceedsCapacity hammers the pool from many
// goroutines and asserts the invariant the whole design leans on: the sum of
// outstanding grants never exceeds capacity, and everything returns to zero.
// Run under -race.
func TestCPUTokens_ConcurrentNeverExceedsCapacity(t *testing.T) {
	const capacity = 6
	tok := newCPUTokens(capacity)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				got := tok.TryAcquire(3)
				if used := tok.InUse(); used > capacity {
					t.Errorf("InUse %d exceeds capacity %d", used, capacity)
				}
				tok.Release(got)
			}
		}()
	}
	wg.Wait()
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse after all released = %d, want 0", got)
	}
}
