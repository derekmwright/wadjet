package worker

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestClaimForcedGCSlot verifies the per-task forced-GC coalescing: at
// most one claim per gap, CAS-safe under concurrent task completions.
// Regression test for the stage-drain STW train (2026-08-14 validation
// run: 40 task completions in one minute, each forcing a synchronous
// runtime.GC before this guard existed).
func TestClaimForcedGCSlot(t *testing.T) {
	var last atomic.Int64
	base := time.Unix(1000, 0)

	if !claimForcedGCSlot(&last, base, 2*time.Second) {
		t.Fatal("first claim must succeed")
	}
	if claimForcedGCSlot(&last, base.Add(500*time.Millisecond), 2*time.Second) {
		t.Fatal("claim inside the gap must fail")
	}
	if claimForcedGCSlot(&last, base.Add(1900*time.Millisecond), 2*time.Second) {
		t.Fatal("claim just inside the gap must fail")
	}
	if !claimForcedGCSlot(&last, base.Add(2100*time.Millisecond), 2*time.Second) {
		t.Fatal("claim after the gap must succeed")
	}
}

// TestClaimForcedGCSlot_Concurrent: a burst of simultaneous completions
// must yield exactly one claim.
func TestClaimForcedGCSlot_Concurrent(t *testing.T) {
	var last atomic.Int64
	last.Store(time.Unix(1000, 0).UnixNano())
	now := time.Unix(1005, 0)

	var claims atomic.Int64
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			if claimForcedGCSlot(&last, now, 2*time.Second) {
				claims.Add(1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if got := claims.Load(); got != 1 {
		t.Fatalf("want exactly 1 claim from concurrent burst, got %d", got)
	}
}
