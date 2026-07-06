package physical

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLoadGate_ByteBudget: loads pack under the byte budget, block past it,
// and unblock on release.
func TestLoadGate_ByteBudget(t *testing.T) {
	ctx := context.Background()
	g := newLoadGate(100, 32)

	if err := g.acquire(ctx, 60); err != nil {
		t.Fatal(err)
	}
	if err := g.acquire(ctx, 40); err != nil {
		t.Fatal(err)
	}

	// Third acquire exceeds the budget — must block until a release.
	blocked := make(chan error, 1)
	go func() { blocked <- g.acquire(ctx, 10) }()
	select {
	case <-blocked:
		t.Fatal("acquire over budget did not block")
	case <-time.After(50 * time.Millisecond):
	}
	g.release(60)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

// TestLoadGate_OversizedLoadAdmitsAlone: a single load larger than the whole
// budget is admitted when nothing is inflight — progress guarantee.
func TestLoadGate_OversizedLoadAdmitsAlone(t *testing.T) {
	ctx := context.Background()
	g := newLoadGate(100, 32)
	if err := g.acquire(ctx, 500); err != nil {
		t.Fatal(err)
	}
	// A second load must wait for the oversized one.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := g.acquire(ctx2, 1); err == nil {
		t.Fatal("second acquire admitted alongside an over-budget load")
	}
	g.release(500)
	if err := g.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

// TestLoadGate_LaneCap: small loads cannot exceed maxLanes even when the
// byte budget has room.
func TestLoadGate_LaneCap(t *testing.T) {
	ctx := context.Background()
	g := newLoadGate(1<<30, 2)
	if err := g.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := g.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := g.acquire(ctx2, 1); err == nil {
		t.Fatal("third acquire admitted past the lane cap")
	}
}

// TestLoadGate_ConcurrentChurn: hammer the gate from many goroutines and
// verify inflight bytes never exceed the budget (single-oversized excepted)
// and everything completes. Run with -race.
func TestLoadGate_ConcurrentChurn(t *testing.T) {
	ctx := context.Background()
	const budget = 1000
	g := newLoadGate(budget, 8)

	var inflight, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		n := int64(50 + (i%7)*40) // all under budget individually
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				if err := g.acquire(ctx, n); err != nil {
					t.Error(err)
					return
				}
				cur := inflight.Add(n)
				for {
					p := peak.Load()
					if cur <= p || peak.CompareAndSwap(p, cur) {
						break
					}
				}
				inflight.Add(-n)
				g.release(n)
			}
		}(n)
	}
	wg.Wait()
	// Peak can exceed budget by at most one admission's worth (the check
	// is admit-if-fits, so the last admitted load fits by definition —
	// peak should never pass budget at all for under-budget loads).
	if p := peak.Load(); p > budget {
		t.Fatalf("inflight peak %d exceeded budget %d", p, budget)
	}
}
