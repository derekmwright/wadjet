// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"errors"
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

// TestLoadGate_AConsumerWaitsOnlyForBytesTheWorkersWillRelease is the consumer's
// admission (#1706): over a full gate it waits while every holder's bytes come
// back without it — each row group the holder still owes is a read-ahead a
// worker is decoding — and takes its bytes at once, past the budget and the
// lane cap, when a holder owes a row group only the consumer decodes. Either
// way its admission is released like any other.
func TestLoadGate_AConsumerWaitsOnlyForBytesTheWorkersWillRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // a waiter left behind by a failure exits
	g := newLoadGate(100, 1)
	held := &fileSlot{}
	held.rgRemaining.Store(2)
	held.ahead.Store(2) // both row groups it owes are being decoded by workers
	if err := g.acquireFor(ctx, 100, held, nil); err != nil {
		t.Fatal(err)
	}

	res := make(chan error, 1)
	consumer := &fileSlot{}
	go func() { res <- g.acquireDemand(ctx, 50, consumer) }()
	select {
	case err := <-res:
		t.Fatalf("the consumer did not wait for bytes the workers will release (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	// One read-ahead finishes and the holder's last row group is handed over
	// undecoded: it now owes a row group only the consumer decodes. A release
	// elsewhere is what wakes a waiter; this one is a lane nobody else holds.
	held.rgRemaining.Store(1)
	held.ahead.Store(0)
	g.mu.Lock()
	g.take(1, nil)
	g.mu.Unlock()
	g.release(1)
	select {
	case err := <-res:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer still waits for bytes only it can release")
	}
	if g.inflight != 150 || g.lanes != 2 {
		t.Fatalf("after the consumer's admission: inflight=%d lanes=%d, want 150 and 2", g.inflight, g.lanes)
	}
	g.releaseFor(50, consumer)
	g.releaseFor(100, held)
	if g.inflight != 0 || g.lanes != 0 || len(g.held) != 0 {
		t.Fatalf("after the releases: inflight=%d lanes=%d held=%d, want 0", g.inflight, g.lanes, len(g.held))
	}
}

// TestLoadGate_AConsumerWaitsWhenTheWorkersAloneFreeEnough: one holder owes the
// consumer a row group, another will be released by the workers. When the
// second's release makes room for the consumer's load beside the first, the
// consumer waits for it — the base's wait, which ends without it — and is
// admitted within the budget; when it would not, the consumer does not wait.
func TestLoadGate_AConsumerWaitsWhenTheWorkersAloneFreeEnough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owes := func(n int64) *fileSlot { s := &fileSlot{}; s.rgRemaining.Store(1); return s }
	free := func() *fileSlot { s := &fileSlot{}; s.rgRemaining.Store(1); s.ahead.Store(1); return s }

	g := newLoadGate(100, loadGateMaxLanes)
	dep, ind := owes(20), free()
	if err := g.acquireFor(ctx, 20, dep, nil); err != nil {
		t.Fatal(err)
	}
	if err := g.acquireFor(ctx, 80, ind, nil); err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	consumer := &fileSlot{}
	go func() { res <- g.acquireDemand(ctx, 50, consumer) }()
	select {
	case err := <-res:
		t.Fatalf("the consumer took 50 past the gate (err=%v) where the workers' release of 80 makes room beside 20", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.releaseFor(80, ind)
	select {
	case err := <-res:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer still waits after the room came back")
	}
	if g.inflight != 70 {
		t.Fatalf("inflight = %d after the wait, want 70 (within the budget)", g.inflight)
	}
	g.releaseFor(50, consumer)

	// 60 owed to the consumer: no release by the workers makes room for 50.
	dep2 := owes(60)
	if err := g.acquireFor(ctx, 40, dep2, nil); err != nil {
		t.Fatal(err)
	}
	ind2 := free()
	if err := g.acquireFor(ctx, 40, ind2, nil); err != nil {
		t.Fatal(err)
	}
	go func() { res <- g.acquireDemand(ctx, 50, consumer) }()
	select {
	case err := <-res:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer waits although 20 + 40 owed to it leave no room for 50 whatever the workers release")
	}
}

// TestLoadGate_AReadAheadYieldsOnlyWhenTheConsumerNeedsItsSlotAndItsRoom: a
// read-ahead waiting in a full gate keeps waiting when nobody comes for its
// slot, and when the consumer comes for it while the room it waits for will
// come back from the workers alone (it is then admitted on the release). It
// yields, admitting nothing, only when the consumer comes for its slot and the
// room is held for row groups only the consumer decodes.
func TestLoadGate_AReadAheadYieldsOnlyWhenTheConsumerNeedsItsSlotAndItsRoom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	free := &fileSlot{}
	free.rgRemaining.Store(1)
	free.ahead.Store(1)
	owes := &fileSlot{}
	owes.rgRemaining.Store(1)

	// Room the workers will free: the consumer coming does not stop the wait.
	g := newLoadGate(100, 1)
	if err := g.acquireFor(ctx, 100, free, nil); err != nil {
		t.Fatal(err)
	}
	wanted := make(chan struct{})
	res := make(chan error, 1)
	waiter := &fileSlot{}
	go func() { res <- g.acquireFor(ctx, 10, waiter, wanted) }()
	close(wanted)
	select {
	case err := <-res:
		t.Fatalf("the read-ahead stopped (%v) although the workers alone free its room", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.releaseFor(100, free)
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("admission after the release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read-ahead was not admitted when the room came back")
	}
	g.releaseFor(10, waiter)

	// Room owed to the consumer: waiting goes on while nobody comes for the
	// slot, and stops, admitting nothing, when the consumer does.
	if err := g.acquireFor(ctx, 100, owes, nil); err != nil {
		t.Fatal(err)
	}
	wanted = make(chan struct{})
	go func() { res <- g.acquireFor(ctx, 10, waiter, wanted) }()
	select {
	case err := <-res:
		t.Fatalf("the read-ahead stopped (%v) before the consumer came for its slot", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(wanted)
	select {
	case err := <-res:
		if !errors.Is(err, errLoadGateYield) {
			t.Fatalf("the read-ahead returned %v, want errLoadGateYield", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read-ahead waits on for room only the consumer, which waits for its slot, can free")
	}
	g.releaseFor(100, owes)
	if g.inflight != 0 || g.lanes != 0 || len(g.held) != 0 {
		t.Fatalf("after the release: inflight=%d lanes=%d held=%d, want 0 — the yield must admit nothing",
			g.inflight, g.lanes, len(g.held))
	}
}
