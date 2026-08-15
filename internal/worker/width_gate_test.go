package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCPUTokens_WaiterFIFOAndPriority(t *testing.T) {
	tok := newCPUTokens(1)
	if got := tok.TryAcquire(1); got != 1 {
		t.Fatalf("TryAcquire = %d, want 1", got)
	}

	w1 := tok.enqueueWaiter()
	w2 := tok.enqueueWaiter()
	select {
	case <-w1.ch:
		t.Fatal("w1 granted while pool exhausted")
	default:
	}
	// Queued waiters block TryAcquire even though nothing is free anyway;
	// the load-bearing case is after Release: the token must go to w1, not
	// to a TryAcquire racing in.
	tok.Release(1)
	select {
	case <-w1.ch:
	case <-time.After(time.Second):
		t.Fatal("w1 not granted after Release")
	}
	if got := tok.TryAcquire(1); got != 0 {
		t.Fatalf("TryAcquire = %d, want 0 while w2 still queued", got)
	}
	tok.cancelWaiter(w1) // held a granted token: cancel returns it → w2 gets it
	select {
	case <-w2.ch:
	case <-time.After(time.Second):
		t.Fatal("w2 not granted after w1's token returned")
	}
	tok.cancelWaiter(w2)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 after both waiters cancelled", got)
	}
	if got := tok.TryAcquire(1); got != 1 {
		t.Fatalf("TryAcquire = %d, want 1 with queue drained", got)
	}
}

func TestCPUTokens_CancelUngrantedWaiter(t *testing.T) {
	tok := newCPUTokens(1)
	tok.TryAcquire(1)
	w := tok.enqueueWaiter()
	tok.cancelWaiter(w)
	tok.Release(1)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 (cancelled waiter must not hold a grant)", got)
	}
}

func TestCPUTokens_EnqueueGrantsImmediatelyWhenFree(t *testing.T) {
	tok := newCPUTokens(2)
	w := tok.enqueueWaiter()
	select {
	case <-w.ch:
	default:
		t.Fatal("waiter not granted despite free capacity")
	}
	tok.cancelWaiter(w)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0", got)
	}
}

func TestWidthGate_BaselineAlwaysClaimable(t *testing.T) {
	// Zero-capacity pool: only the fragment baseline exists. A serial
	// claim/yield cycle must never block.
	g := newWidthGate(newCPUTokens(0))
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		s, err := g.claim(ctx)
		if err != nil || s != slotBaseline {
			t.Fatalf("claim %d = (%v, %v), want baseline", i, s, err)
		}
		g.yield(s)
	}
}

func TestWidthGate_TokenThenBaselineThenBlock(t *testing.T) {
	tok := newCPUTokens(1)
	g := newWidthGate(tok)
	ctx := context.Background()

	s1, _ := g.claim(ctx) // baseline
	s2, _ := g.claim(ctx) // pool token
	if s1 != slotBaseline || s2 != slotToken {
		t.Fatalf("claims = %v, %v; want baseline, token", s1, s2)
	}

	// Third claim parks until a slot frees.
	done := make(chan widthSlot, 1)
	go func() {
		s, err := g.claim(ctx)
		if err != nil {
			t.Errorf("claim: %v", err)
		}
		done <- s
	}()
	select {
	case s := <-done:
		t.Fatalf("third claim returned %v with both slots held", s)
	case <-time.After(50 * time.Millisecond):
	}
	g.yield(s2)
	select {
	case s := <-done:
		g.yield(s)
	case <-time.After(time.Second):
		t.Fatal("parked claim not woken by yield")
	}
	g.yield(s1)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("tokens leaked: %d", got)
	}
}

func TestWidthGate_ClaimHonorsContextCancel(t *testing.T) {
	tok := newCPUTokens(0)
	g := newWidthGate(tok)
	base, _ := g.claim(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := g.claim(ctx)
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("claim returned nil error after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("claim did not return after cancel")
	}
	g.yield(base)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("tokens leaked: %d", got)
	}
}

// TestConsumeMorsels_YieldsWidthWhileStarved is the regression test for the
// 2026-07-21 decode-starvation diagnosis: consumers parked on an empty
// dispenser must hold ZERO pool tokens, so a concurrent token taker (decode-
// ahead standing in as `TryAcquire`) can widen. Under the legacy fixed-width
// shape the fragment held its width tokens through exactly this stall and
// the pool stayed exhausted.
func TestConsumeMorsels_YieldsWidthWhileStarved(t *testing.T) {
	const capacity = 4
	tok := newCPUTokens(capacity)
	g := newWidthGate(tok)
	d := newMorselDispenser(4, false)

	var processed atomic.Int64
	release := make(chan struct{})
	var wg sync.WaitGroup
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = consumeMorsels(ctx, d, g, func(m morsel) error {
				<-release // hold the slot while "processing"
				processed.Add(1)
				m.retire()
				return nil
			}, nil)
		}()
	}

	// Feed one round so every consumer processes concurrently, then starve.
	for i := 0; i < 4; i++ {
		d.ch <- morsel{b: nil, retire: func() {}}
	}
	deadline := time.After(2 * time.Second)
	for tok.InUse() != capacity-1 { // 4 active = 1 baseline + 3 tokens
		select {
		case <-deadline:
			t.Fatalf("active width tokens = %d, want %d", tok.InUse(), capacity-1)
		case <-time.After(time.Millisecond):
		}
	}
	close(release)
	for processed.Load() != 4 {
		select {
		case <-deadline:
			t.Fatalf("processed = %d, want 4", processed.Load())
		case <-time.After(time.Millisecond):
		}
	}
	// Channel now empty: every consumer must yield its slot while parked.
	for tok.InUse() != 0 {
		select {
		case <-deadline:
			t.Fatalf("starved consumers still hold %d tokens (legacy shape)", tok.InUse())
		case <-time.After(time.Millisecond):
		}
	}
	// The decode-side probe: a non-blocking taker gets the full pool.
	if got := tok.TryAcquire(capacity); got != capacity {
		t.Fatalf("TryAcquire during consumer starvation = %d, want %d", got, capacity)
	}
	tok.Release(capacity)

	close(d.ch)
	wg.Wait()
	if got := tok.InUse(); got != 0 {
		t.Fatalf("tokens leaked after close: %d", got)
	}
}

// TestConsumeMorsels_SerialUnderZeroCapacity: with an empty pool the loop
// still makes progress on the baseline slot alone.
func TestConsumeMorsels_SerialUnderZeroCapacity(t *testing.T) {
	tok := newCPUTokens(0)
	g := newWidthGate(tok)
	d := newMorselDispenser(2, false)

	var processed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = consumeMorsels(context.Background(), d, g, func(m morsel) error {
				processed.Add(1)
				m.retire()
				return nil
			}, nil)
		}()
	}
	for i := 0; i < 32; i++ {
		d.ch <- morsel{b: nil, retire: func() {}}
	}
	close(d.ch)
	wg.Wait()
	if got := processed.Load(); got != 32 {
		t.Fatalf("processed = %d, want 32", got)
	}
}

// fakeTokenDonor records §2.2 donation offers and accepts or refuses them.
type fakeTokenDonor struct {
	accept bool
	calls  atomic.Int64
}

func (f *fakeTokenDonor) tryDonateToken() bool {
	f.calls.Add(1)
	return f.accept
}

// TestWidthGate_YieldDonatesPoolTokenToProducer pins the §2.2 donation
// routing: a yielded pool token transfers to an accepting donor WITHOUT a
// pool release, a refusing donor falls back to the pool, and the baseline
// slot is never offered.
func TestWidthGate_YieldDonatesPoolTokenToProducer(t *testing.T) {
	tok := newCPUTokens(1)
	g := newWidthGate(tok)
	donor := &fakeTokenDonor{accept: true}
	g.donor = donor
	ctx := context.Background()

	base, _ := g.claim(ctx)
	s, _ := g.claim(ctx)
	if base != slotBaseline || s != slotToken {
		t.Fatalf("claims = %v, %v; want baseline, token", base, s)
	}
	g.yield(s)
	if got := donor.calls.Load(); got != 1 {
		t.Fatalf("donor offers = %d, want 1", got)
	}
	if got := tok.InUse(); got != 1 {
		t.Fatalf("InUse = %d, want 1 (accepted donation must not also release)", got)
	}
	if got := g.donations.Load(); got != 1 {
		t.Fatalf("donations = %d, want 1", got)
	}
	tok.Release(1) // the donor's decode worker releases in production

	// Baseline yields never consult the donor.
	g.yield(base)
	if got := donor.calls.Load(); got != 1 {
		t.Fatalf("donor consulted on baseline yield (calls = %d)", got)
	}

	// A refusing donor falls back to the pool.
	donor.accept = false
	if s, _ = g.claim(ctx); s != slotBaseline {
		t.Fatalf("claim = %v, want baseline back", s)
	}
	s2, _ := g.claim(ctx)
	if s2 != slotToken {
		t.Fatalf("claim = %v, want token", s2)
	}
	g.yield(s2)
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 (refused donation must release)", got)
	}
	g.yield(s)
}

// TestWidthGate_DonationKillSwitch pins WADJET_WIDTH_DONATE=0: the donor is
// never consulted and pool tokens yield straight to the pool.
func TestWidthGate_DonationKillSwitch(t *testing.T) {
	orig := widthDonate
	widthDonate = false
	defer func() { widthDonate = orig }()

	tok := newCPUTokens(1)
	g := newWidthGate(tok)
	donor := &fakeTokenDonor{accept: true}
	g.donor = donor
	ctx := context.Background()
	base, _ := g.claim(ctx)
	s, _ := g.claim(ctx)
	g.yield(s)
	if donor.calls.Load() != 0 {
		t.Fatal("donor consulted with the kill switch set")
	}
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0", got)
	}
	g.yield(base)
}
