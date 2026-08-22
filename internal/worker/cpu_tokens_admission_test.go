package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// dryConsumerChurn models the regime the SF100 2026-08-22 window measured
// around this pool: morsel consumers that hold a token only while
// processing, find the morsel ring EMPTY when they finish (Σdry 16 794 s,
// effective width 2.88 of 15), and immediately re-queue for the next
// morsel. Every release is therefore re-demanded by a queued consumer,
// which is the closing edge of the loop that starves decode — under the
// old strict-FIFO rule the freed token goes to the queue head and a
// decoder's TryAcquire never observes a free token at all.
//
// Waiter grants are observed through w.ch (closed under the pool lock),
// never through w.granted, which is the pool's own field.
type dryConsumerChurn struct {
	tok    *cpuTokens
	held   int
	queued []*tokenWaiter
}

// newDryConsumerChurn takes the whole pool as processing consumers and
// parks queued ones behind them.
func newDryConsumerChurn(tb testing.TB, tok *cpuTokens, queued int) *dryConsumerChurn {
	tb.Helper()
	c := &dryConsumerChurn{tok: tok}
	for i := int64(0); i < tok.Capacity(); i++ {
		if tok.TryAcquire(1) != 1 {
			tb.Fatalf("consumer %d could not take a token from an idle pool", i)
		}
		c.held++
	}
	for i := 0; i < queued; i++ {
		c.queued = append(c.queued, tok.enqueueWaiter(false))
	}
	return c
}

// cycle runs one consumer turnover: a processing consumer finishes its
// morsel and releases, then finds the ring empty and re-queues dry; any
// consumer whose grant landed starts processing.
func (c *dryConsumerChurn) cycle() {
	if c.held > 0 {
		c.tok.Release(1)
		c.held--
	}
	c.queued = append(c.queued, c.tok.enqueueWaiter(false))
	kept := c.queued[:0]
	for _, w := range c.queued {
		select {
		case <-w.ch: // granted: this consumer is processing again
			c.held++
		default:
			kept = append(kept, w)
		}
	}
	c.queued = kept
}

// stop returns everything the modelled consumers hold.
func (c *dryConsumerChurn) stop() {
	for _, w := range c.queued {
		c.tok.cancelWaiter(w)
	}
	c.queued = nil
	for ; c.held > 0; c.held-- {
		c.tok.Release(1)
	}
}

// TestCPUTokens_DecodeAdmissionUnderDryConsumerQueue is the regression for
// the admission inversion (SF100 2026-08-22 §7.3). With the pool full, dry
// morsel consumers queued, and a decode worker parked on a failed
// acquisition, the OLD policy hands every release to the consumer FIFO and
// the decoder never gets a token however long the loop runs — even though
// only a decoder can refill the ring those consumers are waiting on. The
// new policy admits it on the first turnover.
func TestCPUTokens_DecodeAdmissionUnderDryConsumerQueue(t *testing.T) {
	run := func(t *testing.T, policyOn bool) (cyclesToAdmit int) {
		t.Helper()
		prev := decodeAdmission.Set(policyOn)
		defer decodeAdmission.Set(prev)

		tok := newCPUTokens(8)
		c := newDryConsumerChurn(t, tok, 4)
		defer c.stop()

		// The decoder's own sequence: fail, register the stall, park. (The
		// scan iterator parks on the decode window's condvar and retries on
		// every delivery; the retry is the loop body below.)
		if got := tok.tryAcquireDecode(1); got != 0 {
			t.Fatalf("decoder acquired %d tokens from a full pool", got)
		}
		tok.decodeStallBegin()
		defer tok.decodeStallEnd()

		for i := 1; i <= 64; i++ {
			c.cycle()
			if got := tok.tryAcquireDecode(1); got == 1 {
				tok.releaseDecode(1)
				return i
			}
		}
		return -1
	}

	if got := run(t, false); got != -1 {
		t.Fatalf("old policy admitted decode after %d cycles; the inversion must reproduce", got)
	}
	if got := run(t, true); got != 1 {
		t.Fatalf("new policy admitted decode after %d cycles, want 1", got)
	}
}

// TestCPUTokens_FedConsumersKeepPriorityPastTheFloor pins the concern the
// original strict-priority rule was written for: a consumer with morsels
// QUEUED BEHIND the one in its hand outranks decode past the reserved
// floor. Only the dry case inverts.
func TestCPUTokens_FedConsumersKeepPriorityPastTheFloor(t *testing.T) {
	tok := newCPUTokens(8)
	reserve := tok.reserve
	if reserve < 2 {
		t.Fatalf("reserve = %d, want >= 2 for capacity 8", reserve)
	}
	// Decode fills its floor while the pool is idle.
	for i := int64(0); i < reserve; i++ {
		if tok.tryAcquireDecode(1) != 1 {
			t.Fatalf("decode could not take floor token %d", i)
		}
	}
	// Consumers take the rest and one more parks FED.
	for i := reserve; i < tok.Capacity(); i++ {
		if tok.TryAcquire(1) != 1 {
			t.Fatalf("consumer could not take token %d", i)
		}
	}
	consumersHeld := tok.Capacity() - reserve

	// A FED consumer parks and a decoder stalls past the floor: the next
	// release belongs to the consumer, exactly as the original rule says.
	fed := tok.enqueueWaiter(true)
	if got := tok.tryAcquireDecode(1); got != 0 {
		t.Fatalf("decode acquired %d past its floor with a fed consumer queued", got)
	}
	tok.decodeStallBegin()
	tok.Release(1)
	consumersHeld--
	select {
	case <-fed.ch: // the grant is the fed consumer's token now
		consumersHeld++
	default:
		t.Fatal("fed consumer was not granted the released token")
	}
	tok.Release(1)
	consumersHeld--

	// Same state, but the queued consumer is DRY: the release is held back
	// for the stalled decoder and decode takes it past its floor.
	dry := tok.enqueueWaiter(false)
	select {
	case <-dry.ch:
		t.Fatal("dry consumer was granted a token a stalled decoder was owed")
	default:
	}
	if got := tok.tryAcquireDecode(1); got != 1 {
		t.Fatalf("decode got %d tokens with only dry consumers queued, want 1", got)
	}

	// Teardown: the decoder finishes and stops waiting, which releases the
	// holdback and lets the dry consumer through.
	tok.releaseDecode(1)
	tok.decodeStallEnd()
	select {
	case <-dry.ch:
		consumersHeld++
	default:
		t.Fatal("dry consumer still blocked after decode demand cleared")
	}
	for ; consumersHeld > 0; consumersHeld-- {
		tok.Release(1)
	}
	for i := int64(0); i < reserve; i++ {
		tok.releaseDecode(1)
	}
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0", got)
	}
	if got := tok.decodeInUse(); got != 0 {
		t.Fatalf("decodeInUse = %d, want 0", got)
	}
}

// TestCPUTokens_NoHoldbackWithoutDecodeDemand: the floor is demand-driven,
// so a fragment with no decode-ahead behind it pays nothing — every token
// still reaches queued consumers.
func TestCPUTokens_NoHoldbackWithoutDecodeDemand(t *testing.T) {
	tok := newCPUTokens(4)
	for i := 0; i < 4; i++ {
		tok.TryAcquire(1)
	}
	ws := make([]*tokenWaiter, 0, 4)
	for i := 0; i < 4; i++ {
		ws = append(ws, tok.enqueueWaiter(false))
	}
	for i := 0; i < 4; i++ {
		tok.Release(1)
	}
	for i, w := range ws {
		select {
		case <-w.ch:
		default:
			t.Fatalf("waiter %d not granted with no decode demand registered", i)
		}
	}
	if _, _, _, _, holdbacks := tok.admissionStats(); holdbacks != 0 {
		t.Fatalf("holdbacks = %d, want 0 without registered decode demand", holdbacks)
	}
	for _, w := range ws {
		tok.cancelWaiter(w)
	}
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0", got)
	}
}

// TestCPUTokens_ReserveSizing pins the floor's derivation (~20 % of the
// pool, capped below capacity) rather than the constant itself.
func TestCPUTokens_ReserveSizing(t *testing.T) {
	for _, tc := range []struct{ capacity, want int64 }{
		{0, 0}, {1, 0}, {2, 1}, {4, 1}, {8, 2}, {14, 3}, {16, 4}, {30, 6},
	} {
		if got := decodeReserveFor(tc.capacity); got != tc.want {
			t.Errorf("decodeReserveFor(%d) = %d, want %d", tc.capacity, got, tc.want)
		}
		if tc.capacity > 0 && decodeReserveFor(tc.capacity) >= tc.capacity {
			t.Errorf("reserve for capacity %d leaves consumers nothing", tc.capacity)
		}
	}
}

// TestCPUTokens_ConcurrentAdmissionBalance hammers both classes plus the
// FIFO and donation-style class transfer: the pool must never overcommit
// and both counters must land back at zero. Race-detector coverage for the
// admission paths.
func TestCPUTokens_ConcurrentAdmissionBalance(t *testing.T) {
	const capacity = 6
	tok := newCPUTokens(capacity)
	var peak atomic.Int64
	observe := func() {
		if u := tok.InUse(); u > peak.Load() {
			peak.Store(u)
		}
	}
	stop := make(chan struct{})
	time.AfterFunc(300*time.Millisecond, func() { close(stop) })
	var wg sync.WaitGroup
	// Decoders.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if tok.tryAcquireDecode(1) == 1 {
					observe()
					tok.releaseDecode(1)
					continue
				}
				tok.decodeStallBegin()
				time.Sleep(time.Microsecond)
				tok.decodeStallEnd()
			}
		}()
	}
	// Consumers: fast path, then FIFO park, alternating fed/dry.
	for i := 0; i < 6; i++ {
		fed := i%2 == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if tok.TryAcquire(1) == 1 {
					observe()
					tok.Release(1)
					continue
				}
				w := tok.enqueueWaiter(fed)
				select {
				case <-w.ch: // grant used: the caller owns the token now
					observe()
					tok.Release(1)
				case <-ctx.Done():
					tok.cancelWaiter(w)
					return
				}
			}
		}()
	}
	// Donation-style class transfer: a consumer token reclassified as
	// decode and released on the decode side.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if tok.TryAcquire(1) == 1 {
				tok.adoptDecode(1)
				observe()
				tok.releaseDecode(1)
			}
		}
	}()
	wg.Wait()
	if got := tok.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 after the hammer", got)
	}
	if got := tok.decodeInUse(); got != 0 {
		t.Fatalf("decodeInUse = %d, want 0 after the hammer", got)
	}
	if got := peak.Load(); got > capacity {
		t.Fatalf("pool overcommitted: peak InUse = %d, capacity %d", got, capacity)
	}
}

// TestScanDecodeAhead_AdmittedPastQueuedConsumers is the end-to-end arm:
// a real decode-ahead scan against a pool that is saturated by churning
// dry morsel consumers. Rows must be identical under both policies (this
// changes scheduling only), but the new policy must actually admit the
// decoder past the consumer queue — decode_bypasses > 0 — where the old
// one leaves it on the cursor-exempt serial floor.
func TestScanDecodeAhead_AdmittedPastQueuedConsumers(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 3, 4, 64)

	run := func(t *testing.T, policyOn bool) (bypasses, stalls int64) {
		t.Helper()
		prev := decodeAdmission.Set(policyOn)
		defer decodeAdmission.Set(prev)

		e := &Executor{store: store, spillDir: t.TempDir()}
		e.SetScanDecodeAhead(true, 0)
		e.cpuTokens = newCPUTokens(4)

		c := newDryConsumerChurn(t, e.cpuTokens, 3)
		done := make(chan struct{})
		var churn sync.WaitGroup
		churn.Add(1)
		go func() {
			defer churn.Done()
			for {
				select {
				case <-done:
					c.stop()
					return
				default:
				}
				c.cycle()
				time.Sleep(50 * time.Microsecond)
			}
		}()

		src := newCachedFileStreamSource(e, "", "b", keys)
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		sum, rows := drainValSum(t, ctx, src)
		if err := src.Close(); err != nil {
			t.Fatal(err)
		}
		close(done)
		churn.Wait()

		if sum != wantSum || rows != 3*4*64 {
			t.Fatalf("policy=%v: sum=%d rows=%d, want sum=%d rows=%d", policyOn, sum, rows, wantSum, 3*4*64)
		}
		if held := e.cpuTokens.InUse(); held != 0 {
			t.Fatalf("policy=%v: tokens in use after the scan = %d, want 0", policyOn, held)
		}
		if held := e.cpuTokens.decodeInUse(); held != 0 {
			t.Fatalf("policy=%v: decode tokens in use after the scan = %d, want 0", policyOn, held)
		}
		_, _, _, _, bypasses = e.CPUTokenAdmissionStats()
		_, _, _, stalls, _ = e.ScanDecodeAheadStats()
		return bypasses, stalls
	}

	oldBypasses, oldStalls := run(t, false)
	if oldBypasses != 0 {
		t.Fatalf("old policy recorded %d decode bypasses; it has no such path", oldBypasses)
	}
	if oldStalls == 0 {
		t.Fatal("old policy: decode never token-stalled, so the fixture does not exercise admission")
	}
	newBypasses, _ := run(t, true)
	if newBypasses == 0 {
		t.Fatal("new policy: decode was never admitted past the queued consumers")
	}
}

// BenchmarkDecodeAdmissionUnderDryConsumers is the contended micro-arm the
// SF100 window cannot be run locally for: one decode-ahead scan against a
// pool saturated by churning DRY morsel consumers (dryConsumerChurn), with
// the admission policy on and off. It reports the decoder's token stall
// directly — token_stall_ms/op is the SF100 field this change is aimed at
// (41.6 % of decoder wall there), and decode_bypasses/op counts the
// admissions the old policy refused.
func BenchmarkDecodeAdmissionUnderDryConsumers(b *testing.B) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, _ := writeMultiGroupFixture(b, store, "b", 4, 6, 256)

	arm := func(b *testing.B, policyOn bool) {
		prev := decodeAdmission.Set(policyOn)
		defer decodeAdmission.Set(prev)
		var stallNs, bypasses int64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			e := &Executor{store: store, spillDir: b.TempDir()}
			e.SetScanDecodeAhead(true, 0)
			e.cpuTokens = newCPUTokens(4)
			c := newDryConsumerChurn(b, e.cpuTokens, 3)
			done := make(chan struct{})
			var churn sync.WaitGroup
			churn.Add(1)
			go func() {
				defer churn.Done()
				for {
					select {
					case <-done:
						c.stop()
						return
					default:
					}
					c.cycle()
					time.Sleep(20 * time.Microsecond)
				}
			}()
			src := newCachedFileStreamSource(e, "", "b", keys)
			if err := src.Init(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			drainValSum(b, ctx, src)
			b.StopTimer()
			if err := src.Close(); err != nil {
				b.Fatal(err)
			}
			close(done)
			churn.Wait()
			_, _, tokenNs, _ := e.ScanDecodeAheadStallNs()
			stallNs += tokenNs
			_, _, _, byp, _ := e.CPUTokenAdmissionStats()
			bypasses += byp
			b.StartTimer()
		}
		b.StopTimer()
		if b.N > 0 {
			b.ReportMetric(float64(stallNs)/float64(b.N)/1e6, "token_stall_ms/op")
			b.ReportMetric(float64(bypasses)/float64(b.N), "decode_bypasses/op")
		}
	}
	b.Run("policy=off", func(b *testing.B) { arm(b, false) })
	b.Run("policy=on", func(b *testing.B) { arm(b, true) })
}
