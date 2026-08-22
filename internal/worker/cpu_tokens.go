package worker

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// cpuTokens is a worker-wide counting semaphore over EXTRA compute
// goroutines (morsel-driven execution, docs/design/morsel-execution.md §4.2).
// Every task's first pipeline goroutine is free — the serial baseline is
// always allowed — and only additional parallel consumers take tokens, so
// Σ(extra widths) across concurrent tasks never schedules more compute
// goroutines than the reserved core budget.
//
// TryAcquire is non-blocking BY DESIGN: a fragment or decode worker that
// cannot get tokens degrades to a narrower width instead of waiting.
// Blocking here would recreate the parked-waiter admission shape rejected in
// project_admission_control_rejected_2026-05-18 — a goroutine holding
// upstream resources while gated on capacity that only its own progress
// would release.
//
// enqueueWaiter is the one deliberate exception (work-conserving width,
// §4.2.1): a morsel consumer that already HOLDS work may park for a token,
// because the capacity it waits on is released by OTHER goroutines' bounded
// progress (per-row-group decode holds, other consumers' per-morsel holds)
// and every fragment's baseline slot guarantees a token-free consumer is
// always runnable — the waiter's own progress is never what frees the pool.
//
// # Two admission classes
//
// Waiters are FIFO. They used to take STRICT priority over every
// TryAcquire, on the rule "a queued consumer holds an admitted morsel, so
// feeding it beats widening decode". That rule is right for a FULL morsel
// ring and wrong for an EMPTY one, and the SF100 window of 2026-08-22
// (3 workers × 4 runs × 3 arms, consistent in all 12) measured the empty
// regime as the steady state:
//
//   - dispenser: Σdry 16 794 s vs Σwidth_wait 3 120 s vs producer_wait 0.0 s;
//     effective consumer width 2.88 of 15, consumers parked 41 % of the time;
//   - decode side: ring full (window_full_ms) only 2.9 % of decode_ms, while
//     scan token_stall_ms was 41.6 % of decoder wall (2 540 s vs 3 220 s) and
//     shuffle token stall 66 %;
//   - the decoders are CPU-bound while they are allowed to run (decode_ms/4
//     ≈ 805 CPU-s/run against ≈ 747 CPU-s of decode frames in the profile),
//     and I/O is not the constraint (filePrefetcher.take = 0.26 % of block).
//
// That is a closed loop, and every term of it was measured: decode is shut
// out of tokens → the morsel ring drains → consumers go dry and queue for
// tokens → any queued consumer keeps decode's TryAcquire at 0 → decode
// stays shut out. Feeding a queued consumer when the ring behind it is
// empty buys nothing; only a decoder can refill it.
//
// So decode-ahead is a first-class admission class here, not a
// second-class TryAcquire caller, under two rules — both derived from the
// numbers above, neither a tunable threshold:
//
//  1. RESERVED FLOOR. Decode may hold up to `reserve` tokens taken ahead of
//     the consumer FIFO, and grantLocked holds that many free tokens back
//     from queued consumers while decode demand is registered. Sized from
//     the measured steady demand — per worker per suite run, 3 220 s of
//     scan decode wall and 2 540 s of token stall against a ~180 s suite
//     wall on 3 workers × 4 runs is ≈ 1.5 decoders decoding + ≈ 1.2 queued
//     = 2.7 of a 14-token pool, i.e. ~20 % (decodeReserveFor).
//  2. RING-OCCUPANCY FLIP. While EVERY queued consumer is itself dry
//     (fedWaiters == 0), decode outranks the FIFO without the floor cap: a
//     claim from a consumer whose ring is empty cannot buy throughput until
//     a decoder refills it. One fed waiter — a consumer with morsels queued
//     behind the one in its hand — restores the original strict-priority
//     rule, which is the regime that rule was written for.
//
// Liveness is unchanged: reserve is capped at capacity−1, the holdback
// never exceeds registered demand, and every fragment keeps its token-free
// baseline slot, so consumers can never be wedged by decode admission.
//
// WADJET_DECODE_ADMISSION=0 restores the old policy exactly (no reserve,
// no flip, decode back on plain TryAcquire behind the FIFO). Scheduling
// only — no query's row set depends on it — but the arc it belongs to is
// bisected on EC2, so it gets a switch like any other.
type cpuTokens struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	// reserve is rule 1's floor: how many tokens decode-class admission may
	// hold ahead of the consumer FIFO. 0 disables the floor.
	reserve int64
	// decodeUsed is the decode-class share of used; decodeStalled is the
	// number of decode workers currently parked on a failed acquisition
	// (registered via decodeStallBegin/End, so the holdback never exceeds
	// real demand).
	decodeUsed    int64
	decodeStalled int64
	// waiters is the FIFO grant queue. Granted waiters get used pre-charged
	// and their channel closed; cancellation returns the charge. fedWaiters
	// counts the queued waiters whose morsel ring had depth at claim time.
	waiters    []*tokenWaiter
	fedWaiters int64

	// Admission instrumentation (worker "cpu token admission stats" line).
	decodeAdmits    atomic.Int64 // decode-class tokens granted
	decodeBypasses  atomic.Int64 // …of those, granted while consumers were queued
	decodeHoldbacks atomic.Int64 // times grantLocked stopped short to hold the floor
}

// tokenWaiter is one parked blocking acquisition. granted flips exactly once,
// under the pool lock, together with the channel close.
type tokenWaiter struct {
	ch chan struct{}
	// fed records whether the claiming consumer had morsels queued behind
	// the one in its hand. A fed queue means the ring is not the
	// constraint, so the FIFO keeps strict priority over decode past the
	// reserved floor (rule 2 above).
	fed     bool
	granted bool
}

// decodeAdmission gates the two-class policy. Registered here rather than
// in a planner package because it changes SCHEDULING, not row sets — but
// it is enumerated by the optimization-invariance oracle all the same, and
// bisectable on EC2 by env alone.
var decodeAdmission = optswitch.Register("decode-admission", "WADJET_DECODE_ADMISSION",
	"decode-ahead admission as a first-class token class (reserved floor + ring-occupancy priority flip); off = decode on plain TryAcquire behind a strict consumer FIFO")

// newCPUTokens creates a token pool of the given capacity. Capacity 0 (or
// negative) is legal and means "no extra parallelism ever" — every
// TryAcquire returns 0 and fragments run serial.
func newCPUTokens(capacity int) *cpuTokens {
	if capacity < 0 {
		capacity = 0
	}
	return &cpuTokens{capacity: int64(capacity), reserve: decodeReserveFor(int64(capacity))}
}

// decodeReserveFor sizes rule 1's floor: ~20 % of the pool, which is the
// measured steady decode demand per worker (≈1.5 decoding + ≈1.2 queued of
// 14 tokens = 19.3 %, SF100 2026-08-22 §7.3). Always at most capacity−1 so
// consumers can never be shut out of the pool entirely, and 0 for pools too
// small to split.
func decodeReserveFor(capacity int64) int64 {
	if capacity <= 1 {
		return 0
	}
	r := (capacity + 4) / 5
	if r > capacity-1 {
		r = capacity - 1
	}
	return r
}

// defaultCPUTokenCapacity reserves two cores of headroom for the process's
// fixed goroutine load — GC workers, NATS/gRPC I/O, and the heartbeat loop.
// Heartbeat starvation under compute oversubscription is a tested failure
// mode (heartbeat_starvation_test.go), so the reserve is not optional.
func defaultCPUTokenCapacity() int {
	return runtime.GOMAXPROCS(0) - 2
}

// TryAcquire claims up to n tokens without blocking and returns how many it
// got (0..n). Callers must Release exactly that many when done. Returns 0
// while blocking waiters are queued: those hold admitted work already, and
// granting around them would starve it. This is the CONSUMER-class path
// (the widthGate fast path and legacy fixed-width fragments); decode-ahead
// uses tryAcquireDecode.
func (t *cpuTokens) TryAcquire(n int) int {
	if t == nil || n <= 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.waiters) > 0 {
		return 0
	}
	avail := t.capacity - t.used
	if avail <= 0 {
		return 0
	}
	take := int64(n)
	if take > avail {
		take = avail
	}
	t.used += take
	return int(take)
}

// tryAcquireDecode is the DECODE-class non-blocking acquisition: the scan
// decode-ahead iterator's per-row-group token and the WSHF scanner's
// per-chunk token. It outranks the consumer FIFO while decode is under its
// reserved floor, or while every queued consumer is dry — see the type
// comment. Tokens taken here MUST be returned with releaseDecode.
func (t *cpuTokens) tryAcquireDecode(n int) int {
	if t == nil || n <= 0 {
		return 0
	}
	if !decodeAdmission.On() {
		return t.TryAcquire(n)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	avail := t.capacity - t.used
	if avail <= 0 {
		return 0
	}
	queued := len(t.waiters) > 0
	if queued && !t.decodePreferredLocked() {
		return 0
	}
	take := int64(n)
	if take > avail {
		take = avail
	}
	t.used += take
	t.decodeUsed += take
	t.decodeAdmits.Add(take)
	if queued {
		t.decodeBypasses.Add(take)
	}
	return int(take)
}

// decodePreferredLocked reports whether decode admission outranks queued
// consumers right now: under the reserved floor (rule 1), or while no
// queued consumer has a morsel ring with depth behind it (rule 2).
func (t *cpuTokens) decodePreferredLocked() bool {
	return t.decodeUsed < t.reserve || t.fedWaiters == 0
}

// decodeStallBegin registers one decode worker as parked on a failed
// acquisition; decodeStallEnd retracts it. The pair is what makes the
// floor DEMAND-DRIVEN: with no decoder stalled the holdback is zero and
// consumers get the whole pool, so a decode-free fragment pays nothing.
func (t *cpuTokens) decodeStallBegin() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.decodeStalled++
	t.mu.Unlock()
}

func (t *cpuTokens) decodeStallEnd() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.decodeStalled > 0 {
		t.decodeStalled--
	}
	// Demand just dropped, so the holdback may have too: hand anything now
	// grantable to the consumer FIFO instead of leaving it idle.
	t.grantLocked()
	t.mu.Unlock()
}

// adoptDecode reclassifies n already-charged tokens as decode-class. The
// producer-donation paths (docs/design/shuffle-decode-ahead.md §2.2/§2.3)
// transfer a CONSUMER's token to a decode scanner without touching the
// pool; the scanner releases it with releaseDecode like any other, so the
// class has to move with the ownership.
func (t *cpuTokens) adoptDecode(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	t.decodeUsed += int64(n)
	t.mu.Unlock()
}

// Release returns n previously acquired CONSUMER-class tokens to the pool
// and hands as many as possible straight to queued waiters, FIFO.
func (t *cpuTokens) Release(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	t.used -= int64(n)
	t.grantLocked()
	t.mu.Unlock()
}

// releaseDecode returns n decode-class tokens (from tryAcquireDecode or
// adoptDecode). Kept separate from Release so decodeUsed — which bounds
// rule 1's bypass — stays honest.
func (t *cpuTokens) releaseDecode(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	t.used -= int64(n)
	t.decodeUsed -= int64(n)
	if t.decodeUsed < 0 {
		// Only reachable with the kill switch flipped mid-flight (acquired
		// as consumer-class, released as decode-class). Clamp: `used` is
		// the accounting that must stay exact.
		t.decodeUsed = 0
	}
	t.grantLocked()
	t.mu.Unlock()
}

// grantLocked moves tokens to queued waiters while capacity allows, minus
// whatever the decode floor is holding back. Caller holds t.mu.
func (t *cpuTokens) grantLocked() {
	for len(t.waiters) > 0 {
		avail := t.capacity - t.used
		if avail <= 0 {
			return
		}
		if hb := t.decodeHoldbackLocked(); avail <= hb {
			t.decodeHoldbacks.Add(1)
			return
		}
		w := t.waiters[0]
		t.waiters = t.waiters[1:]
		if w.fed {
			t.fedWaiters--
		}
		t.used++
		w.granted = true
		close(w.ch)
	}
}

// decodeHoldbackLocked is how many free tokens the consumer FIFO may not
// take right now, so a decode worker that wakes from its stall finds one.
// It mirrors decodePreferredLocked — the same two rules, applied to the
// release path instead of the acquire path, because a grant that races
// ahead of a parked decoder is how the old policy closed the loop.
//
// Always bounded by the REGISTERED demand, so it never idles a token no
// decoder is waiting for. Under rule 2 demand is the ONLY bound: while
// every queued consumer is dry, a grant to the FIFO cannot buy throughput
// until a decoder refills the ring, so there is nothing to trade the token
// for. Consumers cannot be wedged by that — each fragment keeps its
// token-free baseline slot, and the decode-ahead iterators' delivery-cursor
// group is itself token-exempt, so both sides always have a runnable
// goroutine that needs nothing from this pool.
func (t *cpuTokens) decodeHoldbackLocked() int64 {
	if !decodeAdmission.On() || t.decodeStalled == 0 {
		return 0
	}
	h := t.reserve - t.decodeUsed
	if t.fedWaiters == 0 {
		h = t.decodeStalled
	}
	if h > t.decodeStalled {
		h = t.decodeStalled
	}
	if h < 0 {
		h = 0
	}
	return h
}

// enqueueWaiter registers a blocking single-token acquisition and returns
// its waiter. fed says whether the claiming consumer's morsel ring had
// depth behind the morsel in its hand — the rule-2 discriminator. The
// caller selects on w.ch (closed = one token now owned) and MUST call
// cancelWaiter if it stops waiting for any reason — including after a
// successful receive it chooses not to use.
//
// If a token is free (and no earlier waiter is queued, and the decode floor
// is not holding it), the grant happens immediately and the returned
// waiter's channel is already closed.
func (t *cpuTokens) enqueueWaiter(fed bool) *tokenWaiter {
	w := &tokenWaiter{ch: make(chan struct{}), fed: fed}
	t.mu.Lock()
	t.waiters = append(t.waiters, w)
	if fed {
		t.fedWaiters++
	}
	t.grantLocked()
	t.mu.Unlock()
	return w
}

// cancelWaiter withdraws a waiter registered by enqueueWaiter. If the grant
// already happened (or races in), the token is returned to the pool.
func (t *cpuTokens) cancelWaiter(w *tokenWaiter) {
	t.mu.Lock()
	if w.granted {
		t.used--
		t.grantLocked()
		t.mu.Unlock()
		return
	}
	for i, q := range t.waiters {
		if q == w {
			t.waiters = append(t.waiters[:i], t.waiters[i+1:]...)
			if w.fed {
				t.fedWaiters--
			}
			break
		}
	}
	t.mu.Unlock()
}

// InUse reports the number of tokens currently held (instrumentation).
func (t *cpuTokens) InUse() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.used
}

// decodeInUse reports the decode-class share of InUse (instrumentation and
// balance assertions).
func (t *cpuTokens) decodeInUse() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.decodeUsed
}

// Capacity reports the pool size (instrumentation).
func (t *cpuTokens) Capacity() int64 {
	if t == nil {
		return 0
	}
	return t.capacity
}

// admissionStats reports the decode-class admission counters for the
// worker's final stats line: how many decode tokens the new class granted,
// how many of those the old strict-FIFO policy would have refused
// (bypasses), and how often a release was held back to keep the floor
// reachable.
func (t *cpuTokens) admissionStats() (capacity, reserve, admits, bypasses, holdbacks int64) {
	if t == nil {
		return 0, 0, 0, 0, 0
	}
	t.mu.Lock()
	capacity, reserve = t.capacity, t.reserve
	t.mu.Unlock()
	return capacity, reserve, t.decodeAdmits.Load(), t.decodeBypasses.Load(), t.decodeHoldbacks.Load()
}

// decodeTokenPool adapts the worker pool to scan.TokenPool with
// decode-class admission: the scan decode-ahead iterator acquires and
// releases through this so its per-row-group token is charged, released
// and PRIORITIZED as decode, and so its token stalls register demand with
// the pool (scan.DecodeAdmission). A plain *cpuTokens would satisfy
// scan.TokenPool too — which is exactly how decode became a second-class
// TryAcquire caller — so the wiring goes through the adapter instead.
type decodeTokenPool struct{ t *cpuTokens }

func (d decodeTokenPool) TryAcquire(n int) int { return d.t.tryAcquireDecode(n) }
func (d decodeTokenPool) Release(n int)        { d.t.releaseDecode(n) }
func (d decodeTokenPool) DecodeStallBegin()    { d.t.decodeStallBegin() }
func (d decodeTokenPool) DecodeStallEnd()      { d.t.decodeStallEnd() }
