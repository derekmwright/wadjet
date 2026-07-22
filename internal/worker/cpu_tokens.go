package worker

import (
	"runtime"
	"sync"
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
// Waiters are FIFO and take strict priority over TryAcquire: a queued
// consumer holds an admitted morsel, so feeding it beats widening decode.
type cpuTokens struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	// waiters is the FIFO grant queue. Granted waiters get used pre-charged
	// and their channel closed; cancellation returns the charge.
	waiters []*tokenWaiter
}

// tokenWaiter is one parked blocking acquisition. granted flips exactly once,
// under the pool lock, together with the channel close.
type tokenWaiter struct {
	ch      chan struct{}
	granted bool
}

// newCPUTokens creates a token pool of the given capacity. Capacity 0 (or
// negative) is legal and means "no extra parallelism ever" — every
// TryAcquire returns 0 and fragments run serial.
func newCPUTokens(capacity int) *cpuTokens {
	if capacity < 0 {
		capacity = 0
	}
	return &cpuTokens{capacity: int64(capacity)}
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
// granting around them would starve it.
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

// Release returns n previously acquired tokens to the pool and hands as many
// as possible straight to queued waiters, FIFO.
func (t *cpuTokens) Release(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.mu.Lock()
	t.used -= int64(n)
	t.grantLocked()
	t.mu.Unlock()
}

// grantLocked moves tokens to queued waiters while capacity allows. Caller
// holds t.mu.
func (t *cpuTokens) grantLocked() {
	for len(t.waiters) > 0 && t.used < t.capacity {
		w := t.waiters[0]
		t.waiters = t.waiters[1:]
		t.used++
		w.granted = true
		close(w.ch)
	}
}

// enqueueWaiter registers a blocking single-token acquisition and returns
// its waiter. The caller selects on w.ch (closed = one token now owned) and
// MUST call cancelWaiter if it stops waiting for any reason — including
// after a successful receive it chooses not to use.
//
// If a token is free (and no earlier waiter is queued), the grant happens
// immediately and the returned waiter's channel is already closed.
func (t *cpuTokens) enqueueWaiter() *tokenWaiter {
	w := &tokenWaiter{ch: make(chan struct{})}
	t.mu.Lock()
	t.waiters = append(t.waiters, w)
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

// Capacity reports the pool size (instrumentation).
func (t *cpuTokens) Capacity() int64 {
	if t == nil {
		return 0
	}
	return t.capacity
}
