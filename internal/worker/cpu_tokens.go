package worker

import (
	"runtime"
	"sync/atomic"
)

// cpuTokens is a worker-wide counting semaphore over EXTRA compute
// goroutines (morsel-driven execution, docs/design/morsel-execution.md §4.2).
// Every task's first pipeline goroutine is free — the serial baseline is
// always allowed — and only additional parallel consumers take tokens, so
// Σ(extra widths) across concurrent tasks never schedules more compute
// goroutines than the reserved core budget.
//
// Acquisition is non-blocking BY DESIGN: a fragment that cannot get tokens
// degrades to a narrower width (ultimately serial) instead of waiting.
// Blocking here would recreate the parked-waiter admission shape rejected in
// project_admission_control_rejected_2026-05-18 — a goroutine holding
// upstream resources while gated on capacity that only its own progress
// would release.
type cpuTokens struct {
	capacity int64
	used     atomic.Int64
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
// got (0..n). Callers must Release exactly that many when done.
func (t *cpuTokens) TryAcquire(n int) int {
	if t == nil || n <= 0 {
		return 0
	}
	for {
		cur := t.used.Load()
		avail := t.capacity - cur
		if avail <= 0 {
			return 0
		}
		take := int64(n)
		if take > avail {
			take = avail
		}
		if t.used.CompareAndSwap(cur, cur+take) {
			return int(take)
		}
	}
}

// Release returns n previously acquired tokens to the pool.
func (t *cpuTokens) Release(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.used.Add(-int64(n))
}

// InUse reports the number of tokens currently held (instrumentation).
func (t *cpuTokens) InUse() int64 {
	if t == nil {
		return 0
	}
	return t.used.Load()
}

// Capacity reports the pool size (instrumentation).
func (t *cpuTokens) Capacity() int64 {
	if t == nil {
		return 0
	}
	return t.capacity
}
