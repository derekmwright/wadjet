# Worker cpu token admission

Source: internal/worker/cpu_tokens.go — type cpuTokens struct {, moved 2026-09-11 (#1026)

cpuTokens is a worker-wide counting semaphore over EXTRA compute
goroutines (morsel-driven execution, docs/design/morsel-execution.md §4.2).
Every task's first pipeline goroutine is free — the serial baseline is
always allowed — and only additional parallel consumers take tokens, so
Σ(extra widths) across concurrent tasks never schedules more compute
goroutines than the reserved core budget.

TryAcquire is non-blocking BY DESIGN: a fragment or decode worker that
cannot get tokens degrades to a narrower width instead of waiting.
Blocking here would recreate the parked-waiter admission shape rejected in
project_admission_control_rejected_2026-05-18 — a goroutine holding
upstream resources while gated on capacity that only its own progress
would release.

enqueueWaiter is the one deliberate exception (work-conserving width,
§4.2.1): a morsel consumer that already HOLDS work may park for a token,
because the capacity it waits on is released by OTHER goroutines' bounded
progress (per-row-group decode holds, other consumers' per-morsel holds)
and every fragment's baseline slot guarantees a token-free consumer is
always runnable — the waiter's own progress is never what frees the pool.

# Two admission classes

Waiters are FIFO. They used to take STRICT priority over every
TryAcquire, on the rule "a queued consumer holds an admitted morsel, so
feeding it beats widening decode". That rule is right for a FULL morsel
ring and wrong for an EMPTY one, and the SF100 window of 2026-08-22
(3 workers × 4 runs × 3 arms, consistent in all 12) measured the empty
regime as the steady state:

  - dispenser: Σdry 16 794 s vs Σwidth_wait 3 120 s vs producer_wait 0.0 s;
    effective consumer width 2.88 of 15, consumers parked 41 % of the time;
  - decode side: ring full (window_full_ms) only 2.9 % of decode_ms, while
    scan token_stall_ms was 41.6 % of decoder wall (2 540 s vs 3 220 s) and
    shuffle token stall 66 %;
  - the decoders are CPU-bound while they are allowed to run (decode_ms/4
    ≈ 805 CPU-s/run against ≈ 747 CPU-s of decode frames in the profile),
    and I/O is not the constraint (filePrefetcher.take = 0.26 % of block).

That is a closed loop, and every term of it was measured: decode is shut
out of tokens → the morsel ring drains → consumers go dry and queue for
tokens → any queued consumer keeps decode's TryAcquire at 0 → decode
stays shut out. Feeding a queued consumer when the ring behind it is
empty buys nothing; only a decoder can refill it.

So decode-ahead is a first-class admission class here, not a
second-class TryAcquire caller, under two rules — both derived from the
numbers above, neither a tunable threshold:

 1. RESERVED FLOOR. Decode may hold up to `reserve` tokens taken ahead of
    the consumer FIFO, and grantLocked holds that many free tokens back
    from queued consumers while decode demand is registered. Sized from
    the measured steady demand — per worker per suite run, 3 220 s of
    scan decode wall and 2 540 s of token stall against a ~180 s suite
    wall on 3 workers × 4 runs is ≈ 1.5 decoders decoding + ≈ 1.2 queued
    = 2.7 of a 14-token pool, i.e. ~20 % (decodeReserveFor).
 2. RING-OCCUPANCY FLIP. While EVERY queued consumer is itself dry
    (fedWaiters == 0), decode outranks the FIFO without the floor cap: a
    claim from a consumer whose ring is empty cannot buy throughput until
    a decoder refills it. One fed waiter — a consumer with morsels queued
    behind the one in its hand — restores the original strict-priority
    rule, which is the regime that rule was written for.

Liveness is unchanged: reserve is capped at capacity−1, the holdback
never exceeds registered demand, and every fragment keeps its token-free
baseline slot, so consumers can never be wedged by decode admission.

WADJET_DECODE_ADMISSION=0 restores the old policy exactly (no reserve,
no flip, decode back on plain TryAcquire behind the FIFO). Scheduling
only — no query's row set depends on it — but the arc it belongs to is
bisected on EC2, so it gets a switch like any other.
