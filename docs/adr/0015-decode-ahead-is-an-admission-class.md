# ADR-0015: Decode-ahead is a CPU-token admission class, not a `TryAcquire` client behind the consumer FIFO

Status: Accepted (landed 2026-08-22, `27a8d10`; SF100-measured in windows 2 and 3)

## Context

Each worker admits parallel work against one pool of CPU tokens (`GOMAXPROCS−2`
= 14 on the bench shape). Morsel consumers take tokens through the **blocking
FIFO**; the scan and shuffle decode-ahead pipelines took them through
**`TryAcquire` only**, which by design returns 0 whenever any consumer is
queued — the strict-priority rule of `docs/design/morsel-execution.md` §4.2.1,
written as *"a queued consumer holds an admitted morsel, so feeding it beats
widening decode"*.

The 2026-08-22 three-arm SF100 window (3 workers × 4 runs × 3 arms, consistent
in all 12 runs) measured that rule operating in the regime it was **not** written
for, and measured every term of a closed loop:

- `token_stall_ms` **2 540 s against `decode_ms` 3 220 s — 41.6 % of the
  decoder's wall**; the shuffle side stalls **66 %**.
- `window_full_ms` is **2.9 %** of `decode_ms`: the morsel ring is *empty* 41 %
  of the time and *full* 3 %.
- `dispenser_producer_wait_ms` = **0.00 s** of 2 713.7 s of fragment elapsed —
  the producer was never blocked on the dispenser's byte budget, so the consumers
  are not what holds it back.
- Effective consumer width **2.88 of k = 15**, consumers parked 41 %, Σ dry-wait
  **16 794 s** against Σ width-wait 3 120 s: the dispenser paces the consumers,
  and the token pool paces the dispenser.
- Decoders are CPU-bound *while allowed to run* (`decode_ms`/4 = 805 CPU-s per
  run against ~747 CPU-s of decode frames in the profile), and I/O is not the
  constraint (`filePrefetcher.take` = 0.26 % of all block delay).

The loop: decode is shut out of tokens → the morsel ring drains → consumers go
dry and queue → any queued consumer holds `TryAcquire` at 0 → decode stays shut
out. Recoverable: **~226 s of decode token stall per worker per suite run against
a ~180 s suite wall.**

## Decision

**Decode-ahead is a first-class admission CLASS in `cpuTokens`, not a
second-class `TryAcquire` caller.** Two rules, sized from the measurements above
rather than tuned:

1. **Reserved floor.** Decode may hold `reserve` tokens taken ahead of the
   consumer FIFO, and `grantLocked` holds that many back from queued consumers
   while decode demand is registered and the floor is unmet. `reserve` ≈ 20 % of
   the pool (`decodeReserveFor`) — the measured steady demand of ≈1.5 decoders
   decoding plus ≈1.2 queued on a 14-token worker = 19.3 %.
2. **Ring-occupancy priority flip.** While every queued consumer is itself
   **dry**, decode outranks the FIFO without the floor cap. One *fed* waiter — a
   consumer with morsels queued behind the one in its hand — restores the
   original strict-priority rule. `widthGate.claim` carries the ring depth
   (`len(dispenser.ch) > 0`) into the waiter.

**Demand is registered, not inferred**: `decodeStallBegin`/`decodeStallEnd`
bracket the decode worker's existing park, so a pool with no decoder waiting
holds nothing back and a decode-free fragment pays nothing.

**Liveness is structural, not tuned**: `reserve ≤ capacity−1`, the holdback never
exceeds registered demand, every fragment keeps its token-free baseline slot, and
both decode-ahead pipelines keep their token-exempt delivery-cursor group — so
each side always has a runnable goroutine needing nothing from the pool. **The
pool still never calls back into a reader**, so the reader → pool lock order is
untouched and no notification bridge exists.

**Kill switch:** `WADJET_DECODE_ADMISSION=0` restores plain `TryAcquire` behind a
strict consumer FIFO, exactly.

## Consequences

- **Every signal it was built to move, moved in the predicted direction, and the
  switch arm restores them.** Window 2 (cand vs `=0`, same binary): `token_stall`
  **−26 %**, `window_full` **+84 %**, scan consumer dry-wait **−44.5 %**, scan
  fragment elapsed −5.6 %, worker CPU flat (+0.1 % — a scheduling change, as
  advertised). Window 3 vs the release: stall −36.5 %, ring-full +176 %, dry-wait
  −34 %. Cleanest instance, Q08 `join-6`, decoding 17.9 GB in both arms: stall
  22.2 vs 53.1 s, ring-full 57.5 vs 20.7 s, decoder wall −11.5 s per run on
  identical bytes — the decoder now runs *ahead* of its consumers, not behind.
- **It is wall-neutral, and that is a fact about where the critical path is, not
  a reason to revert** (window 2: −0.3 s over the 19 non-bimodal queries). Time
  moved between parked states — dry −4 211 s, width-wait +3 124 s, stall −524 s,
  ring-full +165 s, Σ`process_ms` −2.3 % — i.e. the pipeline went from
  decode-starved to roughly balanced and the fragments it un-starved were not the
  pacer. The pacers that window named instead were the gather-merge durable wait
  (fixed by `ed83bb9`, −13.3 s per suite run) and the Go heap lock (ADR-0016).
- The policy fires hard: window 3 measured ~729 k holdbacks against ~1.99 M
  admits per arm — a holdback on **~24 %** of decode-token requests, ~12 %
  bypassing the reserve.
- **Consumer width and admission are separable.** Under the work-conserving gate
  an idle consumer holds nothing, so token demand is per *active morsel*, not per
  consumer; an over-wide fan costs cloned op chains, not pool pressure. Sizing
  `k` from the producer's feed rate is a separate, still-open question — the
  starvation was in admission.
- **A counter that only exists at drain does not exist.** `decode_admits` /
  `decode_bypasses` / `decode_holdbacks` were first emitted only from
  `logFinalScanStats()` at `Stop()`, so window 2 could not read the one counter
  this change built for it and had to argue from four indirect signals; `4bd828a`
  moved them onto the periodic `worker stats` line (ADR-0011 §6).
- Interacts with ADR-0017: with the stage-sink accumulator off, consumers blocked
  in `Consume` keep holding their token and scan stall rises 1 513 → 1 819 s.

## Alternatives rejected

- **Make the scanner a blocking waiter in the consumer FIFO.** Rejected three
  times (`docs/design/shuffle-decode-ahead.md` §3, most recently after this
  window): the went-empty escape needs a grant-notification bridge between the
  pool's waiter channels and the reader's condvar — or deferred-notify surgery to
  avoid a **pool → reader lock inversion** — it queues the scanner behind *other
  fragments'* consumers even when its own are the starved ones, and it weakens
  the "serial progress is never gated" invariant while parked. This decision is
  deliberately not that shape: the pool learns decode's *demand* (a counter around
  an existing park) and changes who a release may go to; the scanner never joins
  the FIFO, never queues, never waits on the pool.
- **The occupancy flip alone, without the reserved floor.** The flip is
  state-dependent, so on its own it alternates as the ring fills and drains — the
  decoder is re-shut-out exactly as it starts refilling the ring. The floor gives
  decode a demand-bounded standing minimum; the flip gives it priority in the
  regime the old rule mis-modelled. Each covers the other's failure mode.
- **Widen the consumer fan `k`, or add scan-side parallelism.** The decoders
  cannot get tokens for the parallelism they already have. The same numbers rule
  out deeper prefetch/readahead (I/O is 0.26 % of block delay) and benign
  consumer over-provisioning (`producer_wait` = 0.00 %, `window_full` = 2.9 % —
  the queue is empty, not full).
- **Widen `uploadSlotsBusy`**, the largest block-profile site. Rejected in the
  same window: uploads run detached, 32 % of queued uploads (107 GiB) were
  cancelled outright with zero failures and no effect on any query's rows or
  wall, and `upload_pause_ms` = 0 in every arm. That queue is background by
  design.

## Related

- ADR-0002 (the morsel machinery), ADR-0006 (the pool is also the memory-admission
  substrate), ADR-0011, ADR-0017 (returns tokens sooner)
- `docs/design/shuffle-decode-ahead.md` §2.4 and §3,
  `docs/design/morsel-execution.md` §4.2.1
- `docs/benchmarks/sf100-window-analysis-2026-08-22.md` §7 (the closed loop),
  `…-window2-analysis-2026-08-22.md` §2 (the A/B)
- `internal/worker/cpu_tokens.go`, `internal/engine/scan/decode_ahead.go`,
  `internal/worker/shuffle_decode_ahead.go`
