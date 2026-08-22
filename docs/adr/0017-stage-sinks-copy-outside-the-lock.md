# ADR-0017: Stage sinks copy outside the lock; the lock covers handoff only

Status: Accepted (landed 2026-08-22, `1b0819c`, following the partitioned-sink
precedent `e50fd1b`; SF100-measured in window 2 against a
`WADJET_STAGE_SINK_ACCUM=0` control)

## Context

`unpartitionedStageSink.Consume` held the sink mutex across
`appendBatchRowsBulk`, so the row **copy itself** ran inside the critical
section: every morsel worker in a linear-parallel fragment serialized on one
lock for a full batch memmove.

The first SF100 block/mutex profiles (2026-08-22 window 1 §2.1) put that one
call at **32.6 % of all worker mutex delay, 39–44 s per worker per suite run,
95.8 % of it in the real `sync.Mutex.Unlock` handoff** — i.e. genuine
contention, not `Cond.Wait`. It was the largest *application* lock in the
profile, and the only one left worth naming after the double-buffered flush
fixed the partitioned side.

**The floor on total serialized time is total copy time**, so amortizing lock
*acquisitions* — larger batches, fewer takes — cannot fix it. The copy has to
leave the lock. `partitionedShuffleSink.appendAndMaybeFlush` had the identical
shape and is the proof it works: 64 % of worker mutex block before `e50fd1b`,
0.8 % after.

## Decision

**A stage sink accumulates rows in producer-local storage and takes its mutex
only to hand over stream ownership and bump counters. No row copy runs under a
sink lock.**

The unpartitioned sink goes one step further than the partitioned template: it
has exactly **one** output stream, so a filled producer-local slab is written as
its own chunk rather than copied a second time into a shared accumulator. `mu`
now covers only the existing `flushing` ownership flag and the counters.

What the rule must preserve, and how each is held:

- **Chunk sizing.** A slab flushes at the sink's own `flushRows`/`flushBytesT`,
  and LIFO checkout keeps a serial producer on one slab — so serial output is
  byte-identical, gated by `TestUnpartitionedStageSink_SlabSerialParity`, which
  compares whole files.
- **Upload and durability order.** Unchanged; this sink never uploads inside
  `Consume` (ADR-0007's policies ride the task, not the sink).
- **Memory.** Appends are charged to a `bufferedBytes` counter at accumulate
  time and a slab flushes early past 4× `flushBytesT` — ~64 MB per sink worst
  case, inside ADR-0006's ledger.
- **Row loss.** `Finalize` drains every registered slab before the footer, gated
  by `TestUnpartitionedStageSink_SlabFinalizeDrain`.

**Kill switch:** `WADJET_STAGE_SINK_ACCUM=0` (`optswitch` `stage-sink-accum`).

## Consequences

- **The target site collapsed by 99.5 % and the switch restores it exactly.**
  SF100 window 2, `unpartitionedStageSink.Consume` mutex delay over 3 workers ×
  4 runs: base **477.7 s** (32.0 % of all mutex) → cand **2.2 s** (0.19 %) →
  switch-off **506.0 s** (35.2 %). Total worker mutex delay 1 493.3 → 1 112.3 s,
  i.e. **−31 s of lock waiting per worker per suite run**. The sink's own
  instrument agrees: Σ`sink_ms` 58.8 s vs 805.9 s with the switch off (13.7×).
- **Wall-neutral at SF100, exactly as the local measurement predicted.** The
  serialized memcpy was real and is gone, but it was never the pacer: with the
  lock, consumer time is charged to `process_ms` (blocked inside the push);
  without it, consumers go dry instead. Suite steady mean over the non-bimodal
  queries is within run scatter. Kept anyway — see ADR-0011: mechanism metrics
  decide, and this one removed the largest application mutex in the profile at no
  CPU, allocation or byte-volume cost (partitioned path untouched, Σ`append_ms`
  871.8 vs 873.4 s; worker CPU within 0.7 %).
- **It is not only a lock win — it feeds ADR-0015.** With the accumulator
  disabled, consumers blocked in `Consume` keep holding their CPU token, and scan
  `token_stall_ms` rises 1 513 → 1 819 s, halfway back to the
  admission-disabled arm. Part of what this rule is worth is decode admission.
- **It is what exposed ADR-0016.** With the application lock gone, the Go runtime
  heap lock became 88 % of all remaining worker mutex delay — the next lever, and
  a different kind of problem.
- **The rule generalizes to every sink added later**: if `Consume` copies, the
  copy is producer-local. A new sink that memmoves under its own mutex will
  re-create this exact profile shape.

## Alternatives rejected

- **Amortize lock acquisitions** (accumulate more rows per take, coarser batches).
  Rejected analytically and confirmed by the profile: the serialized time floor
  *is* the copy time, and 95.8 % of the delay was already in the handoff of a lock
  held for a full batch copy. Fewer, longer critical sections move nothing.
- **Copy the producer-local slab into a shared accumulator under the lock** (the
  partitioned sink's exact shape). Correct but unnecessary here: with a single
  output stream the slab can be written as its own chunk, which avoids a second
  full copy of every row. The partitioned sink keeps its shape because it fans
  one batch across many partition streams.
- **Leave it alone because the wall is flat.** The local bench already showed flat
  wall (this box's sink is bound by the serialized write syscall — 69 % of
  in-`Consume` CPU samples in `syscall.write` — and throughput is independent of
  producer count in *both* arms), and the SF100 A/B was run as the verdict rather
  than assumed. Flat wall with a 99.5 % reduction in a named mechanism metric is
  a keep under ADR-0011, not a discard.

## Related

- ADR-0011 (mechanism metrics decide; kill-switch-on-treatment-binary controls),
  ADR-0015 (decode admission — tokens returned sooner), ADR-0016 (the heap lock
  this exposed), ADR-0006 (memory ledger), ADR-0007 (durability rides the task)
- `docs/benchmarks/sf100-window-analysis-2026-08-22.md` §2.1 and §6.2 (the
  diagnosis and the lever it implied),
  `docs/benchmarks/sf100-window2-analysis-2026-08-22.md` §3 (the A/B)
- `internal/worker/stage_sink_accum.go`
