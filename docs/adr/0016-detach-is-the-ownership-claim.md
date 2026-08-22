# ADR-0016: `Detach` is the ownership claim; producers may reuse vector backing while a batch is unclaimed

Status: Accepted (landed 2026-08-22, `69aecbb`; SF100-measured against a
`WADJET_VECTOR_REUSE=0` control, run `20260822-062217`)

## Context

Once the application-level stage-sink lock was gone (ADR-0017), the SF100 profile
named a single dominant contention site and it was not ours: the **Go runtime
heap lock** — `runtime.mheap.allocSpan` plus the unattributable
`_LostContendedRuntimeLock` bucket — at **88 % of all worker mutex delay, 81 s
per worker per suite run** (`docs/benchmarks/sf100-window2-analysis-2026-08-22.md`
§5). Its largest single caller is `HashJoinProbe.emitViewOutput` at 19.4 %, with
`exec.gatherBuildVector` (8.2 %) inside it. At join-emit widths — up to 32 768
rows — every allocation on that path is a large object, so each one takes the
heap lock directly. Per suite run it charges ~273 GB: `PreAllocBytes` under
`gatherBuildVector` 147 GB, gathered build-column storage 61 GB, view composition
index arrays 53 GB, shared index arrays and shell 9 GB.

None of it was reused, for a structural reason: **`BatchPool` was dead on the
distributed path.** It recycles only what a driver hands back with `Release()`,
and the worker's `fragmentDriver` (`internal/worker/executor_fragment.go`) does
not opt into `ReleaseInputs` — so on the path where the 81 s is measured, every
probe output batch was freshly allocated.

The signal the worker path *does* carry is `Detach`. It already means "a consumer
keeps this past the call that handed it over", and every retaining consumer calls
it: Sort, Window, `CollectSink`, `BatchSink`, `SpillableBatchCollector`,
`SortMergeJoin`, the hash-join build, partitioned aggregation's per-partition
views. The consumers that do *not* — the stage and shuffle sinks,
`HashAggregate`'s key arena — copy what they need before returning. That is the
contract `BatchPool` recycling has always rested on, minus the `Release()`
requirement.

## Decision

**`Detach` is the ownership claim. A producer may overwrite the backing of a
batch it emitted for as long as no consumer has claimed it; one claim surrenders
the whole buffer, permanently, for that batch.**

- `RecordBatch.Detach` records the claim on the batch **and on every column
  `Vector`** (`Vector.Claim`, recursing through `Base` and nested children).
  **The per-vector claim is what makes the contract hold through a *derived*
  batch**: `ColumnPrune`, the set-op emitter and partitioned aggregation's
  `selView` mint a new `RecordBatch` over the same `*Vector` pointers, and a view
  minted downstream over one of the probe's gathered columns propagates the claim
  back through `Vector.Base`.
- `HashJoinProbe` keeps its last output (`probeEmitBuf`) and writes over it —
  shell, probe/build index arrays, view composition buffers, and the eagerly
  gathered build columns via `Vector.ResetForWrite` — **only while no claim has
  landed**. One claim surrenders the whole buffer, since every piece is reachable
  from the batch the consumer kept. `ResetForWrite` clears what it resizes, so a
  reused vector is bit-identical to a fresh one (the gather loops skip null and
  unmatched rows).
- `emitViewOutput` pins its input with `DetachPool` rather than `Detach`: that
  reference is transitive — it dies with this output batch — so claiming the
  input outright would stop an **upstream** probe reusing its storage forever.
  The pool severance the call exists for is unchanged.

**Kill switch:** `WADJET_VECTOR_REUSE=0` (`optswitch` `vector-reuse`) restores a
fresh allocation per output batch.

## Consequences

- **This is an invariant every consumer and every new `Sink` must honor: keep a
  batch (or any vector reachable from it) past the call that handed it over ⇒
  `Detach` it; otherwise copy what you need before returning.** A consumer that
  retains without claiming reads storage the producer has legitimately
  overwritten. `TestJoinEmitReuseMatchesFreshAllocation` drives the probe into
  every consumer kind — copying sink, retaining sink, `ColumnPrune`-derived batch,
  `CollectSink`, Sort, spillable collector, `HashAggregate`, fused second probe —
  under both drivers, single- and multi-batch builds, inner and left joins,
  asserting identical output with reuse on and off. Extend it when a consumer kind
  is added.
- **The claim is sticky and conservative by design.** It is never cleared for the
  life of the batch, so a pipeline breaker downstream disables the upstream
  producer's reuse from that batch onward. That costs allocations exactly where a
  batch genuinely outlives its emit call — the only safe direction to be wrong in.
- **Measured, SF100 window 3 (2026-08-22 05:38–06:40 UTC; runs `20260822-055421`
  cand vs `-062217` with the switch off), the control restoring every number to
  base exactly:** `emitViewOutput` alloc_space **−95.6 %** (47.9 vs 1 095.3 GB),
  `gatherBuildVector` **−99.2 %** (4.5 vs 588.0 GB), total allocation **−260 GB
  per suite run** against ~273 GB predicted, worker mutex delay **−157 s = −13.1 s
  per worker per suite run** (heap-lock family −156.5 s), GC cycles −9.7 %, worker
  CPU **flat (+0.1 %)**. Micro-arm (`BenchmarkProbeEmitViewOutput`, TPC-H widths,
  benchstat n=8): single-batch build 20.9 KiB → 4.6 KiB per output batch
  (**−78 %**, −12 % wall), multi-batch build 118.3 KiB → 2.1 KiB (**−98 %**,
  24 → 7 allocs, −32 % wall).
- **Wall-neutral at SF100, as predicted.** Justified on allocation, lock delay and
  GC, not on the stopwatch; the three arms of that binary did identical work to
  within 0.4 % of suite task-seconds and differ in wall only by scheduling draws
  on the bimodal queries (ADR-0011).
- **The heap lock is still #1, and the remaining half is the scan side**:
  `readRowGroupNative.func2` at 130.8 s of heap-lock delay behind ~1 140 GB per
  run, now unobstructed by the join half. That is the next lever and is *not*
  covered by this ADR — see the deliberate exclusion below.

## Alternatives rejected

- **Teach `fragmentDriver` to opt into `ReleaseInputs` so `BatchPool` works on the
  distributed path.** `Release()` requires the driver to assert that no consumer
  kept the batch — exactly the fact `Detach` already records at the only place
  that knows it, the consumer. Deriving it a second time in the driver duplicates
  the contract and gets it wrong *silently* (a released batch a consumer still
  holds is a wrong answer, not a crash).
- **Reuse the scan row-group output** — 473 GB/run of `NewRecordBatch` plus
  166 GB/run of `PreAllocBytes` under `readColumnNative`, a *larger* number than
  the join path. Deliberately excluded: those batches are held concurrently in the
  decode-ahead ring and offered to the decoded-chunk cache, so their lifetime is
  neither single-consumer nor bounded by one call, and the `Detach` claim does not
  describe them. It needs its own ownership statement before any reuse.
- **Reuse on the eager (non-late-materialized) probe emit path.** It already has
  `BatchPool` and is not what the profile names; `BenchmarkHashJoinProbeFanout` is
  unchanged in all arms — the control that says the change is confined to the path
  the profile named.
- **Claim on the batch shell only** (cheaper, one flag). Silently dropped at the
  first derivation, because `ColumnPrune`, set-op emit and `selView` mint fresh
  shells over the same vectors — precisely the shapes fused join chains produce.
- **`Detach` rather than `DetachPool` on `emitViewOutput`'s input.** It would claim
  a transitive reference that dies with the output batch, permanently disabling
  reuse in the *upstream* probe of every fused join chain — the shape worth the
  most.

## Amendment 2026-08-22 — scan output: the claim is the veto, `retire` is the release

The exclusion above ("Reuse the scan row-group output … needs its own ownership
statement") is now settled, and the answer **extends** this decision rather than
replacing it, so it lands here instead of as a new ADR.

**A scan source is not a single-consumer producer, so `Detach` alone cannot
authorize reuse of its output.** Three readers hold a decoded row group with no
claim recorded anywhere: the decode-ahead ring decodes group *N+1* while *N* is
being consumed (ADR-0015); the morsel dispenser fans one ~280 MB parent out to
`k` consumers as zero-copy views over the same `*Vector` pointers; and
`emitViewOutput`'s view columns read the scan columns through `Vector.Base`
after `Execute` returned — which is exactly why that call uses `DetachPool` and
not `Detach`.

**Decision.** *For output a producer emits to concurrent, unclaimed readers,
reuse requires TWO signals, and neither may be inferred:*

1. **RELEASE** — an explicit statement from the consumer side that everything
   reading this batch is done. On the fragment paths that is `morsel.retire`
   (fired once every sibling view has retired: after the whole op chain **and**
   the sink consume) and, serially, the return from `driver.push`. It is
   strictly later than `ChainDriver`'s `ReleaseInputs` edge, which is what makes
   it safe for the transitive view references `DetachPool` was written to
   protect.
2. **CLAIM** — the veto this ADR already defines, unchanged: `Detach` on the
   batch or `Vector.Claim` on any column, including transitively through a
   derived batch or a downstream view's `Base`. One claimed column surrenders
   the whole backing, permanently.

Release without the claim check is unsound (Sort, Window, the join build and
the spillable collector retain past `retire`, and all of them `Detach`). The
claim check without release is unsound (the three concurrent readers above).
Together they are exactly as conservative as the join half, one signal wider.

**`DetachPool` is unchanged and stays unchanged.** The scan backing pool
deliberately does **not** route through `RecordBatch.pool`: `emitViewOutput`
calls `DetachPool` on every late-materialized probe input, which is the
dominant SF100 scan shape, so hanging reuse off the pool link would disable it
precisely where the 130.8 s of heap-lock delay lives. The pool keeps its own
registry of outstanding backings and only ever takes back one it minted, so no
second owner can be created for storage someone else recycles.

**Kill switch:** `WADJET_SCAN_BACKING_REUSE=0` (`optswitch` `scan-backing-reuse`).

**Scope:** flat leaf columns only. ROW columns are excluded — `ResetForWrite`
refuses nested storage by design and ROW schemas are not what the profile
names — and the parquet page buffers (`ColumnPageReader`, 94.6 s) stay
untouched inside the safety-critical package. Mechanism, predicted SF100 shape
and the failure modes are in `docs/design/scan-output-backing-reuse.md`.

**Consequence for every new consumer, restated:** keeping a batch (or any
vector reachable from it) past the call that handed it over still means
`Detach`. What is new is the other side of the contract — **`retire` is now
load-bearing for correctness, not only for the dispenser's byte budget.**
Anything that retires a morsel before its sink is finished with the batch turns
this into silent corruption.

## Related

- ADR-0002 (`BatchPool` and the selection-vector contract), ADR-0006 (a claimed
  batch's bytes stay charged), ADR-0011, ADR-0017 (the lock whose removal exposed
  the heap lock)
- `docs/design/late-materialization.md` §4 and §4.1 (ownership and lifetime; emit
  storage reuse), §5 (the consumer matrix this invariant extends)
- `docs/benchmarks/sf100-window2-analysis-2026-08-22.md` §5 (the heap-lock ranking
  and its callers)
- `docs/benchmarks/sf100-window3-analysis-2026-08-22.md` (the window-3 B-vs-D
  vector-reuse arm: −260 GB/run alloc, −13.1 s/worker-run mutex, wall-neutral)
- `internal/engine/exec/join_emit_reuse.go`, `internal/engine/batch/batch.go`
  (`Detach`, `DetachPool`), `internal/engine/batch/vector.go` (`Claim`,
  `ResetForWrite`)
- Amendment: ADR-0015 (the ring whose concurrency is why a claim alone is not
  enough), `docs/design/scan-output-backing-reuse.md`,
  `docs/design/morsel-execution.md` §4.1.1 (the `retire` contract and the
  view-safety audit), `internal/engine/scan/backing_pool.go`,
  `internal/worker/morsel_dispenser.go` (`batchRecycler`)
