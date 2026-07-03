# Morsel-Driven Execution — Design Memo

> **Status:** proposed (v1 scoped, not started). **Date:** 2026-07-02.
> **Verified against:** main @ 0cc5419 (post-#177). `file:line` anchors
> drift — confirm before relying on them.

## 1. Goal

Decouple a worker's CPU parallelism from its memory-owner count. Today both
are the same knob: `max_concurrent` task slots, each driving one pipeline
with (essentially) one compute goroutine. The SF100 history is unambiguous
that this knob is **pinned by memory, not CPU** — mc=4 on 16-vCPU workers
survived only after the HashJoin partition-on-arrival fix, and every prior
attempt to raise it died on cumulative live heap, never on cores
(`sf100-distributed.tfvars:17-33`). Steady state leaves ~12 of 16 vCPUs
idle whenever tasks aren't inside one of the existing bursty parallel
sections. Morsel-driven execution (HyPer, Leis et al.) runs **few pipeline
(memory) instances with many worker threads pulling small work units**, so
CPU utilization scales to cores while the number of concurrent
accumulating operators — the thing that OOMs — stays where admission put it.

This is the last Tier-1 keystone on the Trino-gap roadmap; it also unblocks
the streaming-exchange follow-up ("eager consumer dispatch"), which needs
intra-task consumption that can absorb producer-task files as they appear.

## 2. Current state (verified 2026-07-02)

The exploration found that **most of the morsel machinery already exists**
— built for the single-process engine — and the production distributed path
is the one place it is never engaged.

**The production fragment path is serial.** `executeFragment`
(`worker/executor_fragment.go:169`) drives linear fragments with a
2-goroutine producer/consumer split (`runFragmentLinear:326`, source decode
overlapped with the op chain, channel cap 2) and breaker fragments with
fully serial phases (`runFragmentWithBreakers:493` → `exec.Pipeline` built
**without `Workers`** → `runSerial`). Intra-task parallelism today is
step-local and bursty: per-column parquet decode
(`scan/columnar_native.go:134`), per-partition shuffle writes
(`partitioned_shuffle_sink.go:174`), overlapped builds of *distinct* joins
(`executor_fragment.go:851`), async uploads. The operator chain and every
breaker consume/drain phase are one goroutine.

**A morsel engine already exists and runs in production — elsewhere.**
`exec.Pipeline.runParallel` (`engine/exec/pipeline.go:175`): N workers
share one source (a buffered channel *is* the morsel queue; `Next()` is
the pull), each runs a **cloned** op chain (`Cloneable`,
`operators.go:42` — "each clone gets its own scratch buffers"), each feeds
a per-worker **`MergeableSink`** clone, partials merge after the join
barrier. The planner enables it with `Workers = runtime.NumCPU()` whenever
the source is channel-based (`planner/physical/plan.go:1573`), so the
embedded `wadjet.DB`, the coordinator local fast path, and the HTTP server
already execute this way. The distributed fragment runner is the only
`pipeline.Run` caller that bypasses it.

**Operator taxonomy (who can already do this):**

| Operator | Parallel story today |
|---|---|
| Filter/Project/expr ops | `Cloneable` — fresh scratch per clone; trivially morsel-safe |
| HashJoin probe | **Concurrency-safe by design**, proven in production: `broadcastJoinCache` shares one built join across concurrent probe *tasks* with zero probe-path synchronization (`worker/broadcast_join_cache.go:20-25`); per-probe scratch via `h.Probe()`, atomic lazy key resolution. Exception: RIGHT/FULL OUTER mark matched build rows under `h.mu` (`join.go:1834,1938`) — correct but serializing |
| HashAggregate | `MergeableSink`: spill-less per-worker partials with SoA fast merges (`aggregate.go:2728,2743,2859`); merge itself single-threaded |
| Sort | `MergeableSink`: per-worker batch lists, concat merge, single sort at Finalize (`sort.go:312,323`) |
| Limit | Shares itself — atomic counters + `DoneSignaler` (`limit.go:97-103`) |
| **Window** | **NOT mergeable** — no CloneSink; N workers would serialize on its mutex (`window.go:123`). The serial funnel of the operator set |
| Scan sources | Channel-based planner sources support concurrent `Next` (the allowlist at `plan.go:1573`); the fragment source (`cachedFileStreamSource`) is a single-threaded state machine; `RowGroupIter` walks row groups sequentially with unguarded cursors, though `ReadRowGroupNative` itself is safe per-row-group against the immutable `FileReader` |

**Memory machinery is already morsel-ready.** Accounting is a worker-wide
shared pool with per-task child trackers, all atomic/lock-free
(`memory/tracker.go:53-98`; `executor.go:265-277`) — the Trino
MemoryPool shape. `SpillManager` is thread-safe and its relief path
already calls into accumulators from another thread (mutex +
`accState` atomics). `TaskProgress` is atomic. Per-task `EstimatedBytes`
admission (`worker.go:610`) is a per-task-total and is indifferent to
thread count. **More threads per task does not create more memory
owners** — this is the load-bearing fact.

**Known ceilings to design against** (documented in-repo):
- `runParallel`'s single shared source serializes workers per batch —
  5-15% loss on I/O-bound parallel pipelines
  (`docs/performance-bottlenecks.md:234`).
- Parallelism pays only above a size threshold: the parallel join build is
  gated because per-worker insert cost ate the win on small builds
  (`join.go:653-702`). Granularity/worker-count must be adaptive.
- Oversubscription is a real pathology: GOMAXPROCS is unpinned (=16), and
  the existing bursty sections already reach for it; heartbeat starvation
  under heavy mixed goroutine load is a tested failure mode
  (`heartbeat_starvation_test.go:17`).
- Selection-vector scratch aliasing: retaining sinks must snapshot `b.Sel`
  (`pipeline.go:530-542`); a batch belongs to exactly one worker at a time.
- Clone-lifecycle hygiene: transient operator instances must
  `UnpublishOwned` at close or the ledger drifts (`tracker.go:172`, the
  −146 GB incident).

## 3. Design space

### A. Engage the existing parallel model on the fragment path (per-task morsels)

Make `executeFragment` drive its pipelines the way the single-process
engine already does: a concurrent morsel-dispensing source + `Workers = k`
cloned op chains + mergeable breaker sinks. No new scheduler; the bounded
channel is the work queue and idle workers pulling from it is the load
balancing (fixed-worker data parallelism, not stealing — sufficient when
morsels are uniform-ish batches).

### B. Worker-global morsel scheduler with work stealing

A single pool of ~GOMAXPROCS threads owned by the worker process; every
active task contributes pipeline jobs; threads steal morsels across tasks.
The full HyPer shape. Strictly more powerful: global CPU governance falls
out for free, skew between tasks self-balances, `max_concurrent` becomes a
pure memory-admission knob.

### C. Just raise `max_concurrent`

Rejected outright: it multiplies memory owners — the exact thing the SF100
history shows OOMs. It also multiplies broadcast build decodes
(`broadcast_join_cache.go:22`: builds decoded once per concurrent probe
task). Never-OOM says no.

### Decision

**V1 = A, with B's CPU governance extracted as a small shared primitive;
full work-stealing deferred.** Rationale:

1. A reuses interfaces that are **already proven in production** on three
   entry paths; the delta is confined to the fragment runner and the
   fragment sources. B requires a new scheduler, a per-pipeline state
   machine, and rewiring task lifecycle (progress, cancel, admission) —
   and its marginal win over A is cross-task stealing, which matters most
   when tasks are skewed *and* scarce; at mc=4 with fan-out task counts ≥
   capacity, per-task parallelism recovers most of the idle CPU.
2. The one piece of B that v1 cannot skip is **global CPU budgeting**: k
   morsel workers × mc tasks + the existing bursty sections must not
   oversubscribe GOMAXPROCS (the starvation pathology). A tiny process-wide
   semaphore ("CPU tokens", capacity ≈ GOMAXPROCS) acquired by morsel
   workers and the existing errgroup sections gives A the governance
   without B's scheduler. This primitive *is* B's kernel; if stealing is
   ever needed, it grows from here.
3. Sequencing with streaming exchange: eager consumer dispatch wants
   "pipeline consumes inputs as they appear" — A's dispenser source is
   exactly that seam.

## 4. Mechanism (v1)

### 4.1 The morsel dispenser (fragment source concurrency)

Replace the fragment's single-threaded source pull with a **dispenser**: 
one producer goroutine per input *file* stage feeding a bounded channel of
`*batch.RecordBatch` (the morsel = one batch, 2048 rows), consumed by k
pipeline workers via concurrent `Next()`. This is the same shape the
planner sources already have, applied to `cachedFileStreamSource`:

- **Granularity:** the dispenser decodes at existing boundaries — WSHF
  chunk / parquet row group — inside the producer; consumers never touch
  decode state. Row-group-parallel *decode* (multiple producer goroutines
  over one file's row groups, which `ReadRowGroupNative`'s immutability
  makes legal) is a v1.5 upgrade behind the same interface; v1 keeps one
  producer per source and gets consumer-side parallelism only. This
  sidesteps the unguarded `RowGroupIter` cursors rather than retrofitting
  atomics into them.
- The bounded channel (small, ~2×k) preserves the never-OOM property:
  decode cannot run ahead of consumption by more than the channel depth,
  same as today's `batchChanCap = 2` split.
- No shared source mutex — the channel replaces it, addressing the
  documented 5-15% `sourceMu` ceiling.

### 4.1.1 v1.5: byte-bounded dispenser + zero-copy morsel views (shipped)

The SF100 A/B (2026-07-03) refuted two v1 assumptions and v1.5 replaces
the raw channel with `morselDispenser` (`internal/worker/morsel_dispenser.go`)
on both parallel paths:

1. **"A batch ≈ 2048 rows" is false on the parquet path.** A source batch
   from `cachedFileStreamSource` is one decoded ROW GROUP (~280 MB for
   SF100 lineitem). The 2·k channel + k in-hand batches held ~7 GB of
   tracker-invisible heap per task; ×`max_concurrent` that cleared
   GOMEMLIMIT and 30m-deadlined Q17/Q18. The dispenser therefore admits
   decoded batches against a **byte budget** (`morselDispenserBudgetBytes`,
   512 MB), released when the batch retires — counts bound nothing.
2. **Row-group-sized units also defeat the morsel model** — one consumer
   owns 280 MB while the others idle, join-probe output pools size off the
   input's ActiveLen (so transients are row-group-sized too), and
   backpressure only checkpoints between batches. The dispenser splits
   oversized batches into ~2048-row **zero-copy views**: shared parent
   column vectors, private Sel (capped three-index subslices of one
   parent-sized array), refcounted retirement returning the parent's bytes
   to the budget. Audited safety contract: linear-path ops (`exec.Filter`,
   `exec.Project`, `HashJoinProbe`, `DynamicFilterEmitOp`) never write
   input column storage and mutate only the batch's own Sel *field*.
   `Bitmap.HasNulls`' compute-and-cache memo is pre-warmed by the producer
   before views fan out (same rule as the ColRef warmup). Views must NOT
   reach retaining sinks — `Sort` stores batches and charges the Sel-blind
   `MemBytes()` — so breaker-path splitting is gated to `HashAggregate`
   sinks (which copy rows out during Consume).
3. **Linear fragments now pressure-collapse** like breakers: their
   transients are tracker-invisible by design, so the trigger is
   `memory.HeapBackpressureActive` (70% of GOMEMLIMIT); on the first trip
   consumers stop and the remaining input drains serially through the
   original chain. The failed A/B showed `morselCollapses = 0` while the
   heap blew past the limit precisely because only the breaker path had a
   collapse rule.

Retire-after-consume is the accounting handoff: once a sink has consumed a
morsel, whatever it keeps is either copied out (HashAggregate group state,
shuffle-sink accumulators) or charged to the memory ledger (Sort), so
dispenser bytes and tracked bytes never double-count or gap.

**v1.6 amendments (SF10 A/B 2026-07-03 follow-up).** The first SF10 run of
v1.5 was 22/22 row-identical but ~15% slower than the phase-2 arm, with CPU
up only ~6% — the loss was wait/serialization from making 2048 rows the
unit of *everything*. Three boundaries get their own granularity back:

- **Adaptive view size** (`viewRowsFor`): split into ~4·k views per parent,
  clamp [DefaultBatchSize, 64k]. Unconditional 2048-row views turned a 1M-row
  row group into ~500 channel handoffs + mutexed sink consumes and kept the
  producer from decoding ahead. ~4·k keeps intra-parent parallelism with
  ~16× fewer handoffs; the cap bounds view-scaled transients (probe output
  pools size off ActiveLen) and the backpressure checkpoint interval.
- **Sink-side chunk coalescing** (`unpartitionedStageSink`): a .wshf chunk
  is the downstream stage's batch and decode unit, so chunk size is the
  sink's decision — consumed rows accumulate (via the shared
  `appendBatchRowsBulk`) and flush at 64k rows / 16 MB. Chunk-per-consume
  under morsel-sized callers fragmented stage outputs ~100× (s2Decode
  26.6s→35.6s, crc32 +1.5s in worker profiles) and cascaded small batches
  into every downstream stage. Flat schemas only; nested schemas keep the
  legacy chunk-per-consume path.
- **Size-gated shuffle fan-out** (`partitionedShuffleSink`): the
  per-Consume errgroup burst (up to GOMAXPROCS goroutines, inside the sink
  mutex) only pays above `shuffleBurstGateRows` (4096); smaller consumes
  append inline.

**v1.7 amendment — splitting is a safety mechanism, gated by parent size.**
The second SF10 A/B (v1.6, results/20260703-124706) recovered only Q01:
adaptive view size fixed the scan-agg path, but every join/probe fragment
where splitting engaged stayed +10-45% vs phase-2 (Q13 2×), with CPU ~flat —
per-view consume serializes on the mutexed fragment sink, and no view size
fixes that from the dispenser side. The resolution is that splitting was
never a throughput feature: phase-2's batch-grained parallelism already won
at SF10 (−3.3% suite) on ≤35 MB decode units. Splitting exists to make
OVERSIZED units (SF100's ~280 MB decoded row groups) memory-safe under k
consumers. So `dispatch` splits only parents whose cost exceeds
`budget/(2k)` (`splitMinCost`) — small enough that 2k fit in flight means
every consumer has queue depth without views; bigger than that means
unsplit consumption would either blow the heap (k × parent in hand) or
serialize behind the byte budget. At SF10 nothing typical crosses the gate
(phase-2 behavior, validated); at SF100 lineitem row groups do (the
Q17/Q18 failure shape). The byte budget, pressure collapse, and sink
coalescing stay unconditional. Known follow-up if profiles ever show the
gate crossed AND sink-bound: move the coalesced-chunk encode/write outside
the sink mutex (double-buffered flush) so consume is append-only.

### 4.2 Worker count policy (adaptive, threshold-gated)

`k = 1` (today's behavior) unless the fragment's input estimate clears a
bytes threshold, then `k = min(cpuTokensAvailable, fragmentParallelismCap)`.
Sizing inputs already exist: `task.EstimatedBytes`, per-file sizes, and the
join-build size-gate precedent. Small fragments must not pay clone/merge
overhead — this is the `join.go` lesson institutionalized. The CPU-token
semaphore (capacity `GOMAXPROCS`, minus a reserve for the runtime/heartbeat
goroutines) bounds Σ(k) across concurrent tasks *and* is taken by the
existing errgroup bursts, so the process never schedules more compute
goroutines than cores.

### 4.3 Pipeline breakers

- **HashAggregate / Sort:** per-worker `CloneSink` partials merged at the
  barrier — existing, tested machinery. Two rules make it never-OOM-safe
  (the one real memory tension in this design):
  1. **Clones reserve.** Spill-less partials are fine for low-cardinality
     scan-side aggregation, but a high-NDV aggregate's per-worker partial
     state is k× the serial footprint. Clone accumulation must be
     accounted against the task's child tracker, and
  2. **Pressure collapses k.** When `ShouldSpillFor` trips during
     parallel consume, workers drain-and-merge into the spill-armed
     primary and the fragment continues serial (k=1) with today's
     partial-drain spill machinery. Parallelism is a fair-weather
     optimization; the memory story under pressure is exactly the current,
     SF100-validated one. No new spill format, no spilling clones.
- **Window:** stays serial in v1 (no `CloneSink`; the shared-mutex funnel
  is correctness-safe, just not parallel). A partition-key-sharded Window
  is a self-contained follow-up.
- **RIGHT/FULL OUTER probes:** stay under the existing `h.mu` match-marking
  — correct, serializing only the marking. INNER/LEFT/SEMI/ANTI probe
  fully parallel (production-proven).
- **Merge phase:** single-threaded as today; a tree merge is a follow-up
  only if profiles show the barrier merge on the critical path.
- **End-state note (so collapse is not mistaken for the architecture):**
  the long-term form of parallel aggregation is **partition-owned
  partials** — workers own disjoint group-key partitions, so no group is
  duplicated across partials (no k× state multiplication), and merge and
  spill parallelize per partition. HashAggregate's existing
  `fibHash & (drainK-1)` drain-partition scheme is already the right
  sharding; the upgrade is routing clone partials onto it. Pressure-
  collapse is the never-OOM guard until that lands, not the destination.

### 4.4 What carries over unchanged

Per-task admission (`EstimatedBytes`, `waitForPoolHeadroom`), the
`max_concurrent` semaphore semantics (now purely a memory-owner/admission
knob), coordinator task-count policy (§7), TaskProgress (atomic — safe
from k workers; the "idle task" wedge detector gets coarser and that is
acceptable: the per-task deadline and stage-idle backstops remain), task
context cancellation (`runParallel` already fans cancel out via its
worker ctx), batch single-owner discipline and `Sel` snapshot rules.

## 5. Never-OOM analysis

The design adds exactly one new memory consumer: k−1 extra clone partials
per breaker fragment. Bounded by (a) clone reservations hitting the same
shared pool that admission and spill already govern, (b) the
pressure-collapse rule (§4.3) converting parallel consume back to the
validated serial spill path, and (c) the bytes threshold keeping k=1 for
small fragments. Scan-side fused partial aggregates (the common case) have
group counts bounded by the fusion design and were the original
justification for spill-less clones. The dispenser channel bounds decode
run-ahead. No operator gains an unaccounted buffer; no sleep-based
backpressure anywhere (channel + semaphore only).

## 6. Blast radius

Changes live in: `worker/executor_fragment.go` (engage Workers + dispenser
+ pressure-collapse), fragment source wrappers around
`cachedFileStreamSource`/`partition_shard_source`, a new `cpuTokens`
primitive (worker-scoped), and small additions in `exec` (formalize the
`Source.Next` concurrency contract that today is an ad-hoc type allowlist
at `plan.go:1573`; pressure-collapse hook on the parallel run). The
embedded DB / fast path / HTTP paths are untouched (already parallel);
`exec.Pipeline.runParallel` gains the collapse hook and token awareness,
which those paths inherit.

## 7. Coordinator interaction

V1 leaves task granularity alone: task counts keyed to
`ClusterCapacity = Σ MaxConcurrent` still bound per-task memory, and
`splitFilesEvenly`/`ScanShardCount` still spread bytes. Morsels change how
fast a slot drains, not what a slot holds. Two follow-ups become available
later, deliberately out of v1: raising per-task file counts (fewer, larger
tasks now that a task can eat them in parallel — fewer stage-boundary
files also compounds with streaming exchange), and reporting a CPU
dimension in heartbeats for bin-packing. Don't touch either until v1's
utilization data says where the next bottleneck is.

## 8. Rollout and gates

- Flag-gated: `--morsel-workers` (serve flag; 0/absent = auto policy from
  §4.2, 1 = today's serial behavior as the kill switch). Config zero value
  = serial, dormant-safe.
- Gates in order: unit (dispenser contract, clone/merge parity per
  operator, pressure-collapse mid-consume, CPU-token accounting,
  Sel-snapshot under reorder), `-race` on all of it (this feature is
  *made* of data races waiting to happen), multi-worker e2e row parity
  serial vs parallel, `tpch-harness --mode=local` both flag states, SF10
  A/B, then SF100 with the standard preflight — SF100 is the real gate:
  it is where mc-vs-memory history lives, so watch Q17/Q18/Q21 heap/spill
  behavior, worker RSS, and reap counts, not just wall.
- Instrumentation from day one: per-fragment k chosen, CPU-token
  utilization, pressure-collapse count, clone partial peak bytes, merge
  barrier time.

## 9. Rejected / deferred

- **Raise max_concurrent** — multiplies memory owners; the SF100 record is
  the refutation (§3C).
- **Full work-stealing scheduler now** — B's kernel (CPU tokens) ships in
  v1; the stealing layer waits until per-task parallelism's utilization
  data shows cross-task skew actually matters at mc=4.
- **Retrofit atomics into `RowGroupIter` for consumer-side decode** —
  producer-side decode behind the dispenser achieves the same overlap
  without making a hot cursor loop atomic; revisit only if single-producer
  decode is the measured ceiling (then add producers per file, §4.1).
- **Spilling clone partials** — collapse-to-serial reuses the validated
  spill path instead of inventing a concurrent one.
- **NUMA awareness** — single-socket cloud VMs; not applicable.

## 10. Kickoff open questions — answers

1. **Morsel granularity:** one `RecordBatch` via a bounded-channel
   dispenser; decode stays producer-side at row-group/WSHF-chunk
   boundaries (§4.1).
2. **Shared vs partitioned breaker state:** per-worker clone partials +
   merge (existing `MergeableSink`), with reservation + pressure-collapse
   rules; probes share the immutable build (production-proven); Window
   serial in v1 (§4.3).
3. **Scheduler shape:** per-task fixed-worker pull from the dispenser +
   process-wide CPU-token semaphore; no global queue yet (§3, §4.2).
4. **Memory ledger:** unchanged shared pool + child trackers (already
   thread-safe); clones reserve; admission untouched (§5).
5. **Progress/cancel:** atomic progress is k-safe; cancel fans out via the
   parallel run's worker ctx; wedge detection backstops unchanged (§4.4).
