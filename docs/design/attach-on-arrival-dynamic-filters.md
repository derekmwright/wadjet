# Attach-on-arrival dynamic-filter consumes

Status: shipped (2026-08-07). Kill switch: `WADJET_DF_ATTACH_ON_ARRIVAL=0`.

## The architectural bug

Every dynamic-filter consume edge is implemented as a hard scheduling
dependency: the planner appends the emitter's stage ID to the consumer's
`Dependencies`, and the stage-DAG executor blocks the consumer's dispatch
until the emitter completes and the coordinator merges its partials. That
barrier is the correct *coverage-maximizing* choice, but it is not required
for correctness — a bloom filter here is **drop-only**: it prunes rows that
cannot survive the downstream join, and every false positive is re-verified
by the join itself. A consumer that scans its first rows unfiltered and
installs the bloom mid-stream produces byte-identical results.

For consumes whose emitter chain is scan-only (the dimension-cascade class:
tiny filtered dimension scans), the barrier buys almost nothing — the
emitter finishes within the consumer's own startup window — while costing
2–4 s of start serialization on **every** marked query (measured on Q05/
Q07/Q21 at SF100, 2026-08-06 wlogs: scan-0 stat-dep waits on the
nation→supplier→hop-B merge chain). The class this fixes: any query with a
dimension-cascade or leaf-scan dynamic filter pays a per-query dispatch
serialization tax proportional to per-stage round-trip overhead, not to any
real data dependency.

## Per-edge mode: WAIT vs ATTACH (structural, plan-time)

A consume edge gets **ATTACH** (non-blocking dispatch, late install) iff:

1. **The consumer does not itself emit dynamic filters.** A re-emitting
   consumer (cascade mid-scan B0) derives its emitted key set from its
   post-consume output; attaching its consume late would silently widen the
   downstream filter into uselessness (supplier scans fast — by the time
   nation's bloom arrived, hop-B would ship every suppkey and the fact scan
   would see 379 M rows again). Transitive filter quality requires the
   barrier on re-emitters.
2. **The emitter is a leaf scan stage.** Join-fed emitters (semi/anti build
   filters: the filter arrives only after a multi-stage probe chain
   completes) arrive too late relative to the consumer's scan length for
   opportunistic attach to preserve their value — the consumer would finish
   mostly unfiltered and downstream shuffle/build volume would explode back
   to raw size. Those edges keep the barrier by construction, not by a
   tuned threshold.
3. **The consumer dispatches real fragment tasks** (FilterExprs /
   ProjectExprs / SecurityProjectExprs / fused-exchange — the
   `dispatchScanFilterStage` shape, and not fused scan-aggregate, whose
   dispatch path carries no dynamic-filter plumbing). Pass-through scans
   have no runtime pipeline to attach into; they keep the barrier.

Q21/Q05/Q07 under this rule: cascade hop-B (B0→fact) = ATTACH; cascade
hop-A (D→B0) = WAIT; semi/anti build filters = WAIT. No per-query logic
anywhere — the rule is stage-structural.

## Mechanism

Normalization pass `applyAttachOnArrival` runs after all dyn-filter marking
passes (legacy, semi/anti, cascade) and before the plan validators:

- sets `DynamicFilterConsume.AttachOnArrival = true`;
- **removes the stat-dep edge** from the consumer's `Dependencies` (unless
  another wait-mode consume still references the same source);
- sets `LateAttach = true` on the matching emit spec.

Coordinator:

- `mergeCompleteBuildStats` **always stages** a `LateAttach` filter to the
  deterministic key `queries/<qid>/dynfilter-merged/<stage>/<filter>.wdf`
  (inline-size filters included — the consumer polls that key, so it must
  exist). Withheld filters (incomplete partial coverage) are simply never
  staged; consumers then never attach, which is the existing "no filter"
  degradation.
- Consumer dispatch emits a **deferred spec** for attach-mode consumes:
  `DynamicFilterSpec{Deferred: true, BloomBucket, BloomKey}` with the key
  computed from the consume edge (root query ID + source stage + filter
  ID). No bloom bytes travel in the task.

Worker:

- Deferred specs bypass the synchronous fetch in
  `materializeDynamicFilters`. The fragment's `bloomFilteredSource` gets a
  *pending* entry backed by a per-executor singleflight poller (one
  goroutine per distinct staged key, 250 ms interval, context-bound).
- `Next` — single-threaded by the source contract — checks pending entries
  non-blockingly per batch; on resolution it promotes the bloom into an
  active `BloomFilterOp` mid-stream and logs
  `dynamic_filter: late attach installed` with `batches_before_attach`
  (the A/B engagement marker).

## Correctness argument

- Drop-only invariant: the bloom only ever *removes* rows, and only rows
  the downstream join would drop anyway. Unfiltered head rows = false
  positives = handled by the join. Identical results regardless of attach
  timing, including never.
- Emitter failure → errgroup cancels the query (unchanged). Filter
  withheld → key never appears → consumer runs unfiltered (unchanged
  degradation path).
- Task retries re-register with the poller; staging is an idempotent
  overwrite of the same key.
- Completeness gate is untouched: a bloom is staged only after ALL emitter
  task partials merged.

## Scheduling hazard — observed, root-caused, fixed (2026-08-07)

With the barrier gone, the consumer competes with the emitter chain for
scheduling resources. First SF10-local run realized the worst case:
supplier (B0) was not even DISPATCHED until lineitem's scan stage
completed, so every bloom "never arrived" (batches_seen=400). Worker
slots were free the whole time — the blocker was the coordinator's
**stage-dispatch semaphore** (`dispatchSlots = ClusterCapacity`, slots
held for a stage's full runtime, no notion of weight or urgency): the
bulk scan held a slot for 4 s while the 29 ms dimension scan's dependent
sat queued.

Two-part fix, both structural:

1. **Coordinator: dyn-filter-emitting scan stages bypass the dispatch
   semaphore.** They are dimension-class tiny by the marking passes' own
   eligibility rules (cascade ≤2M rows), so the semaphore's
   stampede/memory rationale does not apply, and they gate the filtering
   of concurrent bulk work — latency-critical by construction.
2. **Worker: a priority task lane.** The semaphore bypass alone still
   lost the race — the second SF10 run showed the emitters' TASKS
   queueing behind bulk tasks for the worker's MaxConcurrent slots
   (supplier's task waited ~10 s behind lineitem+orders fan-out; merge
   staged just after the consumer finished). Emitter-scan tasks now
   publish with `Task.Priority` onto `wadjet.pritasks.>` (own WorkQueue
   stream — the main tasks stream's consumer filter may not overlap
   under WorkQueue retention), and workers drain that lane with
   `priorityLaneSlots` dedicated slots outside MaxConcurrent, running
   the same `taskLoop`. The barrier was the degenerate form of this
   priority (infinite priority via serialization); the lane is the
   honest form.

Mixed-version note: a coordinator with this change requires workers that
subscribe the priority stream (deploys here ship both together); an old
worker would leave priority tasks parked until the stream's MaxAge.

## What this trades

Head-of-scan filtering coverage on attach-mode edges only: rows scanned
before the bloom lands ship unfiltered into the consumer's downstream
stage. For the cascade class the emitters are sub-second dimension scans
and the consumers are multi-second fact scans, so the expected loss is a
few percent of shipped rows in exchange for removing the full stat-dep
serialization (dispatch round-trips + merge latency) from every marked
query's critical path. Edges where that trade inverts (join-fed emitters)
stay WAIT by rule 2.

## Incremental partial publication (2026-08-07)

Kill switch: `WADJET_DF_INCREMENTAL_PARTIALS=0`.

The first SF100 pair (ctl f6a47c6 / trt 1cb836e, 2026-08-07) showed the
residual on attach-mode edges is **merge availability latency**: the bloom
becomes pollable only after emitter stage completion → NATS result gather →
coordinator batch-fetch of partials → union → merged-key PUT → consumer's
next poll. Q21's long fact scan absorbed that chain (~85% of the row cut
retained); Q07's shorter scan did not (17/48 consumer tasks never-arrived,
~90% of the cut lost).

Fix: consumers discover the emitter's **per-task partials directly from S3**
as workers upload them, OR-union progressively, and activate the union the
moment the last partial lands. The whole coordinator merge chain leaves the
availability path; what remains is (last partial PUT + ≤250 ms poll).

The correctness constraint shapes the design: a bloom missing ANY task's
keys falsely rejects rows (same reason `mergeCompleteBuildStats` withholds
incomplete filters), so partial coverage never filters — the union
accumulates across ticks and installs only at full coverage.

Mechanism:

- **Count-in-key**: the coordinator stamps `DynamicFilterEmit.StagePartials`
  (= the stage's task count) and `DynamicFilterEmit.PartialPrefix` at
  dispatch; the emitting worker uploads partials to
  `<PartialPrefix><taskID>-<filterID>.of<N>.wdf`, prefix verbatim. Any
  single partial thus tells a consumer the completeness target — necessary
  because the consumer usually dispatches BEFORE the emitter stage exists
  (task IDs are random and the count is dispatch-time knowledge). Task
  retries reuse the task ID → same key, idempotent overwrite, no double
  count. A retried attempt's artifact is byte-deterministic (same files →
  same key set → same bloom), so a consumer that read the earlier attempt's
  upload holds identical bits.
- **`DynamicFilterSpec.PartialPrefix`** on Deferred specs names the same
  prefix (`queries/st-<stage>-<qid>/dynfilter/<stage>/` — the stage-scoped
  query ID emitter scan tasks publish under). Both sides are stamped from
  one coordinator helper (`dynamicFilterPartialPrefix`), so emit and
  consume agree by construction. Kill switch off ⇒ consumer prefix omitted
  ⇒ merged-key polling only.
- **Poller**: each 250 ms tick GETs the merged key first (authoritative,
  and the fallback for mixed versions), then LISTs the prefix, GETs new
  partials, ORs them, and resolves when merged-count reaches N. Guards
  mirror the coordinator's merge: size-mismatched or oversized partials are
  rejected permanently and never count — the filter then never activates,
  exactly the coordinator's withhold degradation (which also triggers in
  that case, so neither path filters).

The coordinator merge + merged-key staging are unchanged and still run —
they serve wait-mode consumes, late-registering consumers (post-merge
polls resolve in one GET), and the mixed-version fallback.

## Guarded re-emit (2026-08-07, rule-1 relaxation)

Status: **DEFAULT OFF** (opt-in `WADJET_DF_GUARDED_REEMIT=1`). The SF100
pair (ctl 6c173cf 16:07 / trt 944e640 16:29, results/20260807-16*) proved
the guard mechanism itself sound — guard_wait_ms median 46ms/max 255ms,
retro-filter at exact dim selectivity, no deadlock, no tombstones — but
exposed the relaxation's structural blind spot: **the start barrier also
protects the mid-scan's OUTPUT volume**. At SF100 the mid's 1-2s scan
always ends before the dim bloom arrives (~2-4s), so its own consume never
installs and its output ships 100% unfiltered — full supplier (8×240K
rows) into the broadcast replicate + join build instead of the
nation-filtered ~8% — costing Q05/Q07/Q21 +33-60% in both guarded arms
(hop-B `task completed rows=240000` vs the ctl's filtered outputs is the
row-level proof). A future shape must start the mid scan early WITHOUT
shipping the unfiltered head — e.g. a worker-side scan-start hold on the
deferred bloom (barrier moved into the worker, saving the dispatch
round-trip only). The guard machinery (exec emit guards, GuardConsumes
wire, lane deep class) remains in place for that follow-up and for
experiments.

After incremental publication, cold Q07's fact-filter availability was
bounded by the emitter chain itself: hop-B (the cascade mid-scan) could not
DISPATCH until the dim stage completed and merged — measured directly
(SF10-local coord.log: scan-1 dispatched at the same millisecond the dim
merge finished; at SF100 cold that segment is the 2-4s the union missed the
consumer scan by).

Rule 1 existed because a re-emitter's late-attached consume silently widens
its emitted bloom (head-of-scan rows bypass the dim filter and poison the
key set). The relaxation removes the barrier while preserving the emitted
set EXACTLY:

- The mid-scan's consume converts to attach mode like any other edge; the
  whole chain (dim, mid, fact) dispatches at t=0 on the priority lane.
- The mid's emit op is GUARDED: while the consumed bloom is pending, rows
  buffer as (emit-key, guard-column-value) pairs — bounded at the 2M-row
  cascade eligibility ceiling, overflow degrades to unguarded (wider,
  drop-only correct, never a lost key) — and retro-filter through the bloom
  when it settles (mid-scan or at finalize). The finalize wait runs until
  the guard's poll TERMINATES — resolve or genuine withhold — with the
  poller's own deadline as the only cap: the first SF100 pair proved any
  shorter cap unsound (a 10s cap expired 300ms-10s before the dim chain
  delivered, flushed unguarded, and destroyed the downstream fact filter's
  selectivity; cold Q05/Q21 +50%). Waiting is strictly ≤ the old start
  barrier, which waited for the same chain before scanning at all. Null
  guard values drop when the bloom resolved (BloomFilterOp parity); a
  withheld guard passes everything (the consume side's degradation,
  mirrored — and now the ONLY unguarded-flush case).
- Eligibility is structural: the re-emitter must be a bare scan (no
  FilterExprs, no projections) so repositioning its emit from the sink
  (AtOutput) to the scan head (AtScan, pre-prune — where the guard column
  still exists) provably observes the same rows and columns. Cascade mids
  are exactly this shape; semi/anti build-filter emitters (join-fed,
  filtered) keep the barrier.

What changes on the chain: fact-bloom availability was
`dim scan → dim merge → hop-B dispatch → hop-B scan → partial PUT + poll`;
it becomes `max(dim chain, hop-B scan) → retro-filter (µs) → partial PUT +
poll`. The mid's own OUTPUT gains its pre-attach head rows (downstream
joins re-verify — the standard attach trade, small for dimension-class
mids).

Markers: planner `attach-on-arrival ... guarded_reemit=true`; worker
`guarded emit finalized` with buffered/dropped-by-guard/overflow counts.

## Observability

- Planner: `AttachOnArrivalConsumesPlanned` counter +
  `dynamic_filter: attach-on-arrival` log line per converted edge.
- Coordinator: `staged_to_s3` count in the existing merge log now includes
  forced stagings; deferred spec count in the consume-attach log.
- Worker: `late attach installed` (with batches/rows before attach, plus
  `source=merged|partials` and the partial count) and
  `late attach never arrived` (scan ended first) lines;
  `incremental partial union complete` when the consumer-side union wins
  the race against the coordinator merge.
