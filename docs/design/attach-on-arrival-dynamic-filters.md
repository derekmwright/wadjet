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

## Observability

- Planner: `AttachOnArrivalConsumesPlanned` counter +
  `dynamic_filter: attach-on-arrival` log line per converted edge.
- Coordinator: `staged_to_s3` count in the existing merge log now includes
  forced stagings; deferred spec count in the consume-attach log.
- Worker: `late attach installed` (with batches/rows before attach) and
  `late attach never arrived` (scan ended first) lines.
