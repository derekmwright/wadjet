# Eager consumer dispatch (streaming-exchange Phase C bridge)

> **Status:** DESIGN FOR REVIEW — no code. **Date:** 2026-07-12.
> **Verified against:** main @ `d076bda`. Anchors below were confirmed
> against that commit; they drift.
> **Prerequisite state:** morsel-driven execution DONE (PR #181, default
> auto since PR #198) — the condition streaming-exchange.md §3C set for
> starting this work. Peer fetch + async upload (Phases A/B) default-on
> everywhere since PR #217.

## 1. Goal and evidence

Remove the per-edge stage barrier for shuffle consumers: a consumer stage
starts while its producer stage is still running, consuming each producer
*task's* output files as that task finishes (pull-based fetch retained,
no operator changes — pipeline breakers are sinks and don't care about
input arrival order).

Why this is the top lever now (SF100 profiling run
`results/20260712-034241`, SE-on, post zstd fix):

- Workers run **2.5 of 16 cores** (was 1.4 before this week's fixes; the
  remaining 84% idle is structural, not CPU).
- Largest critical-path block class: fragment/stage barrier waits
  (`runFragmentLinearParallel` ~5.5k goroutine-s/worker) — downstream
  stages idle while the slowest producer task of the previous stage
  drains.
- CPU-side costs (memmove/SetFrom ~16%, s2 ~8%) are second-order while
  utilization is this low.
- Trino's remaining ~5.6× advantage is streaming execution between
  stages; we materialize + barrier per edge.

The win mechanism is overlap: producer stages run in waves
(`tasks > cluster slots`), and today the consumer waits for the last
straggler before reading the *first* wave's files. Eager dispatch lets
consumer scan/decode/join-build work overlap the producer tail. It does
NOT change when the consumer can *finish* (its last input still lands
last) — this bounds the win honestly (§9).

## 2. What the code does today (verified)

- **The barrier:** `executeStageDAG` allocates one `done` channel per
  stage (`execute_stage_dag.go:361`); each stage goroutine blocks on all
  dependencies' channels (`:410-420`); `close(done[s.ID])` fires only
  after the whole stage completes (`:487-490`). Release is all-or-nothing.
- **Consumer inputs are built after the barrier:** task specs carry
  explicit per-partition S3 key lists (`Task.Inputs map[string][]string`,
  built by `buildTaskInputsForStage` → `partitionFilesForWorker` from the
  completed producer's `StageOutput.Files`). The worker never Lists a
  prefix (`newPartitionShardSource` has no non-test caller); the file set
  is frozen at task build (`cachedFileStreamSource` iterates a fixed
  slice, `stream_source.go:234`).
- **Keys are deterministic:** producers write
  `<ResultPrefix>partition=%04d/<TaskID>.wshf` (`executor.go:1021`).
  Producer TaskIDs and ResultPrefix are coordinator-assigned at producer
  dispatch — so the *candidate* key universe for a consumer is knowable
  before any producer task runs. What is NOT knowable upfront: which
  candidates exist (empty partitions write no file) and where they are
  (peer location), both of which today arrive via `ResultNotification` →
  `peerFileRegistry.Record` (`peer_locations.go:65-86`) only for
  completed tasks.
- **Skew-split needs full accounting:** `planSkewSplitTasks` returns nil
  unless `len(PartitionBytes) == NumPartitions` for both deps
  (`skew_split.go:124-130`); those vectors reduce over final surviving
  attempts of *every* producer task (`task_retry.go:236-260`).
- **Worker slots don't yield:** admission is a `MaxConcurrent` semaphore
  (`worker.go:470`); a running task holds its slot until completion or
  deadline. A consumer waiting on a missing input blocks in
  `awaitDurableObject` for 15 s then fails the task
  (`peer_exchange.go:172-219`). There is no requeue.
- **Retry contract (Phase B, settled):** task retries are verbatim
  re-sends against durable-by-key inputs; `MissingInputKey` with a dead,
  non-durable producer → `ErrInputLost` → one-shot whole-query rerun
  with streaming exchange disabled (`coordinator.go:787-792`). The
  terminal gather already streams (no barrier) and is out of scope.

## 3. Design overview: manifest-fed consumers

Three pieces, all coordinator/worker plumbing — zero operator changes.

### 3.1 Producer-task manifests

The coordinator already observes every producer task completion
(`taskRetrier.Observe` → `noteTaskResult`). New: for an eager edge it
re-publishes a compact **manifest** per completed producer task on a
root-query-scoped NATS subject (`wadjet.eager.<rootID>.<stageID>`):

```
ProducerTaskManifest {
  StageID, TaskID, Attempt  // attempt fencing, §5
  Files []string            // the keys that EXIST (empty partitions absent)
  WorkerID                  // peer-fetch hint
  Final bool                // true on the stage's last terminal task
}
```

Metadata-only (~a few KB); no payload bytes flow through the
coordinator. NATS pub/sub fits (control-plane scale: tens of messages
per stage); the gRPC data-plane stays untouched in v1.

### 3.2 Eager consumer tasks

An eager consumer task spec replaces the frozen `Inputs` list for the
eager edge with:

```
EagerInput {
  StageID            // producer stage
  ProducerTaskIDs [] // full candidate set, known at dispatch
  ResultPrefix, NumPartitions
  PartitionRange     // this task's contiguous range, as today
}
```

Worker side: a new `manifestStreamSource` implementing the existing
`Source` interface, wrapping today's `cachedFileStreamSource` machinery:
it subscribes to the manifest subject **before** signalling readiness
(subscribe-then-replay: the coordinator includes already-completed
manifests in the task spec so no completion is missed), and feeds files
for its partition range into the fetch tier (peer → S3) exactly as
today, in producer-task-completion order. `Next()` blocks when no
fetched batch is available AND not all producer tasks have reported;
returns EOF after processing `Final` + all candidates resolved.
Everything downstream of `Next()` is unchanged.

Non-eager aliases in the same task (e.g. a broadcast build side) keep
today's frozen lists.

### 3.3 Dispatch policy (deadlock-safe by construction)

The consumer stage goroutine no longer waits for `done[dep]`; it waits
for **dispatch clearance**:

- **Producer-lane reservation.** Eager consumer tasks assigned to a
  worker are capped at `MaxConcurrent − 1` until the producer stage has
  no undispatched/running tasks on that worker; the reserved lane
  guarantees producer progress (worst case producers drain serially,
  which is today's behavior). The coordinator enforces this from
  heartbeat `ActiveTasks` + its own dispatch bookkeeping — no worker
  changes.
- **Join edges: after the early skew decision** (§6) — clearance is the
  decision point, not full producer completion.
- The stage-level `dispatchSem` (`execute_stage_dag.go:387-397`) is
  acquired as today; eager stages hold their slot longer (they start
  earlier), so the sem floor stays ≥ 2 to avoid self-starvation of
  producers' stage goroutines. (`ClusterCapacity` sizing already
  guarantees this.)

A feature flag `--eager-dispatch` (default **off** until SF100 + skew
fixture validation; kill switch thereafter) gates the whole path at the
coordinator; workers treat `EagerInput` presence as the signal, so a
mixed-version cluster degrades loudly (unknown field → task fails →
retry → same failure → stage fails) rather than silently. Deploys are
atomic in practice (bin_version), so this is a non-issue operationally.

## 4. The §3C objections, revisited for eager dispatch

The four grounds that rejected full pipelined co-scheduling, and where
eager dispatch stands:

1. **Admission-control deadlock.** Full pipelining gates one side on
   resources the other releases. Eager dispatch keeps both sides
   pull-based and makes deadlock structurally impossible via the
   producer-lane reservation (§3.3): producers never wait on consumers
   for anything; consumers only wait on producer *outputs*, and producer
   progress is guaranteed by the reserved lane. No gang admission.
2. **Retry granularity.** Unchanged from Phase B for producers (outputs
   are whole files, overwrite-safe by key). Consumers gain one new
   failure mode — consumed a file whose producer attempt was later
   superseded — handled by fencing (§5) with the existing consumer-task
   retry as the recovery, and the existing one-shot SE-disabled rerun as
   the backstop. Task-level retry survives; the #109/#110 investment is
   not forfeited.
3. **Memory floor.** No new resident classes: consumers hold exactly the
   operator state they hold today, just starting earlier; manifests are
   metadata; fetched files land in the same NVMe temp + mmap path.
   Wall-clock overlap does raise *cluster-wide* concurrent memory use
   (two stages' working sets alive at once on a worker) — but that is
   already true today whenever sibling DAG branches run concurrently,
   and the per-task budget/admission estimate machinery (`waitForPoolHeadroom`,
   estimates doubling on retry) governs it identically. No new ledger
   surface.
4. **Sequencing.** Morsel landed (PR #181/#198). The consumer-side
   pipeline this feeds is the post-morsel executor. Prerequisite
   satisfied; `docs/design/morsel-execution.md`'s stale "proposed"
   status line is corrected alongside this memo.

## 5. Attempt fencing and the consumed-file-set

The one genuinely new correctness surface (named by §3C when it defined
this bridge).

Hazard: consumer reads producer task T attempt 1's file; T's worker dies
pre-durability; T retries as attempt 2 and **overwrites the same key**
with (deterministically identical, but not guaranteed byte-identical —
batch order within the file may differ) content. The consumer's already-
consumed rows came from attempt 1; mixing attempt 2's file for the same
task on another partition of the same consumer task is fine (row sets
are identical per §5 of streaming-exchange.md: same task spec, same
inputs, deterministic partitioning) — **but a partially-read file being
overwritten mid-GET is not**, and "identical rows" relies on producer
determinism we should fence, not assume.

Contract:

- Manifests carry `Attempt`. The consumer records
  `consumed[taskID] = attempt` at first byte read of any of that task's
  files (files are streamed to local temp in one GET, then mmap'd — a
  single-object read is atomic at S3/peer level, so per-file torn reads
  cannot happen; the fence is about cross-file/cross-attempt mixing).
- If a manifest arrives for `taskID` with `Attempt >` a consumed entry,
  the consumer task **fails itself with a new `StaleInputAttempt`
  marker** (poison-pill, loud). Files not yet consumed are simply
  re-pointed at the newest attempt's manifest.
- Coordinator classification: `StaleInputAttempt` → retry the consumer
  task (attempts cap 3, as today). By the retry, the producer attempt
  set is stable (the retrier republished the manifest only after the new
  attempt completed). If instability persists (producer flapping), the
  existing `ErrInputLost`/rerun-with-SE-disabled backstop terminates the
  pathology.
- Frequency argument: this fires only when a producer worker dies
  mid-stage AND a consumer already consumed its output — the same rare
  window Phase B's `ErrInputLost` handles today; the cost is one
  consumer-task retry instead of a query rerun.

## 6. Skew-split: early decision via ratio invariance

Skew-split (default on) decides at join dispatch from complete
`PartitionBytes` (§2). Eager dispatch must not wait for that — and does
not need to:

- Every producer task hash-partitions its input slice across **all**
  partitions, so each completed task contributes a full cross-partition
  byte profile. The split trigger `ratio = probeBytes(group) /
  meanGroupBytes` is **scale-invariant**: numerator and denominator both
  scale ~linearly with completed-task fraction. The ratio estimate from
  the first completed wave converges fast and is unbiased for hash
  partitioning.
- The absolute floor (`skewSplitMinGroupBytes` 256 MiB) uses a linear
  projection: `projected = observed × numTasks / completedTasks`.
- **Decision point:** when `completedTasks ≥ max(workerCount,
  ⌈numTasks/4⌉)` for both deps — one full wave, enough that every
  producer's slice shape is represented. The decision is then **frozen**
  (split layout = assignment of producer TaskIDs × hot-partition files to
  sub-tasks, all nameable from the candidate set) and consumers dispatch.
- Degrade rule unchanged in spirit from `stage_output.go:48-49`: any
  doubt (accounting missing, projection below floor but near it,
  probeSplit path) → **no split, never a wrong split**. A group that
  full data would have split but the early decision missed = today's
  behavior (no split), i.e. eager dispatch can cost at most the split
  win on that edge, never correctness.
- The skew A/B fixture (`benchmarks/skew`) gates this exactly: eager-on
  must reproduce the −41% straggler win within noise, with split
  markers present in the log (the observability counters from PR #213
  era already exist).

## 7. What v1 does NOT do

- No mid-file streaming, no batch-level flow (files remain the unit;
  producers finalize whole partition files exactly as today).
- No operator changes; no change to pipeline breakers, morsel scheduling,
  or the memory ledger.
- No gather changes (already streaming), no broadcast-edge changes
  (broadcast stays S3+KV per streaming-exchange.md §B), no scalar-
  subquery edges (string-substitution dependencies keep the barrier).
- No worker-side slot yield/requeue. If profiling shows reserved-lane
  idle time matters, that is a v2 item.
- No change to the small-query local fast path (no stages to overlap).

## 8. Phases and gates

Phase C1 — protocol + non-join edges (agg/sort/distinct/repartition
consumers):
1. Manifest publication + `manifestStreamSource` + dispatch clearance
   with producer-lane reservation; flag default off.
2. Gates: unit tests (manifest replay, fencing poison, empty-partition
   candidates, Final ordering); `tpch-harness --mode=local` both flag
   states; TPC-H SF0.01 22/22 both flag states; race-built harness arm.
Phase C2 — join edges + early skew decision:
3. Ratio-invariance decision + frozen split layout; skew fixture A/B
   (eager×split matrix, all four arms row-identical, split markers
   verified); harness local.
Phase C3 — activation evidence:
4. SF100 same-window pair (needs deploy approval), flag on vs off, on
   current main. Success = 22/22 row-identical, suite improvement beyond
   the ±15% single-pair envelope or a per-query mechanism-marked win
   pattern (eager overlap counters — add `EagerEdgesPlanned` /
   `ManifestsConsumedEarly` counters mirroring the SortMergeJoinsPlanned
   observability precedent so the treatment is provable in the log).
5. Default flip in a separate PR after soak, mirroring the
   skew-split/morsel rollout pattern.

## 9. Expected effect, honestly bounded

Overlap saves `min(consumer head work, producer tail wall)` per eager
edge. It cannot beat the last producer file's arrival; single-wave
producer stages (tasks ≤ slots) yield little; multi-wave stages (the
Q18/Q21 repartition shape: dozens of tasks over 12 effective slots)
yield the most. The measurable prediction: fragment-barrier block time
(~5.5k gs/worker in `results/20260712-034241`) shrinks in proportion to
overlap achieved, and worker utilization rises above 2.5/16 cores on
multi-stage queries. If the SF100 pair shows no movement in those two
markers, the thesis is wrong and the flag stays off — no threshold
tuning to force it.

### 10.1 Q08 outlier root-caused (2026-07-20): known pressure-floor
### variance, not an eager defect

The +93% came from ONE join-6 probe-split task (b87311e0, worker
i-0e39…): 62.1s for ~1.37M rows while its two siblings did the same
volume in ~24s. Fragment progress lines show uniform ~22k rows/s from
start (siblings ~55k rows/s), pool at 1% — no stall, no queueing, no
spill, and the worker ran nothing else for the task's last 53s. The
worker's decode-ahead stats in that window show the refault pressure
sensor active (refault_rate ≈ 22k/s, activations climbing): the fused
probe scan inside the join task was pinned to the 2-deep pressure
floor for its whole run — the same partial-residency background-
refault mechanism documented as Q05's 117-165s cross-window band
(scan-decode-pipelining.md §9.4). Eager only reshuffled scheduling so
the collapse landed on Q08 in this pair (control's Q05 drew the short
straw at 165.1s that evening; the eager arm's Q05 ran 116.9s).
No eager-specific defect found.

## 11. Projected-tail clearance gate (implemented 2026-07-20; flag
## still OFF pending the gated SF100 pair)

§10's split verdict is conditional convergence: clearance converts
exactly when the producer still has a long completion tail to overlap.
The gate encodes that:

- `eagerFeed` timestamps successful producer-task completions
  (firstDone/lastDone in noteCompletion).
- At clearance, EVERY eager consumer — C1 non-join included — now
  holds for `decisionReady` (a full wave + a quarter of producer
  tasks), so a wave of arrival stats exists. The eagerness lost is the
  first wave, the population §9 predicted and §10 measured as yielding
  nothing.
- `projectedTailSeconds()` = mean observed inter-arrival × tasks
  remaining; fewer than two completions or nothing remaining → 0
  (decline toward the barrier, never toward a wrong clearance).
- Clearance requires `max(tail over the stage's eager feeds) ≥
  eagerMinTailSeconds` (default 12, the §10 envelope: every measured
  edge ≥ ~12s converted, every edge ≤ ~10s paid tax;
  WADJET_EAGER_MIN_TAIL_SECONDS overrides, 0 restores ungated C3
  behavior for A/B). Below the floor the stage logs
  "projected tail below floor — keeping barrier" and takes the
  barrier — byte-identical to flag-off planning.

Acceptance for the gated SF100 pair, derived from §10's table: the
gate must clear Q05/Q21/Q18/Q04/Q03-class edges (their steady wins
summed ≈ 100s ≈ 6-7% of suite) and keep the barrier on the
Q02/Q07/Q09/Q10/Q14/Q15/Q16/Q20 population (whose §10 taxes summed to
the cancellation). Success = suite steady meaningfully below the
25.45m reference with no query outside its documented variance band;
failure = flag stays off and §10's verdict stands as final.

Gates run at implementation time: unit (projectedTailSeconds math,
TestEagerTailGate barrier contract), full coordinator suite, TPC-H
SF0.01, SF1 harness with eager on at the default floor (gate declines
everywhere at SF1 scale — plans byte-identical to flag-off) and at
floor 0 (ungated clearance engages; rows identical).
