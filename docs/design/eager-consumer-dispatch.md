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

## 10. C3 SF100 validation (2026-07-20): flat suite, split verdict —
## flag stays OFF; revival = spread-gated clearance

Pair: control = results/20260719-205317 (bin 79d50cb, eager off,
steady 25.45m), treatment = results/20260720-131435 (same binary,
`eager_dispatch=true`, same-evening window). Rows 22/22 identical both
passes; eager engaged for real (44 "consumer cleared early", 42
governed waves, 1 projected-skew barrier keep).

**Suite steady 25.21m (−1.0%) — net flat, matching the 2026-07-11
verdict but now with per-query resolution from the per-task arrival
lines (task_retry.go "task result", added in PR #242):**

The measured ceiling per barrier edge is `spread(P) = complete(P) −
first_task_result(P)` clipped by the consumer span (scratchpad
eager_spread.py). Steady-pass ceiling on the control run = 349s =
23.9% of DAG wall, concentrated in exchange-repartition → hash_join
edges. The treatment converted it EXACTLY where the ceiling was large:

| Q   | ceiling | steady delta | captured |
|-----|--------:|-------------:|---------:|
| Q05 |   75.1s | −48.2s (−29.2%) | 64% |
| Q18 |   34.7s | −23.5s (−9.0%)  | 68% |
| Q04 |   14.6s | −14.0s (−12.2%) | ~96% |
| Q21 |   59.5s | −10.1s (−4.3%)  | 17% |
| Q03/Q22 | 26.4/11.9s | −5.4/−2.2s | 20/18% |

and paid a broad tax everywhere else: Q02 +20%, Q14 +18%, Q15 +19%,
Q20 +17%, Q17 +13%, Q12 +10%, Q01/Q06/Q09 +8-10%, and one outlier far
beyond tax (Q08 +93%, +39s against a 6.6s ceiling — unexplained,
needs its window read before any revival ships). Wins ≈ losses ≈
100s. The tax mechanism is the one §9 predicted for low-yield edges:
eager consumer tasks occupy scheduler attention and worker slots
while blocked on manifests, and single-wave producers yield nothing
to overlap.

**Verdict: default stays OFF. The thesis is neither confirmed nor
refuted — it is CONDITIONAL.** Overlap converts when the producer's
completion spread is large relative to per-task overhead, and costs
when it is not. Revival design (not implemented): gate eager clearance
per edge on the coordinator's own live projection — it already
accumulates per-task arrival stats in the eager feed — e.g. clear only
when projected producer spread exceeds a floor AND the consumer's
estimated input size justifies holding slots. Same architectural shape
as the skew ratio gate and the aggregatePartialSplit size floor:
mechanism on, engagement gated by measured signal, never a blanket
flag. Q08 must be root-caused first.

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

## 12. Gated SF100 arm (2026-07-20 evening): +2.1% — the tax is the
## flag-on machinery, not clearance. Arc closed, flag stays OFF.

Arm: results/20260720-210103 (bin 0e05d02 = #245 merged,
eager_dispatch=true, default 12s floor) vs the same controls as §10.
Rows 44/44 identical. Gate mechanics exactly as designed: 9 clearances
(vs 44 ungated), 37 "projected tail below floor" barrier keeps, and
the retained clearances converted (Q04 −11%, Q05 −23%, Q13 −5%,
Q15 −6%).

The decisive negative: the §10 taxed population did NOT recover even
with 80% fewer clearances — Q02 +18%, Q09 +13%, Q19 +13% reproduce
their ungated taxes almost exactly, and Q08 +88% / Q20 +41% have now
appeared in BOTH eager arms and NEITHER control, which withdraws
§10.1's variance attribution for Q08: the mechanism is linked to
flag-on through a path clearance-gating does not touch. Suite steady
25.98m (+2.1% vs the 25.45m control).

Conclusion: the eager tax is dominated by the always-on flag-on
machinery — per-producer-task manifest publication, the 3s
republisher, and the NATS fan-out that runs for every eligible
producer stage regardless of whether any consumer clears — plus an
unidentified interaction behind the Q08/Q20 signature. A clearance
gate cannot recover costs clearances never caused.

**Arc closed. --eager-dispatch stays default OFF; three SF100 arms
(§10, §12) are the evidence. The #245 gate remains in the tree — it
is inert under flag-off and strictly better than ungated if the flag
is ever flipped.** Revival preconditions, in order: (1) identify the
Q08/Q20 flag-on mechanism from the 20260720-131435 and -210103
worker logs; (2) re-engineer manifest activation to be
clearance-driven (publish only for stages whose consumer actually
cleared) so the machinery costs nothing when the gate declines;
(3) only then re-pair. The overlap wins are real (Q05 −23/−29%
twice, Q04 −11/−12% twice) — the plumbing has to stop costing more
than they earn.

## 13. Precondition (1) resolved (2026-07-25): the Q08/Q20 signature
## was refault-sensor v2 whole-run pinning, since fixed by #260

Worker-log forensics on both eager arms (gated Q08 window
21:34:22-21:35:41, task 0a1884bb; gated Q20 final_aggregate-13 task
02964f4c; plus §10.1's eager-arm task b87311e0):

- In the gated arm, Q08 had ZERO clearances (join-10/join-14 declined
  at the tail gate; join-6 is not eager-eligible), and workers never
  subscribe to manifest subjects unless a task carries EagerInputs —
  manifest publication and the republisher are coordinator-side NATS
  publishes with no subscribers. The always-on machinery is therefore
  invisible to worker execution. The §12 attribution ("per-task
  manifest publication + 3s republisher + NATS fan-out") is WITHDRAWN
  as the direct mechanism.
- The reproducible signature in all three flag-on stragglers: one task
  of a small multi-task stage (Q08 join-6: 3 probe-split broadcast-join
  tasks; Q20 final_aggregate-13: 3 tasks) runs its identical row volume
  at a uniform ~1/3 rate from its first progress line, on a worker that
  is otherwise idle for most of the run, pool ~1%, siblings normal.
  That is the documented refault-pressure floor: decode-ahead admission
  collapsed to the occupancy floor while the sensor stays active
  (§10.1 measured the sensor active with refault_rate ≈ 22k/s).
- The flag-linkage is a SCHEDULING side effect, not a load effect:
  with the flag on, the consumer stage dispatches earlier relative to
  the sibling repartition burst (gated/eager join-6 dispatched t+2.1s/
  t+6.0s vs control t+9.9s; control's later dispatch cleared the
  repartition-9 write burst first). Landing the fused probe scan inside
  the burst window meant the sensor was active AT SCAN START, and
  sensor v2 semantics then held the collapse for the task's entire
  run — ambient refault rates at SF100 (60-115k pages/s in EVERY run,
  controls included) never go quiet, so v2 never released.
- That exact failure mode was independently root-caused in the
  2026-07-22 pagecache-sensor arc and fixed by #260: refault-sensor v3
  episode cap (`ActiveBounded(refaultEpisodeCap)`,
  internal/engine/memory/pressure_os.go, wired at
  internal/worker/executor_fragment.go:597). An activation episode that
  outlives the cap despite collapse is declared non-causal and ignored.
  A task can no longer be pinned for its whole run; the +22-40% class
  this caused is measured gone (#260's SF100 validation, steady −12.4%).

Consequently the Q08/Q20 blocker is RESOLVED by the current tree.
Precondition (2) — clearance-driven manifest activation — remains
worthwhile as hygiene (don't publish for stages whose consumers never
clear), and §12's broad low-single-digit taxes (Q02/Q09/Q19) remain
unexplained on the 25m-era binary but are below the noise floor of the
current 7m2x-era suite; the re-pair (precondition 3) re-measures them
on the world that matters.

2026-07-25 re-measure of the ceiling on bdef5ce (peerwire-treat run,
results/20260725-123737, steady DAG wall 424s): measured edge ceiling
133s = 31.3%, concentrated Q18 40.4s (55.8%), Q05 18.1s, Q21 16.2s,
Q10 10.3s, Q07 9.3s, Q09 7.9s. Post-fusion plan shape moved the
largest edges to JOIN-stage producers (Q18 join-8→join-17, Q05
join-4→join-6, Q21 join-12→join-16): a fused chain's terminal join now
emits the partitioned files the next join consumes. Manifest
publication today exists only for exchange-repartition producers
(orchestrate_repartition.go); covering join-producer edges and making
fused consumers eligible (the §3.2 dep-count gate rejects stages whose
Dependencies grew by chained-build deps) are the two design extensions
the revival needs.

POST-FUSION ceiling (main da5301a, results/20260725-162139, steady DAG
wall 399s): 88s = 22.2%. Q21 19.1s, Q18 17.1s, Q03 13.3s, Q10 6.8s,
Q13 6.4s, Q11 6.3s, Q05 5.1s. Composition: plain 2×-repartition hash
joins (already C2-eligible) carry roughly half; fused chains (Q18
join-8, chained_joins=1) and compute-stage producer edges (Q21
join-12→join-16 9.9s, Q18 final_aggregate-14→join-8 6.1s) the rest.
Every measured spread except Q21's is BELOW the 12s tail floor — the
§11 calibration is stale on the 7m2x-era binary (per-task overhead and
consumer spans shrank ~3×); the re-pair must re-derive the floor
(treatment arm runs WADJET_EAGER_MIN_TAIL_SECONDS at ~3-5s).

## 14. Revival implementation (2026-07-25, perf/eager-revival)

A1 — clearance-driven manifest activation (precondition 2): eagerFeed
gains an `active` flag set at first consumer clearance
(execute_stage_dag.go eagerActive branch). The publisher hook still
folds every completion into the replay list and the completion
accounting (clearance decisions read those), but the NATS publish is
gated on activation; the republisher starts at activation instead of
producer dispatch; a one-time backlog flush at activation closes the
snapshot→subscribe window for completions that landed before
clearance. A feed no consumer clears on generates ZERO NATS traffic.
Regression: TestEagerTailGate asserts zero manifests published when
every consumer declines.

A2 — fused-chain consumer eligibility: the C2 dep-count gate accepts
`2+len(FusedJoins)+len(ChainedJoins)` (stage-chain fusion grows deps
by one per ChainedJoinSpec). Only the primary probe/build feed
eagerly; chained-build deps are barrier-complete before the clearance
decision runs (dependency wait loop ordering). eagerJoinWouldSplit
mirrors dispatchComputeStage's skew-skip for partitioned chains.
Engagement marker: EagerChainedEdgesPlanned + chained_joins attr on
the clearance line. E2E: Q18-shaped chain clears eagerly with
row-identical results (TestEagerChainedJoinDispatchE2E).

A3 — compute-stage producer manifests: dispatchComputeStage wires the
same feed + onSuccess publisher as runShuffleSide for hash-partitioned
hash_join / final_aggregate stages with a plain unpartitioned sink.
Task DISPATCH ORDER is the partition (the positional contract
StageOutput.Files already gives partitionFilesForWorker); the manifest
source maps plain files to partitions via the ProducerTaskIDs ordinal,
while partition= files keep filename semantics — so shuffle and
compute producers coexist on one mechanism. Producer declines: skew /
rr-agg / probe splits (they remap task→partition), gather fusion (no
retry → no fencing recovery), exchange sinks (v1 scope),
dynamic-filter and scalar participants, coordinator-read stages.
Consumer eligibility (eagerFeedableDep) accepts repartition or
compute-producer deps for both C1 and C2. E2E: with fusion pinned off,
join-9 clears on TWO compute feeds (probe join-4 + build
final_aggregate-6) and aggregate-10 cascades as a C1 consumer of
join-9 — three eager stages deep, rows identical
(TestEagerComputeProducerE2E).

### §14.1 Clean-window SF100 pair (2026-07-26): neutral ex-Q18, one
### blocking wedge — flag stays OFF

Pair: control results/20260726-103757 vs treatment
results/20260726-111531 (same binary 65f7604, same morning window,
fresh cluster per arm, clean catalog after the #278 tripling incident,
treatment WADJET_EAGER_DISPATCH=1 + WADJET_EAGER_MIN_TAIL_SECONDS=3).
Rows 44/44 identical — the clearance/manifest/fencing mechanism is
correctness-clean at SF100 under real engagement (clearances incl. a
chained_joins=1 fused chain and C1 finals; governed waves; activation-
gated publication).

- Suite steady: control 544.7s vs treatment 869.4s (+59.6%) — but the
  ENTIRE delta is Q18: 54.9s → 379.6s steady (447.2s cold). Excluding
  Q18 both arms are 489.8s — exactly neutral, wins ≈ taxes:
  wins Q16 −59% Q12 −57% Q19 −56% Q22 −53% Q14 −36% Q21 −23%;
  taxes Q15 +111% Q13 +86% Q11 +74% Q07 +27% Q20 +23%. Same
  conditional-convergence economics as §10, now at floor 3s (44
  clearance decisions; single pair, control steady sat high in its
  variance band — per-query deltas are indicative, not conclusive).
- Q18 WEDGE (blocking): join-8 (cleared, chained) span 137s vs ~25s;
  then final_aggregate-19 — which DECLINED at the floor and ran the
  barrier path — had all 3 merge tasks idle ~192s before doing 3.8s of
  work (fragment phases: elapsed 195.8s, src 3.8s, ops 0, sink 0;
  "task progress idle; stopping AckWait extension idle_for=1m57s" on
  all 3 workers; worker pool drained slowly 5.8→2.9 GB across the
  stall; no retries, no fencing errors, self-resolves). The stall
  survives the query's own clearance decisions — some resource or
  publication path held by the earlier eager stages drains on a
  minutes-scale timer. Root cause unknown; needs a local repro
  (the A3 e2e's 21s toy-scale stall is the likely small sibling).
- Side observation: the #277 fused-chain panic (6 hits in the control
  arm) did NOT occur once in the treatment arm — consistent with its
  timing-dependence.

VERDICT: --eager-dispatch stays default OFF. Preconditions for the
next pair: (1) root-cause and fix the Q18 wedge locally; (2) re-pair
(and per ADR-0011 run ≥2 pairs before believing per-query deltas).
The win population is real but currently cancelled by the tax
population even at floor 3 — if the wedge fix doesn't also shift the
economics, the revival needs the per-edge spread gate sharpened
(clear only edges whose §13 measured class converts) rather than a
global floor.

### §14.2 Pair 2 (2026-07-26 afternoon, binary 1a72f0e with the
### replay-alias fix): wave taxes gone, Q18 steady crawl is THE blocker

Pair: control results/20260726-122025 (steady 415.0s — fastest
flag-off suite recorded) vs treatment results/20260726-134212
(eager=1 floor=3). Rows 44/44 identical again.

- The §14.1 tax population COLLAPSED with the replay fix: Q13 +86%→−3%,
  Q11 +74%→−33%, Q15 +111%→−47%. Wins held or grew: Q16 −52%, Q15 −47%,
  Q04 −42%, Q11 −33%. New modest taxes: Q07 +37%, Q17 +31%, Q05 +30%,
  Q02/Q06/Q01 +20-30% (small queries), Q22 +107% (3.9→8.1s).
- STEADY ex-Q18: control 369.1s vs treatment 388.7s (+5.3%) — flat to
  slightly negative. COLD: +2.5% (Q18 cold behaved!).
- Q18 STEADY: 46.0 → 475.8s (+935%) — the residual crawl reproduced
  2/2 pairs and is now STEADY-ONLY + flag-on-only. Combined §14.1
  evidence: merge/join tasks read ~1 file per 20s on otherwise-idle
  workers, in cleared AND declined modes, phase counters near zero
  (block outside timed sections). This single query is the entire
  blocker; ex-Q18 the flag is a wash pending edge-class gating.

VERDICT unchanged: flag stays OFF. The one prerequisite for any
further pairing is root-causing the Q18 steady crawl — now known to
need warm/steady cluster state plus the flag, pointing at state
carried from the cold pass (feed/republisher lifecycle, peer-hint or
upload-queue interactions) rather than the clearance decisions
themselves. Investigation is local-first (EC2 freeze until August).

Known v1 constraints for the re-pair:
- eagerStageSlot cap=1 serializes cascading clearances (a producer
  that itself cleared holds the slot until its stage completes, so its
  consumer takes the barrier). The A3 e2e widens the slot to 2 to
  exercise the cascade; production keeps 1 until the pair says
  otherwise.
- The 12s tail floor declines nearly every current-world edge (§13);
  the treatment arm overrides WADJET_EAGER_MIN_TAIL_SECONDS≈3.
- Republisher cadence (3s) bounds the snapshot→subscribe heal window;
  at toy scale that dominates eager wall (the 21s A3 e2e), at SF100 it
  is noise against multi-second spreads.
