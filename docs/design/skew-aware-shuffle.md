# Adaptive skew-aware shuffle

Status: DRAFT design — 2026-07-10. Grounded against main c9e139a by explorer
sweep; all anchors verified current at write time.

## 1. Problem

Shuffled hash joins consume partition-aligned inputs: the task for partition
p reads build[p] AND probe[p] (`partitionFilesForWorker`,
stage_output.go:98 — contiguous whole-partition binding). One hot key ⇒ one
oversized partition ⇒ one straggler task ⇒ the stage's wall clock, and a
per-task memory budget breach on exactly the worst worker. The only current
mitigation is data-blind static over-partitioning
(`shufflePartitionMultiplier = 4`, orchestrate_repartition.go:24), which
dilutes moderate skew but cannot split a single hot key and multiplies file
fan-out for every query, skewed or not. TPC-H is nearly uniform; security
workloads (the actual target) are not — and NULL-heavy keys hash to ONE
partition by construction (sink null marker), a guaranteed hot partition.

## 2. Design — measure at write, decide at the seam, adapt the layout

Everything rides existing machinery; four narrow additions:

### (a) Per-partition size accounting in the shuffle sink
`partitionWriter.numRows` (partitioned_shuffle_sink.go:92) already counts
rows per partition persistently. Add the missing `bytesWritten int64`
mirror (accumulate `bytesAdded` from `appendBatchRowsBulk` at the same two
sites that bump numRows). Zero extra passes, no new locks.

### (b) Per-partition arrays on ResultNotification
`executeShuffle`'s upload loop already `os.Stat`s every partition file
(executor.go:1016/:1091) and then DISCARDS the breakdown into scalar
`SizeBytes`. Add `PartitionRows []int64` + `PartitionBytes []int64`
(indexed by partition id, omitempty) to `ResultNotification`
(messages.go:445) and populate them from the sink counters + stat sizes.

### (c) Skew decision at the stage-completion seam
`orchestrateRepartition` (orchestrate_repartition.go:116) holds the
per-partition `ShuffleLayout` after `g.Wait()` — BEFORE the consuming join
dispatches. Reduce the per-task partition vectors element-wise (the same
bucketing `runShuffleSide` already does for files at :288-300) into
`PartitionBytes[p]` on the layout. Decide per partition:

    splitFactor[p] = ceil(bytes[p] / targetTaskBytes)   capped at workerCount
    hot iff splitFactor[p] > 1 AND bytes[p] > absolute floor (e.g. 256 MiB)
    AND build[p] fits the replication bound (see safety below)

Relative-only ratios (max/mean) are NOT the trigger — an absolute
bytes-vs-budget threshold is what protects memory; ratio is logged for
observability only.

### (d) Hot-partition-aware task layout
In `dispatchComputeStage` (execute_stage_dag.go:1830) +
`buildTaskInputsForStage` (:3099): non-hot partitions keep whole-partition
binding; a hot partition p becomes k sub-tasks that SPLIT probe[p]'s files
(`splitFilesEvenly`, the broadcast probe-split pattern at :3238) and
REPLICATE build[p] to each (:3236 pattern). Correctness: a probe row for
key k needs the complete build side for k, which replication preserves;
join output partials merge exactly like broadcast probe-split already does
(re-aggregation/sort/dedup at the coordinator — existing machinery).

**Safety bound**: replicate build[p] only when its bytes fit the per-task
budget (`estimateComputeTaskBytes` substrate, :3065). If the BUILD side of
p is itself the hot side, do NOT split — the worker's grace
partition-on-arrival (join_partition_arrival.go:48, 64 sub-partitions +
largest-first spill) already bounds memory inside the single task; splitting
probe against an over-budget replicated build would multiply the OOM, not
fix it.

## 3. Observability + flag

- `--skew-split` (default off v1), env `WADJET_SKEW_SPLIT`, terraform var —
  the late-mat/bushy plumbing pattern.
- `SkewSplitsPlanned` counter + one Info log per decision naming
  (stage, partition, bytes, splitFactor) — the mechanism marker required by
  the same-window A/B discipline (a wall delta without a fired marker is
  window drift, not signal).
- TPC-H will barely trigger it (uniform keys); the validating benchmark
  needs a skewed dataset — either the harness micro tables with a hot-key
  generator arm, or Q18-shape data with a manufactured hot orderkey.
  Gate = rows identical + straggler wall on the skewed set, NOT TPC-H suite
  wall (expect ~0 there; do not chase it).

## 4. Phases

1. **Accounting** (a)+(b): sink counter, wire arrays, coordinator reduction
   onto ShuffleLayout. No behavior change; unit tests assert the vectors.
2. **Decision + layout** (c)+(d) behind the flag: hot-partition detection +
   split-probe/replicate-build task construction. Gates: harness
   `--mode=local` both arms row-identical (3-worker fusion/dispatch paths —
   the 1-worker in-process suites CANNOT catch dispatch bugs, learned twice
   in the bushy arc); a new skewed-fixture correctness test that forces a
   split and asserts row parity vs unsplit; native-DAG suite green.
3. **Validation**: skewed-dataset A/B on SF10-class deploy (marker-proven),
   then default-flip decision separately.

## 5. Risks / notes

- Retries: `retrier.Observe` replays failed tasks — per-partition vectors
  must come from the FINAL surviving task set (bucket by partition as
  runShuffleSide already does for files, not by raw notification count).
- Streaming exchange: peer-fetch hints are per-file; split tasks inherit
  file-level hints unchanged. Async upload sizes are known at notification
  time (local stat), so the vectors don't wait on S3.
- DAG-shape validation (`ValidateNativeDAGShape`) counts join deps — split
  tasks change task counts, not stage dependencies; no validator change
  expected. Verify.
- Legacy pipeline path (`buildShufflePipelineTasks`) stays non-adaptive v1;
  the native-DAG path is the live one.
