> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Native-DAG Distributed Execution

**Date:** 2026-04-22
**Supersedes runtime-integration half of:** `docs/_archive/specs/2026-04-20-distribution-property-phase-2-design.md`, `docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md`
**Keeps:** `docs/_archive/specs/2026-04-20-distribution-property-phase-1.md` (Phase 1 — merged as PR #44, provides `RequiredKind`, `Satisfies`, `OutputDistribution`, `AssertExchangeConsistency`)
**Keeps:** Phase 2a planner-side work on `feat/distribution-property-phase-2` (commits `707a0f2` through `d1e8175` — `EnsureDistribution`, Exchange stage types, goldens)

## Context

Wadjet can't run multi-step distributed shuffles today. Its coordinator's runtime model — a four-mode switch that either runs the whole plan on one worker, probe-splits a single scan across workers with broadcast builds, or does a two-sided shuffle of one join pair — was simplified in the 2026-04-03 "Phase 5 collapse" and can only express single-shuffle plans. Queries needing chained shuffles (Q21's lineitem ⋈ orders ⋈ suppliers, Q12/Q17/Q03 at SF10+) either OOM on broadcast duplication or slow down 10-20× as broadcasts compound.

Phase 1 delivered the distribution-property IR (`RequiredDistribution`, `OutputDistribution`, `Satisfies`). Phase 2a delivered `EnsureDistribution` — a planner pass that inserts `StageExchangeRepartition / Replicate / Gather` stages wherever a child's output doesn't satisfy its parent's requirement. The Exchange-annotated plan is structurally correct and matches Trino/Spark's IR.

Phase 2b tried to collapse the Exchange DAG back into the legacy single-`pipeline-0` runtime shape. That broke SF10 catastrophically (Q18 timeout, 5 queries returning wrong rows, 3.24× total slowdown) because the collapse discards multi-step shuffle information by design. See `memory/project_phase2b_sf10_regression_2026-04-21.md`.

This design replaces the runtime integration with native DAG execution: the coordinator walks the stage DAG in topological order, dispatches one stage's worth of tasks at a time, waits for each stage to materialize its output to S3, and moves to the next. Workers consume stages via the existing push-based pipeline executor with generalized source (read shuffle output of previous stage) and sink (write shuffle output OR stream to Gather reply subject).

## Goals

1. **Execute Exchange-annotated plans as emitted by `EnsureDistribution`.** No collapse, no single-`pipeline-0` shape, no switch.
2. **Multi-step shuffles work.** Chained joins that each need repartitioning run as N+1 shuffle stages + final pipeline stage, not one big broadcast.
3. **Restore the 2m02s SF10 baseline.** Close the regressions traced to 2026-04-03: Q12/Q17 ~20× gap, Q03 ~14× gap, Q21 OOM/slow. Match or beat main today on every TPC-H query at SF10.
4. **Align with Trino/Spark architecture.** Coordinator owns stage boundaries; workers own intra-stage operator execution. This is the canonical shape; stop fighting it.
5. **Keep the pipeline executor untouched.** It's the crown jewel — vectorized, push-based, well-tested. Only the source/sink boundaries need to learn shuffle I/O.

## Non-Goals (deferred)

- **Streaming pipelining between stages.** Trino's batch mode and Spark SQL both materialize exchange output to S3. Adding in-memory push-between-stages is a research project. Not here.
- **Cost-based plan choice of distribution strategy.** `EnsureDistribution` today picks the single satisfying exchange variant per edge. Future work adds a cost model to choose between e.g. replicate-build vs. hash-repartition. Orthogonal to this spec.
- **Fault tolerance / partial re-execution.** If a stage task fails, the whole query fails. Trino runs this way until its opt-in "recovery" mode. Spark's DAGScheduler does partial re-execution; we don't need it yet.
- **Adaptive query execution.** Spark AQE re-plans after each shuffle using runtime statistics. Design later.
- **Deleting cached shuffle files mid-query.** Each query's shuffle output is cleaned up at query end, not between stages. Simpler GC.

## Architecture

### Shape: W3 — coordinator owns stage DAG, worker owns intra-stage

Coordinator:
- Topologically sorts the Exchange-annotated stage DAG from `EnsureDistribution`.
- For each stage in order, synthesizes `N` worker tasks (where `N = workerCount` for pipeline stages or `N = shuffleSourceFiles` for shuffle stages), dispatches via NATS, awaits completion notifications.
- Records each stage's output layout (S3 prefix + partition file index) in a per-query `stageOutputs map[stageID]StageOutput`.
- Feeds each subsequent stage its input layout: `PreScannedInputs[alias] = stageOutputs[childStageID].FilesForPartition(p)`.
- Terminal Gather stage: dispatches one task per worker that streams its output via NATS reply to the coordinator's subscription, which materializes the `QueryResult`.

Workers:
- Receive a `Task` specifying: input alias → S3 prefix(es) of upstream stage output; operators to run; output destination (shuffle prefix, final prefix, or reply subject).
- Use the existing pipeline executor with:
  - Source = `partitionShardSource` (reads `.wshf` from assigned partition(s)) or `streamSource` (reads parquet from pre-computed inputs).
  - Sink = `partitionedShuffleSink` (writes hash-partitioned `.wshf`) or `gatherReplySink` (new: streams batches back to coordinator's reply subject).
- No knowledge of the stage DAG or other workers.

This is Trino's fragment model and Spark SQL's stage model. Not new architecture — **the canonical shape we deviated from.**

### Stage contract

Every stage — whether Exchange or compute — is one of three task types with a common contract:

```go
type Task struct {
    // ...existing fields...

    // Inputs maps scan/alias name → S3 prefixes of upstream stage output.
    // For intra-query stage chaining (the new use): Inputs["left"] = prefix
    // of the previous Repartition stage's output. Worker uses the existing
    // partitionShardSource when any prefix contains "partition=" segments.
    //
    // Preserves PreScannedInputs semantics for backward-compat with
    // build-cache / aggregate pre-compute paths (until those lift to
    // planner rewrites in later phases).
    Inputs map[string][]string

    // Output is the S3 prefix where this task's output is materialized.
    // For TaskTypeShuffle: worker writes "<Output>partition=NNNN/<taskID>.wshf".
    // For TaskTypePipeline (intermediate): same format, using the task's
    //   PartitionShufflingKeys / NumPartitions to hash-partition output.
    // For TaskTypeGather: Output is empty; worker streams to ReplySubject.
    Output string

    // ReplySubject is the NATS subject the worker publishes batch chunks
    // to. Only set for TaskTypeGather. Enables real-operator Gather semantics
    // (no coordinator-side S3 merge needed).
    ReplySubject string

    // GatherOrdering (Gather only) specifies optional sort-merge parameters
    // for the coordinator to apply when reassembling worker streams.
    GatherOrdering []SortKeySpec
    GatherLimit    int
}
```

`Inputs` replaces / generalizes the existing `PreScannedInputs` + `BuildCachePreScans` patchwork. `Output` generalizes `ResultPrefix` to always be a stage output (shuffle or final-result). `ReplySubject` + `GatherOrdering` move Gather from coordinator S3-merge to real NATS streaming.

The worker's source factory inspects each `Inputs[alias]` entry: if files match `partition=NNNN/*.wshf`, use `partitionShardSource`; if parquet files, use `streamSource`; if S3 prefix (folder), list and dispatch. This unifies today's three scan paths.

### Stage types + coordinator dispatch

#### StageExchangeRepartition

Planner emitted: child's output is hash-partitioned on `Exchange.Keys` across `Exchange.Count` partitions.

Coordinator dispatch (one-liner):
```go
stageOutput = c.dispatchShuffleStage(ctx, queryID, stage, stageOutputs[stage.Deps[0]])
```

Under the hood: dispatch `min(workerCount, len(childOutputFiles))` tasks, each reading a slice of the child's output files and hash-partitioning into `Exchange.Count` partitions at `s3://bucket/queries/<queryID>/<stageID>/`. This is exactly `runShuffleSide` today, generalized:
- Input = previous stage's output prefix, not a raw scan
- Output = this stage's output prefix, not a hard-coded shuffle prefix

#### StageExchangeReplicate

Planner emitted: child's output should be replicated to all consumer tasks (broadcast build).

Coordinator dispatch:
- Dispatch one task to one worker that reads the child's output and writes to a non-partitioned `.wshc` (shuffle-cache) file.
- Record `stageOutput = {Kind: Replicated, Files: [that one file]}`.
- Consumer stages receive this file in `Inputs[alias]` and all workers read it in full.

This is today's `preScanBuildTables` + build-cache behavior, but now triggered by a planner-emitted stage instead of coordinator heuristics.

#### StageExchangeGather

Terminal stage. Consumer is always the coordinator.

Coordinator dispatch:
- Pick a single worker (or one per partition if ordered gather).
- Send `TaskTypeGather` with `ReplySubject = distributed.QueryResultSubject(queryID)` and `Inputs[alias]` = child's output prefix.
- Worker reads child output, applies `GatherOrdering` if set (merge-sort across its partitions) and `GatherLimit` if set, streams batches back via NATS reply.
- Coordinator already has the subscription active; receives batches, assembles `QueryResult`, done.

No coordinator-side S3 merge. No `mergeProbePartials` side channel. No `ExtractMergeInfo` from the logical plan — the Gather stage carries its own ordering/limit.

#### StagePipeline / StageScan / StageAggregate / StageJoin / StageSort / StageWindow

Compute stages. Coordinator dispatches `workerCount` pipeline tasks. Inputs from `stageOutputs[deps]`. Output to this stage's S3 prefix. If this pipeline's output has an `Exchange*` consumer stage on the other side, the Exchange stage handles repartitioning — the pipeline stage itself just writes partition-0 output (single partition, unpartitioned `.wshf`).

Today's `buildShufflePipelineTasks` is a special case of this: "pipeline stage whose inputs are two Repartition outputs." Generalizes cleanly.

### Coordinator loop: `executeStageDAG`

Replaces the current four-mode switch **and** the Phase 2b `lowerExchangeDAG`. Lives in `internal/coordinator/execute_stage_dag.go`:

```go
func (c *Coordinator) executeStageDAG(
    ctx context.Context,
    queryID, sql string,
    stages []physical.Stage,
    workerCount int,
) (*QueryResult, error) {

    stageOutputs := make(map[string]StageOutput, len(stages))

    for _, stage := range stages { // already topo-sorted by planner
        inputs := c.collectInputs(stage, stageOutputs)
        switch stage.Type {
        case physical.StageExchangeRepartition:
            out, err := c.dispatchShuffleStage(ctx, queryID, stage, inputs, workerCount)
            if err != nil { return nil, err }
            stageOutputs[stage.ID] = out

        case physical.StageExchangeReplicate:
            out, err := c.dispatchReplicateStage(ctx, queryID, stage, inputs)
            if err != nil { return nil, err }
            stageOutputs[stage.ID] = out

        case physical.StageExchangeGather:
            return c.dispatchGatherStage(ctx, queryID, stage, inputs)

        default: // pipeline/scan/aggregate/join/sort/window
            out, err := c.dispatchPipelineStage(ctx, queryID, sql, stage, inputs, workerCount)
            if err != nil { return nil, err }
            stageOutputs[stage.ID] = out
        }
    }
    return nil, fmt.Errorf("plan terminated without Gather stage: %s", queryID)
}
```

Topologically-sorted input (already guaranteed by `EnsureDistribution` and the existing planner invariant). Gather is always the last stage in a PlanDistributed output. If any non-Gather stage produces an error, the function returns; `dispatchShuffleStage`/etc. are responsible for NATS subscription/dispatch/collection per the existing `runShuffleSide` pattern.

**No state machine. No mutating `physStages` into `pipeline-0`. No side channels. Topo-walk.**

### StageOutput type

```go
type StageOutput struct {
    Kind         OutputKind // Partitioned | Replicated | Final
    NumPartitions int        // Partitioned only
    Files        [][]string  // Partitioned: Files[p] = S3 keys for partition p
                              // Replicated: Files[0] = single-file broadcast cache
                              // Final: coordinator-side result, not S3
    Schema       []parquet.Column // for downstream parseability
}
```

Lives in `internal/coordinator/stage_output.go`. Consumed by `collectInputs` which translates `stage.Dependencies` into the right `Inputs` map shape for the next task.

### Worker changes (minimal)

Today's worker accepts `PreScannedInputs map[string][]string`. The new contract accepts `Inputs map[string][]string` which is structurally identical — just generalized semantically. The worker:

1. **Source selection** — for each `Inputs[alias]`, inspect file patterns. If any `.wshf` with `partition=NNNN` segments → `partitionShardSource`; if parquet → `streamSource`; if both → error (planner bug). No behavior change for existing probe-split / build-cache paths.
2. **Sink selection** — if Task has `ShuffleKeys` + `NumPartitions` → `partitionedShuffleSink` (existing). If `ReplySubject` set → new `gatherReplySink`. Else → default parquet-output sink (existing).
3. **Gather reply sink** — new file `internal/worker/gather_reply_sink.go`. Serializes each output batch to MessagePack, publishes to `ReplySubject`. On Finalize, publishes a terminal "done" message with row count and any query-level error.

Everything else in the worker stays. The pipeline executor doesn't learn new operators; it just gets different Source/Sink instances at construction time.

### Coordinator subscription: gather reply handling

New file `internal/coordinator/gather_receiver.go`. Single helper that subscribes to `QueryResultSubject(queryID)`, receives batches from the Gather worker task(s), applies coordinator-side merge-sort if `GatherOrdering` was set and multiple workers were dispatched (ordered gather), applies top-level `Limit` if set, returns a populated `QueryResult`.

This replaces the existing `mergeProbePartials` coordinator-side merge. The logic is equivalent (merge-sort, apply limit) but the input is a stream of NATS messages instead of an S3 list.

## Integration with Phase 2a

Phase 2a's 17 commits stand. `EnsureDistribution` is correct and produces the Exchange-annotated plans this design consumes. Three small planner changes needed:

1. **Always emit a terminal Gather.** Today `EnsureDistribution` only appends Gather when the root's output is non-Singleton. For native-DAG execution, every distributed plan must end in Gather so `executeStageDAG` has a clean termination (Trino's "output fragment" invariant). For single-worker plans where the root is Singleton, append a trivial Gather (one worker, no ordering) that streams the pipeline output through to the coordinator via `ReplySubject`. Removes a branching condition from the coordinator loop and matches Trino's fragment model exactly.
2. **Gather stage ordering + limit fields.** `StageExchangeGather` today carries `Ordering []SortKeySpec` on the `Exchange` payload. Extend with `Limit int` so the coordinator can skip the redundant `ExtractMergeInfo` on the logical plan. Small change in `EnsureDistribution` to populate `Limit` and `Ordering` from a trailing Limit/Sort node if present.
3. **Pipeline stage output partitioning hint.** When a compute pipeline stage feeds a `StageExchangeRepartition`, the pipeline task's output can be pre-partitioned by the child Exchange's keys — saves a later shuffle. Optimization; defer to a follow-up phase if it doesn't fall out naturally.

## Deletions

Once this design lands and SF10 parity is proven (see Testing), delete:

- `internal/coordinator/coordinator.go:~460-610` — the four-mode switch if it's still present (already gone on branch, confirming deletion).
- The empty-BuildAlias skip in `lowerExchangeDAG` — superseded by real DAG walk.
- `lowerExchangeDAG` itself — superseded by `executeStageDAG`.
- `mergeProbePartials` S3-side merge — superseded by `gatherReplySink` + `gather_receiver`.
- `ExtractMergeInfo(logicalPlan)` coordinator-side call — Gather stage carries its own ordering/limit.
- Legacy pickers if any survived (`PickShuffleCandidate`, `CanProbeSplit` — already deleted in Phase 2b Task 10).
- `PreScannedInputs` / `BuildCachePreScans` field duplication on `physical.Stage` — replaced by the generalized `Inputs` field on `distributed.Task`. The stage-level fields lose their reason to exist after the switch is gone; they were plumbing for the old single-`pipeline-0` pattern.

## Testing

### Local test harness — MUST be extended

`cmd/tpch-harness --mode=local --slice=small` uses 22 MB of parquet. That's below every shuffle threshold. It caught zero of the Phase 2b regressions. Before this design's implementation plan runs, **extend the harness** with either:

- **S3-mode local:** `--mode=local --source=s3 --bucket=wadjet-bench-sf10-use2 --region=us-east-2` — spawns local coordinator + N workers, points at the real SF10 bucket. Uses ~$0 of AWS egress if dev box is already in us-east-2 or if we accept slower reads from WSL. Most faithful test.
- **SF1-scale sample generator:** ~1 GB of locally-generated TPC-H at SF1, materialized into `/tmp/sf1-sample/`. Runs on local file store. Same speed as today but with shuffle-triggering data.

I'll pick one during the writing-plans pass; both unlock local reproduction of the SF10 regression class that cost $1.20 tonight. S3 mode is probably right — it's SF10 itself, not an approximation.

### Gate structure

Per stage of implementation plan:

1. **Unit tests per dispatch helper** (`dispatchShuffleStage`, `dispatchReplicateStage`, `dispatchPipelineStage`, `dispatchGatherStage`) — mock NATS, verify task-schema construction + result collection.
2. **Integration tests** via in-process harness (`internal/coordinator/distributed_test.go` pattern) — 2 workers, real NATS, real pipeline executor, verify 3-stage query (scan → repartition → join → gather) produces correct rows.
3. **SF0.01 TPC-H 22/22** — sanity check; expected pass throughout.
4. **Local harness SF1-or-S3 mode — MUST run before each EC2 deploy.** Per `feedback_tpch_harness_local_gate.md`.
5. **SF10 EC2 A/B: main vs. branch — no query more than 10% slower, row counts match.** This is the real gate. Expect per-query improvements on Q12/Q17/Q03/Q21 as the multi-step shuffle path engages.
6. **SF100 Q03 stress** — broadcast-duplication fix validates at scale. If this works, closes the `project_sf100_q03_broadcast_duplication_2026-04-18.md` story.

## Rollout

Proposed commit order (the writing-plans skill will detail; this is the outline):

1. **Extend Task schema** — add `Inputs`, `Output`, `ReplySubject`, `GatherOrdering`, `GatherLimit` fields. Backward-compat: old `PreScannedInputs`/`ResultPrefix` writes still work via field copy.
2. **New sink: `gatherReplySink`** + worker construction path. Unit tests.
3. **New source selection** — worker inspects `Inputs` for partition-shuffle vs. parquet vs. single-file patterns. Integration test: worker handles both old and new task shapes.
4. **Stage output type + `collectInputs` helper** on coordinator.
5. **`dispatchShuffleStage`** — refactor `runShuffleSide` to accept `Inputs` instead of a raw `physical.Stage`, make it generic across scan-input vs. previous-stage-output.
6. **`dispatchReplicateStage`** — wraps `preScanBuildTables`, generalized.
7. **`dispatchPipelineStage`** — wraps existing pipeline task construction.
8. **`dispatchGatherStage`** + `gather_receiver`.
9. **`executeStageDAG`** — the topo-walk loop. Invoked from `executeDistributed` replacing `lowerExchangeDAG`.
10. **Revert Phase 2b's `lowerExchangeDAG` + coordinator cut-over code**. This includes the empty-BuildAlias guard and all the C-minimal lowering logic. Replaced by `executeStageDAG`.
11. **Delete PreScannedInputs/BuildCachePreScans from `physical.Stage`** once `Inputs` on `Task` is the sole path.
12. **Delete `mergeProbePartials` + `ExtractMergeInfo(logicalPlan)`** once Gather stage carries its own ordering/limit.
13. **Extend EnsureDistribution** — populate `Gather.Limit` from trailing Limit node.
14. **Parity gate re-capture** — regenerate `benchmarks/tpch/testdata/parity/*.json` from main (not from feat) once SF0.01 correctness is proven.
15. **Local harness extension — S3 mode.**
16. **SF10 EC2 A/B — MUST be green before merge.**

## Open project-management decisions (for user)

These are not technical-correctness decisions and depend on your judgment about risk, rollout, and what you want to see. I'll not assume answers.

- **Phase 2b disposition.** Two options:
  - **(a) Hard revert** of commits `dd3d0f5..78fd60e` (the entire Phase 2b batch), build this design on top of Phase 2a.
  - **(b) Fix forward** — replace `lowerExchangeDAG` with `executeStageDAG` in-place, accept the commit history as "2b was wrong, here's 2c."
  - I lean toward (a) — clean history, clear narrative, no confusing "what Phase 2b code is still live" question during review. But it drops the PARITY harness, `--pg-addr` fix, and a few other incidental improvements that would need re-cherry-picking. (a) is ~2 min of git work; (b) is cleaner diff but muddier log.
- **One PR or two?** This design is ~15 commits of real work. Options:
  - Single PR on top of current branch, 30-35 commits total (Phase 2a + 2c/native-DAG + test infra).
  - Split at the harness extension: PR 1 = harness S3 mode (4-5 commits); PR 2 = native-DAG runtime (~12-15 commits) on top of the harness PR.
- **Test harness scope.** S3 mode is the most faithful; SF1-sample generator is more reproducible without AWS. I'll execute on S3 mode unless you flag a reason (e.g., we run in environments where AWS SSO is unavailable and you'd rather pay for a local SF1 generator).
- **EC2 cost tolerance during development.** This design probably needs 2-3 SF10 A/B deploys before it's right (~$3-5). Green-light or flag ceiling.
- **Acceptable transition window.** Between when `executeStageDAG` lands and when the old code is fully deleted, both paths exist and must stay consistent. Trino/Spark went through this — they shipped both paths gated on a flag for a release or two. Are you OK with a `Coordinator.UseNativeDAG bool` gate that defaults true but lets us fall back on one revert if we catch something in the wild? (I think yes, but it's your tolerance.)

## Success criteria

This design is done when:

1. `executeStageDAG` is the sole distributed-execution path.
2. SF10 EC2 A/B: branch matches or beats main on all 22 queries; total within 10% of the 2m02s historical baseline (not today's 5m15s main, which carries the Phase 5 regression).
3. SF100 Q03 passes (today OOMs on broadcast-duplication).
4. Local `tpch-harness` reproduces any distributed regression end-to-end.
5. `internal/coordinator/` has one dispatch loop, not four. No side-channel state on `physical.Stage` beyond what's operator-structural.

## References

- `memory/project_phase2b_sf10_regression_2026-04-21.md` — tonight's failure + root cause
- `memory/project_distribution_phase_2_integration_gap_2026-04-21.md` — under-scoping of Task 19
- `memory/project_q21_broadcast_hazard_iteration_2026-04-19.md` — Q21 broadcast whack-a-mole → "fundamentally mismatches Trino/Spark"
- `memory/project_sf10_regression_2026-04-18.md`, `project_q12_q17_regression_2026-04-18.md` — Phase-5-collapse-era perf regressions this design is designed to reverse
- `memory/feedback_tpch_harness_local_gate.md` — local-gate requirement for this class of change
- `memory/feedback_execute_on_research_backed_decisions.md` — why this spec commits to decisions rather than asking
- Trino documentation: https://trino.io/docs/current/overview/concepts.html (fragments, exchange operators)
- Spark SQL internals: https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution (ShuffleExchangeExec stages)
- `docs/_archive/specs/2026-04-20-distribution-property-phase-1.md` — merged Phase 1 foundation
- `docs/_archive/specs/2026-04-20-distribution-property-phase-2-design.md` — Phase 2 spec (this design replaces the runtime-integration portion)
- `docs/_archive/specs/2026-04-21-distribution-property-phase-2b-design.md` — Phase 2b spec (this design supersedes)
