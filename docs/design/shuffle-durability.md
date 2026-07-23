# Shuffle durability policy (`--shuffle-durability=eager|lazy|off`)

Status: implemented 2026-07-23. Default `eager` (pre-knob behavior) pending
SF100 A/B validation.

## Motivation: measured ground truth

The step-0 instrumentation of the shuffle NVMe/durability arc (per-tier
shuffle-read + upload byte ledgers, PR #261) measured the SF100 benchmark
suite pair at:

- **S3 PUT of stage-output scratch: 424.7 GB (wire, s2-compressed; ~528 GB
  raw)** per cold+steady suite pair.
- **S3 shuffle read-back: 0 files, 0 bytes.** Every exchange input was
  served by the local mmap tier (237.7 GB), the peer gRPC tier (351.9 GB),
  or NATS KV (5 MB).
- Uploads complete within query lifetimes, so the existing CancelQuery
  abort saves almost nothing (57 files per pair).

In healthy operation the durable copy of shuffle scratch is pure
fault-tolerance insurance that is never read. Its costs are real, though:
NIC egress contention with peer streams (~1.9 Gbps average per worker,
PUT + peer-serve together ≈ CloudWatch NetworkOut), page-cache churn (the
background upload reads every adopted output back through the page cache —
the same cache-composition regime the Q18 refault arc fought), and S3
request/transfer cost.

This is the Trino spectrum — fault-tolerant execution with exchange
spooling vs streaming exchange — expressed as a per-deployment policy knob
instead of a structural rewrite.

## Modes

Policy is a coordinator config (`coordinator.Config.ShuffleDurability`,
CLI `--shuffle-durability`, tpch-bench `WADJET_SHUFFLE_DURABILITY`) and is
stamped per task (`Task.UploadPolicy`) on native-DAG stage/shuffle tasks by
the streaming-exchange annotator (`annotateTaskPeerLocations`). Pipeline
and gather tasks are untouched (always synchronous). Only meaningful under
`--streaming-exchange` — the peer tier is what makes the durable copy
optional.

- **eager** (default): background uploads start as outputs finalize.
  Exactly the pre-knob Phase-B behavior.
- **lazy**: the worker builds the upload jobs but queues them unstarted,
  grouped by root query. They run only on a demand signal (below). Jobs
  still queued when the query turns terminal are **elided** — the S3 PUT
  never happens.
- **off**: jobs are elided at completion time. No queue, no release.
  Producer loss before consumption degrades to the whole-query fallback.

## Demand signals (lazy)

1. **Consumer missing-input retry against a live producer.** The
   taskRetrier's fatal classifier (`classifyFatalResult`) already inspects
   every missing-input failure; when the producer is alive and the key
   non-durable it now broadcasts `SubjectUploadRelease` with the root
   query ID. Workers holding queued jobs for that root start them
   (`uploadManager.ReleaseQuery`); the retry converges on the S3 copy even
   if the peer path stays broken. Released roots start subsequent lazy
   jobs immediately.
2. **Coordinator-side stage read fallback.** `fetchStageOutputData`
   publishes the same release before its bounded re-poll — belt and
   braces; see the eager exception below.
3. **Worker drain.** `uploadManager.Flush` (Drain phase 2) releases every
   queued job before waiting, so a gracefully drained worker leaves the
   durable copies behind and its consumers fall through to S3 as before.

The release broadcast is best-effort and idempotent: a lost message costs
one more retry round (which re-triggers it), an unknown/already-released
root is a no-op.

## What stays eager regardless

**Scalar-subquery producer stages.** The coordinator reads those outputs
itself (`substituteScalarDependencies` → `fetchStageOutputData`) and has no
peer tier — S3 (with the KV fast path underneath `fetchResultData`) is its
only read path. `executeStageDAG` registers every stage ID appearing in any
stage's `ScalarDependencies` under the root (`Coordinator.coordReadStages`,
dropped in `cleanupQuery`); the annotator exempts them from the policy.

**Adoption-failure sync fallback.** When `LocalStageCache.Adopt` fails
(cross-device rename etc.) the local file dies with the task spill dir and
peers can never serve it; the executor uploads synchronously in all modes.

**Streaming-disabled re-executions.** The annotator already returns early
for them: pure synchronous S3 semantics, no policy.

## Failure semantics

Unchanged in kind, changed in frequency:

- eager: producer death after UploadComplete → retry reads S3. Before
  upload lands → `ErrInputLost` → one-shot streaming-disabled re-execution
  (pre-existing fallback, `withStreamingExchangeDisabled`).
- lazy: producer death with jobs queued → same `ErrInputLost` whole-query
  re-execution (the durability bit never flipped, so `classifyFatalResult`
  classifies correctly with no changes). Graceful drains are safe via
  Flush.
- off: any producer loss mid-query — including graceful drains — is a
  whole-query re-execution. That is the advertised trade.

Known bounded race (parity with eager): a straggler task finishing after
its query's cancel broadcast re-creates the root's upload scope. Under
eager the upload lands anyway and the coordinator's CleanStale GC reaps
it; under lazy the job sits queued until worker shutdown and is elided
there without being counted per-query. Bounded by one straggler task, no
correctness effect.

## Mixed-version safety

`Task.UploadPolicy` is an additive JSON field. Old workers drop it and
upload eagerly (safe). Old coordinators never set it (eager). The release
subject is subscribed unconditionally; without the flag nothing publishes
to it.

## Observability

The #261 upload ledger gains an elided side: `upload_elided` /
`upload_elided_bytes` (local pre-compression bytes, same basis as
`upload_cancelled_bytes`) in the 60-second `"shuffle io stats"` marker and
the drain-time final line. The A/B decision numbers: under `lazy`,
`upload_done_bytes → ~0` (scalar producers + drain flushes only),
`upload_elided_bytes ≈ the former PUT volume`, `s3_files` stays 0, rows
44/44.

## Validation

- Unit + `-race`: worker (upload manager lazy/off/flush/release paths),
  coordinator (annotator policy stamping, coordinator-read exemption,
  streaming-disabled exemption).
- TPC-H SF0.01 correctness; tpch-harness `--mode=local` SF0.01 + SF1, both
  arms.
- SF100 same-window A/B (main-class baseline first, then `lazy`), wins
  expected to concentrate in the top materializers Q18 (69.6 GB scratch),
  Q21, Q05.

## Future

Placement-aware consumer assignment (the 351.9 GB peer-stream share) is
the next lever and is orthogonal: it changes who reads, this knob changes
what is written.
