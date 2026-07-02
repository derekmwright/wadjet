# Streaming Exchange — Design Memo

> **Status:** proposed (v1 scoped, not started). **Date:** 2026-07-02.
> **Verified against:** main @ 9402cbc (post-#173). All `file:line` anchors
> checked against that commit; they drift — confirm before relying on them.

## 1. Goal

Cut the per-stage-boundary fixed cost of distributed queries. Today **every
intermediate stage boundary round-trips through S3**: the producer's critical
path ends with S2-compress + S3 PUT (8-way parallel,
`worker/executor.go:959-1063`), and the consumer's begins with an S3 prefix
List + GET streamed to NVMe + mmap (`worker/stream_source.go:288-470`). This is
structurally Trino's fault-tolerant (spooled) mode with no streaming mode
beside it. The local fast path (#170/#172) removed these fixed costs for
*small* queries by skipping the DAG entirely; streaming exchange removes them
for everything else by letting consumers read shuffle data **directly from the
producing workers over gRPC**, with S3 retained as the durability substrate
for retry.

Non-goal for v1: removing the stage barrier (pipelined co-scheduling). §4
explains why that's deferred, and why the barrier is less of a cost than it
looks for hash exchanges.

## 2. Current state (verified 2026-07-02)

Facts the design leans on, each confirmed on main:

**Transport.** The gRPC data plane (`internal/dataplane/`) is a strict
hub-and-spoke star: one bidi `Connect` stream per worker, **worker dials
coordinator**, typed `oneof` envelopes (Hello/Welcome, TaskDispatch down,
ResultBatch/TaskProgress up). Workers run **no gRPC server** and know no peer
addresses — `WorkerHeartbeat` (`distributed/messages.go:477`) and `Hello`
(`proto/dataplane/v1/dataplane.proto:55`) carry no listen address. There is
today **zero worker→worker communication of any kind**. Flow control is pure
HTTP/2 (blocking recv loop → blocked Send); no message-size limits are
configured (gRPC's 4 MiB default recv cap is in force). Plaintext
intra-cluster; identity is a self-asserted worker_id. The proto reserves
envelope fields 4–15 for future message types.

**Shuffle write.** `partitionedShuffleSink` writes local `part-%04d.wshf`
files incrementally (64 KiB per-partition flush buffers, memory bounded by
N×64 KiB). The executor then uploads each non-empty partition to
`<ResultPrefix>partition=%04d/<taskID>.wshf`, S2-compressing when it saves
≥10%, and **adopts the uncompressed local file into `LocalStageCache` keyed by
the S3 key** (`executor.go:1053-1059`) — i.e. producers *already* retain
shuffle output on local NVMe for the query's lifetime, and it is *already*
addressable by the same key a consumer uses.

**Shuffle read.** `cachedFileStreamSource.openNextFile` is already a tiered
lookup: **Tier 0** same-worker `LocalStageCache` mmap → **Tier 1** NATS KV
(`wadjet_results_data`, ≤4 MB payloads, 5-min TTL, never authoritative) →
**Tier 2** S3 → NVMe temp → mmap. Consumers get their partition slice via
`partitionFilesForWorker` (contiguous slices, `coordinator/stage_output.go:90`)
and list the partition prefix (`partition_shard_source.go:18`).

**Barrier.** `executeStageDAG` blocks each stage on its dependencies'
done-channels (`execute_stage_dag.go:410-420`). Note the barrier is
*semantically forced* for hash exchanges as currently shaped: every producer
task writes one file into **every** partition, so no partition's file set is
complete until every producer task finishes.

**Retry.** `taskRetrier` (`coordinator/task_retry.go`): 3 attempts, immediate
re-dispatch to a scheduler-chosen (usually different) worker, EstimatedBytes
doubled per attempt. **A retry is a verbatim re-send of the task spec**, whose
inputs are durable deterministic S3 keys (`Task.InputFiles/BuildFiles`,
`distributed/messages.go:104,116`); outputs are overwrite-safe (same TaskID →
same key). Nothing downstream ever references the producing worker's identity
— `ResultNotification.WorkerID` is liveness/logging only. **There is no
stage-rerun or query-rerun machinery; only task-level retry**, and the retrier
is deliberately failure-class-agnostic. Retry is disabled for gather-fused
stages because rows have already streamed to the client.

**Fallback precedent.** The local fast path's adaptive bail-out
(`local_fastpath.go:117-133`): abort the optimistic path, re-dispatch through
the durable path, safe because reads are idempotent and nothing reached the
client. Streaming exchange reuses this shape (§5).

## 3. Design space

Three candidate shapes, ordered by how much of today's machinery they disturb.

### A. Local shuffle read, synchronous upload (peer fetch as a cache tier)

Keep the write path byte-for-byte identical — local files, S2, synchronous S3
PUT, stage completes after upload as today. Add one read tier: consumers fetch
partition files **from the producing worker's `LocalStageCache` over gRPC**
(Tier 1.5, between KV and S3). Any peer-fetch failure — dead producer, evicted
file, dial error, version skew — falls through to S3, where the file is
*guaranteed* to exist because the producer stage could not have completed
without it.

- Saves: the consumer-side S3 List + GET (LAN NVMe read vs object-store RTT),
  plus S3 GET request costs.
- Retry semantics: **unchanged, provably.** Peer fetch is exactly what
  `LocalStageCache` (Tier 0) and NATS KV (Tier 1) already are — a best-effort
  cache over a durable base. The retrier, worker-death reaping, and stage
  failure paths are untouched.
- Does not save: producer-side compress + PUT stays on the critical path.

### B. Local shuffle read, asynchronous upload (deferred durability)

Same read tier as A, but the producer task reports completion (and the barrier
releases) when its **local** files are finalized; the S3 upload continues in
the background. Consumers overwhelmingly read from peers; S3 is written as
insurance and becomes load-bearing only on failure.

- Saves: A's read-side win **plus** compress + PUT leave the producer critical
  path. This is the full per-boundary latency win available without touching
  the barrier.
- Bonus: uploads still pending when the query completes can be **cancelled**
  — for short/medium queries most shuffle bytes never reach S3 at all, which
  directly cuts the S3 request/storage bill that `queries/` scratch traffic
  has historically dominated.
- Cost: "stage complete" no longer implies "inputs durable." A producer
  worker dying between local-finalize and upload-complete strands its
  not-yet-uploaded files — the one failure today's machinery cannot absorb,
  since consumer-task retry re-reads the same missing input and there is no
  stage rerun. §5 settles this.

### C. Pipelined co-scheduling (true streaming, barrier removed)

Producers and consumers run concurrently; batches flow worker→worker as
produced (Trino streaming mode). Rejected for v1 on four grounds:

1. **Admission-control deadlock shape.** Consumer stages must hold worker
   slots + memory while producers run. Gating one side on resources the other
   side releases is precisely the rejected-and-documented admission-control
   deadlock (build holds memory, probe is gated, probe is what releases
   build). Co-scheduling needs gang admission across stage pairs — a new
   scheduler, not an extension.
2. **Retry granularity collapses.** A mid-stream consumer has partially
   consumed non-replayable input; a mid-stream producer death invalidates
   consumer state that cannot be un-consumed. Task-level retry stops working;
   the unit becomes the stage pair or the query (Trino's streaming mode
   answer: fail the query). That forfeits the #109/#110 investment that was
   just validated in production, for the same wins B captures more cheaply.
3. **Memory floor rises.** Both stages' operators resident per worker
   simultaneously, against the never-OOM North Star, and streaming buffers
   between them need ledger + backpressure design (per-batch heap
   backpressure in shuffle/gather was a 4× Q05 regression; flow control must
   come from HTTP/2 windows, which the multiplexed single-stream coordinator
   plane makes hairy — head-of-line blocking across everything sharing the
   stream).
4. **Sequencing.** Morsel-driven execution is the next approved workstream
   and re-plumbs intra-stage scheduling. Pipelining built now would be built
   against the pre-morsel executor and redone.

There is a genuine intermediate — **eager consumer dispatch**: keep pull-based
file fetch but dispatch consumers early and let them stream producer-task
files as each producer *task* (not stage) finishes; pipeline breakers are
sinks, so incremental consumption needs no operator changes. It couples
consumer progress to producer attempt identity (a retried producer task
rewrites files a consumer may have partially read), so it needs
consumed-file-set tracking and attempt fencing. Worth doing **after morsel**,
as the bridge toward C. Not v1.

### Decision

**V1 = A then B, in that order, as one workstream.** A ships the entire new
transport surface (worker gRPC server, peer addressing, fetch protocol, tier
integration) with **zero fault-tolerance delta** — every new mechanism is
exercised in production while S3 still guarantees correctness. B then flips
upload off the critical path and adds the one new failure classification it
requires. C is explicitly deferred until after morsel-driven execution.

This is dependency-ordered, not risk-averse: A's plumbing is a strict
prerequisite of B and C alike, and it is independently valuable (read-side
RTT + S3 GET costs) from the day it lands.

## 4. Mechanism (v1)

### 4.1 Peer transport

- **Workers host a gRPC server** — a new lean `PeerExchange` service, *not* an
  extension of the coordinator-facing `DataPlane.Connect` bidi envelope:

  ```proto
  service PeerExchange {
    rpc FetchShuffle(FetchShuffleRequest) returns (stream ShuffleChunk);
  }
  message FetchShuffleRequest { string query_id = 1; string key = 2; }  // key = the S3 object key
  message ShuffleChunk { bytes data = 1; }                              // raw WSHF bytes, ≤256 KiB per frame
  ```

  Server-streaming per fetch (rather than one multiplexed bidi stream per
  peer pair) is deliberate: each fetch gets its **own HTTP/2 stream**, so a
  slow consumer backpressures only its own fetch — no head-of-line blocking
  across tasks/queries sharing a peer link, and flow control comes entirely
  from HTTP/2 windows (per the rejected-approaches constraint: never
  sleep-based pauses). One cached `grpc.ClientConn` per peer pair; N workers
  → N−1 conns each, trivial at realistic cluster sizes.
- **The S3 key is the fetch identity.** `LocalStageCache` is already keyed by
  S3 key, so the serve side is a cache lookup + `io.Copy` in 256 KiB frames
  (the proven bufio size; comfortably under the 4 MiB gRPC default). Local
  files are the uncompressed originals — send raw in v1; s2-on-the-wire (codec
  pools exist) is a measured follow-up, likely unnecessary intra-AZ.
- **Serve-side cap:** a per-worker semaphore on concurrent FetchShuffle
  streams protects NVMe/NIC from fan-in spikes (M consumers × their fetch
  concurrency). Pull-based reads + the kept barrier mean there is no N×M
  standing connection matrix — only bounded transient fetches.
- **Memory:** serve side is fixed small buffers off disk; consumer side
  streams to NVMe temp + mmap **identically to the S3 tier** (same
  `openShuffleFile` path, same mmap-relief hooks). No new resident-set shape;
  ledger impact is the fixed frame buffers, accounted like the S3 read path.

### 4.2 Addressing

- Worker's peer listen address rides in the data-plane `Hello` (proto reserves
  fields; add `listen_addr`) and in `WorkerHeartbeat` for the NATS-only
  fallback path. The **coordinator remains the single source of truth** — no
  gossip, no worker-side discovery.
- `ResultNotification.WorkerID` (today liveness-only) becomes data-bearing:
  the coordinator records, per completed producer task, *which worker holds
  its files* (the winning attempt's worker — correct under retry by
  construction, since the winner's notification carries it).
- `StageOutput` gains per-file producer locations; `buildTaskInputsForStage`
  threads them into consumer task specs as **hints alongside the canonical S3
  keys** — e.g. `InputLocations map[string]string` (key → peer addr) next to
  the existing `InputFiles`. A consumer with no hint (or a stale one) simply
  reads S3; a retried consumer task re-sent verbatim still works because the
  keys, not the hints, are canonical.

### 4.3 Read tiering (final shape)

```
Tier 0  same-worker LocalStageCache mmap          (exists)
Tier 1  NATS KV, payloads ≤ 4 MB                  (exists — keep: beats a dial for tiny results)
Tier 1.5 peer FetchShuffle → NVMe → mmap          (NEW)
Tier 2  S3 → NVMe → mmap                          (exists — the durable base)
```

Broadcast/replicate outputs **stay on S3 + KV in v1**: M consumers fetching
the same build from one producer makes that producer a fan-out hotspot,
which S3 absorbs today for free. Tree/chained replication is a possible
follow-up, not v1. The terminal gather already streams over the data plane
and is untouched.

## 5. Retry and spool semantics (the settled contract)

**Principle: S3 remains the durability substrate; every streaming artifact is
a cache.** The task-spec invariant — *a retry is a verbatim re-send whose
inputs are resolvable by key from durable storage* — is preserved in both
phases; the only question is *when* durability is achieved.

**Phase A (sync upload): no semantic change.** Durability precedes stage
completion, exactly as today. Peer-fetch failure of any kind falls through to
S3 silently; the retrier never learns streaming exists.

**Phase B (async upload):**

- **Every shuffle file is still always uploaded** (started immediately at
  local-finalize, at background priority), *unless the query completes first*,
  in which case pending uploads are cancelled — durability is only required
  while a consumer might still need the file. Local files may be **evicted
  after upload-complete** under disk pressure (a strictly better ENOSPC story
  than today's unconditional retention).
- The producer sends a lightweight per-task **UploadComplete** notification
  (a new envelope in the reserved 4–15 range) flipping a coordinator-side
  durability bit per task output.
- **Consumers stay dumb.** On peer-fetch failure: try S3; on S3 miss, fail
  the task reporting the missing key. All classification is coordinator-side.
- **The retrier gains its first failure-class distinction.** On a
  missing-input task failure the coordinator checks (producer liveness,
  durability bit):
  - Producer **alive** (upload in flight) → normal consumer-task retry; the 3
    attempts double as bounded wait-for-durable. No new machinery.
  - Producer **dead** with the input **not durable** → `ErrInputLost`:
    terminal for the stage immediately (don't burn blind retries that cannot
    succeed), and the query **re-executes once with streaming exchange
    disabled** — the fast-path bail-out pattern one level up. Safe for the
    same reason: reads are idempotent and no rows have reached the client
    (intermediate boundaries complete before the terminal gather dispatches).
    If rows *have* streamed (gather-fused terminal stage), the query fails to
    the client exactly as a gather-fused task failure does today.

**Rejected alternatives for the fallback:**

- *Producer-retains-until-consumer-ack* (no tee): couples producer disk to
  consumer progress, complicates reaping, and still dies with the producer.
  Strictly dominated by the tee, whose cost is today's write path run off the
  critical path.
- *Stream-only + recursive producer rerun* (Spark lineage): regenerating a
  lost producer task needs *its* inputs, which under streaming are themselves
  possibly-lost local files — recursion, attempt fencing, and a lineage store.
  The tee bounds the non-durable frontier to seconds per file; buying lineage
  machinery to avoid background PUTs is a bad trade. Single-level producer
  rerun can be revisited later if `ErrInputLost` re-executions are ever
  observed at a measurable rate; the streaming-disabled rerun makes that rate
  visible cheaply (log + counter).

## 6. Security note

Today's plane is plaintext intra-cluster with self-asserted worker IDs, and
`FetchShuffle` adds a data-read endpoint on every worker — shuffle files can
contain ABAC-filtered data (post-#173, security projections are applied
worker-side *before* shuffle, so files contain only permitted values, but
they are still query results). V1 matches the existing intra-cluster trust
posture (plaintext, no per-request auth) and inherits `TLSConfig` plumbing
where the operator enables it. Cheap hardening worth including from the
start: the coordinator mints a random per-query fetch token, distributed in
task specs and required in `FetchShuffleRequest` — rejects cross-query and
out-of-cluster reads without any key infrastructure.

## 7. Rollout and gates

- Flag-gated end to end: `--streaming-exchange` (serve flag; Config zero value
  = disabled, matching the local-fastpath/mmap-relief dormant-safe pattern).
  Kill switch = the same flag; Phase B additionally honors per-query disable
  for the `ErrInputLost` re-execution.
- Correctness gates, in order (per the standing hard rules):
  1. Unit: fetch protocol round-trip, tier fallthrough on injected peer
     failure, token rejection.
  2. `multi_worker_test.go`-pattern e2e: shuffle correctness with the flag on
     and off, row-identical; fault injection killing a producer worker in the
     Phase-B vulnerability window → assert the streaming-disabled re-execution
     returns correct rows.
  3. `cmd/tpch-harness --mode=local` both flag states — before any EC2.
  4. SF10 A/B (this is a structural latency change; measurement is the point,
     not benchmark-chasing), then SF100 with the standard preflight. Watch
     per-query rows against the baseline result file before flagging
     anomalies.
- Instrumentation from day one: per-tier read counters (tier0/kv/peer/s3),
  peer-fetch failure fallthrough count, Phase-B upload-cancelled bytes (the $
  savings), `ErrInputLost` occurrences.

## 8. Kickoff open questions — answers

1. **Fallback semantics:** tee always; Phase A synchronous (unchanged), Phase
   B asynchronous with cancel-on-query-complete; consumer failure resolves
   via retry-into-S3 when the producer lives, streaming-disabled query
   re-execution when it doesn't (§5).
2. **Co-scheduling vs eager fetch vs barrier:** barrier kept in v1 — it is
   semantically forced for hash exchanges anyway (§2); full co-scheduling
   rejected for v1 on deadlock/retry/memory/sequencing grounds (§3C); eager
   consumer dispatch is the post-morsel bridge.
3. **Discovery:** listen addr in Hello + heartbeat; coordinator distributes
   per-file location hints in task specs; no gossip (§4.2).
4. **Fan-in:** pull-based per-file fetches on dedicated HTTP/2 streams, one
   conn per peer pair, serve-side semaphore; no standing N×M matrix (§4.1).
5. **Tier interaction:** KV stays for tiny, peer for medium, S3 for
   everything as the base; broadcast and gather untouched in v1 (§4.3).
