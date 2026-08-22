# Coordinator-side stage reads (the scalar-substitution barrier)

Status: implemented 2026-08-22. Default on, kill switch
`WADJET_COORD_PEER_READS=0`.

Companion to `docs/design/shuffle-durability.md` (which copy exists when)
and ADR-0007 (why the durable copy is optional at all). This note covers
the one consumer that is *not* a worker: the coordinator itself.

## The measurement

SF100 window 4 (`docs/benchmarks/sf100-window4-analysis-2026-08-22.md` §7)
located a stall that is present in **every** arm, including the
`v0.17.0-clawback` baseline:

> B r4, `shuffle side complete` for `exchange-repartition-3` at
> 22:08:35.939, both dependencies satisfied, and `scalar substitution
> stage_id=join-4 placeholder=scalar_1 producer=final_aggregate-6` does not
> log until 22:08:36.995 — **1.06 s of whole-cluster idle waiting for the
> coordinator to read one 80-byte scalar-subquery result out of S3**.

Three substitution sites exist per suite run — Q11 `final_aggregate-8`,
Q15 `join-9`, Q22 `join-4` — and those are exactly the three queries with
nonzero cluster-idle inside their worker envelope in every arm. Total
**1.5–2.1 s per steady suite run**. Three of the four cited waits (0.53 /
0.55 / 1.06 s) sit on a ~0.5 s grid, suggestive of `fetchStageOutputData`'s
re-poll loop (`500ms` sleep between attempts) missing one or two polls —
but the fourth (0.40 s) does not, so this is inferred from the
quantization, not measured directly. The `tier=`/`wait_ms=` attributes this
change adds to the `scalar substitution` log line (see Observability below)
will settle it exactly on the next SF100 window. The producing worker held
the file on local NVMe the whole time either way.

This is the same class as `ed83bb9` (peer hints for gather-merge inputs),
which took a 13.45 s → 0.11 s wait off the worker-side merge tail by
letting the consumer try the producer's local copy first. The coordinator
never got that tier — `peer_locations.go` said so out loud: *"the
coordinator has no peer tier; S3 is its only read path."*

## What the read path did

`substituteScalarDependencies` (`internal/coordinator/scalar_extract.go`)
→ `readScalarFromStageOutput` → `fetchStageOutputData`
(`internal/coordinator/peer_locations.go`) → `fetchResultData`
(`internal/coordinator/coordinator.go`), which was:

1. `resultKV.Get` — the NATS KV small-output cache. Producers mirror every
   stage output ≤ `natsKVResultThreshold` (4 MB) into the shared
   `wadjet_results_data` bucket, so an 80-byte scalar is always eligible.
   The bucket is a **5-minute-TTL, 1 GB-capped** cache, and at SF100 the
   shuffle traffic that flows through it makes both limits reachable — a
   miss here is ordinary, and it is silent (the producer's failed `Put` is
   logged at Debug).
2. `store.Get` on S3.
3. On error, `fetchStageOutputData`'s bounded re-poll: release deferred
   uploads, then `Get` every 500 ms for 15 s, with a fast input-lost exit
   when the producer is dead and the key never went durable.

Both (1) and (2) can miss at the moment the coordinator asks, because the
coordinator is reacting to the *result notification*, and under Phase-B
async upload the notification is sent as soon as the local file is adopted
— the S3 PUT is queued behind whatever else that worker is uploading. The
copy that is guaranteed to exist at that instant is the producer's local
one, and it was the only copy nobody could read.

## The change

`internal/coordinator/stage_read.go` makes the coordinator's stage-output
read tiered, mirroring the worker's acquisition tiers
(`internal/worker/src_acq_stats.go`):

| tier | source | when it answers |
|---|---|---|
| `kv` | `wadjet_results_data` NATS KV | payload ≤ 4 MB, `Put` landed, within TTL |
| `peer` | producing worker's `LocalStageCache`, over `PeerExchange.FetchShuffle` | producer alive and still holds the key |
| `s3` | durable store (+ the pre-existing bounded re-poll) | always, eventually |

`fetchResultDataTiered` is the single implementation;
`fetchResultData` is a thin wrapper, so the tier serves **every**
coordinator-side stage read, not just the scalar one:

- `substituteScalarDependencies` → `readScalarFromStageOutput` →
  `fetchStageOutputData` (the measured site),
- `readFinalResults` → `fetchResultData` — the probe-split merge's partial
  reads and any final-stage result files that exceed the inline threshold.

Both are `queries/<id>/...` scratch produced by a task the registry
recorded, so both qualify. (BuildStats / dynamic filters do not read stage
objects from the coordinator — they ride the task result notification —
and the local fast path reads base tables, which the registry never
records. Nothing else needed covering.)

Covering `readFinalResults` also closes a latent gap on the side: unlike
scalar producers, terminal stages are **not** in `coordReadStages`, so
under `lazy`/`off` their outputs carry the deferred policy — and
`readFinalResults` has no re-poll loop behind its `fetchResultData`. In
practice terminal output reaches the coordinator through the gather sink
and that S3 path is only exercised by probe-split merge (whose pipeline
tasks upload synchronously), so nothing was observed failing; the peer tier
now gives that read a live source regardless of upload policy. Not a fix
being claimed — a hazard narrowed.

The peer fetch reuses the machinery already in production for worker →
worker reads, unchanged:

- **who holds it** — `peerFileRegistry.Lookup(key)` (fed by
  `noteTaskResult` from every dispatcher's result path) → worker ID →
  `WorkerRegistry.PeerAddr` (freshness-checked; draining workers still
  serve).
- **permission** — the query's fetch token. `annotateTaskPeerLocations`
  minted it per ROOT query ID and every worker that ran one of the query's
  tasks recorded it; `Executor.ResolveShuffleFile` validates the presented
  token against its own before resolving the key in the LocalStageCache.
  The coordinator reads that token back with the new
  `peerFileRegistry.ExistingTokenFor`, which **never mints**: a token
  minted after the fact would match nothing and buy a guaranteed
  `PermissionDenied` round trip.
- **payload** — the server may serve a WSHC (s2) envelope when
  `--peer-wire-compression` is on; `fetchFromProducerPeer` sniffs the magic
  itself (`isPeerStagePayload`, `stage_read.go`) before ever returning the
  bytes — WSHF or WSHC is a hit, anything else (WSHZ, garbage, a short
  read) is a miss that falls through to S3, mirroring the worker's
  `codecForMagic` (`internal/worker/peer_exchange.go`). WSHZ is deliberately
  excluded: it is the zstd envelope `--exchange-zstd` uses for **S3**
  uploads only, never for what a peer holds on local disk, so a peer
  serving it would mean the local copy is not what this tier expects — a
  tier failure, not a payload for decode to choke on. Downstream, the
  coordinator's existing `decompressShuffleData` unwraps WSHC and
  `decodeInlineResult` sniffs WSHF-vs-parquet — unchanged, and now
  guaranteed to never see a magic the tier itself didn't already recognize.

Every declination and every failure falls through to the durable copy. The
tier can only change which byte-identical copy of an object is read.

## Fetch bound and re-poll interaction

Two more failure shapes than "the producer declines or the stream breaks"
needed covering, both regression-tested in `stage_read_test.go`:

- **a producer that accepts the stream and never answers.** Nothing before
  this change bounded that wait: `fetchFromProducerPeer` handed the
  caller's context straight to `PeerClient.FetchShuffle`, so a wedged
  producer could hold the read open for as long as the caller's own
  deadline allowed — potentially the worker-sized bounds
  (`dataplane.peerServeAcquireTimeout`, 10 s; `dataplane.PeerFetchIdleTimeout`,
  15 s), which are sized for full shuffle transfers, not a scalar sitting on
  a dispatch-blocking critical path. `stage_read.go`'s `peerStageReadTimeout`
  (2 s, a `var` so tests can shrink it) now wraps the fetch in its own
  `context.WithTimeout`; expiry is an ordinary miss like any other, and the
  caller falls through to S3 within that bound.
- **the re-poll loop re-dialing a producer that already answered once.**
  `fetchStageOutputData`'s bounded re-poll (500 ms sleep, 15 s budget) was
  calling `fetchResultDataTiered` — peer tier included — on every iteration.
  Once the producer has declined or failed, asking it again on the same
  cadence buys nothing: the loop exists to wait out the **durable upload**,
  not the producer, whose answer cannot change faster than that upload
  lands. `fetchResultDataTiered` now takes a `tryPeer bool`: the initial
  call in `fetchStageOutputData` (before the loop starts) passes `true`,
  every iteration inside the loop passes `false`, so a coordinator-side
  stage read asks the peer tier at most once regardless of how many times
  it re-polls S3. `fetchResultData` (the single-shot wrapper used by
  `readFinalResults`) always passes `true` — it has no loop to be "later
  iterations" of.

## Durability argument, per mode

The invariant the change rests on: **the objects the coordinator reads are
uploaded eagerly in all three modes, and that did not change.**
`executeStageDAG` registers every stage named in any stage's
`ScalarDependencies` in `Coordinator.coordReadStages`, and
`annotateTaskPeerLocations` exempts those stages from
`Task.UploadPolicy` under `lazy`/`off`. So:

- **eager** — the durable copy is being written concurrently. The peer read
  simply beats it and skips an S3 GET. Nothing about the upload changes;
  `MarkDurable` still arrives, `IsDurable` is still the reap-grace and
  `classifyFatalResult` input.
- **lazy** — coordinator-read stages (scalar producers) are exempt, so
  their uploads are *started*, not queued: same picture as eager. The
  non-scalar reads (`readFinalResults` → `fetchResultData`) get no such
  exemption for terminal stages, and — corrected here — no release or
  re-poll either: `fetchResultData` is a thin wrapper over
  `fetchResultDataTiered` (`coordinator.go` ~1472) with neither a
  `SubjectUploadRelease` publish nor a retry loop; that machinery lives
  only in `fetchStageOutputData`, the scalar path (`coordinator.go` ~1355
  calls `fetchResultData` directly, not `fetchStageOutputData`). So under
  lazy, a terminal stage's peer tier is this read's only alternative to a
  durable copy that may not have even started uploading — a miss on both
  is a single failed S3 `Get` that fails the query outright, same as before
  this change. The peer tier narrows that hazard (see "Covering
  `readFinalResults`..." above); it does not add the release/re-poll
  machinery this bullet previously — incorrectly — claimed it had.
- **off** — the exemption is the same, so scalar-producer outputs are still
  PUT. A producer that dies before serving leaves the coordinator the S3
  copy, exactly as before.

That exemption is now load-bearing for a second reason, and
`stageReadByCoordinator`'s comment says so: unlike a worker consumer, the
coordinator cannot be retried onto another node. Losing the only copy of a
scalar producer's output fails the query. The peer tier is an accelerator
sitting **on top of** the durable guarantee, never a replacement for it —
so the eager exemption stays even though "the coordinator has no peer
tier" is no longer the reason for it.

`streamingDisabledFor(root)` short-circuits the tier for the one-shot
`ErrInputLost` re-execution, matching `annotateTaskPeerLocations`: that
path runs pure S3 semantics with no hints and no tokens.

## Kill switch

`WADJET_COORD_PEER_READS=0` (optswitch `coord-peer-reads`) restores the
KV→S3 path exactly. Registered in `internal/optswitch`, so the
optimization-invariance oracle enumerates it for free
(`go test -run TestTPCHOptimizationInvariance ./benchmarks/tpch/`). The
switch cannot change a row set — it selects between byte-identical copies
of one object — but the oracle arm is what proves that rather than
asserting it.

## Observability

- `scalar substitution` gains **`tier=kv|peer|s3`** and **`wait_ms=`**. One
  line per placeholder per query, so the next SF100 window can grep which
  copy answered each of the three sites and what it cost, instead of
  reconstructing the gap from timestamps of adjacent log lines (window 4 §7
  had to).
- `Coordinator.StageReadTierCounts()` returns `(kv, peer, s3, peerMisses)`
  across all coordinator-side stage reads. `peerMisses` counts attempted
  fetches that fell through — the coordinator-side analogue of the worker's
  `peer_fallthroughs`, whose value in window 2 was that it was **zero**
  where a hint was never even tried.
- Declinations and failures log at Debug with the key, producer and
  address.

## Expected SF100 shape

- `scalar substitution … tier=peer` on all three sites, `wait_ms` in the
  single digits.
- Q22's `shuffle side complete` → `scalar substitution` gap → ~0 (it was
  1.06 s in window-4 B r4, 0.54 s in B r3, 0.55 s in C r3).
- Whole-cluster idle inside the worker envelope on Q11/Q15/Q22 → ~0, worth
  **−1.5 to −2.1 s per steady suite run** (window-4 measured A 1.5 / B 2.1 /
  C 1.7 s), i.e. ~1 % of a 146 s steady suite.
- No wire-volume change: the bytes moved are ~80 per site. This is a
  latency lever on the critical path, not a bandwidth one.
- `peerMisses` should be 0. A nonzero rate means hints are going stale
  between the result notification and the read, which would be new
  information about producer lifetimes.

## Validation

- `go test ./internal/coordinator/ ./internal/worker/`, `-race` on both.
- `internal/coordinator/stage_read_test.go`: peer tier serves with **zero**
  durable `Get`s (counting `objstore.MemStore` wrapper, live in-process
  `dataplane.PeerServer`, the durable copy present so a zero count proves a
  choice); the switch off restores tier `s3` with exactly one `Get`; six
  fallthrough shapes (dead producer, unknown producer, no token, streaming
  disabled, token rejected, key no longer held) each still return the right
  bytes off the durable copy; a peer that answers with a WSHZ/garbage/short
  payload is a miss, not a decode error; a producer that accepts the stream
  and never answers falls through to S3 within `peerStageReadTimeout` (not
  the worker-sized serve-slot/idle bounds); and a re-poll forced through
  multiple iterations (a flaky durable store) dials the producer exactly
  once, never once per iteration. Every peer-asserting test forces
  `coordPeerReads.Set(true)` so the suite is switch-robust —
  `WADJET_COORD_PEER_READS=0 go test ./internal/coordinator/` passes too.
- TPC-H SF0.01 22/22; `TestTPCHOptimizationInvariance`;
  `task harness:local`.
