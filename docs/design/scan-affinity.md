# Scan-task file→worker affinity

Status: landed 2026-08-08; DEFAULT ON since the base-table peer tier
(§peer tier) completed the class the same day. Kill switches:
`WADJET_SCAN_AFFINITY=0` (placement), `WADJET_BASE_PEER_TIER=0`
(peer fetches + serving).

## The diagnosis (SF100, 2026-08-08 wlogs, zero EC2)

The base-table NVMe cache is PER WORKER, and scan fan-outs split files
across tasks with no memory of who cached what. Over a cold suite each
worker therefore first-touches essentially the whole dataset from S3:
the cache ledger showed 107 misses / ~26 GB per worker — the ~26 GB
dataset pulled ~3× for a 3-worker cluster — with zero evictions and
full replication by suite end.

WHICH query pays each per-worker first touch depends on scheduler
timing, and that distribution is the dominant cold-run "variance"
signature. Q04's cold scans ran 4-5 s (warm) in the one run where
population had finished by Q04 and 21-25 s (S3-bound) in the other six;
the same ±20 s tax roamed to Q06 in the run Q04 dodged it. During
disturbed-Q04's window the three workers pulled +7.1/+11.4/+4.8 GB of
miss bytes. This — not upload drains (refuted, see
upload-foreground-qos.md v4 verdict) — is the cold coin-flip.

## The fix

Rendezvous hashing (`affinityOwner`: argmax over fnv64(file, worker))
gives every base-table file ONE canonical owner among the active
workers. Scan fan-outs (`dispatchScanFilterStage`,
`dispatchScanAggregateStage`, `runShuffleSide`) group files by owner
(`affineFileSets`), preserving the fan-out's total task count with
per-owner proportional shares; each task carries its owner as
`Task.AffinityWorkerID`. The scheduler prefers that worker — after
locality, before binpack, under the same same-batch anti-stacking cap —
and any fallback placement just misses the cache exactly as the
pre-affinity fan-out did. Placement is a preference, never correctness.

Consequences:

1. First touches happen once per file CLUSTER-WIDE: cold-suite S3 reads
   drop from N×|dataset| to |dataset|.
2. The warm/cold boundary becomes deterministic — the roaming ±20 s
   cold tax collapses onto each table's true first scan.
3. Per-worker cache footprint drops from |dataset| to |dataset|/N
   (matters when the dataset outgrows the NVMe budget: affinity also
   makes eviction pressure 1/N).
4. Rendezvous minimal-remap: membership churn remaps only the departed
   worker's files.

Degenerate shapes skip affinity: fewer files than 2× workers (a
single-file row-group shard fan-out pinned to one owner would serialize
the stage), empty worker registry, or the kill switch.

## Non-goals

Stage-output (queries/ scratch) reads keep locality placement (#263) —
their producers, not a hash, define the right worker. NATS-pull dispatch
ignores the hint (placement needs the targeted gRPC data plane); the
hint degrades to today's behavior there.

## SF100 verdict (pair 20260808-125821 ctl / 131723 trt)

Correctness clean (44/44, rows identical, Q19 ULP only), placement
honored (`placement=affine:13/10` on scan batches). Cold run: first-
touch misses 327→227 (80.5→70.9 GB total S3 across the suite, 53 GB of
it in run 1), cold suite −11.7%, and the roaming disturbance died —
Q06 cold 19.1 s (ctl) → 2.0 s (trt). Steady run: +12.8% REAL
regression — control's caches were fully replicated after run 1
(run-2 miss delta 0.00 GB on every worker) while affinity's
PARTITIONED caches left the non-affine base-table readers
(late-materialization column gathers inside join tasks, broadcast
builds, sub-2×workers tables) paying ~18 GB of run-2 first-touches.

The mechanism is correct but the CLASS is "every base-table reader",
not "scan fan-outs". Completion: a peer tier on the base-table cache
miss path — a non-owner fetches the owner's NVMe copy over the peer
wire (machinery exists for stage outputs) instead of S3, populating
locally. Then first touches hit S3 once per file cluster-wide
regardless of reader, convergence is NIC-speed, and the flag flips on.

## Peer tier (landed 2026-08-08, same day)

A non-owner's whole-file cache miss (`BaseTableCache.Get`) fetches the
file's rendezvous owner's NVMe copy over the existing PeerExchange
wire, spools it into the local cache (fsync + rename + admit), and
serves the admitted copy; ANY failure — no live owner, owner cache
cold, dial refused, mid-stream reset, corrupt payload — falls through
to the S3 path exactly as before. Population semantics mirror the tee:
inflight-deduped, best-effort, invisible to the caller's stream.

Mechanics:

- **One ownership definition.** `distributed.AffinityOwner` (moved
  from the coordinator) hashes bare object keys over the sorted live,
  non-draining worker set — the same domain the coordinator's fan-out
  placement uses. The worker learns membership the same way the
  coordinator does: subscribing `SubjectHeartbeat` (heartbeats already
  carry `PeerAddr`), with the registry's 90s staleness TTL. A
  transiently divergent view costs one NotFound → S3 fallthrough.
- **Serving.** `Executor.ResolveShuffleFile` branches on the
  `basetable:<bucket>/<key>` key shape and resolves ONLY entries
  already resident in the cache, via the cache's own index
  (`PeerLocalPath`) — no path construction from request input. There
  is no per-query capability token for this class (the owner may never
  have run the consumer's query); serving matches the peer plane's
  intra-cluster trust posture, optionally gated cluster-wide by
  `WADJET_PEER_SECRET`, with TLS as the hardening seam.
- **Admission guard.** The peer stream advertises no size, so clean
  EOF plus parquet framing (PAR1 head AND tail) gates admission into
  the safety-critical parquet read path; anything else is discarded
  and the miss goes durable.
- **Ledger separation.** Peer-served misses count as `peer_hits` /
  `peer_bytes` (and `peer_serves`/`peer_serve_bytes` on the owner),
  never as hits or S3 misses — `miss_bytes` keeps measuring exactly
  the reads that left the cluster, so the first-touch ledger that
  produced this diagnosis stays valid.
- **Ranged reads** (`GetReaderAt`, footer-sized) still pass through to
  S3 without populating, as before — the whole-file Get on every scan
  path remains the populator. Follow-up if footer misses ever ledger
  as material.

Expected SF100 shape vs the 131723 trt arm: identical cold win, and
the steady +12.8% regression gone — run-2 non-affine first-touches
become NIC-speed peer fetches (`peer_hits` > 0, run-2 `miss_bytes`
delta ≈ 0 on every worker, matching control's full-replication run-2).
