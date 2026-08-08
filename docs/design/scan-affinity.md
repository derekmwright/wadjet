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

## SF100 verdict (pair 20260808-144825 ctl 1fd166a / 155850 trt 90eeb17)

KEEPER — defaults stay ON. Same-window pair, 2 runs each, wlogs both
arms, all EC2 destroyed.

- Correctness: 44/44 both arms, rows identical, value signatures
  identical (not even the usual Q19 ULP flicker).
- **Cold S3 collapse**: ctl replicated fully in run 1 (107-109 misses
  / ~26.5 GB PER WORKER, ~80 GB total = 3× dataset — the diagnosis
  shape exactly). Trt: 43/46/49 misses, 9.6-11.0 GB per worker,
  **31.4 GB total ≈ 1.2× dataset (−61%)**. Peer wire moved ~35 GB
  (peer_hits 41-53 / 10.7-13.7 GB per worker, serves balanced across
  all three), fallthroughs 7-12 each (cold races: consumer dials
  before the owner populated — the accepted first-touch gap).
- **Run-2 criterion (the affinity-alone killer): PASS.** Trt run-2
  per-worker S3 miss delta +0.81/0/0 GB vs ctl's +0.33/0/0 GB — the
  ~18 GB run-2 S3 penalty from partitioned caches is gone; the peer
  tier absorbed the non-affine readers (peer_hits kept climbing
  through run 2 while misses stayed flat).
- Walls: cold suite −11.2% (503→447s), Q04 30.8→8.6s / Q03 −49% /
  Q14 −35% — the roaming disturbance died, replicating the 131723
  cold win. Run-2 walls +4.5% on plan-identical arms with matched
  miss ledgers and a freak window (BOTH arms' run-2 ran ~60% slower
  than their own colds — ctl 796s vs historical steady ~460s):
  window noise by the measurement doctrine, no row-level mechanism.

## First-touch single-flight (owner read-through)

The verdict above left one residual: concurrent cold misses fan out to
S3 until the owner populates (31.4 GB total vs ~26.5 GB dataset — the
0.2× above 1×, visible as `peer_fallthroughs` 7-12 per worker). The
consumer side was already right — every miss dials the owner first —
but a not-yet-resident owner answered NotFound and bounced the
consumer to S3, while the owner itself would fetch the same file again
on its own first touch.

Owner read-through closes it: `resolveBaseTableFile`, on a residency
miss for a file this worker OWNS (same rendezvous hash over the same
heartbeat domain the consumer tier uses), populates from the inner
store once and serves the admitted copy —
`BaseTableCache.ReadThrough`. S3 first-touch becomes single-flight
cluster-wide: whoever demands a file first, exactly one S3 GET (the
owner's) satisfies the cluster.

- **Single-flight.** Concurrent peer fetches for one key coalesce onto
  one populate op. The fetch runs detached with its own timeout
  (60s) — a canceled waiter neither strands other waiters nor aborts a
  populate whose bytes the owner wants anyway. A concurrent local tee
  for the same key is waited out and its residency reused, never raced
  with a second S3 stream. (This deliberately does NOT touch the local
  miss path's no-blocking-single-flight stance —
  base-table-nvme-cache.md — consumer lifetimes stay uncoupled; only
  the detached owner-side populate blocks, and only peers wait on it.)
- **Ownership guard.** Non-owned misses still answer NotFound: a
  divergent membership view must not let a peer make a worker fetch
  and cache arbitrary S3 objects it will never own. The consumer's S3
  fallthrough covers that (transient, ≤90s) window, as before.
- **Admission.** The read-through spool enforces the inner store's
  advertised size on top of the PAR1 head+tail framing check.
- **Ledger.** `readthroughs`/`readthrough_bytes`/`readthrough_fails`
  on the owner (S3 reads performed for peer-redirected demand — kept
  out of `misses`/`miss_bytes`, which remain local demand that left
  the cluster); the serve itself still counts as
  `peer_serves`/`peer_serve_bytes`, the consumer still ledgers
  `peer_hits`/`peer_bytes`.
- **Serve-slot exposure.** A read-through holds one of the peer
  server's 16 serve slots for the S3 fetch duration (seconds, cold
  only). Overflow rejects with ResourceExhausted past the 10s acquire
  bound and the consumer goes durable — graceful degradation, no new
  stall mode.
- **Kill switch.** `WADJET_BASE_PEER_READTHROUGH=0` restores
  NotFound-on-nonresident exactly; `WADJET_BASE_PEER_TIER=0` still
  kills the whole tier.

Expected SF100 shape: cold-run cluster-wide `miss_bytes` +
`readthrough_bytes` ≈ 1.0× dataset (down from 1.2×),
`peer_fallthroughs` ≈ 0, `readthroughs` > 0 on every worker, rows and
value signatures identical, steady behavior unchanged (read-through is
first-touch-only by construction — resident keys never enter it).

### SF100 verdict (pair 20260808-204142 ctl 774177b / 210616 trt daece0d)

KEEPER — default stays ON. Same-window pair, runs=2 each, wlogs both
arms, all EC2 destroyed. The mechanism landed exactly on the predicted
shape:

- **First-touch S3 = 1.0× dataset.** Ctl 34.1 GB (1.29×: misses
  48-55 / 10.7-11.9 GB per worker, `peer_fallthroughs` 6/18/17). Trt
  **25.6 GB ≈ 0.97×** (22.6 GB misses + 3.0 GB read-throughs),
  `peer_fallthroughs` **0/0/0** — the cold first-touch race is gone
  entirely. `readthroughs` 4/12/6 per worker, `readthrough_fails` 0;
  `peer_hits` rose 124→154 as redirected peers stayed on the wire
  instead of going durable.
- **Correctness:** rows identical 44/44; single vsig delta is the
  known 1-ULP Q19 float-order flicker (ctl's own two runs disagree in
  the same last digit).
- **Walls: window-neutral.** Cold 409→423s (+3.3%), steady 719→685s
  (−4.8%) — both inside the SF100 noise band, movers are the usual
  chaotic set (Q08 −25%, Q09 +35%, Q21 −13/−20%). Consistent with the
  parent verdict: at 0.2×-dataset scale the duplication cost was
  roaming disturbance, not wall-dominating bandwidth — the win is the
  structural completion (S3 sees each file once cluster-wide,
  convergence at NIC speed) plus ~8.5 GB less cold S3 per suite.

`WADJET_BASE_PEER_READTHROUGH=0` reproduces the ctl arm.
