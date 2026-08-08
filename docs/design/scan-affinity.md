# Scan-task file→worker affinity

Status: landed 2026-08-08. Kill switch `WADJET_SCAN_AFFINITY=0`.

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
