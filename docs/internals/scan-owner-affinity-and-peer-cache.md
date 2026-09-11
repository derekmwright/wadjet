# Scan owner affinity and peer cache

Source: internal/coordinator/scan_affinity.go — scanAffinityEnabled, moved 2026-09-11 (#1026)

Scan-task file→worker affinity (docs/design/scan-affinity.md).

The base-table NVMe cache is PER WORKER, and scan fan-outs split files
across tasks with no memory of who cached what — so each worker
independently first-touches (S3-reads) essentially the whole dataset
over a cold suite (SF100 2026-08-08 diagnosis: 107 misses / ~26 GB per
worker = ~3× the dataset from S3), and WHICH query pays each per-worker
first touch depends on scheduler timing. That distribution is the
dominant cold-run variance signature: the same ±20 s tax roamed between
Q04 and Q06 across runs on identical plans.

Rendezvous hashing gives every file one canonical owner among the
active workers: fan-outs group files by owner, tasks carry the owner as
AffinityWorkerID, and the scheduler prefers (never requires) that
placement. First touches then happen once per file CLUSTER-WIDE, later
scans of the same table are warm on every query, and the per-worker
cache footprint drops from |dataset| to |dataset|/N.

DEFAULT ON since the base-table peer tier landed (worker
base_table_peer.go). Affinity alone delivered the cold-run win (SF100
pair 20260808-{125821,131723}: first-touch misses 327→227, cold suite
−11.7%, the roaming Q06 disturbance 19.1s→2.0s) but regressed steady
+12.8%: non-affine base-table readers — late-materialization column
gathers in join tasks, broadcast builds, sub-2×workers tables — kept
reading files their worker doesn't own, and with PARTITIONED caches
those reads missed to S3 where full replication used to hit (~18 GB of
run-2 first-touches). The peer tier completes the class: a non-owner's
miss fetches the owner's NVMe copy over the peer wire and populates
locally, so every reader converges at NIC speed and S3 sees each file
once cluster-wide. WADJET_SCAN_AFFINITY=0 is the kill switch for the
placement half; WADJET_BASE_PEER_TIER=0 kills the peer tier half.
