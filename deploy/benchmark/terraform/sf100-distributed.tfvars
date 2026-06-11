# SF100 distributed — c7gd workers with NVMe for spill-to-disk
# ~24 GB Parquet (100 GB raw), 600M lineitem rows
# Cost: ~$2.50/hr (coordinator + 3 workers)
#
# MUST match profile: deploy/benchmark/profiles/sf100-distributed.yaml

scale_factor              = 100
mode                      = "distributed"
coordinator_instance_type = "c7g.2xlarge"   # 8 vCPU, 16 GB
worker_instance_type      = "c7gd.4xlarge"  # 16 vCPU, 32 GB, 237 GB NVMe
worker_count              = 3
data_bucket               = "wadjet-bench-sf100-use2"
data_prefix               = ""              # SF100 data at root (lineitem/, orders/, ...), NOT under tables/
generate_data             = false           # data is pre-staged; do not regenerate
skip_queries              = ""
query_timeout             = "30m"           # SF100 heavy queries need >10m default; mirrors sf10-distributed
# max_concurrent history (chronological):
# - 2026-05-07: lowered 4->3 after the Q17 investigation (4 concurrent
#   tasks x ~5-6 GB live overwhelmed the 21.6 GB GOMEMLIMIT, GC thrashed,
#   heartbeats starved, coord reaped).
# - 2026-05-09 (PR #90) and 2026-05-10 (db4feab, PR #91/#92) mc=4 attempts
#   each failed: PR #90 completed Q17 but the suite died post-Q17 on the
#   Q18/Q21 drain; db4feab stalled at Q05 (a hash-join build/probe memory
#   pressure, NOT the drain path) — worker reaped, ~10 min no stage
#   completions. Production pinned at mc=3.
# - 2026-06-03 (d5997af, HashJoin flat-path retirement / partition-on-arrival
#   unification): mc=4 now PASSES the full suite. SF100 22/22 byte-identical
#   to the mc=3 baseline (results/20260603-174555), total 1h6m vs ~71m at
#   mc=3 — FASTER and complete. Both prior death points cleared: Q05 (3m35s)
#   and the Q17->Q18 drain (Q18 7m16s vs 10m8s at mc=3). The fix makes the
#   HashJoin build spill O(partition) instead of doing a reactive O(total)
#   repartition+rebuild under pressure, which is what OOM'd mc=4 before.
#   Raised to 4 — the concurrency payoff the engine was always targeting.
max_concurrent            = 4
data_plane                = "grpc" # Phase C+D+E gRPC data-plane (task dispatch + results + gather + TaskProgress). NATS retained for heartbeats + cancellation + KV only. SF100 is where the design was actually targeted — Q17 dispatch-stall + NATS lock-contention pathologies.
catalog_snapshot_prefix   = "catalog/" # restore post-discovery catalog; first boot writes it (PR #115)
