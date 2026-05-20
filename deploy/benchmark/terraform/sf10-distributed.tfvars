# SF10 distributed — Graviton3, 4 nodes
# ~12 GB data, 60M lineitem rows, coordinator + 3 workers
# Total session cost: ~$6-7
#
# MUST match profile: deploy/benchmark/profiles/sf10-distributed.yaml

scale_factor             = 10
mode                     = "distributed"
coordinator_instance_type = "c7g.2xlarge"   # 8 vCPU, 16 GB (no spill on coord, c7g fine)
worker_instance_type      = "c7gd.4xlarge"  # 16 vCPU, 32 GB, 1x950 GB NVMe SSD. NVMe required: 2026-05-03 SF10 Q18 stalled with c7g.4xlarge because spill landed on tmpfs (RAM-backed /tmp) and ENOSPC'd the worker. c7gd matches SF100 baseline for cluster topology consistency.
worker_count              = 3
workers_per_node         = 1   # 2026-05-03: dropped from 2 to 1 — cgroup throttle confirmed via Q05/Q09/Q10 10-20× speedup vs workers_per_node=2 run.
max_concurrent           = 2   # 2026-05-03: dropped from 4 to 2 — Q11 stalled with 4 because each broadcast_join task peaks ~3.5 GB; 4 concurrent = 14 GB transient + GC garbage saturates 32 GB envelope. Cluster concurrency = 1 × 2 × 3 = 6 task slots (vs 12 prior). Slower throughput but Q11 should complete.
data_bucket              = "wadjet-bench-sf10-use2"
skip_queries             = ""
query_timeout            = "30m"  # SF10 heavy queries (Q03/Q05/Q21 lineitem joins) need >10m; the 26 April AWS run hit the 10m cap on Q03 even though stages were progressing
data_plane               = "grpc" # Phase C+D+E gRPC data-plane (task dispatch + results + gather + TaskProgress). NATS retained for heartbeats + cancellation + KV only.
