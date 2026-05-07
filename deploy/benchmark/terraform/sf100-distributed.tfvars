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
query_timeout             = "30m"           # SF100 heavy queries need >10m default; mirrors sf10-distributed
# 2026-05-07 Q17 investigation: 4 concurrent stage tasks each with ~5-6 GB live
# working set (decoded parquet + hash + partition buffers) overwhelmed the
# 21.6 GB GOMEMLIMIT, GC thrashed, heartbeats starved, coord reaped. Lowering
# concurrency to 3 grows per-task budget to ~6.5 GB (PER_PROC_POOL / 3) and
# bounds 3-task live footprint to ~18 GB — fits with headroom for GC garbage
# and operator allocations. Existing MaxConcurrent semaphore gates BEFORE
# msg.NextMsg(), so JetStream AckWait never fires on a blocked task.
max_concurrent            = 3
