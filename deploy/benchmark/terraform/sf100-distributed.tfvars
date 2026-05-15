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
# 2026-05-07 Q17 investigation: 4 concurrent stage tasks each with ~5-6 GB
# live working set overwhelmed the 21.6 GB GOMEMLIMIT, GC thrashed,
# heartbeats starved, coord reaped. Lowered to 3.
#
# 2026-05-09 SF100 deploy of 7a8c0d6 (PR #90 groupstate-soa-split) at
# max_concurrent=4: Q17 PASSED in 22m18s (the =4 unblock goal), but the
# suite did not complete — a query post-Q17 (Q18 or Q21 based on staged
# shuffle outputs `final_aggregate-13` + `join-16`) hit a separate
# memory bottleneck and the run never reached the S3 upload step. Q17
# was +5m13s vs the =3 baseline (17m5s). The refactor is correct but
# =4 across the whole 22Q suite remains memory-bound on a different
# code path. Stay at =3 until the post-Q17 bottleneck (likely
# drainSimpleAggsToPartialGroups peak, ~749 MB at SF10) is addressed.
# 2026-05-10 deploy of db4feab (PR #91 + #92 typed-keyvals) at mc=4:
# Q01-Q04 PASSED (Q03 -53s vs 7m18 baseline, Q04 -1m49 vs 8m57). Q05
# (5-way customer/orders/lineitem/supplier/nation/region join) stalled
# after ~16 min: worker reaped (1m43s no heartbeat with 2 in-flight
# tasks), then ~10 min no stage completions. Different signature than
# PR #90's post-Q17 stall — this is a hash-join build/probe memory
# pressure at SF100, NOT the drain path. Production stays at mc=3 until
# the Q05-shape HashJoin peak is addressed. Cost of this attempt ~$1.55.
max_concurrent            = 3
