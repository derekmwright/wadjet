# SF10 distributed — $1.94/hr (Graviton3, 4 nodes)
# ~12 GB data, 60M lineitem rows, coordinator + 3 workers
# Total session cost: ~$6-7

scale_factor             = 10
mode                     = "distributed"
coordinator_instance_type = "c7g.2xlarge"  # 8 vCPU, 16 GB
worker_instance_type      = "c7g.4xlarge"  # 16 vCPU, 32 GB (2xlarge OOM'd Q21)
worker_count              = 3
workers_per_node         = 2   # 2 wadjet processes per node × 3 nodes = 6 worker processes. Each ~12GB envelope (75% of 32GB / 2). Tradeoff vs workers_per_node=4: less crash isolation but halves S3 upload contention (2x4x2=16 connections/node vs 32). With more headroom per process, shuffle output buffering doesn't pressure the 5GB-cap regime that 4-procs forced.
max_concurrent           = 4   # per process; effective cluster concurrency = 2 × 4 × 3 = 24 task slots (same as before with workers_per_node=4)
data_bucket              = "wadjet-bench-sf10-use2"
skip_queries             = ""
query_timeout            = "30m"  # SF10 heavy queries (Q03/Q05/Q21 lineitem joins) need >10m; the 26 April AWS run hit the 10m cap on Q03 even though stages were progressing
