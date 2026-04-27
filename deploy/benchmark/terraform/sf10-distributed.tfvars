# SF10 distributed — $1.94/hr (Graviton3, 4 nodes)
# ~12 GB data, 60M lineitem rows, coordinator + 3 workers
# Total session cost: ~$6-7

scale_factor             = 10
mode                     = "distributed"
coordinator_instance_type = "c7g.2xlarge"  # 8 vCPU, 16 GB
worker_instance_type      = "c7g.4xlarge"  # 16 vCPU, 32 GB (2xlarge OOM'd Q21)
worker_count              = 3
data_bucket              = "wadjet-bench-sf10-use2"
skip_queries             = ""
query_timeout            = "30m"  # SF10 heavy queries (Q03/Q05/Q21 lineitem joins) need >10m; the 26 April AWS run hit the 10m cap on Q03 even though stages were progressing
