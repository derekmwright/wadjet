# SF10 distributed — $1.45/hr (Graviton3, 4 nodes)
# ~12 GB data, 60M lineitem rows, coordinator + 3 workers
# Total session cost: ~$4-5

scale_factor             = 10
mode                     = "distributed"
coordinator_instance_type = "c7g.2xlarge"  # 8 vCPU, 16 GB (xlarge OOM'd on SF10)
worker_instance_type      = "c7g.2xlarge"  # 8 vCPU, 16 GB
worker_count              = 3
data_bucket              = "wadjet-bench-sf10-use2"
