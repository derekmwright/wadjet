# SF10 distributed — $1.01/hr (Graviton3, 4 nodes)
# ~12 GB data, 60M lineitem rows, coordinator + 3 workers
# Total session cost: ~$3

scale_factor             = 10
mode                     = "distributed"
coordinator_instance_type = "c7g.xlarge"   # 4 vCPU, 8 GB
worker_instance_type      = "c7g.2xlarge"  # 8 vCPU, 16 GB
worker_count              = 3
