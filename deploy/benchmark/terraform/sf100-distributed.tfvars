# SF100 distributed — $3.45/hr (Graviton3, 5 nodes)
# ~120 GB data, 600M lineitem rows, coordinator + 4 workers
# Total session cost: ~$12

scale_factor             = 100
mode                     = "distributed"
coordinator_instance_type = "c7g.2xlarge"  # 8 vCPU, 16 GB
worker_instance_type      = "c7g.8xlarge"  # 32 vCPU, 64 GB
worker_count              = 4
