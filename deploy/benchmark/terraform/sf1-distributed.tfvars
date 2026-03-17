# SF1 distributed — ~$0.58/hr (Graviton3, 4 nodes)
# ~1 GB data, 6M lineitem rows, coordinator + 3 workers
# Total session cost: ~$1

region                    = "us-east-2"
scale_factor              = 1
mode                      = "distributed"
coordinator_instance_type = "c7g.xlarge"   # 4 vCPU, 8 GB
worker_instance_type      = "c7g.xlarge"   # 4 vCPU, 8 GB
worker_count              = 3
data_bucket               = "wadjet-bench-sf1"
