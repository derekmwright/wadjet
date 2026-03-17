# SF1 standalone — $0.29/hr (Graviton3)
# ~1 GB data, 6M lineitem rows, single node
# Total session cost: ~$0.40

region                = "us-east-2"
scale_factor          = 1
mode                  = "standalone"
worker_instance_type  = "c7g.2xlarge"  # 8 vCPU, 16 GB
data_bucket           = "wadjet-bench-sf1"
