# SF10 security standalone — $0.58/hr (Graviton3)
# ~2.5 GB data, 60M rows across 5 tables, single node
# Total session cost: ~$0.80

scale_factor         = 10
mode                 = "standalone"
benchmark_type       = "security"
worker_instance_type = "c7g.4xlarge"  # 16 vCPU, 32 GB
generate_data        = true
