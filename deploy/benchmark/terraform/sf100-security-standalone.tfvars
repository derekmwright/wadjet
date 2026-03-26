# SF100 security standalone — $1.15/hr (Graviton3)
# ~25 GB data, 600M rows across 5 tables, single node
# Total session cost: ~$2-3

scale_factor         = 100
mode                 = "standalone"
benchmark_type       = "security"
worker_instance_type = "c7g.8xlarge"  # 32 vCPU, 64 GB
generate_data        = true
