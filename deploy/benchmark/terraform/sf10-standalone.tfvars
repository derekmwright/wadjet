# SF10 standalone — $0.58/hr (Graviton3)
# ~12 GB data, 60M lineitem rows, single node
# Total session cost: ~$1.20

scale_factor  = 10
mode          = "standalone"
worker_instance_type = "c7g.4xlarge"  # 16 vCPU, 32 GB
data_bucket   = "wadjet-bench-sf10-use2"
generate_data = true
bin_version   = ""  # build from source to pick up latest fixes
