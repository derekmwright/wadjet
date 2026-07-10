# Skew-split A/B — ARM B (treatment, --skew-split ON).
# Discovers the fixture staged by the arm A run (sf10-skew-off.tfvars);
# differs from arm A ONLY in skew_split and generate_data.

scale_factor              = 10
mode                      = "distributed"
coordinator_instance_type = "c7g.2xlarge"
worker_instance_type      = "c7gd.4xlarge"
worker_count              = 3
workers_per_node          = 1
max_concurrent            = 2
data_bucket               = "wadjet-bench-sf10-use2"
skip_queries              = ""
query_timeout             = "30m"
data_plane                = "grpc"
catalog_snapshot_prefix   = ""

skew_suite     = "1"
data_prefix    = "tables-skew/"
generate_data  = false
benchmark_runs = 3
skew_split     = "1"
