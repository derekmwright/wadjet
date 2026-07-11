# Skew-split A/B — ARM A (control, --skew-split OFF) + fixture staging.
# docs/design/skew-aware-shuffle.md Phase 3. Runs the benchmarks/skew hot-key
# suite (tpch-bench --skew-suite via WADJET_SKEW_SUITE) instead of TPC-H.
# generate_data=true stages the fixture under tables-skew/ on first boot
# (loadSkewData deletes stale objects under the prefix before writing).
# Cluster shape matches sf10-distributed.tfvars exactly — same-window arm B
# (sf10-skew-on.tfvars) differs ONLY in skew_split and generate_data.

scale_factor              = 10
mode                      = "distributed"
coordinator_instance_type = "c7g.2xlarge"
worker_instance_type      = "c7gd.4xlarge" # NVMe: control arm's straggler task may spill
worker_count              = 3
workers_per_node          = 1
max_concurrent            = 2
data_bucket               = "wadjet-bench-sf10-use2"
skip_queries              = ""
query_timeout             = "30m"
data_plane                = "grpc"
catalog_snapshot_prefix   = "" # TPC-H-only shortcut; skew discovery is 2 tables, seconds

skew_suite     = "1"
data_prefix    = "tables-skew/"
generate_data  = true
benchmark_runs = 3
skew_split     = ""
