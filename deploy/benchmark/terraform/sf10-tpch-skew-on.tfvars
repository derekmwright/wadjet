# TPC-H no-harm arm for the --skew-split default-flip decision:
# standard SF10 TPC-H suite with the flag ON. TPC-H keys are uniform, so the
# expectation is ~0 suite delta and zero "skew split planned" markers — this
# arm exists to prove the flag doesn't harm uniform workloads. Pair with a
# same-window sf10-distributed.tfvars control run (identical except
# skew_split); never compare across time windows.

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
catalog_snapshot_prefix   = "catalog/"

skew_split = "1"
