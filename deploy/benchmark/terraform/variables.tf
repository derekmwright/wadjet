variable "profile" {
  description = "Benchmark profile name (e.g., sf100-distributed). Reads ../profiles/<profile>.yaml for benchmark config. Individual -var flags override profile values when explicitly set."
  type        = string
  default     = ""
}

variable "region" {
  description = "AWS region (null = use profile or default us-east-2)"
  type        = string
  default     = null
}

variable "scale_factor" {
  description = "TPC-H scale factor (1, 10, or 100). null = use profile or default 1."
  type        = number
  default     = null

  validation {
    condition     = var.scale_factor == null || contains([1, 10, 100], var.scale_factor)
    error_message = "scale_factor must be 1, 10, or 100"
  }
}

variable "mode" {
  description = "Benchmark mode: standalone or distributed. null = use profile or default standalone."
  type        = string
  default     = null

  validation {
    condition     = var.mode == null || contains(["standalone", "distributed"], var.mode)
    error_message = "mode must be standalone or distributed"
  }
}

variable "coordinator_instance_type" {
  description = "EC2 instance type for coordinator (distributed mode). null = use profile or default c7g.2xlarge."
  type        = string
  default     = null
}

variable "worker_instance_type" {
  description = "EC2 instance type for standalone or worker nodes. null = use profile or default c7g.2xlarge."
  type        = string
  default     = null
}

variable "worker_count" {
  description = "Number of workers (distributed mode only). null = use profile or default 3."
  type        = number
  default     = null
}

variable "go_version" {
  description = "Go version to install"
  type        = string
  default     = "1.24.1"
}

variable "memory_budget" {
  description = "Per-task memory budget in bytes (0 = unlimited)"
  type        = number
  default     = 0
}

variable "benchmark_runs" {
  description = "Number of benchmark runs per query. null = use profile or default 1."
  type        = number
  default     = null
}

variable "use_spot" {
  description = "Use spot instances. null = use profile or default true."
  type        = bool
  default     = null
}

variable "data_bucket" {
  description = "Existing S3 bucket with TPC-H data. null = use profile; empty string = create ephemeral bucket."
  type        = string
  default     = null
}

variable "bin_version" {
  description = "Pre-built binary version to pull from S3 (git short SHA or 'latest'). If empty, builds from source."
  type        = string
  default     = "latest"
}

variable "generate_data" {
  description = "Set to true to regenerate TPC-H data instead of using pre-seeded bucket. null = use profile."
  type        = bool
  default     = null
}

variable "run_duckdb_comparison" {
  description = "Run DuckDB TPC-H comparison after the Wadjet benchmark"
  type        = bool
  default     = false
}

variable "skip_queries" {
  description = "Comma-separated query numbers to skip (e.g. '2,17'). null = use profile."
  type        = string
  default     = null
}

variable "query_timeout" {
  description = "Per-query timeout (Go duration, e.g. '10m', '30m'). null = use profile or default 10m."
  type        = string
  default     = null
}

variable "max_concurrent" {
  description = "Maximum concurrent tasks per worker process (lower = less memory, slower execution). With workers_per_node > 1 the effective cluster concurrency is workers_per_node × max_concurrent × node_count."
  type        = number
  default     = 4
}

variable "mmap_relief" {
  description = "Pass --mmap-relief to workers: MADV_DONTNEED relief of cold mmap'd cache files under heap pressure (Phase 5). Default false = dormant."
  type        = bool
  default     = false
}

variable "mmap_relief_threshold_mb" {
  description = "--mmap-relief-threshold-mb: TOTAL process RSS ceiling in MB; relieve cold mmap to bring RSS back to this level. Set below the worker memory.max (e.g. ~16000 on the ~20 GB SF100 c7gd per-proc envelope). Only used when mmap_relief = true."
  type        = number
  default     = 16000
}

variable "spill_floating_budget" {
  description = "Pass --spill-floating-budget to workers: activate the floating-budget spill threshold (Phase 3a). Default false = static 40%/90%. Requires mmap relief to be safe."
  type        = bool
  default     = false
}

variable "workers_per_node" {
  description = "Number of independent wadjet worker processes to launch per node. >1 gives crash isolation: when one process OOMs, sibling workers on the same node keep running. Each process gets a per-process memory envelope (auto-derived from /N of total) and registers separately with the coord."
  type        = number
  default     = 1
}

variable "cache_bytes" {
  description = "Worker LRU file cache size in bytes (0 = auto-detect based on memory and budget)"
  type        = number
  default     = 0
}

variable "data_prefix" {
  description = "S3 prefix for table data (e.g. 'tables/' or '' for root-level paths). null = use profile."
  type        = string
  default     = null
}

variable "benchmark_type" {
  description = "Benchmark suite to run: tpch or security. null = use profile or default tpch."
  type        = string
  default     = null

  validation {
    condition     = var.benchmark_type == null || contains(["tpch", "security"], var.benchmark_type)
    error_message = "benchmark_type must be tpch or security"
  }
}

variable "arch" {
  description = "CPU architecture: arm64 (Graviton) or x86_64 (Intel/AMD). null = use profile or default arm64."
  type        = string
  default     = null

  validation {
    condition     = var.arch == null || contains(["arm64", "x86_64"], var.arch)
    error_message = "arch must be arm64 or x86_64"
  }
}

variable "reverse_bloom_inner_threshold" {
  description = "Build-side row count above which inner-join reverse-bloom fires. 0 = use code default (50M). Set to a huge number (e.g. 999999999999) to disable the optimization for hunting bugs."
  type        = number
  default     = 0
}

variable "join_debug" {
  description = "Set to 1 to enable HashJoin diagnostic prints (DBG HashJoin.Build, DBG HashJoinProbe.Close)."
  type        = string
  default     = ""
}

variable "dynamic_filters" {
  description = "Set to 1 to enable Trino-style semi-join dynamic-filter pushdown on the coordinator. Off by default for v1 rollout."
  type        = string
  default     = ""
}

variable "use_native_dag" {
  description = "Route distributed queries through the Phase 3 native-DAG executor (feat/distribution-property-phase-3)."
  type        = bool
  default     = false
}

variable "data_plane" {
  description = "Worker↔coord data-plane transport. Empty or 'nats' uses the legacy NATS reply-subject path; 'grpc' enables the bidi gRPC stream (Phases C+D+E). See project_split_plane_design_2026-05-20."
  type        = string
  default     = ""
}

variable "data_plane_port" {
  description = "TCP port for the gRPC data-plane listener on the coordinator. Workers dial coord on this port. Only used when data_plane=grpc."
  type        = number
  default     = 9091
}

# Recommended instance types per scale factor:
#   Graviton3 ARM (c7g):
#     SF1:   c7g.2xlarge  (8 vCPU, 16 GB) — $0.29/hr
#     SF10:  c7g.4xlarge  (16 vCPU, 32 GB) — $0.58/hr
#     SF100: c7g.8xlarge  (32 vCPU, 64 GB) — $1.15/hr
#   Intel Sapphire Rapids (c7i):
#     SF10:  c7i.4xlarge  (16 vCPU, 32 GB) — $0.71/hr
#     SF10 coordinator: c7i.2xlarge (8 vCPU, 16 GB) — $0.36/hr
