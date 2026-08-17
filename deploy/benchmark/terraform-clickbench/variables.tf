variable "region" {
  description = "AWS region (the hits bucket lives in us-east-2)"
  type        = string
  default     = "us-east-2"
}

variable "data_bucket" {
  description = "Bucket with hits parts under tables/hits/ and staged binaries under bin/. Also receives results/clickbench/."
  type        = string
  default     = "wadjet-bench-clickbench-use2"
}

variable "instance_type" {
  description = "Official ClickBench listing hardware. Do not change for submission runs."
  type        = string
  default     = "c6a.4xlarge"
}

variable "bin_version" {
  description = "Binary version under bin/<ver>/amd64/ (git short SHA or 'latest')."
  type        = string
  default     = "latest"
}

variable "tries" {
  description = "Runs per query; the first is the cold run."
  type        = number
  default     = 3
}

variable "machine" {
  description = "Machine string for the results JSON."
  type        = string
  default     = "c6a.4xlarge, 500gb gp2"
}

variable "comment" {
  description = "Comment for the results JSON."
  type        = string
  default     = ""
}

variable "auto_shutdown" {
  description = "Shut the instance down (terminate) when the run completes. Disable for interactive debugging via SSM."
  type        = bool
  default     = true
}

variable "mem_budget_bytes" {
  description = "Per-query memory budget passed to clickbench-bench. Default 18GiB on the 32GB c6a.4xlarge: the auto-detected 75%-of-RAM budget (23GB) sat too close to the 27.6GB GOMEMLIMIT — with GOGC off, budget-full aggregate state plus uncollected garbage and merge overhead crossed physical RAM on Q19 (100M-row high-cardinality GROUP BY) and the kernel killed the process."
  type        = number
  default     = 19327352832
}

variable "profile_queries" {
  description = "Comma-separated query numbers to CPU/heap-profile in an extra isolate pass after the timed suite (empty = none). Profiles upload under results/clickbench/<ts>/profiles/."
  type        = string
  default     = ""
}

variable "bench_env" {
  description = "Env-var prefix for the bench invocation (e.g. \"WADJET_SCAN_WORKERS=8\"); empty = none"
  type        = string
  default     = ""
}

variable "notify_queue_name" {
  description = "SQS queue clickbench-bench pushes lifecycle events to (internal/benchnotify). Consumed by deploy/benchmark/watch-events.sh. Distinct from the TPC-H module's queue — separate state, separate name."
  type        = string
  default     = "wadjet-bench-events-clickbench"
}
