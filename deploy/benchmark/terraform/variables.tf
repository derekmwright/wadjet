variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "scale_factor" {
  description = "TPC-H scale factor (1, 10, or 100)"
  type        = number
  default     = 1

  validation {
    condition     = contains([1, 10, 100], var.scale_factor)
    error_message = "scale_factor must be 1, 10, or 100"
  }
}

variable "mode" {
  description = "Benchmark mode: standalone or distributed"
  type        = string
  default     = "standalone"

  validation {
    condition     = contains(["standalone", "distributed"], var.mode)
    error_message = "mode must be standalone or distributed"
  }
}

variable "coordinator_instance_type" {
  description = "EC2 instance type for coordinator (distributed mode)"
  type        = string
  default     = "c7g.xlarge"
}

variable "worker_instance_type" {
  description = "EC2 instance type for standalone or worker nodes"
  type        = string
  default     = "c7g.2xlarge"
}

variable "worker_count" {
  description = "Number of workers (distributed mode only)"
  type        = number
  default     = 3
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
  description = "Number of benchmark runs per query"
  type        = number
  default     = 3
}

variable "use_spot" {
  description = "Use spot instances (~60-70% cheaper, risk of reclamation mid-benchmark)"
  type        = bool
  default     = false
}

variable "data_bucket" {
  description = "Existing S3 bucket with TPC-H data. If empty, creates an ephemeral bucket. Use this to preserve data across cluster rebuilds."
  type        = string
  default     = ""
}

# Recommended instance types per scale factor (Graviton3 ARM):
#   SF1:   c7g.2xlarge  (8 vCPU, 16 GB) — $0.29/hr
#   SF10:  c7g.4xlarge  (16 vCPU, 32 GB) — $0.58/hr
#   SF100: c7g.8xlarge  (32 vCPU, 64 GB) — $1.15/hr
