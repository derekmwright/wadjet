variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "data_bucket" {
  description = "S3 bucket with TPC-H parquet at root prefixes (lineitem/, orders/, ...) — the SF100 comparison bucket. Also receives results/ and hosts bin/trino-assets/."
  type        = string
  default     = "wadjet-bench-sf100-use2"
}

variable "coordinator_instance_type" {
  description = "Trino coordinator instance type. Matches the wadjet bench coordinator for the paired run; for single-node validation use c7gd.4xlarge (query workers need the memory)."
  type        = string
  default     = "c7g.2xlarge"
}

variable "worker_instance_type" {
  description = "Trino worker instance type — locked to the wadjet bench worker shape."
  type        = string
  default     = "c7gd.4xlarge"
}

variable "worker_count" {
  description = "Trino worker nodes. 0 = single-node validation mode: the coordinator schedules work on itself (node-scheduler.include-coordinator)."
  type        = number
  default     = 3
}

variable "trino_version" {
  description = "Trino release to install."
  type        = string
  default     = "470"
}

variable "runs" {
  description = "Suite repetitions (run 1 cold, run 2 page-cache-warm)."
  type        = number
  default     = 2
}

variable "queries" {
  description = "Comma-separated query subset (empty = all 22). Validation uses q01,q02,q06,q10."
  type        = string
  default     = ""
}

variable "auto_run" {
  description = "Run the comparison automatically at boot (true) or wait for a manual SSM invocation (false)."
  type        = bool
  default     = true
}
