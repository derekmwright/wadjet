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
