# SPDX-License-Identifier: MIT
#
# Every value has a default: `tofu -chdir=deploy/gates/terraform apply` takes
# no -var and no environment variable. The values are ADR-0046's caps
# (docs/adr/0046-gate-runners-off-the-host.md); changing one is a deploy
# outside the standing approval and needs its own. tooling/gaterun.sh carries
# the same names as constants — change both together.

variable "profile" {
  description = "AWS CLI profile the one-time apply runs as (the benchmark account)."
  type        = string
  default     = "citc"
}

variable "region" {
  description = "Region for the bucket, the launch template and the gate reaper."
  type        = string
  default     = "us-east-2"
}

variable "bucket" {
  description = "The gate bucket: runner summaries and logs under gates/<sha>/<run>/, the toolchain cache under gates/cache/. NEVER a bench bucket (those are do-not-wipe)."
  type        = string
  default     = "wadjet-gates-use2"
}

variable "retention_days" {
  description = "Every object in the gate bucket expires after this many days."
  type        = number
  default     = 14
}

variable "template_name" {
  description = "Launch template name; tooling/gaterun.sh launches with --launch-template LaunchTemplateName=<this>."
  type        = string
  default     = "wadjet-gate-runner"
}

variable "instance_type" {
  description = "ADR-0046 shape: one spot c7a.8xlarge (32 vCPU, 64 GiB) per request; on a capacity refusal tooling/gaterun.sh overrides the type with the 2026-10-06 fallback list (c6a / c7i / c6i .8xlarge)."
  type        = string
  default     = "c7a.8xlarge"
}

variable "root_volume_gb" {
  description = "Root gp3 volume: Go toolchain, module and build caches, test temp dirs, the oracle image."
  type        = number
  default     = 200
}

variable "max_runtime_minutes" {
  description = "The gate reaper terminates any instance tagged Name=wadjet-gate-runner older than this (the on-box watchdog shuts down at 38)."
  type        = number
  default     = 40
}

variable "reaper_schedule" {
  description = "How often the gate reaper looks."
  type        = string
  default     = "rate(5 minutes)"
}

variable "github_token_parameter" {
  description = "SSM SecureString holding a read-only GitHub token. Derek creates it (README); tofu only grants read on it. Optional while the repository is public: the runner clones anonymously when it is absent."
  type        = string
  default     = "/wadjet/gates/github-token"
}
