# SPDX-License-Identifier: MIT

output "bucket" {
  value = aws_s3_bucket.gates.bucket
}

output "launch_template" {
  value = "${aws_launch_template.runner.name} (${aws_launch_template.runner.id}, default version ${aws_launch_template.runner.default_version})"
}

output "ami" {
  value = data.aws_ami.al2023_x86.id
}

output "gate_reaper" {
  value = "${aws_lambda_function.reaper.function_name}: Name=wadjet-gate-runner older than ${var.max_runtime_minutes} min, ${var.reaper_schedule}"
}

output "next" {
  value = "tooling/gaterun.sh --check"
}
