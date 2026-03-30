output "profile" {
  value = var.profile != "" ? var.profile : "(ad-hoc)"
}

output "bucket" {
  value = local.bucket_name
}

output "region" {
  value = local.eff_region
}

output "mode" {
  value = local.eff_mode
}

output "scale_factor" {
  value = local.eff_scale
}

# Standalone outputs
output "standalone_instance_id" {
  value = local.eff_mode == "standalone" ? aws_instance.standalone[0].id : null
}

output "standalone_instance_type" {
  value = local.eff_mode == "standalone" ? aws_instance.standalone[0].instance_type : null
}

# Distributed outputs
output "coordinator_instance_id" {
  value = local.eff_mode == "distributed" ? aws_instance.coordinator[0].id : null
}

output "coordinator_private_ip" {
  value = local.eff_mode == "distributed" ? aws_instance.coordinator[0].private_ip : null
}

output "worker_instance_ids" {
  value = local.eff_mode == "distributed" ? aws_instance.worker[*].id : []
}

output "worker_private_ips" {
  value = local.eff_mode == "distributed" ? aws_instance.worker[*].private_ip : []
}

# SSM session commands
output "ssm_standalone" {
  value = local.eff_mode == "standalone" ? "aws ssm start-session --target ${aws_instance.standalone[0].id} --region ${local.eff_region}" : null
}

output "ssm_coordinator" {
  value = local.eff_mode == "distributed" ? "aws ssm start-session --target ${aws_instance.coordinator[0].id} --region ${local.eff_region}" : null
}

# Estimated hourly cost
output "estimated_hourly_cost" {
  value = local.eff_mode == "standalone" ? "See instance type pricing for ${local.eff_worker_type}" : "Coordinator (${local.eff_coord_type}) + ${local.eff_workers}x workers (${local.eff_worker_type})"
}

output "spot_enabled" {
  value = local.eff_spot
}
