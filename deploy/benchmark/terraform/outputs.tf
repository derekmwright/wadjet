output "bucket" {
  value = local.bucket_name
}

output "region" {
  value = var.region
}

output "mode" {
  value = var.mode
}

output "scale_factor" {
  value = var.scale_factor
}

# Standalone outputs
output "standalone_instance_id" {
  value = var.mode == "standalone" ? aws_instance.standalone[0].id : null
}

output "standalone_instance_type" {
  value = var.mode == "standalone" ? aws_instance.standalone[0].instance_type : null
}

# Distributed outputs
output "coordinator_instance_id" {
  value = var.mode == "distributed" ? aws_instance.coordinator[0].id : null
}

output "coordinator_private_ip" {
  value = var.mode == "distributed" ? aws_instance.coordinator[0].private_ip : null
}

output "worker_instance_ids" {
  value = var.mode == "distributed" ? aws_instance.worker[*].id : []
}

output "worker_private_ips" {
  value = var.mode == "distributed" ? aws_instance.worker[*].private_ip : []
}

# SSM session commands
output "ssm_standalone" {
  value = var.mode == "standalone" ? "aws ssm start-session --target ${aws_instance.standalone[0].id} --region ${var.region}" : null
}

output "ssm_coordinator" {
  value = var.mode == "distributed" ? "aws ssm start-session --target ${aws_instance.coordinator[0].id} --region ${var.region}" : null
}

# Estimated hourly cost
output "estimated_hourly_cost" {
  value = var.mode == "standalone" ? "See instance type pricing for ${var.worker_instance_type}" : "Coordinator (${var.coordinator_instance_type}) + ${var.worker_count}x workers (${var.worker_instance_type})"
}

output "spot_enabled" {
  value = var.use_spot
}
