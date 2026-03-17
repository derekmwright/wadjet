output "bucket" {
  value = aws_s3_bucket.benchmark.bucket
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
output "standalone_ip" {
  value = var.mode == "standalone" ? aws_instance.standalone[0].public_ip : null
}

output "standalone_instance_type" {
  value = var.mode == "standalone" ? aws_instance.standalone[0].instance_type : null
}

# Distributed outputs
output "coordinator_ip" {
  value = var.mode == "distributed" ? aws_instance.coordinator[0].public_ip : null
}

output "coordinator_private_ip" {
  value = var.mode == "distributed" ? aws_instance.coordinator[0].private_ip : null
}

output "worker_ips" {
  value = var.mode == "distributed" ? aws_instance.worker[*].public_ip : []
}

output "worker_private_ips" {
  value = var.mode == "distributed" ? aws_instance.worker[*].private_ip : []
}

# SSH commands
output "ssh_standalone" {
  value = var.mode == "standalone" ? "ssh -i ~/.ssh/${var.key_name}.pem ec2-user@${aws_instance.standalone[0].public_ip}" : null
}

output "ssh_coordinator" {
  value = var.mode == "distributed" ? "ssh -i ~/.ssh/${var.key_name}.pem ec2-user@${aws_instance.coordinator[0].public_ip}" : null
}

# Estimated hourly cost
output "estimated_hourly_cost" {
  value = var.mode == "standalone" ? "See instance type pricing for ${var.worker_instance_type}" : "Coordinator (${var.coordinator_instance_type}) + ${var.worker_count}x workers (${var.worker_instance_type})"
}

output "spot_enabled" {
  value = var.use_spot
}
