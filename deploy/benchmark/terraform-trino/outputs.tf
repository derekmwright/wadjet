output "coordinator_instance_id" {
  value = aws_instance.coordinator.id
}

output "worker_instance_ids" {
  value = aws_instance.worker[*].id
}

output "ssm_coordinator" {
  value = "aws ssm start-session --target ${aws_instance.coordinator.id} --region ${var.region}"
}
