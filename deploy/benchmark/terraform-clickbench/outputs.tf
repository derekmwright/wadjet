output "instance_id" {
  value = aws_instance.clickbench.id
}

output "results_hint" {
  value = "aws s3 ls s3://${var.data_bucket}/results/clickbench/ --region ${var.region} --profile citc"
}
