output "instance_id" {
  value = aws_instance.clickbench.id
}

output "results_hint" {
  value = "aws s3 ls s3://${var.data_bucket}/results/clickbench/ --region ${var.region} --profile citc"
}

# Run-event queue (push monitoring). Consume with:
#   deploy/benchmark/watch-events.sh "$(tofu output -raw notify_queue_url)"
output "notify_queue_url" {
  value = aws_sqs_queue.bench_events.url
}

output "watch_events" {
  value = "deploy/benchmark/watch-events.sh ${aws_sqs_queue.bench_events.url} ${var.region}"
}
