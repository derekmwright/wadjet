terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  description = "AWS region where benchmark instances run"
  type        = string
  default     = "us-east-2"
}

variable "max_runtime_hours" {
  description = "Terminate instances running longer than this (hours)"
  type        = number
  default     = 2
}

variable "schedule" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "rate(30 minutes)"
}

# --- Lambda function ---

data "archive_file" "reaper" {
  type        = "zip"
  source_file = "${path.module}/lambda/reaper.py"
  output_path = "${path.module}/.build/reaper.zip"
}

resource "aws_lambda_function" "reaper" {
  function_name    = "wadjet-bench-reaper"
  role             = aws_iam_role.reaper.arn
  handler          = "reaper.handler"
  runtime          = "python3.12"
  timeout          = 30
  filename         = data.archive_file.reaper.output_path
  source_code_hash = data.archive_file.reaper.output_base64sha256

  environment {
    variables = {
      MAX_RUNTIME_HOURS = tostring(var.max_runtime_hours)
      TAG_KEY           = "Project"
      TAG_VALUE         = "wadjet-bench"
    }
  }
}

# --- IAM ---

resource "aws_iam_role" "reaper" {
  name = "wadjet-bench-reaper"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "reaper" {
  name = "wadjet-bench-reaper"
  role = aws_iam_role.reaper.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DescribeInstances"
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances"]
        Resource = "*"
      },
      {
        Sid      = "TerminateBenchInstances"
        Effect   = "Allow"
        Action   = ["ec2:TerminateInstances"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "aws:ResourceTag/Project" = "wadjet-bench"
          }
        }
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
    ]
  })
}

# --- EventBridge schedule ---

resource "aws_cloudwatch_event_rule" "reaper" {
  name                = "wadjet-bench-reaper"
  description         = "Terminate wadjet-bench instances running > ${var.max_runtime_hours}h"
  schedule_expression = var.schedule
}

resource "aws_cloudwatch_event_target" "reaper" {
  rule = aws_cloudwatch_event_rule.reaper.name
  arn  = aws_lambda_function.reaper.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reaper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reaper.arn
}

# --- Outputs ---

output "lambda_arn" {
  value = aws_lambda_function.reaper.arn
}

output "schedule" {
  value = var.schedule
}

output "max_runtime_hours" {
  value = var.max_runtime_hours
}
