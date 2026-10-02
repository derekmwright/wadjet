# SPDX-License-Identifier: MIT
#
# The gate runner's standing infrastructure (ADR-0046). Applied ONCE, by hand:
#
#   tofu -chdir=deploy/gates/terraform init
#   tofu -chdir=deploy/gates/terraform apply
#
# It creates: the gate bucket (14-day expiry), the runner's IAM role, a
# security group with no ingress, the launch template every request launches
# from, and the gate reaper (terminates Name=wadjet-gate-runner instances older
# than 40 minutes). Re-running it is idempotent; re-run it after
# deploy/gates/runner.sh changes, because the runner IS the template's
# user_data. Nothing here launches an instance: tooling/gaterun.sh does, with
# `aws ec2 run-instances --launch-template`, and the instance takes its
# per-request inputs (sha, lanes, run id) from its own tags via IMDS.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.0" }
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
}

locals {
  # Project=wadjet-bench is MANDATORY: the benchmark reaper
  # (deploy/benchmark/reaper) filters on exactly this tag and terminates
  # anything older than 2h. Name=wadjet-gate-runner is what the gate reaper,
  # the dispatcher's concurrency count and the README's queries filter on.
  runner_tags = {
    Project = "wadjet-bench"
    Name    = "wadjet-gate-runner"
  }
}

data "aws_caller_identity" "current" {}

data "aws_ami" "al2023_x86" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-*-x86_64"]
  }
}

data "aws_vpc" "default" {
  default = true
}

# --- The gate bucket ---

resource "aws_s3_bucket" "gates" {
  bucket = var.bucket
  tags   = { Project = "wadjet-bench", Name = var.bucket }
}

resource "aws_s3_bucket_public_access_block" "gates" {
  bucket                  = aws_s3_bucket.gates.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "gates" {
  bucket = aws_s3_bucket.gates.id
  rule {
    id     = "expire-everything"
    status = "Enabled"
    filter {}
    expiration {
      days = var.retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# --- The runner's role ---

resource "aws_iam_role" "runner" {
  name = "wadjet-gate-runner"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
  tags = local.runner_tags
}

resource "aws_iam_role_policy" "runner" {
  name = "wadjet-gate-runner"
  role = aws_iam_role.runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "ListGates"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = ["arn:aws:s3:::${var.bucket}"]
        Condition = { StringLike = { "s3:prefix" = ["gates/*"] } }
      },
      {
        # gates/<sha>/<run>/ for the summary and the logs; gates/cache/ for
        # the Go toolchain, go-task and module-cache tarballs.
        Sid      = "ReadWriteGates"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.bucket}/gates/*"]
      },
      {
        Sid      = "GitHubToken"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = ["arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${var.github_token_parameter}"]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "runner_ssm" {
  role       = aws_iam_role.runner.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "runner" {
  name = "wadjet-gate-runner"
  role = aws_iam_role.runner.name
}

resource "aws_security_group" "runner" {
  name        = "wadjet-gate-runner"
  description = "Gate runners: egress only (access is SSM-only)"
  vpc_id      = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.runner_tags
}

# --- The launch template ---

resource "aws_launch_template" "runner" {
  name                                 = var.template_name
  description                          = "ADR-0046 gate runner: spot ${var.instance_type}, self-terminating"
  image_id                             = data.aws_ami.al2023_x86.id
  instance_type                        = var.instance_type
  instance_initiated_shutdown_behavior = "terminate"
  update_default_version               = true
  vpc_security_group_ids               = [aws_security_group.runner.id]

  iam_instance_profile {
    arn = aws_iam_instance_profile.runner.arn
  }

  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type             = "one-time"
      instance_interruption_behavior = "terminate"
    }
  }

  # The runner reads sha / lanes / run / bucket from its own tags.
  metadata_options {
    http_endpoint          = "enabled"
    http_tokens            = "required"
    instance_metadata_tags = "enabled"
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.root_volume_gb
      volume_type           = "gp3"
      iops                  = 6000
      throughput            = 500
      delete_on_termination = true
    }
  }

  # The runner is the user_data, unchanged per request.
  user_data = base64gzip(file("${path.module}/../runner.sh"))

  tag_specifications {
    resource_type = "instance"
    tags          = local.runner_tags
  }
  tag_specifications {
    resource_type = "volume"
    tags          = local.runner_tags
  }
  tag_specifications {
    resource_type = "spot-instances-request"
    tags          = local.runner_tags
  }

  tags = local.runner_tags
}

# --- The gate reaper: the 40-minute backstop ---
#
# The same code as deploy/benchmark/reaper (whose 2h bound on Project=wadjet-bench
# stays the outer backstop), deployed a second time keyed on the Name tag.

data "archive_file" "reaper" {
  type        = "zip"
  source_file = "${path.module}/../../benchmark/reaper/lambda/reaper.py"
  output_path = "${path.module}/.build/gate-reaper.zip"
}

resource "aws_iam_role" "reaper" {
  name = "wadjet-gate-reaper"
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
  name = "wadjet-gate-reaper"
  role = aws_iam_role.reaper.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances"]
        Resource = "*"
      },
      {
        Effect    = "Allow"
        Action    = ["ec2:TerminateInstances"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Name" = "wadjet-gate-runner" } }
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
    ]
  })
}

resource "aws_lambda_function" "reaper" {
  function_name    = "wadjet-gate-reaper"
  role             = aws_iam_role.reaper.arn
  handler          = "reaper.handler"
  runtime          = "python3.12"
  timeout          = 30
  filename         = data.archive_file.reaper.output_path
  source_code_hash = data.archive_file.reaper.output_base64sha256
  environment {
    variables = {
      TAG_KEY             = "Name"
      TAG_VALUE           = "wadjet-gate-runner"
      MAX_RUNTIME_MINUTES = tostring(var.max_runtime_minutes)
    }
  }
  tags = { Project = "wadjet-bench" }
}

resource "aws_cloudwatch_event_rule" "reaper" {
  name                = "wadjet-gate-reaper"
  description         = "Terminate wadjet-gate-runner instances older than ${var.max_runtime_minutes} minutes"
  schedule_expression = var.reaper_schedule
}

resource "aws_cloudwatch_event_target" "reaper" {
  rule = aws_cloudwatch_event_rule.reaper.name
  arn  = aws_lambda_function.reaper.arn
}

resource "aws_lambda_permission" "reaper" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reaper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reaper.arn
}
