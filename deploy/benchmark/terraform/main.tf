terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --- S3 bucket for benchmark data ---

resource "aws_s3_bucket" "benchmark" {
  bucket_prefix = "caelum-bench-"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "benchmark" {
  bucket = aws_s3_bucket.benchmark.id

  rule {
    id     = "expire-all"
    status = "Enabled"
    expiration {
      days = 7
    }
  }
}

# --- S3 VPC gateway endpoint (free, no per-GB charge) ---

data "aws_route_tables" "default" {
  vpc_id = data.aws_vpc.default.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = data.aws_vpc.default.id
  service_name = "com.amazonaws.${var.region}.s3"

  route_table_ids = data.aws_route_tables.default.ids
}

# --- Security group ---

resource "aws_security_group" "bench" {
  name_prefix = "caelum-bench-"
  vpc_id      = data.aws_vpc.default.id

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  # NATS (coordinator ↔ workers)
  ingress {
    from_port = 4222
    to_port   = 4222
    protocol  = "tcp"
    self      = true
  }

  # HTTP API
  ingress {
    from_port = 8080
    to_port   = 8080
    protocol  = "tcp"
    self      = true
  }

  # gRPC API
  ingress {
    from_port = 9090
    to_port   = 9090
    protocol  = "tcp"
    self      = true
  }

  # pgwire
  ingress {
    from_port = 5433
    to_port   = 5433
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- IAM role for S3 access ---

resource "aws_iam_role" "bench" {
  name_prefix = "caelum-bench-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name_prefix = "caelum-bench-s3-"
  role        = aws_iam_role.bench.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:HeadObject",
        "s3:HeadBucket",
      ]
      Resource = [
        aws_s3_bucket.benchmark.arn,
        "${aws_s3_bucket.benchmark.arn}/*",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "bench" {
  name_prefix = "caelum-bench-"
  role        = aws_iam_role.bench.name
}

# --- User data script (shared) ---

locals {
  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail

    # Install Go
    cd /tmp
    curl -fsSL "https://go.dev/dl/go${var.go_version}.linux-arm64.tar.gz" -o go.tar.gz
    rm -rf /usr/local/go && tar -C /usr/local -xzf go.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin:/root/go/bin' >> /root/.bashrc
    export PATH=$PATH:/usr/local/go/bin:/root/go/bin

    # Clone and build
    dnf install -y git
    cd /root
    git clone https://github.com/derekmwright/caelum.git
    cd caelum
    go build -o /usr/local/bin/caelum ./cmd/caelum

    # Build benchmark binary
    go test -c -o /usr/local/bin/caelum-bench ./benchmarks/tpch/

    echo "CAELUM_BUCKET=${aws_s3_bucket.benchmark.bucket}" >> /etc/environment
    echo "CAELUM_REGION=${var.region}" >> /etc/environment
    echo "BUILD_COMPLETE=1" >> /etc/environment
  EOF
}

# --- Standalone mode: single instance ---

resource "aws_instance" "standalone" {
  count = var.mode == "standalone" ? 1 : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = aws_iam_instance_profile.bench.name
  subnet_id              = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = var.scale_factor <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        instance_interruption_behavior = "terminate"
        spot_instance_type             = "one-time"
      }
    }
  }

  user_data = base64encode(local.user_data)

  tags = {
    Name = "caelum-bench-standalone"
    Role = "standalone"
    SF   = "SF${var.scale_factor}"
  }
}

# --- Distributed mode: coordinator + workers ---

resource "aws_instance" "coordinator" {
  count = var.mode == "distributed" ? 1 : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.coordinator_instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = aws_iam_instance_profile.bench.name
  subnet_id              = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        instance_interruption_behavior = "terminate"
        spot_instance_type             = "one-time"
      }
    }
  }

  user_data = base64encode(local.user_data)

  tags = {
    Name = "caelum-bench-coordinator"
    Role = "coordinator"
    SF   = "SF${var.scale_factor}"
  }
}

resource "aws_instance" "worker" {
  count = var.mode == "distributed" ? var.worker_count : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = aws_iam_instance_profile.bench.name
  subnet_id              = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = var.scale_factor <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        instance_interruption_behavior = "terminate"
        spot_instance_type             = "one-time"
      }
    }
  }

  user_data = base64encode(local.user_data)

  tags = {
    Name = "caelum-bench-worker-${count.index}"
    Role = "worker"
    SF   = "SF${var.scale_factor}"
  }
}
