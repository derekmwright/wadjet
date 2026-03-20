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
# If data_bucket is set, use an existing bucket (preserves data across cluster rebuilds).
# Otherwise, create an ephemeral bucket with 7-day expiry.

locals {
  create_bucket = var.data_bucket == ""
  bucket_name   = local.create_bucket ? aws_s3_bucket.benchmark[0].bucket : var.data_bucket
  bucket_arn    = local.create_bucket ? aws_s3_bucket.benchmark[0].arn : "arn:aws:s3:::${var.data_bucket}"
}

resource "aws_s3_bucket" "benchmark" {
  count         = local.create_bucket ? 1 : 0
  bucket_prefix = "wadjet-bench-"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "benchmark" {
  count  = local.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.benchmark[0].id

  rule {
    id     = "expire-all"
    status = "Enabled"
    filter {}
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
  name_prefix = "wadjet-bench-"
  vpc_id      = data.aws_vpc.default.id

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
  name_prefix = "wadjet-bench-"

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
  name_prefix = "wadjet-bench-s3-"
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
        local.bucket_arn,
        "${local.bucket_arn}/*",
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.bench.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "bench" {
  name_prefix = "wadjet-bench-"
  role        = aws_iam_role.bench.name
}

# --- User data scripts ---

locals {
  use_prebuilt = var.bin_version != "" && var.data_bucket != ""

  # Pull pre-built binaries from S3 (~10s vs ~5min build)
  prebuilt_script = <<-SCRIPT
    #!/bin/bash
    set -euo pipefail
    export HOME=/root

    # Download pre-built arm64 binaries from the data bucket
    aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/wadjet" /usr/local/bin/wadjet --region ${var.region}
    aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/tpch-bench" /usr/local/bin/tpch-bench --region ${var.region}
    chmod +x /usr/local/bin/wadjet /usr/local/bin/tpch-bench

    # Clone repo for benchmark scripts only
    dnf install -y git
    git clone --depth=1 https://github.com/citc-tech/wadjet.git /root/wadjet

    echo "WADJET_BUCKET=${local.bucket_name}" >> /etc/environment
    echo "WADJET_REGION=${var.region}" >> /etc/environment
    echo "BUILD_COMPLETE=1" >> /etc/environment
  SCRIPT

  # Build from source (when no pre-built binaries available)
  source_build_script = <<-SCRIPT
    #!/bin/bash
    set -euo pipefail
    export HOME=/root
    export GOPATH=/root/go
    export GOMODCACHE=/root/go/pkg/mod

    # Install Go
    cd /tmp
    curl -fsSL "https://go.dev/dl/go${var.go_version}.linux-arm64.tar.gz" -o go.tar.gz
    rm -rf /usr/local/go && tar -C /usr/local -xzf go.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin:/root/go/bin' >> /root/.bashrc
    export PATH=$PATH:/usr/local/go/bin:/root/go/bin

    # Clone and build
    dnf install -y git
    cd /root
    git clone https://github.com/citc-tech/wadjet.git
    cd wadjet
    go build -o /usr/local/bin/wadjet ./cmd/wadjet
    go build -o /usr/local/bin/tpch-bench ./cmd/tpch-bench

    echo "WADJET_BUCKET=${local.bucket_name}" >> /etc/environment
    echo "WADJET_REGION=${var.region}" >> /etc/environment
    echo "BUILD_COMPLETE=1" >> /etc/environment
  SCRIPT

  build_script = local.use_prebuilt ? local.prebuilt_script : local.source_build_script

  # Standalone: build + auto-run benchmark
  standalone_user_data = <<-EOF
    ${local.build_script}

    # Auto-run standalone benchmark
    export WADJET_BUCKET="${local.bucket_name}"
    export WADJET_REGION="${var.region}"
    export BENCHMARK_RUNS="${var.benchmark_runs}"
    export GENERATE_DATA="${var.generate_data ? "1" : "0"}"
    cd /root/wadjet
    bash deploy/benchmark/run-benchmark.sh standalone SF${var.scale_factor} 2>&1 | tee /root/benchmark.log
  EOF

  # Coordinator: build + auto-run distributed benchmark
  coordinator_user_data = <<-EOF
    ${local.build_script}

    # Auto-run distributed benchmark (starts NATS, waits for workers)
    export WADJET_BUCKET="${local.bucket_name}"
    export WADJET_REGION="${var.region}"
    export BENCHMARK_RUNS="${var.benchmark_runs}"
    cd /root/wadjet
    bash deploy/benchmark/run-benchmark.sh distributed SF${var.scale_factor} ${var.worker_count} 2>&1 | tee /root/benchmark.log
  EOF
}

# --- Standalone mode: single instance ---

resource "aws_instance" "standalone" {
  count = var.mode == "standalone" ? 1 : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
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

  user_data = base64encode(local.standalone_user_data)

  tags = {
    Name = "wadjet-bench-standalone"
    Role = "standalone"
    SF   = "SF${var.scale_factor}"
  }
}

# --- Distributed mode: coordinator + workers ---

resource "aws_instance" "coordinator" {
  count = var.mode == "distributed" ? 1 : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.coordinator_instance_type
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

  user_data = base64encode(local.coordinator_user_data)

  tags = {
    Name = "wadjet-bench-coordinator"
    Role = "coordinator"
    SF   = "SF${var.scale_factor}"
  }
}

resource "aws_instance" "worker" {
  count = var.mode == "distributed" ? var.worker_count : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
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

  user_data = base64encode(<<-EOF
    ${local.build_script}

    # Start wadjet worker connecting to coordinator (retry until NATS is ready)
    for i in $(seq 1 60); do
      /usr/local/bin/wadjet serve \
        --mode=worker \
        --nats-url="nats://${aws_instance.coordinator[0].private_ip}:4222" \
        --endpoint="s3.${var.region}.amazonaws.com" \
        --ssl \
        --bucket="${local.bucket_name}" \
        --region="${var.region}" \
        --storage-type=s3 &
      WORKER_PID=$!
      sleep 5
      if kill -0 $WORKER_PID 2>/dev/null; then
        echo "WORKER_STARTED=1" >> /etc/environment
        break
      fi
      echo "Worker attempt $i failed, retrying in 10s..."
      sleep 10
    done
  EOF
  )

  tags = {
    Name = "wadjet-bench-worker-${count.index}"
    Role = "worker"
    SF   = "SF${var.scale_factor}"
  }
}
