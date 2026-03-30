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
  # When using a profile, region comes from the YAML.
  # For ad-hoc runs without a profile, set var.region directly.
  region = local.eff_region
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
    values = [local.eff_arch == "arm64" ? "al2023-ami-*-arm64" : "al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --- S3 bucket for benchmark data ---
# If data_bucket is set, use an existing bucket (preserves data across cluster rebuilds).
# Otherwise, create an ephemeral bucket with 7-day expiry.

# --- Profile-driven configuration ---
# When var.profile is set, read the YAML profile and derive all benchmark
# config from it. Individual variables serve as fallbacks for ad-hoc runs.
locals {
  has_profile = var.profile != ""
  _raw_p      = local.has_profile ? yamldecode(file("../profiles/${var.profile}.yaml")) : null

  # Effective values: profile wins when set, otherwise use individual vars.
  # Using try() to safely access profile fields that may be absent in the YAML.
  eff_region       = local.has_profile ? try(local._raw_p.storage.region, var.region) : var.region
  eff_scale        = local.has_profile ? try(local._raw_p.benchmark.scale_factor, var.scale_factor) : var.scale_factor
  eff_mode         = local.has_profile ? try(local._raw_p.cluster.mode, var.mode) : var.mode
  eff_workers      = local.has_profile ? try(local._raw_p.cluster.workers, var.worker_count) : var.worker_count
  eff_coord_type   = local.has_profile ? try(local._raw_p.cluster.coordinator_instance, var.coordinator_instance_type) : var.coordinator_instance_type
  eff_worker_type  = local.has_profile ? try(local._raw_p.cluster.worker_instance, var.worker_instance_type) : var.worker_instance_type
  eff_arch         = local.has_profile ? try(local._raw_p.cluster.arch, var.arch) : var.arch
  eff_spot         = local.has_profile ? try(local._raw_p.cluster.use_spot, var.use_spot) : var.use_spot
  eff_data_bucket  = local.has_profile ? try(local._raw_p.storage.bucket, var.data_bucket) : var.data_bucket
  eff_runs         = local.has_profile ? try(local._raw_p.benchmark.runs, var.benchmark_runs) : var.benchmark_runs
  eff_prefix       = local.has_profile ? try(local._raw_p.storage.data_prefix, var.data_prefix) : var.data_prefix
  eff_timeout      = local.has_profile ? try(local._raw_p.benchmark.query_timeout, var.query_timeout) : var.query_timeout
  eff_generate     = local.has_profile ? try(!local._raw_p.benchmark.skip_load, var.generate_data) : var.generate_data
  eff_skip_queries = local.has_profile ? try(join(",", [for q in local._raw_p.benchmark.skip_queries : tostring(q)]), var.skip_queries) : var.skip_queries
  eff_bench_type   = local.has_profile ? try(local._raw_p.benchmark.type, var.benchmark_type) : var.benchmark_type

  create_bucket = local.eff_data_bucket == ""
  bucket_name   = local.create_bucket ? aws_s3_bucket.benchmark[0].bucket : local.eff_data_bucket
  bucket_arn    = local.create_bucket ? aws_s3_bucket.benchmark[0].arn : "arn:aws:s3:::${local.eff_data_bucket}"
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
  service_name = "com.amazonaws.${local.eff_region}.s3"

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
  use_prebuilt = var.bin_version != "" && local.eff_data_bucket != ""

  # Pull pre-built binaries from S3 (~10s vs ~5min build)
  prebuilt_script = <<-SCRIPT
    #!/bin/bash
    set -uo pipefail
    export HOME=/root

    # Retry helper: retries a command up to 5 times with exponential backoff
    retry() {
      local max=5 delay=2
      for i in $(seq 1 $max); do
        "$@" && return 0
        echo "Attempt $i/$max failed: $*" >&2
        sleep $delay
        delay=$((delay * 2))
      done
      echo "FATAL: $* failed after $max attempts" >&2
      return 1
    }

    # Download pre-built arm64 binaries from the data bucket
    retry aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/wadjet" /usr/local/bin/wadjet --region ${local.eff_region}
    retry aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/tpch-bench" /usr/local/bin/tpch-bench --region ${local.eff_region}
    aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/security-bench" /usr/local/bin/security-bench --region ${local.eff_region} || true
    chmod +x /usr/local/bin/wadjet /usr/local/bin/tpch-bench /usr/local/bin/security-bench 2>/dev/null || true

    # Download deploy scripts from S3 (staged alongside binaries by stage-binaries.sh)
    mkdir -p /root/wadjet/deploy/benchmark
    aws s3 sync "s3://${local.bucket_name}/bin/${var.bin_version}/scripts/" /root/wadjet/deploy/benchmark/ --region ${local.eff_region} || true
    chmod +x /root/wadjet/deploy/benchmark/*.sh 2>/dev/null || true

    # Download benchmark profile (read by tpch-bench --config)
    %{if local.has_profile}
    aws s3 cp "s3://${local.bucket_name}/bin/${var.bin_version}/profiles/${var.profile}.yaml" /usr/local/bin/benchmark-profile.yaml --region ${local.eff_region} || true
    %{endif}

    echo "WADJET_BUCKET=${local.bucket_name}" >> /etc/environment
    echo "WADJET_REGION=${local.eff_region}" >> /etc/environment
    echo "BENCHMARK_TYPE=${local.eff_bench_type}" >> /etc/environment
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
    go build -o /usr/local/bin/security-bench ./cmd/security-bench

    echo "WADJET_BUCKET=${local.bucket_name}" >> /etc/environment
    echo "WADJET_REGION=${local.eff_region}" >> /etc/environment
    echo "BENCHMARK_TYPE=${local.eff_bench_type}" >> /etc/environment
    echo "BUILD_COMPLETE=1" >> /etc/environment
  SCRIPT

  build_script = local.use_prebuilt ? local.prebuilt_script : local.source_build_script

  # Profile env var: set when a benchmark profile is available on the instance
  profile_env = local.has_profile ? "export BENCHMARK_PROFILE=/usr/local/bin/benchmark-profile.yaml" : "# No profile"

  # Standalone: build + auto-run benchmark (+ optional DuckDB comparison)
  standalone_user_data = <<-EOF
    ${local.build_script}

    # Auto-run standalone benchmark
    export WADJET_BUCKET="${local.bucket_name}"
    export WADJET_REGION="${local.eff_region}"
    export BENCHMARK_TYPE="${local.eff_bench_type}"
    export BENCHMARK_RUNS="${local.eff_runs}"
    export GENERATE_DATA="${local.eff_generate ? "1" : "0"}"
    export DATA_PREFIX="${local.eff_prefix}"
    export SKIP_QUERIES="${local.eff_skip_queries}"
    export QUERY_TIMEOUT="${local.eff_timeout}"
    ${local.profile_env}
    cd /root/wadjet

    %{if var.run_duckdb_comparison}
    # Disable auto-shutdown in run-benchmark.sh so DuckDB comparison can run after
    sed -i 's/^sudo shutdown.*/#&/' deploy/benchmark/run-benchmark.sh
    %{endif}

    bash deploy/benchmark/run-benchmark.sh standalone SF${local.eff_scale} 2>&1 | tee /root/benchmark.log

    %{if var.run_duckdb_comparison}
    export RESULTS_DIR=/root/benchmark-results

    %{if local.eff_bench_type == "security"}
    bash /root/wadjet/deploy/benchmark/run-duckdb-security-comparison.sh "$WADJET_BUCKET" "$WADJET_REGION" 2>&1 | tee /root/duckdb-comparison.log
    %{else}
    export Q11_FRACTION=$(python3 -c "print(f'{0.0001/${local.eff_scale}:.10f}')")
    bash /root/wadjet/deploy/benchmark/run-duckdb-comparison.sh "$WADJET_BUCKET" "$WADJET_REGION" 2>&1 | tee /root/duckdb-comparison.log
    %{endif}

    # Upload DuckDB results alongside Wadjet results
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    aws s3 cp /root/benchmark-results/ "s3://$WADJET_BUCKET/results/$TIMESTAMP/" --recursive --region "$WADJET_REGION"

    sudo shutdown -h now
    %{endif}
  EOF

  # Coordinator: build + auto-run distributed benchmark
  # tpch-bench starts its own embedded NATS on :4222 and waits for workers.
  # Do NOT start "wadjet serve" here — it would grab port 4222 and conflict.
  coordinator_user_data = <<-EOF
    ${local.build_script}

    # Auto-run distributed benchmark (bench binary embeds its own coordinator + NATS)
    export WADJET_BUCKET="${local.bucket_name}"
    export WADJET_REGION="${local.eff_region}"
    export BENCHMARK_TYPE="${local.eff_bench_type}"
    export BENCHMARK_RUNS="${local.eff_runs}"
    export GENERATE_DATA="${local.eff_generate ? "1" : "0"}"
    export DATA_PREFIX="${local.eff_prefix}"
    export SKIP_QUERIES="${local.eff_skip_queries}"
    export QUERY_TIMEOUT="${local.eff_timeout}"
    ${local.profile_env}
    cd /root/wadjet

    bash deploy/benchmark/run-benchmark.sh distributed SF${local.eff_scale} ${local.eff_workers} 2>&1 | tee /root/benchmark.log
  EOF
}

# --- Standalone mode: single instance ---

resource "aws_instance" "standalone" {
  count = local.eff_mode == "standalone" ? 1 : 0

  ami                                  = data.aws_ami.al2023.id
  instance_type                        = local.eff_worker_type
  instance_initiated_shutdown_behavior = local.eff_spot ? null : "terminate"
  vpc_security_group_ids               = [aws_security_group.bench.id]
  iam_instance_profile                 = aws_iam_instance_profile.bench.name
  subnet_id                            = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = local.eff_scale <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  dynamic "instance_market_options" {
    for_each = local.eff_spot ? [1] : []
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
    Name    = "wadjet-bench-standalone"
    Role    = "standalone"
    SF      = "SF${local.eff_scale}"
    Project = "wadjet-bench"
  }
}

# --- Distributed mode: coordinator + workers ---

resource "aws_instance" "coordinator" {
  count = local.eff_mode == "distributed" ? 1 : 0

  ami                                  = data.aws_ami.al2023.id
  instance_type                        = local.eff_coord_type
  instance_initiated_shutdown_behavior = local.eff_spot ? null : "terminate"
  vpc_security_group_ids               = [aws_security_group.bench.id]
  iam_instance_profile                 = aws_iam_instance_profile.bench.name
  subnet_id                            = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = local.eff_scale <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  dynamic "instance_market_options" {
    for_each = local.eff_spot ? [1] : []
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
    Name    = "wadjet-bench-coordinator"
    Role    = "coordinator"
    SF      = "SF${local.eff_scale}"
    Project = "wadjet-bench"
  }
}

resource "aws_instance" "worker" {
  count = local.eff_mode == "distributed" ? local.eff_workers : 0

  ami                                  = data.aws_ami.al2023.id
  instance_type                        = local.eff_worker_type
  instance_initiated_shutdown_behavior = local.eff_spot ? null : "terminate"
  vpc_security_group_ids               = [aws_security_group.bench.id]
  iam_instance_profile                 = aws_iam_instance_profile.bench.name
  subnet_id                            = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = local.eff_scale <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  dynamic "instance_market_options" {
    for_each = local.eff_spot ? [1] : []
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

    # Verify binary was downloaded successfully
    if [ ! -x /usr/local/bin/wadjet ]; then
      echo "FATAL: wadjet binary not found or not executable" >&2
      exit 1
    fi

    # Mount NVMe instance store (c7gd, i4g, etc.) for spill-to-disk.
    # Falls back to /tmp on instances without NVMe (c7g, etc.).
    SPILL_DIR="/tmp"
    NVME_DEV=$(lsblk -dno NAME,TYPE | awk '$2=="disk" && $1~/nvme[0-9]+n1/ && $1!~/nvme0n1/{print "/dev/"$1; exit}')
    if [ -n "$NVME_DEV" ]; then
      echo "Formatting NVMe instance store: $NVME_DEV"
      mkfs.xfs -f "$NVME_DEV"
      mkdir -p /mnt/nvme
      mount "$NVME_DEV" /mnt/nvme
      mkdir -p /mnt/nvme/spill
      SPILL_DIR="/mnt/nvme/spill"
      echo "NVMe spill directory ready at $SPILL_DIR"
    fi

    # Wait for coordinator NATS to be reachable before starting worker
    COORD_IP="${aws_instance.coordinator[0].private_ip}"
    echo "Waiting for NATS on $COORD_IP:4222..."
    NATS_READY=0
    for i in $(seq 1 120); do
      if timeout 2 bash -c "echo > /dev/tcp/$COORD_IP/4222" 2>/dev/null; then
        echo "NATS reachable after $i attempts"
        NATS_READY=1
        break
      fi
      sleep 5
    done
    if [ "$NATS_READY" -eq 0 ]; then
      echo "FATAL: NATS not reachable after 120 attempts" >&2
      exit 1
    fi

    # Start wadjet worker (retry on failure — coordinator may restart during benchmark)
    while true; do
      /usr/local/bin/wadjet serve \
        --mode=worker \
        --nats-url="nats://$COORD_IP:4222" \
        --endpoint="s3.${local.eff_region}.amazonaws.com" \
        --ssl \
        --bucket="${local.bucket_name}" \
        --region="${local.eff_region}" \
        --storage-type=s3 \
        --max-concurrent=${var.max_concurrent} \
        --spill-dir="$SPILL_DIR" \
        ${var.memory_budget > 0 ? "--memory-budget=${var.memory_budget}" : ""} \
        ${var.cache_bytes > 0 ? "--cache-bytes=${var.cache_bytes}" : ""} 2>&1
      EXIT_CODE=$?
      echo "Worker exited with code $EXIT_CODE, restarting in 5s..."
      echo "WORKER_STARTED=1" >> /etc/environment
      sleep 5
    done
  EOF
  )

  tags = {
    Name    = "wadjet-bench-worker-${count.index}"
    Role    = "worker"
    SF      = "SF${local.eff_scale}"
    Project = "wadjet-bench"
  }
}
