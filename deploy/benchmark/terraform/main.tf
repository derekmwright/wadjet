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
    # Exclude "minimal" AMI — it doesn't include SSM agent.
    values = [local.eff_arch == "arm64" ? "al2023-ami-2023.*-kernel-*-arm64" : "al2023-ami-2023.*-kernel-*-x86_64"]
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

  # Effective values: explicit -var wins > profile > ultimate default.
  # Variables use null defaults so we can detect explicit overrides.
  eff_region       = var.region != null ? var.region : (local.has_profile ? try(local._raw_p.storage.region, "us-east-2") : "us-east-2")
  eff_scale        = var.scale_factor != null ? var.scale_factor : (local.has_profile ? try(local._raw_p.benchmark.scale_factor, 1) : 1)
  eff_mode         = var.mode != null ? var.mode : (local.has_profile ? try(local._raw_p.cluster.mode, "standalone") : "standalone")
  eff_workers      = var.worker_count != null ? var.worker_count : (local.has_profile ? try(local._raw_p.cluster.workers, 3) : 3)
  eff_coord_type   = var.coordinator_instance_type != null ? var.coordinator_instance_type : (local.has_profile ? try(local._raw_p.cluster.coordinator_instance, "c7g.2xlarge") : "c7g.2xlarge")
  eff_worker_type  = var.worker_instance_type != null ? var.worker_instance_type : (local.has_profile ? try(local._raw_p.cluster.worker_instance, "c7g.2xlarge") : "c7g.2xlarge")
  eff_arch         = var.arch != null ? var.arch : (local.has_profile ? try(local._raw_p.cluster.arch, "arm64") : "arm64")
  eff_spot         = var.use_spot != null ? var.use_spot : (local.has_profile ? try(local._raw_p.cluster.use_spot, true) : true)
  eff_data_bucket  = var.data_bucket != null ? var.data_bucket : (local.has_profile ? try(local._raw_p.storage.bucket, "") : "")
  eff_runs         = var.benchmark_runs != null ? var.benchmark_runs : (local.has_profile ? try(local._raw_p.benchmark.runs, 1) : 1)
  eff_prefix       = var.data_prefix != null ? var.data_prefix : (local.has_profile ? try(local._raw_p.storage.data_prefix, "tables/") : "tables/")
  eff_timeout      = var.query_timeout != null ? var.query_timeout : (local.has_profile ? try(local._raw_p.benchmark.query_timeout, "10m") : "10m")
  eff_generate     = var.generate_data != null ? var.generate_data : (local.has_profile ? try(!local._raw_p.benchmark.skip_load, false) : false)
  eff_skip_queries = var.skip_queries != null ? var.skip_queries : (local.has_profile ? try(join(",", [for q in local._raw_p.benchmark.skip_queries : tostring(q)]), "") : "")
  eff_bench_type   = var.benchmark_type != null ? var.benchmark_type : (local.has_profile ? try(local._raw_p.benchmark.type, "tpch") : "tpch")

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

  # gRPC data-plane (workers ↔ coord). Phases C+D+E. Workers dial coord
  # on this port when --data-plane=grpc; idle when --data-plane=nats.
  ingress {
    from_port = var.data_plane_port
    to_port   = var.data_plane_port
    protocol  = "tcp"
    self      = true
  }

  # Peer exchange (worker ↔ worker FetchShuffle, streaming exchange).
  # A small range: each worker PROCESS on a host binds base+idx
  # (workers_per_node processes per host). Idle unless streaming_exchange
  # is set; harmless to keep open intra-SG.
  ingress {
    from_port = var.peer_exchange_port
    to_port   = var.peer_exchange_port + 15
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
    echo "WADJET_REVERSE_BLOOM_INNER_THRESHOLD=${var.reverse_bloom_inner_threshold}" >> /etc/environment
    echo "WADJET_JOIN_DEBUG=${var.join_debug}" >> /etc/environment
    echo "WADJET_DYNAMIC_FILTERS=${var.dynamic_filters}" >> /etc/environment
    echo "WADJET_SORT_MERGE_JOIN_BYTES=${var.sort_merge_join_bytes}" >> /etc/environment
    echo "WADJET_LATE_MATERIALIZATION=${var.late_materialization}" >> /etc/environment
    echo "WADJET_BUSHY_JOIN_REORDER=${var.bushy_join_reorder}" >> /etc/environment
    echo "WADJET_SKEW_SPLIT=${var.skew_split}" >> /etc/environment
    echo "WADJET_BLOCK_PROFILE_RATE=${var.block_profile_rate}" >> /etc/environment
    echo "WADJET_MUTEX_PROFILE_FRACTION=${var.mutex_profile_fraction}" >> /etc/environment
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
    echo "WADJET_REVERSE_BLOOM_INNER_THRESHOLD=${var.reverse_bloom_inner_threshold}" >> /etc/environment
    echo "WADJET_JOIN_DEBUG=${var.join_debug}" >> /etc/environment
    echo "WADJET_DYNAMIC_FILTERS=${var.dynamic_filters}" >> /etc/environment
    echo "WADJET_SORT_MERGE_JOIN_BYTES=${var.sort_merge_join_bytes}" >> /etc/environment
    echo "WADJET_LATE_MATERIALIZATION=${var.late_materialization}" >> /etc/environment
    echo "WADJET_BUSHY_JOIN_REORDER=${var.bushy_join_reorder}" >> /etc/environment
    echo "WADJET_SKEW_SPLIT=${var.skew_split}" >> /etc/environment
    echo "WADJET_BLOCK_PROFILE_RATE=${var.block_profile_rate}" >> /etc/environment
    echo "WADJET_MUTEX_PROFILE_FRACTION=${var.mutex_profile_fraction}" >> /etc/environment
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
    export WADJET_REVERSE_BLOOM_INNER_THRESHOLD="${var.reverse_bloom_inner_threshold}"
    export WADJET_JOIN_DEBUG="${var.join_debug}"
    export WADJET_DYNAMIC_FILTERS="${var.dynamic_filters}"
    export WADJET_SORT_MERGE_JOIN_BYTES="${var.sort_merge_join_bytes}"
    export WADJET_LATE_MATERIALIZATION="${var.late_materialization}"
    export WADJET_BUSHY_JOIN_REORDER="${var.bushy_join_reorder}"
    export WADJET_SKEW_SPLIT="${var.skew_split}"
    export WADJET_SKEW_SUITE="${var.skew_suite}"
    export WADJET_BLOCK_PROFILE_RATE="${var.block_profile_rate}"
    export WADJET_MUTEX_PROFILE_FRACTION="${var.mutex_profile_fraction}"
    export USE_NATIVE_DAG="${var.use_native_dag ? "1" : "0"}"
    # Phase C/D/E data-plane selection. Empty/"nats" = legacy NATS reply
    # subjects; "grpc" routes task dispatch + results + gather +
    # TaskProgress over a per-worker bidi gRPC stream on
    # ${var.data_plane_port}.
    export TPCH_DATA_PLANE="${var.data_plane}"
    export TPCH_DATA_PLANE_ADDR=":${var.data_plane_port}"
    # Streaming exchange (peer shuffle reads + async upload). Coordinator
    # side of the flag; the worker cloud-init adds --streaming-exchange
    # from the same terraform var.
    export TPCH_STREAMING_EXCHANGE="${var.streaming_exchange ? "1" : "0"}"
    # Catalog snapshot/restore prefix (PR #115). Non-empty = restore the
    # post-discovery catalog from s3://<bucket>/<prefix> instead of paying
    # ~15 min of discovery+ANALYZE per deploy; first boot writes it.
    export TPCH_CATALOG_SNAPSHOT_PREFIX="${var.catalog_snapshot_prefix}"
    # Force GOGC=100 so heap is bounded by 2x live data instead of growing to
    # GOMEMLIMIT before triggering mark-assist. Without this, scan-3 SF10
    # tasks accumulated parquet-decode garbage to ~10 GB peak heap before
    # GC fired; the resulting mark-assist starved the heartbeat goroutine
    # for >90s and the coord reaped workers, kicking off the Q03 redelivery
    # loop observed on the 2026-05-01 streaming-refactor deploy.
    export WADJET_GOGC=100
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
  instance_initiated_shutdown_behavior = "stop"
  vpc_security_group_ids               = [aws_security_group.bench.id]
  iam_instance_profile                 = aws_iam_instance_profile.bench.name
  subnet_id                            = data.aws_subnets.default.ids[0]

  root_block_device {
    volume_size = local.eff_scale <= 10 ? 50 : 200
    volume_type = "gp3"
    throughput  = 250
    iops        = 3000
  }

  # Coordinator always runs on-demand — it's cheap (c7g.2xlarge = $0.29/hr)
  # and the entire benchmark fails if it gets spot-terminated. Workers use
  # spot since they auto-reconnect on restart.
  # shutdown_behavior = stop so we can inspect logs if the benchmark crashes.

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
    # Workers spill shuffle output to /tmp; SF10 Q18's lineitem shuffle
    # exceeded the 50 GB SF10 default with workers_per_node=1 (write
    # ENOSPC at column 15 / l_comment per project_diagnostic_b26b889).
    # 200 GB unconditionally — extra cost on SF1 is negligible (gp3 at
    # ~$0.08/GB-month, prorated to a 30-min run is fractions of a cent),
    # and removes the SF-tier conditional as a foot-gun.
    volume_size = 200
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

    # Export tunables that the wadjet binary reads via os.Getenv at startup.
    # /etc/environment is not read by inline cloud-init shells, so the env
    # vars must be export'd here BEFORE the worker process is launched.
    export WADJET_REVERSE_BLOOM_INNER_THRESHOLD="${var.reverse_bloom_inner_threshold}"
    export WADJET_JOIN_DEBUG="${var.join_debug}"
    export WADJET_DYNAMIC_FILTERS="${var.dynamic_filters}"
    export WADJET_SORT_MERGE_JOIN_BYTES="${var.sort_merge_join_bytes}"
    export WADJET_LATE_MATERIALIZATION="${var.late_materialization}"
    export WADJET_BUSHY_JOIN_REORDER="${var.bushy_join_reorder}"
    export WADJET_SKEW_SPLIT="${var.skew_split}"

    # Verify binary was downloaded successfully
    if [ ! -x /usr/local/bin/wadjet ]; then
      echo "FATAL: wadjet binary not found or not executable" >&2
      exit 1
    fi

    # Spill-to-disk location selection. NVMe instance store (c7gd, i4g)
    # is fastest; fall back to EBS-backed /var/spill for instances
    # without instance store (c7g.* etc.).
    #
    # 2026-05-03: previously fell back to /tmp, which on Amazon Linux 2023
    # is tmpfs (RAM-backed, default 50% of RAM = 16 GB on c7g.4xlarge).
    # Q18 SF10 lineitem shuffle silently hit ENOSPC against tmpfs while
    # the 200 GB EBS root volume sat unused; cluster wedged on retry
    # loops until 30m timeout. /var/spill on the EBS root volume is
    # slower than NVMe but unbounded by RAM and grows with the configured
    # volume_size (200 GB).
    # Select the instance store by device MODEL, never by name: NVMe
    # enumeration order is racy, and the old name-based exclusion of
    # nvme0n1 picked the EBS ROOT disk on a worker where the instance
    # store enumerated first (2026-06-12 SF100 run 105815: mkfs/mount
    # failed, spill fell through to the 200G root, Q21/Q22 ENOSPC).
    NVME_DEV=""
    for d in $(lsblk -dno NAME,TYPE | awk '$2=="disk"{print $1}'); do
      model=$(cat /sys/block/$d/device/model 2>/dev/null || true)
      case "$model" in
        *"Instance Storage"*) NVME_DEV="/dev/$d"; break ;;
      esac
    done
    if [ -n "$NVME_DEV" ]; then
      if lsblk -no MOUNTPOINTS "$NVME_DEV" 2>/dev/null | grep -q . ; then
        echo "FATAL: instance store $NVME_DEV already mounted/partitioned — refusing to format" >&2
        exit 1
      fi
      echo "Formatting NVMe instance store: $NVME_DEV"
      mkfs.xfs -f "$NVME_DEV"
      mkdir -p /mnt/nvme
      if ! mount "$NVME_DEV" /mnt/nvme; then
        echo "FATAL: failed to mount $NVME_DEV at /mnt/nvme — refusing to run with spill on the root volume" >&2
        exit 1
      fi
      mkdir -p /mnt/nvme/spill
      SPILL_DIR="/mnt/nvme/spill"
      echo "NVMe spill directory ready at $SPILL_DIR"
    else
      mkdir -p /var/spill
      SPILL_DIR="/var/spill"
      echo "EBS-backed spill directory ready at $SPILL_DIR (no NVMe instance store on this instance type)"
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

    # Launch ${var.workers_per_node} independent wadjet worker processes.
    # Each gets its own GOMEMLIMIT slice (envelope/N) and explicit pool +
    # cache budgets so the per-process memory math is correct regardless
    # of what the host cgroup reports. Auto-detect would read the host's
    # full memory because systemd-run's `--scope MemoryMax` doesn't
    # propagate to /sys/fs/cgroup paths Go's auto-detect inspects on
    # AL2023, so we hand each process its slice explicitly via flags.
    #
    # systemd-run still gives each process a hard MemoryMax cgroup
    # ceiling for crash isolation: when one process's runaway
    # allocations exceed its slice, the kernel kills only that process
    # and the supervising shell loop restarts it.
    WORKERS_PER_NODE=${var.workers_per_node}
    TOTAL_BYTES=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo)
    PER_PROC_BYTES=$((TOTAL_BYTES * 75 / 100 / WORKERS_PER_NODE))
    # GOMEMLIMIT is 90% of per-process slice (Go runtime soft limit; the
    # kernel cgroup ceiling is the hard backstop).
    PER_PROC_GOMEMLIMIT=$((PER_PROC_BYTES * 9 / 10))
    # Cache + pool: give the cache 10% of GOMEMLIMIT, pool gets the rest.
    PER_PROC_CACHE=$((PER_PROC_GOMEMLIMIT / 10))
    PER_PROC_POOL=$((PER_PROC_GOMEMLIMIT - PER_PROC_CACHE))
    # Per-task budget: pool / max_concurrent. Planner uses this for
    # operator sizing; actual memory accounting flows through the shared
    # pool. Computed here so it doesn't fall through to auto-detect
    # (which reads the ROOT cgroup memory.max — i.e. host RAM, not the
    # systemd-run scope's MemoryMax).
    PER_TASK_BUDGET=$((PER_PROC_POOL / ${var.max_concurrent}))
    echo "Starting $WORKERS_PER_NODE worker process(es): per-proc envelope=$((PER_PROC_BYTES/1024/1024/1024))GiB, GOMEMLIMIT=$((PER_PROC_GOMEMLIMIT/1024/1024/1024))GiB, pool=$((PER_PROC_POOL/1024/1024/1024))GiB, per-task=$((PER_TASK_BUDGET/1024/1024/1024))GiB"

    start_worker() {
      local idx=$1
      local worker_spill="$SPILL_DIR/w$idx"
      mkdir -p "$worker_spill"
      while true; do
        systemd-run --quiet --unit="wadjet-worker-$idx-$$-$(date +%s)" \
          --scope -p "MemoryMax=$PER_PROC_BYTES" -p "MemoryHigh=$PER_PROC_GOMEMLIMIT" \
          --setenv="GOMEMLIMIT=$PER_PROC_GOMEMLIMIT" \
          --setenv="WADJET_GOGC=100" \
          /usr/local/bin/wadjet serve \
            --mode=worker \
            --nats-url="nats://$COORD_IP:4222" \
            --endpoint="s3.${local.eff_region}.amazonaws.com" \
            --ssl \
            --bucket="${local.bucket_name}" \
            --region="${local.eff_region}" \
            --storage-type=s3 \
            --max-concurrent=${var.max_concurrent} \
            --spill-dir="$worker_spill" \
            --cache-bytes=$PER_PROC_CACHE \
            --memory-budget=$PER_TASK_BUDGET \
            --shared-pool-budget=$PER_PROC_POOL \
            %{if var.data_plane == "grpc"~}
            --data-plane=grpc \
            --coord-data-plane="$COORD_IP:${var.data_plane_port}" \
            %{endif~}
            %{if var.streaming_exchange~}
            --streaming-exchange \
            --peer-exchange-addr=":$((${var.peer_exchange_port} + idx))" \
            %{endif~}
            --mmap-relief=${var.mmap_relief} \
            --mmap-relief-threshold-mb=${var.mmap_relief_threshold_mb} \
            --bounded-dirty-writes=${var.bounded_dirty_writes} \
            --morsel-workers=${var.morsel_workers} \
            %{if var.spill_floating_budget~}
            --spill-floating-budget \
            %{endif~}
            2>&1 | sed "s/^/[w$idx] /"
        EXIT_CODE=$?
        echo "[w$idx] exited code=$EXIT_CODE, restarting in 5s..."
        sleep 5
      done
    }

    echo "WORKER_STARTED=1" >> /etc/environment
    for i in $(seq 0 $((WORKERS_PER_NODE - 1))); do
      start_worker $i &
    done
    wait
  EOF
  )

  tags = {
    Name    = "wadjet-bench-worker-${count.index}"
    Role    = "worker"
    SF      = "SF${local.eff_scale}"
    Project = "wadjet-bench"
  }
}
