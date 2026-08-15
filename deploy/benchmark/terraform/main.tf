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
  eff_base_cache   = var.base_table_cache_bytes != null ? var.base_table_cache_bytes : (local.has_profile ? try(local._raw_p.benchmark.base_table_cache_bytes, 0) : 0)
  eff_decoded_cache = var.decoded_cache_bytes != null ? var.decoded_cache_bytes : (local.has_profile ? try(local._raw_p.benchmark.decoded_cache_bytes, 0) : 0)
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
    # Profile-first knobs added 2026-08-14 after the profile/tfvars drift incident:
  # three arms deployed with -var=profile= silently ran NATS data plane,
  # unpaced uploads, s2 wire, and no locality placement (the tfvars-only
  # values), producing the afternoon stall storms. Explicit -var still wins.
  eff_data_plane     = var.data_plane != null ? var.data_plane : (local.has_profile ? try(local._raw_p.benchmark.data_plane, "") : "")
  eff_locality       = var.locality_placement != null ? var.locality_placement : (local.has_profile ? try(local._raw_p.benchmark.locality_placement, false) : false)
  eff_upload_pace    = var.upload_pace_mbps != null ? var.upload_pace_mbps : (local.has_profile ? try(local._raw_p.benchmark.upload_pace_mbps, 0) : 0)
  eff_exchange_zstd  = var.exchange_zstd != null ? var.exchange_zstd : (local.has_profile ? try(local._raw_p.benchmark.exchange_zstd, 0) : 0)
  eff_catalog_prefix = var.catalog_snapshot_prefix != null ? var.catalog_snapshot_prefix : (local.has_profile ? try(local._raw_p.benchmark.catalog_snapshot_prefix, "") : "")
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

    # Wait for IAM instance-profile creds to reach IMDS: a fresh apply
    # creates the profile seconds before launch and the coordinator can
    # boot ahead of propagation (2026-08-11 ctl arm bootstrap death).
    for i in $(seq 1 60); do
      T=$(curl -s -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 60" http://169.254.169.254/latest/api/token)
      [ -n "$(curl -s -H "X-aws-ec2-metadata-token: $T" http://169.254.169.254/latest/meta-data/iam/security-credentials/)" ] && break
      sleep 5
    done

    # journald hardening (log-jam mechanism, 2026-08-14 — see
    # internal/logio): bench boxes are ephemeral, so the journal gets no
    # disk durability. Storage=volatile keeps it in /run (RAM) — no EBS
    # writeback in the log path — and the raised rate limit stops
    # journald from stalling/suppressing under benchmark log volume.
    # journalctl (wlog collection) reads the runtime journal fine.
    mkdir -p /etc/systemd/journald.conf.d
    printf '[Journal]\nStorage=volatile\nRuntimeMaxUse=2G\nRateLimitIntervalSec=30s\nRateLimitBurst=100000\n' \
      > /etc/systemd/journald.conf.d/99-bench.conf
    systemctl restart systemd-journald
    echo "journald: volatile storage + raised rate limits"

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
    echo "WADJET_AGG_OVER_EXCHANGE=${var.agg_over_exchange}" >> /etc/environment
    echo "WADJET_STAGE_FUSION=${var.stage_fusion}" >> /etc/environment
    echo "WADJET_STAGE_FUSION_AGG=${var.stage_fusion_agg}" >> /etc/environment
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
    echo "WADJET_AGG_OVER_EXCHANGE=${var.agg_over_exchange}" >> /etc/environment
    echo "WADJET_STAGE_FUSION=${var.stage_fusion}" >> /etc/environment
    echo "WADJET_STAGE_FUSION_AGG=${var.stage_fusion_agg}" >> /etc/environment
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
    export WADJET_AGG_OVER_EXCHANGE="${var.agg_over_exchange}"
    export WADJET_STAGE_FUSION="${var.stage_fusion}"
    export WADJET_STAGE_FUSION_AGG="${var.stage_fusion_agg}"
    export WADJET_SKEW_SUITE="${var.skew_suite}"
    export WADJET_BLOCK_PROFILE_RATE="${var.block_profile_rate}"
    export WADJET_MUTEX_PROFILE_FRACTION="${var.mutex_profile_fraction}"
    export USE_NATIVE_DAG="${var.use_native_dag ? "1" : "0"}"
    # Phase C/D/E data-plane selection. Empty/"nats" = legacy NATS reply
    # subjects; "grpc" routes task dispatch + results + gather +
    # TaskProgress over a per-worker bidi gRPC stream on
    # ${var.data_plane_port}.
    export TPCH_DATA_PLANE="${local.eff_data_plane}"
    export TPCH_DATA_PLANE_ADDR=":${var.data_plane_port}"
    # Streaming exchange (peer shuffle reads + async upload). Coordinator
    # side of the flag; the worker cloud-init adds --streaming-exchange
    # from the same terraform var.
    export TPCH_STREAMING_EXCHANGE="${var.streaming_exchange ? "1" : "0"}"
    # Eager consumer dispatch (Phase C1). Coordinator-side flag only;
    # workers act on EagerInputs in the task spec. Default-off engine
    # flag, so the env parse is explicit opt-in ("1"), not a kill switch.
    export WADJET_EAGER_DISPATCH="${var.eager_dispatch ? "1" : "0"}"
    # Projected-tail clearance floor in seconds (eager-consumer-dispatch.md
    # §11/§14). The in-binary default (12) was calibrated on the 25m-era
    # suite; current-world spreads sit mostly below it, so treatment arms
    # override here. Ignored when eager_dispatch is off.
    export WADJET_EAGER_MIN_TAIL_SECONDS="${var.eager_min_tail_seconds}"
    # Shuffle durability policy (docs/design/shuffle-durability.md).
    # Coordinator-side only — workers act on Task.UploadPolicy stamped at
    # dispatch. eager (default) = pre-knob behavior.
    export WADJET_SHUFFLE_DURABILITY="${var.shuffle_durability}"
    # Input-locality placement (docs/design/locality-placement.md).
    # Coordinator-side only; explicit opt-in pending SF100 validation.
    export WADJET_LOCALITY_PLACEMENT="${local.eff_locality ? "1" : "0"}"
    # Catalog snapshot/restore prefix (PR #115). Non-empty = restore the
    # post-discovery catalog from s3://<bucket>/<prefix> instead of paying
    # ~15 min of discovery+ANALYZE per deploy; first boot writes it.
    export TPCH_CATALOG_SNAPSHOT_PREFIX="${local.eff_catalog_prefix}"
    # Force GOGC=100 so heap is bounded by 2x live data instead of growing to
    # GOMEMLIMIT before triggering mark-assist. Without this, scan-3 SF10
    # tasks accumulated parquet-decode garbage to ~10 GB peak heap before
    # GC fired; the resulting mark-assist starved the heartbeat goroutine
    # for >90s and the coord reaped workers, kicking off the Q03 redelivery
    # loop observed on the 2026-05-01 streaming-refactor deploy.
    export WADJET_GOGC=${var.gogc}
    export WADJET_TASK_GC=${var.task_gc}
    # Scan-output column pruning (pruneScanOutputColumns, 03b1a10):
    # coordinator-side planner pass. Default on; =false is the A/B /
    # kill-switch arm.
    export WADJET_SCAN_OUTPUT_PRUNE="${var.scan_output_prune ? "1" : "0"}"
    # Byte-balanced affinity fan-outs (scan-affinity-byte-balance.md):
    # coordinator-side. Default on; "0" is the A/B / kill-switch arm.
    export WADJET_AFFINITY_BYTE_BALANCE="${var.affinity_byte_balance}"
    # Scan→shuffle fusion (fuseScanShuffle, 765ce81): coordinator-side
    # planner pass. Default on; =false is the A/B / kill-switch arm.
    export WADJET_FUSE_SCAN_SHUFFLE="${var.fuse_scan_shuffle ? "1" : "0"}"
    export WADJET_FUSE_JOIN_SHUFFLE="${var.fuse_join_shuffle ? "1" : "0"}"
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

  # gzip: cloud-init auto-decompresses; the raw bootstrap outgrew the
  # 16KB user-data cap (2026-08-11, stall-watchdog + ena-poll growth).
  user_data_base64 = base64gzip(local.standalone_user_data)
  # Without this, a user-data change on an instance surviving in state
  # (e.g. a self-stopped coordinator) is applied in place and cloud-init
  # never re-runs per-instance scripts on restart — the box boots with a
  # stale bootstrap and the benchmark silently never starts (2026-08-11).
  user_data_replace_on_change = true

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

  user_data_base64 = base64gzip(local.coordinator_user_data)
  # See standalone resource: stale-bootstrap restart guard (2026-08-11).
  user_data_replace_on_change = true

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

  user_data_base64 = base64gzip(<<-EOF
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
    export WADJET_AGG_OVER_EXCHANGE="${var.agg_over_exchange}"
    export WADJET_STAGE_FUSION="${var.stage_fusion}"
    export WADJET_STAGE_FUSION_AGG="${var.stage_fusion_agg}"
    export WADJET_BLOCK_PROFILE_RATE="${var.block_profile_rate}"
    export WADJET_MUTEX_PROFILE_FRACTION="${var.mutex_profile_fraction}"
    # Outbound burst smoothing (docs/design/upload-burst-smoothing.md):
    # aggregate background stage-output PUT budget in MB/s. 0 = off.
    export WADJET_UPLOAD_PACE_MBPS="${local.eff_upload_pace}"
    # WSHZ upload envelope (docs/design/exchange-zstd-wire.md): zstd
    # instead of s2 for S3 stage/shuffle uploads. 1 = on, 0 = off
    # (matches the in-binary default: off).
    export WADJET_EXCHANGE_ZSTD="${local.eff_exchange_zstd}"

    # Verify binary was downloaded successfully
    if [ ! -x /usr/local/bin/wadjet ]; then
      echo "FATAL: wadjet binary not found or not executable" >&2
      exit 1
    fi

    # NIC/ENA sampler: journal cumulative rx/tx bytes and ENA
    # allowance-exceeded counters every 10s. Wall deltas on
    # burst-networked bench types (c7gd "up to 15 Gbps") are
    # uninterpretable without throttle context — the entire 2026-08-09
    # steady-residual arc chased in-host causes before ENA counters
    # settled it (docs/benchmarks/network-bound-diagnosis-2026-08-09.md).
    # 10s cadence (was 30s) for the barrier-overlap arc: the 2026-08-12
    # baseline showed bw_out bursts at stage barriers, and attributing
    # them to stage classes needs sub-barrier resolution
    # (deploy/benchmark/attribute-ena.py joins these samples against
    # the coordinator's stage-DAG timeline). This is the ONLY ena-poll
    # sampler — a 60s duplicate briefly coexisted (995409d) and doubled
    # every sample; keep it single.
    # grab-worker-logs.sh ships journald wholesale, so samples ride
    # along in wlogs with no separate collection step. Filter with:
    #   zcat wlog-*.gz | grep ena-poll
    NET_IF=$(ls /sys/class/net | grep -E '^(eth|ens|enp)' | head -1)
    if [ -n "$NET_IF" ]; then
      ( while true; do
          logger -t ena-poll "if=$NET_IF rx=$(cat /sys/class/net/$NET_IF/statistics/rx_bytes) tx=$(cat /sys/class/net/$NET_IF/statistics/tx_bytes) $(ethtool -S $NET_IF 2>/dev/null | grep allowance_exceeded | tr -s ' ' | tr '\n' ' ')"
          sleep 10
        done ) &
      echo "ena-poll sampler started on $NET_IF (10s cadence -> journald)"
    fi

    # Frozen-spin watchdog: the 2026-08-10 q22-R2 stall was a worker
    # process whose Go scheduler froze for 3m43s while one goroutine
    # pegged a core (STW-stuck signature; docs/benchmarks/
    # rebaseline-2026-08-10.md). Detection: the metrics port stops
    # answering while process CPU keeps accruing (4s unresponsive AND
    # >=1.5s CPU; a healthy worker always answers pprof). Response,
    # evidence first (the 2026-08-11 firing proved SIGQUIT captures
    # nothing — wadjet handles it as graceful drain): (1) best-effort
    # pprof goroutine dump into journald, works when only partially
    # stuck; (2) SIGABRT — uncaught, so the Go runtime's fatal-signal
    # handler dumps ALL goroutine stacks (GOTRACEBACK=all, set on the
    # worker unit) to stderr -> journald -> wlogs even while the
    # scheduler is STW-wedged, then exits; the restart loop +
    # JetStream redelivery keep the suite going.
    # Per-thread CPU deltas + kernel stacks: specimen 8 (2026-08-13)
    # proved the burn can live in a runtime-level thread executing NO
    # goroutine (SIGABRT dump: 100 goroutines, none running, host idle,
    # process CPU accruing) — goroutine dumps alone cannot name that
    # thread; per-TID utime deltas over 1s plus /proc kernel stacks can.
    # Shared by both watchdog signatures below; the wlog uploader ships
    # /var/log/stall-*.threads automatically.
    thread_capture() {
      local pid=$1 out=$2
      { for t in /proc/$pid/task/*/; do
          tid=$(basename "$t")
          echo "TID $tid stat1: $(awk '{print $14, $15, $3}' "$t/stat" 2>/dev/null)"
        done
        sleep 1
        for t in /proc/$pid/task/*/; do
          tid=$(basename "$t")
          echo "TID $tid stat2: $(awk '{print $14, $15, $3}' "$t/stat" 2>/dev/null)"
          echo "TID $tid kstack: $(tr '\n' '|' < "$t/stack" 2>/dev/null)"
        done
        # Log-pipeline state (log-jam theory, 2026-08-14): if journald or
        # the sed relay is wedged, the worker's stderr pipe backs up and
        # (pre-async-writer binaries) freezes every logging goroutine.
        for lp in $(pgrep -x systemd-journald; pgrep -x sed); do
          echo "LOGPIPE $lp $(cat /proc/$lp/comm 2>/dev/null) stat: $(awk '{print $14, $15, $3}' /proc/$lp/stat 2>/dev/null) kstack: $(tr '\n' '|' < /proc/$lp/stack 2>/dev/null)"
        done; } > "$out" 2>/dev/null
    }

    ( declare -A lastok lastut
      while true; do
        sleep 1
        for pid in $(pgrep -f "wadjet serve --mode=worker"); do
          ut=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null) || continue
          now=$(date +%s%N)
          if curl -s --max-time 1 "http://127.0.0.1:9100/debug/pprof/cmdline" -o /dev/null; then
            lastok[$pid]=$now; lastut[$pid]=$ut
            continue
          fi
          prevok=$${lastok[$pid]:-$now}; prevut=$${lastut[$pid]:-$ut}
          unresp_ms=$(( (now - prevok) / 1000000 ))
          dut=$(( ut - prevut ))
          if [ "$unresp_ms" -gt 4000 ] && [ "$dut" -gt 150 ]; then
            logger -t stall-watchdog "FROZEN-SPIN pid=$pid unresp_ms=$unresp_ms cpu_jiffies=$dut capturing stacks"
            D=/var/log/stall-$pid-$(date +%s).stacks
            T=/var/log/stall-$pid-$(date +%s).threads
            thread_capture "$pid" "$T"
            logger -t stall-watchdog "thread capture ($(wc -c <"$T") bytes)"
            # NOTE: the full dump is NOT logger-ed into journald anymore
            # (2026-08-14): shipping 350KB dumps through syslog feeds the
            # journald stall this watchdog hunts — the log-jam mechanism
            # (see internal/logio). Files ship via the wlog uploader glob.
            if curl -s --max-time 2 "http://127.0.0.1:9100/debug/pprof/goroutine?debug=2" -o "$D" && [ -s "$D" ]; then
              logger -t stall-watchdog "pprof stacks captured ($(wc -c <"$D") bytes)"
            else
              logger -t stall-watchdog "pprof grab failed (fully wedged) - relying on SIGABRT dump"
            fi
            # Recovery grace (2026-08-13 creep run, 6 firings): short
            # episodes (~5s freeze) are often OVER by the time the ~3s
            # capture finishes — the SIGABRT then kills a recovered
            # worker for nothing. Evidence is already on disk at this
            # point; only kill if the port is still dead.
            if curl -s --max-time 2 "http://127.0.0.1:9100/debug/pprof/cmdline" -o /dev/null; then
              logger -t stall-watchdog "port recovered post-capture - skipping SIGABRT (evidence kept)"
            else
              logger -t stall-watchdog "sending SIGABRT"
              kill -ABRT "$pid"
            fi
            unset "lastok[$pid]" "lastut[$pid]"
            sleep 5
          elif [ "$unresp_ms" -gt 75000 ]; then
            # Third signature (2026-08-14): port dead >75s with CPU
            # QUIET falls between frozen-spin (needs CPU accrual) and
            # silent-stall (needs a responsive port). Evidence-only.
            logger -t quiet-freeze "QUIET-FREEZE pid=$pid unresp_ms=$unresp_ms cpu_jiffies=$dut capturing threads (no signal sent)"
            T=/var/log/stall-$pid-$(date +%s).threads
            thread_capture "$pid" "$T"
            logger -t quiet-freeze "thread capture ($(wc -c <"$T") bytes)"
            lastok[$pid]=$now; lastut[$pid]=$ut
          fi
        done
      done ) &
    echo "stall-watchdog started (pprof grab + SIGABRT on frozen-spin signature)"

    # Silent-stall watchdog: the CPU-QUIET cousin of the frozen spin
    # (three q22-R2 control-arm specimens, 2026-08-13 week): app log goes
    # FULLY silent for ~3m30-3m45 while heartbeats survive, the metrics
    # port stays responsive, and the NIC sits at keepalives — then the
    # episode self-heals and all repartition outputs leave in one burst.
    # The frozen-spin watchdog above is blind to it (its trigger is
    # port-unresponsive AND CPU accruing; this shape is neither).
    # Detection: the worker emits an unconditional "liveness marker"
    # line every 30s (worker.go startLivenessMarkerLoop), so zero
    # journal lines from a live worker over 75s (>2 missed intervals)
    # while the port answers = the signature. Response is EVIDENCE-ONLY:
    # .threads capture + pprof goroutine dump (the port is responsive,
    # so the grab works). NO SIGABRT — the episode self-heals and the
    # process is healthy enough to dump; killing it would poison the arm
    # for nothing. 180s per-pid cooldown ≈ 1-2 captures per episode.
    ( declare -A lastfire
      while true; do
        sleep 15
        for pid in $(pgrep -f "wadjet serve --mode=worker"); do
          curl -s --max-time 1 "http://127.0.0.1:9100/debug/pprof/cmdline" -o /dev/null || continue
          idx=$(tr '\0' '\n' < /proc/$pid/cmdline 2>/dev/null | sed -n 's|.*--spill-dir=.*/w\([0-9]\+\)$|\1|p')
          [ -n "$idx" ] || continue
          nowep=$(date +%s)
          prev=$${lastfire[$pid]:-0}
          [ $(( nowep - prev )) -lt 180 ] && continue
          lines=$(journalctl --since=-75s -o cat --no-pager 2>/dev/null | grep -c "^\[w$idx\] ")
          if [ "$lines" -eq 0 ]; then
            active=$(curl -s --max-time 1 "http://127.0.0.1:9100/metrics" | awk '/^wadjet_worker_active_tasks/ {print $2}')
            logger -t silent-stall "SILENT-STALL pid=$pid idx=$idx active_tasks=$${active:-?} no app-log lines in 75s, port responsive - capturing (no signal sent)"
            T=/var/log/stall-$pid-$nowep.threads
            thread_capture "$pid" "$T"
            logger -t silent-stall "thread capture ($(wc -c <"$T") bytes)"
            D=/var/log/stall-$pid-$nowep.stacks
            if curl -s --max-time 2 "http://127.0.0.1:9100/debug/pprof/goroutine?debug=2" -o "$D" && [ -s "$D" ]; then
              logger -t silent-stall "pprof stacks captured ($(wc -c <"$D") bytes)"
            else
              logger -t silent-stall "pprof grab failed despite responsive port"
            fi
            lastfire[$pid]=$nowep
          fi
        done
      done ) &
    echo "silent-stall watchdog started (evidence-only capture on app-log silence)"

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

    # sed -u on the log pipe: without it, stdio block-buffers quiet-phase
    # worker output for minutes, and journald arrival time (what the
    # silent-stall watchdog measures) lags emission — a fake silence.
    start_worker() {
      local idx=$1
      local worker_spill="$SPILL_DIR/w$idx"
      mkdir -p "$worker_spill"
      while true; do
        systemd-run --quiet --unit="wadjet-worker-$idx-$$-$(date +%s)" \
          # MemoryHigh deliberately NOT set (2026-08-15): it sat at
          # PER_PROC_GOMEMLIMIT, but the cgroup charges heap AND mmap'd
          # file pages — so a GOMEMLIMIT-governed heap plus any cache
          # pages guarantees memory.current > memory.high, and the
          # kernel direct-reclaim-throttles the worker for behaving as
          # configured (gogc=off probe: memory.events high=670k, PSI
          # full avg10=10%, Q02 4x; gogc=100 crosses the same line at
          # heap peaks = the memcg frames in the frozen-spin
          # specimens). MemoryMax alone is the guard: memcg reclaims
          # clean file pages before OOM, which is the graceful path.
          --scope -p "MemoryMax=$PER_PROC_BYTES" \
          --setenv="GOMEMLIMIT=$PER_PROC_GOMEMLIMIT" \
          --setenv="GOTRACEBACK=all" \
          --setenv="GODEBUG=gctrace=1" \
          --setenv="WADJET_GOGC=${var.gogc}" \
          --setenv="WADJET_TASK_GC=${var.task_gc}" \
          --setenv="WADJET_REFAULT_PRESSURE_RATE=${var.refault_pressure_rate}" \
          --setenv="WADJET_REFAULT_EPISODE_CAP=${var.refault_episode_cap_seconds}" \
          --setenv="WADJET_ROWGROUP_TOUCH=${var.rowgroup_touch}" \
          --setenv="WADJET_TOUCH_POPULATE=${var.touch_populate}" \
          --setenv="WADJET_SCAN_PREAD=${var.scan_pread}" \
          --setenv="WADJET_SCAN_PREAD_HOT=${var.scan_pread_hot}" \
          --setenv="WADJET_SHUFFLE_PREAD=${var.shuffle_pread}" \
          --setenv="WADJET_SINK_DIRECT_CHUNK=${var.sink_direct_chunk}" \
          --setenv="WADJET_DF_LATE_GROUP_ATTACH=${var.df_late_group_attach}" \
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
            %{if local.eff_data_plane == "grpc"~}
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
            --base-table-cache-bytes=${local.eff_base_cache} \
            --decoded-cache-bytes=${local.eff_decoded_cache} \
            --streaming-shuffle-read=${var.streaming_shuffle_read} \
            --scan-decode-ahead=${var.scan_decode_ahead} \
            --async-scratch-purge=${var.async_scratch_purge} \
            --peer-wire-compression=${var.peer_wire_compression} \
            %{if var.spill_floating_budget~}
            --spill-floating-budget \
            %{endif~}
            2>&1 | sed -u "s/^/[w$idx] /"
        EXIT_CODE=$?
        echo "[w$idx] exited code=$EXIT_CODE, restarting in 5s..."
        sleep 5
      done
    }

    echo "WORKER_STARTED=1" >> /etc/environment
    for i in $(seq 0 $((WORKERS_PER_NODE - 1))); do
      start_worker $i &
    done

    # Auto wlog upload (benchmark-turnaround backlog): run-benchmark.sh
    # publishes s3://<bucket>/wlog-request containing the results prefix
    # after its own uploads; each worker polls for a CHANGE from the
    # boot-time value (a stale marker from a prior deploy is ignored) and
    # self-uploads its journal under <prefix>/wlogs/. Zero marginal
    # operator time per arm; grab-worker-logs.sh stays as the manual
    # fallback for crashed/aborted runs that never publish the marker.
    (
      TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
      IID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
      LAST=$(aws s3 cp "s3://${local.bucket_name}/wlog-request" - --region ${local.eff_region} 2>/dev/null || true)
      while true; do
        sleep 30
        M=$(aws s3 cp "s3://${local.bucket_name}/wlog-request" - --region ${local.eff_region} 2>/dev/null || true)
        if [ -n "$M" ] && [ "$M" != "$LAST" ]; then
          LAST="$M"
          journalctl --no-pager | gzip -c > "/root/wlog-$IID.gz"
          aws s3 cp "/root/wlog-$IID.gz" "s3://${local.bucket_name}/$M/wlogs/wlog-$IID.gz" --region ${local.eff_region} || true
          # Full stall-watchdog stack dumps: the syslog copy (logger -t
          # stall-stacks) gets rate-limited to a fraction of the dump
          # (2026-08-13: 214 of ~1500 lines survived). Ship the files.
          for D in /var/log/stall-*.stacks /var/log/stall-*.threads; do
            [ -e "$D" ] || continue
            gzip -c "$D" | aws s3 cp - "s3://${local.bucket_name}/$M/wlogs/$(basename "$D")-$IID.gz" --region ${local.eff_region} || true
          done
          # Heap-pressure .pb.gz profiles (worker heap-pressure profiler
          # writes to NVMe spill dirs): the 2026-08-14 frozen-spin arc
          # lost every profile to teardown — they carry the pointer-
          # density evidence that decides the GC mark-cost fix.
          for P in /mnt/nvme/spill/*/heap-profiles/*.pb.gz; do
            [ -e "$P" ] || continue
            aws s3 cp "$P" "s3://${local.bucket_name}/$M/wlogs/heap-profiles/$(basename "$P")-$IID.pb.gz" --region ${local.eff_region} || true
          done
          echo "wlog auto-uploaded to $M/wlogs/"
        fi
      done
    ) &
    wait
  EOF
  )
  # See standalone resource: stale-bootstrap restart guard (2026-08-11).
  user_data_replace_on_change = true

  tags = {
    Name    = "wadjet-bench-worker-${count.index}"
    Role    = "worker"
    SF      = "SF${local.eff_scale}"
    Project = "wadjet-bench"
  }
}
