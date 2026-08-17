# ClickBench official-spec single node — c6a.4xlarge (x86), 500GB gp2,
# separate state from the TPC-H bench module. The instance carries the
# wadjet-bench-* Name prefix so the reaper lambda covers it.
#
# Flow: boot → download clickbench-bench (amd64) from s3://<bucket>/bin/ →
# download hits parts to /data/hits → run the official methodology
# (cold + warm tries per query, page-cache drop) → upload results JSON +
# log to s3://<bucket>/results/clickbench/<ts>/ → shut down (terminate).
#
# Stage binaries first: deploy/benchmark/stage-binaries.sh <bucket> us-east-2

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

provider "aws" {
  region  = var.region
  profile = "citc"
}

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

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "clickbench" {
  name_prefix = "wadjet-bench-clickbench-"
  vpc_id      = data.aws_vpc.default.id
  # No ingress: access is SSM-only.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "wadjet-bench-clickbench" }
}

resource "aws_iam_role" "clickbench" {
  name_prefix = "wadjet-bench-cb-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "clickbench_s3" {
  name_prefix = "s3-"
  role        = aws_iam_role.clickbench.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.data_bucket}", "arn:aws:s3:::${var.data_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.data_bucket}/results/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "clickbench_ssm" {
  role       = aws_iam_role.clickbench.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "clickbench" {
  name_prefix = "wadjet-bench-cb-"
  role        = aws_iam_role.clickbench.name
}

locals {
  user_data = <<-EOF
    #!/bin/bash
    set -x
    exec > /root/bootstrap.log 2>&1

    retry() { for i in 1 2 3 4 5; do "$@" && return 0; sleep 5; done; return 1; }

    retry aws s3 cp "s3://${var.data_bucket}/bin/${var.bin_version}/amd64/clickbench-bench" /usr/local/bin/clickbench-bench --region ${var.region}
    chmod +x /usr/local/bin/clickbench-bench

    mkdir -p /data/hits
    TS=$(date +%Y%m%d-%H%M%S)

    # Disk + memory telemetry alongside the run: settles disk-bound vs
    # CPU-bound for warm-try anomalies (Q05/Q33 warming is knife-edge —
    # their multi-GB aggregation state competes with the page cache).
    yum install -y sysstat >/dev/null 2>&1 || true
    nohup iostat -x -t 5 > /root/iostat.log 2>&1 &
    nohup bash -c 'while true; do echo "== $(date -u +%H:%M:%S)"; grep -E "MemFree|Cached|Dirty" /proc/meminfo; sleep 5; done' > /root/meminfo.log 2>&1 &

    ${var.bench_env} /usr/local/bin/clickbench-bench \
      --mem-budget "${var.mem_budget_bytes}" \
      --s3-bucket "${var.data_bucket}" \
      --s3-region "${var.region}" \
      --s3-prefix "tables/hits/" \
      --data-dir /data/hits \
      --tries ${var.tries} \
      --machine "${var.machine}" \
      --comment "${var.comment}" \
      --profile-queries "${var.profile_queries}" \
      --profile-dir /root/profiles \
      --out /root/results.json 2>&1 | tee /root/clickbench.log

    aws s3 cp /root/results.json "s3://${var.data_bucket}/results/clickbench/$TS/results.json" --region ${var.region} || true
    aws s3 cp /root/clickbench.log "s3://${var.data_bucket}/results/clickbench/$TS/clickbench.log" --region ${var.region} || true
    aws s3 cp /root/bootstrap.log "s3://${var.data_bucket}/results/clickbench/$TS/bootstrap.log" --region ${var.region} || true
    aws s3 cp /root/iostat.log "s3://${var.data_bucket}/results/clickbench/$TS/iostat.log" --region ${var.region} || true
    aws s3 cp /root/meminfo.log "s3://${var.data_bucket}/results/clickbench/$TS/meminfo.log" --region ${var.region} || true
    aws s3 cp /root/profiles "s3://${var.data_bucket}/results/clickbench/$TS/profiles/" --recursive --region ${var.region} || true

    %{if var.auto_shutdown}
    shutdown -h now
    %{endif}
  EOF
}

resource "aws_instance" "clickbench" {
  ami                                  = data.aws_ami.al2023_x86.id
  instance_type                        = var.instance_type
  instance_initiated_shutdown_behavior = "terminate"
  vpc_security_group_ids               = [aws_security_group.clickbench.id]
  iam_instance_profile                 = aws_iam_instance_profile.clickbench.name
  subnet_id                            = data.aws_subnets.default.ids[0]

  # Official listing spec: 500GB gp2.
  root_block_device {
    volume_size = 500
    volume_type = "gp2"
  }

  user_data_base64            = base64gzip(local.user_data)
  user_data_replace_on_change = true

  tags = {
    Name    = "wadjet-bench-clickbench"
    Role    = "clickbench"
    Project = "wadjet-bench"
  }
}
