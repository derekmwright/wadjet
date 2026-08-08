# Trino comparison cluster — same hardware shapes as the wadjet bench
# (deploy/benchmark/terraform), separate state. Instances carry the
# wadjet-bench-* Name prefix so the reaper lambda covers them.
#
# Layout: 1 coordinator (+ optional NVMe workers) running Trino over the
# Glue-backed Hive catalog registered against s3://<data_bucket>/ parquet.
# Assets (queries, DDL, runner) come from s3://<data_bucket>/bin/trino-assets/
# — stage with deploy/benchmark/trino/stage-trino-assets.sh before apply.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

provider "aws" {
  region  = var.region
  profile = "citc"
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-*-arm64"]
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

resource "aws_security_group" "trino" {
  name_prefix = "wadjet-bench-trino-"
  vpc_id      = data.aws_vpc.default.id
  # Trino uses HTTP :8080 for discovery, exchange, and the CLI — internal only.
  ingress {
    from_port = 8080
    to_port   = 8080
    protocol  = "tcp"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "wadjet-bench-trino" }
}

resource "aws_iam_role" "trino" {
  name_prefix = "wadjet-bench-trino-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "trino_s3_glue" {
  name_prefix = "s3-glue-"
  role        = aws_iam_role.trino.id
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
      },
      {
        # FTE arm: exchange spooling scratch (retry-policy=TASK with the
        # filesystem exchange manager). Cleaned up by the runner post-suite.
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = ["arn:aws:s3:::${var.data_bucket}/trino-exchange/*"]
      },
      {
        # Glue data-catalog access for the Hive connector metastore. Scoped
        # to catalog+database+table ARNs in this account/region.
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable",
          "glue:UpdateTable", "glue:GetPartition", "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "trino_ssm" {
  role       = aws_iam_role.trino.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "trino" {
  name_prefix = "wadjet-bench-trino-"
  role        = aws_iam_role.trino.name
}

locals {
  single_node = var.worker_count == 0
  # Heap sizing: leave ~25% headroom for the OS + native S3 buffers.
  # Trino validates query.max-memory-per-node + heap headroom (default 30%
  # of -Xmx) <= heap, so per-node query memory stays under ~60% of heap.
  coord_heap          = local.single_node ? "24G" : "12G"
  worker_heap         = "24G"
  coord_query_per_node = local.single_node ? "14GB" : "6GB"

  # Shared install snippet: Corretto 23 (Trino needs Java 23+ on recent
  # releases; corretto.aws "latest" URL tracks patch releases) + Trino
  # tarball + CLI. Runs on every node.
  install = <<-SCRIPT
    set -euxo pipefail
    dnf install -y python3 tar gzip
    cd /opt
    curl -fsSL -o corretto.tar.gz "https://corretto.aws/downloads/latest/amazon-corretto-23-aarch64-linux-jdk.tar.gz"
    mkdir -p /opt/java && tar -xzf corretto.tar.gz -C /opt/java --strip-components=1
    export JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH
    curl -fsSL -o trino.tar.gz "https://repo1.maven.org/maven2/io/trino/trino-server/${var.trino_version}/trino-server-${var.trino_version}.tar.gz"
    tar -xzf trino.tar.gz && mv trino-server-${var.trino_version} /opt/trino
    curl -fsSL -o /usr/local/bin/trino-cli "https://repo1.maven.org/maven2/io/trino/trino-cli/${var.trino_version}/trino-cli-${var.trino_version}-executable.jar"
    chmod +x /usr/local/bin/trino-cli
    ln -sf /opt/java/bin/java /usr/local/bin/java
    mkdir -p /opt/trino/etc/catalog /var/trino/data
  SCRIPT

  # NVMe instance-store mount for spill/exchange scratch on c7gd shapes.
  nvme_mount = <<-SCRIPT
    NVME=$(lsblk -dno NAME,MODEL | awk '/Instance Storage/{print "/dev/"$1; exit}')
    if [ -n "$NVME" ]; then
      mkfs.xfs -f "$NVME" && mkdir -p /mnt/nvme && mount "$NVME" /mnt/nvme
      mkdir -p /mnt/nvme/trino-spill && chmod 777 /mnt/nvme/trino-spill
    fi
  SCRIPT

  hive_catalog = <<-PROPS
    connector.name=hive
    hive.metastore=glue
    hive.metastore.glue.region=${var.region}
    hive.non-managed-table-writes-enabled=false
    fs.native-s3.enabled=true
    s3.region=${var.region}
  PROPS
}

resource "aws_instance" "coordinator" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.coordinator_instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.trino.id]
  iam_instance_profile   = aws_iam_instance_profile.trino.name

  root_block_device {
    volume_size = 60
    volume_type = "gp3"
  }

  tags = { Name = "wadjet-bench-trino-coordinator" }

  user_data = <<-EOF
    #!/bin/bash
    exec > /var/log/trino-bootstrap.log 2>&1
    ${local.install}
    ${local.nvme_mount}

    cat > /opt/trino/etc/node.properties <<'NP'
    node.environment=bench
    node.data-dir=/var/trino/data
    NP
    cat > /opt/trino/etc/jvm.config <<JVM
    -server
    -Xmx${local.coord_heap}
    -XX:+UseG1GC
    -XX:InitialRAMPercentage=80
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:-OmitStackTraceInFastThrow
    JVM
    cat > /opt/trino/etc/config.properties <<CP
    coordinator=true
    node-scheduler.include-coordinator=${local.single_node}
    http-server.http.port=8080
    discovery.uri=http://localhost:8080
    query.max-memory=${local.single_node ? "14GB" : "42GB"}
    query.max-memory-per-node=${local.coord_query_per_node}
    CP
    cat > /opt/trino/etc/catalog/hive.properties <<'HP'
    ${local.hive_catalog}
    HP

    JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH /opt/trino/bin/launcher start

    # Pull comparison assets and run.
    mkdir -p /opt/trino-assets
    aws s3 cp "s3://${var.data_bucket}/bin/trino-assets/" /opt/trino-assets/ --recursive --region ${var.region}
    chmod +x /opt/trino-assets/run-trino-comparison.sh
    export TRINO_BUCKET="${var.data_bucket}" TRINO_REGION="${var.region}"
    export TRINO_RUNS="${var.runs}" TRINO_QUERIES="${var.queries}"
    export TRINO_WANT_NODES="${local.single_node ? 1 : var.worker_count + 1}"
    %{if var.auto_run}
    /opt/trino-assets/run-trino-comparison.sh
    %{endif}
  EOF
}

resource "aws_instance" "worker" {
  count                  = var.worker_count
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.trino.id]
  iam_instance_profile   = aws_iam_instance_profile.trino.name

  root_block_device {
    volume_size = 60
    volume_type = "gp3"
  }

  tags = { Name = "wadjet-bench-trino-worker-${count.index}" }

  user_data = <<-EOF
    #!/bin/bash
    exec > /var/log/trino-bootstrap.log 2>&1
    ${local.install}
    ${local.nvme_mount}

    cat > /opt/trino/etc/node.properties <<'NP'
    node.environment=bench
    node.data-dir=/var/trino/data
    NP
    cat > /opt/trino/etc/jvm.config <<JVM
    -server
    -Xmx${local.worker_heap}
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:-OmitStackTraceInFastThrow
    JVM
    cat > /opt/trino/etc/config.properties <<CP
    coordinator=false
    http-server.http.port=8080
    discovery.uri=http://${aws_instance.coordinator.private_ip}:8080
    query.max-memory=42GB
    query.max-memory-per-node=14GB
    CP
    cat > /opt/trino/etc/catalog/hive.properties <<'HP'
    ${local.hive_catalog}
    HP

    JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH /opt/trino/bin/launcher start
  EOF
}
