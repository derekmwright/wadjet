# Wadjet TPC-H Benchmark on EC2

Terraform + scripts for running TPC-H benchmarks on dedicated Graviton3 (ARM) EC2 instances with S3.

## Cost Estimates (us-east-1, on-demand, Graviton3)

| Config | Instances | Hourly | ~Session Cost |
|--------|-----------|--------|---------------|
| SF1 standalone | 1x c7g.2xlarge | $0.29 | ~$0.40 |
| SF10 standalone | 1x c7g.4xlarge | $0.58 | ~$1.20 |
| SF10 distributed | 1x c7g.xlarge + 3x c7g.2xlarge | $1.01 | ~$3 |
| SF100 distributed | 1x c7g.2xlarge + 4x c7g.8xlarge | $3.45 | ~$12 |

Graviton3 is ~20% cheaper than equivalent x86 (c6i) instances. Go compiles natively for arm64.

## Quick Start

```bash
cd deploy/benchmark/terraform

# SF1 standalone
terraform init
terraform apply -var-file=sf1.tfvars -var="key_name=my-key"

# SF10 distributed (3 workers)
terraform apply -var-file=sf10-distributed.tfvars -var="key_name=my-key"

# SF100 distributed (4 workers)
terraform apply -var-file=sf100-distributed.tfvars -var="key_name=my-key"
```

## Running Benchmarks

### Standalone

```bash
# SSH to the instance
ssh -i ~/.ssh/my-key.pem ec2-user@<standalone-ip>

# Wait for build to complete (~2 min)
source /etc/environment
tail -f /var/log/cloud-init-output.log

# Run benchmark
cd /root/wadjet
sudo -i
./deploy/benchmark/run-benchmark.sh standalone SF1
./deploy/benchmark/run-benchmark.sh standalone SF10
```

### Distributed

```bash
# SSH to coordinator
ssh -i ~/.ssh/my-key.pem ec2-user@<coordinator-ip>

# On each worker (use private IPs from terraform output):
ssh -i ~/.ssh/my-key.pem ec2-user@<worker-ip>
sudo -i
./deploy/benchmark/start-worker.sh <coordinator-private-ip>

# On coordinator, run benchmark:
sudo -i
COORDINATOR_PRIVATE_IP=<coordinator-private-ip> \
  ./deploy/benchmark/run-benchmark.sh distributed SF10 3
```

## Teardown

```bash
terraform destroy -var-file=sf10-distributed.tfvars -var="key_name=my-key"
```

## Customizing

Override instance types for specific workloads:

```bash
# Bigger standalone for SF10
terraform apply -var-file=sf10-standalone.tfvars \
  -var="key_name=my-key" \
  -var="worker_instance_type=c6i.4xlarge"

# More workers for distributed
terraform apply -var-file=sf10-distributed.tfvars \
  -var="key_name=my-key" \
  -var="worker_count=5"

# Spot instances (~60-70% cheaper, risk of reclamation)
terraform apply -var-file=sf1.tfvars \
  -var="key_name=my-key" \
  -var="use_spot=true"

# Memory-constrained (test spill-to-disk)
export MEMORY_BUDGET=536870912  # 512 MB
./run-benchmark.sh standalone SF10
```

**Spot instances**: Defaults to off. Use `-var="use_spot=true"` to opt in. Saves ~60-70% but nodes can be reclaimed mid-benchmark. Best for SF1 where session cost is already low ($0.40). Not recommended for SF100 distributed (~$12 session) where a single reclaimed worker wastes the full run.

**Persistent data bucket**: For iterative tuning (especially SF100), create a bucket once and reuse it across cluster rebuilds:

```bash
# Create a persistent bucket
aws s3 mb s3://wadjet-bench-sf100

# First run — generates data
terraform apply -var-file=sf100-distributed.tfvars \
  -var="key_name=my-key" \
  -var="data_bucket=wadjet-bench-sf100"

# Tear down compute, keep data
terraform destroy -var-file=sf100-distributed.tfvars \
  -var="key_name=my-key" \
  -var="data_bucket=wadjet-bench-sf100"

# Rebuild with different config — skips data gen automatically
terraform apply -var-file=sf100-distributed.tfvars \
  -var="key_name=my-key" \
  -var="data_bucket=wadjet-bench-sf100" \
  -var="worker_count=6"
```

The benchmark script auto-detects existing parquet files and skips generation. Set `FORCE_DATAGEN=1` to regenerate.
