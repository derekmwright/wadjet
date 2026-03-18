#!/usr/bin/env bash
#
# Wadjet TPC-H benchmark runner for EC2 instances.
#
# Usage:
#   ./run-benchmark.sh standalone SF1           # Single-node SF1 (uses S3 data)
#   ./run-benchmark.sh distributed SF1 3        # Distributed SF1, 3 workers
#
# Environment (set by Terraform user data):
#   WADJET_BUCKET  — S3 bucket name
#   WADJET_REGION  — AWS region
#   GENERATE_DATA  — Set to "1" to regenerate data instead of using pre-seeded bucket
#
# Outputs results to /root/benchmark-results/

set -euo pipefail

MODE="${1:-standalone}"
SF="${2:-SF1}"
WORKER_COUNT="${3:-3}"
RUNS="${BENCHMARK_RUNS:-3}"
GENERATE="${GENERATE_DATA:-0}"

BUCKET="${WADJET_BUCKET:?Set WADJET_BUCKET}"
REGION="${WADJET_REGION:?Set WADJET_REGION}"
S3_ENDPOINT="s3.${REGION}.amazonaws.com"
RESULTS_DIR="/root/benchmark-results"
PROF_DIR="${RESULTS_DIR}/profiles"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="${RESULTS_DIR}/${MODE}-${SF}-${TIMESTAMP}.txt"

mkdir -p "$RESULTS_DIR" "$PROF_DIR"

# Map SF string to numeric scale factor
case "$SF" in
  SF1|sf1)     SCALE=1   ;;
  SF10|sf10)   SCALE=10  ;;
  SF100|sf100) SCALE=100 ;;
  *) echo "Unknown scale factor: $SF (use SF1, SF10, or SF100)"; exit 1 ;;
esac

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULT_FILE"; }

# Capture system info
{
  echo "=== Wadjet TPC-H Benchmark ==="
  echo "Mode:         $MODE"
  echo "Scale Factor: $SF (${SCALE}x)"
  echo "Workers:      $WORKER_COUNT"
  echo "Runs:         $RUNS"
  echo "Instance:     $(curl -s http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo unknown)"
  echo "Instance ID:  $(curl -s http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo unknown)"
  echo "Bucket:       $BUCKET"
  echo "Date:         $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo ""
  echo "--- CPU ---"
  lscpu | grep -E "^(Model name|CPU\(s\)|Thread|Core|Socket|CPU MHz)"
  echo ""
  echo "--- Memory ---"
  free -h | head -2
  echo ""
} | tee "$RESULT_FILE"

# ---- Build common S3 flags ----

S3_FLAGS=(
  --endpoint="${S3_ENDPOINT}"
  --ssl
  --bucket="${BUCKET}"
  --region="${REGION}"
)

# Default: use pre-seeded data. Set GENERATE_DATA=1 to regenerate.
LOAD_FLAGS=()
if [ "$GENERATE" = "1" ]; then
  log "GENERATE_DATA=1: will generate and load data"
else
  LOAD_FLAGS=(--skip-load)
  log "Using pre-seeded data from s3://${BUCKET} (set GENERATE_DATA=1 to regenerate)"
fi

# ---- Run benchmark ----

if [ "$MODE" = "standalone" ]; then
  log "Running TPC-H SF${SCALE} standalone benchmark (${RUNS} runs)..."

  /usr/local/bin/tpch-bench \
    --scale="${SCALE}" \
    --runs="${RUNS}" \
    "${S3_FLAGS[@]}" \
    "${LOAD_FLAGS[@]}" \
    --cpuprofile="${PROF_DIR}/cpu-standalone.prof" \
    --memprofile="${PROF_DIR}/mem-standalone.prof" \
    --profdir="${PROF_DIR}" \
    2>&1 | tee -a "$RESULT_FILE"

elif [ "$MODE" = "distributed" ]; then
  log "Running TPC-H SF${SCALE} distributed benchmark (${RUNS} runs, ${WORKER_COUNT} workers)..."

  /usr/local/bin/tpch-bench \
    --scale="${SCALE}" \
    --runs="${RUNS}" \
    --workers="${WORKER_COUNT}" \
    "${S3_FLAGS[@]}" \
    "${LOAD_FLAGS[@]}" \
    --nats-port=4222 \
    --cpuprofile="${PROF_DIR}/cpu-distributed.prof" \
    --memprofile="${PROF_DIR}/mem-distributed.prof" \
    --profdir="${PROF_DIR}" \
    2>&1 | tee -a "$RESULT_FILE"
fi

# ---- Upload results to S3 ----

log ""
log "=== Uploading results to S3 ==="
aws s3 cp "$RESULTS_DIR/" "s3://${BUCKET}/results/${TIMESTAMP}/" --recursive 2>&1 | tee -a "$RESULT_FILE"

log ""
log "Results uploaded to: s3://${BUCKET}/results/${TIMESTAMP}/"
log "Benchmark complete. Shutting down instance."

# Auto-shutdown to avoid burning compute after benchmark completes.
# Terraform destroy will clean up the instance and associated resources.
sudo shutdown -h now
