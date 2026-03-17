#!/usr/bin/env bash
#
# Wadjet TPC-H benchmark runner for EC2 instances.
#
# Usage:
#   ./run-benchmark.sh standalone SF1           # Single-node SF1
#   ./run-benchmark.sh standalone SF10          # Single-node SF10
#   ./run-benchmark.sh distributed SF10 3       # Distributed SF10, 3 workers
#   ./run-benchmark.sh standalone SF100         # Single-node SF100
#
# Environment (set by Terraform user data):
#   WADJET_BUCKET  — S3 bucket name
#   WADJET_REGION  — AWS region
#
# Outputs results to /root/benchmark-results/

set -euo pipefail

MODE="${1:-standalone}"
SF="${2:-SF1}"
WORKER_COUNT="${3:-3}"
RUNS="${BENCHMARK_RUNS:-3}"

BUCKET="${WADJET_BUCKET:?Set WADJET_BUCKET}"
REGION="${WADJET_REGION:?Set WADJET_REGION}"
S3_ENDPOINT="s3.${REGION}.amazonaws.com"
RESULTS_DIR="/root/benchmark-results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="${RESULTS_DIR}/${MODE}-${SF}-${TIMESTAMP}.txt"

mkdir -p "$RESULTS_DIR"

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

# ---- Phase 1: Data generation + S3 load ----

cd /root/wadjet

# Check if data already exists in the bucket (supports pre-seeded / preserved buckets)
DATA_EXISTS=$(aws s3 ls "s3://${BUCKET}/tables/" --recursive 2>/dev/null | grep -c "\.parquet$" || true)
if [ "$DATA_EXISTS" -gt 0 ] && [ "${FORCE_DATAGEN:-}" != "1" ]; then
  log "Phase 1: Found existing data in s3://${BUCKET}/ ($DATA_EXISTS parquet files), skipping generation."
  log "  Set FORCE_DATAGEN=1 to regenerate."
else
  log "Phase 1: Generating TPC-H SF${SCALE} data and loading to S3..."
  /usr/local/bin/wadjet-seed \
    --scale="${SCALE}" \
    --endpoint="${S3_ENDPOINT}" \
    --bucket="${BUCKET}" \
    --region="${REGION}" \
    --skip-if-exists 2>&1 | tee -a "$RESULT_FILE"
  log "Data generation complete."
fi

# ---- Phase 2: Standalone benchmark ----

if [ "$MODE" = "standalone" ]; then
  log "Phase 2: Running TPC-H SF${SCALE} standalone benchmark (${RUNS} runs)..."

  for run in $(seq 1 "$RUNS"); do
    log "--- Run ${run}/${RUNS} ---"
    TPCH_SCALE=$SCALE go test -v -run TestTPCHQueriesLarge -timeout 60m -count=1 \
      ./benchmarks/tpch/ 2>&1 | tee -a "$RESULT_FILE"
  done
fi

# ---- Phase 2: Distributed benchmark ----

if [ "$MODE" = "distributed" ]; then
  COORD_IP="${COORDINATOR_PRIVATE_IP:?Set COORDINATOR_PRIVATE_IP}"
  NATS_URL="nats://${COORD_IP}:4222"

  log "Phase 2: Starting coordinator on ${COORD_IP}..."

  # Start coordinator in background
  wadjet serve \
    --mode=coordinator \
    --storage.endpoint="${S3_ENDPOINT}" \
    --bucket="${BUCKET}" \
    --storage-type=s3 \
    --http-addr=:8080 \
    --grpc-addr=:9090 \
    --nats-port=4222 \
    ${MEMORY_BUDGET:+--memory-budget=$MEMORY_BUDGET} \
    &
  COORD_PID=$!
  sleep 3

  log "Coordinator started (PID $COORD_PID). Waiting for workers..."

  # Workers should already be started via their own user data.
  # Wait for them to register.
  for i in $(seq 1 30); do
    REGISTERED=$(curl -sf http://localhost:8080/v1/health 2>/dev/null | grep -o '"workers":[0-9]*' | cut -d: -f2 || echo 0)
    if [ "$REGISTERED" -ge "$WORKER_COUNT" ]; then
      log "All $WORKER_COUNT workers registered."
      break
    fi
    log "Waiting for workers... ($REGISTERED/$WORKER_COUNT registered)"
    sleep 5
  done

  log "Running distributed TPC-H SF${SCALE} benchmark (${RUNS} runs)..."

  for run in $(seq 1 "$RUNS"); do
    log "--- Run ${run}/${RUNS} ---"

    # Run queries via the wadjet CLI against the coordinator
    for qnum in $(seq 1 22); do
      PADDED=$(printf "%02d" "$qnum")
      START=$(date +%s%N)

      wadjet query --endpoint="localhost:8080" \
        "$(python3 -c "
import sys; sys.path.insert(0, '/root/wadjet/benchmarks/tpch')
# Queries are in Go, so we use the test binary instead
" 2>/dev/null || true)"

      # Use the compiled test binary for consistent query execution
      TPCH_SCALE=$SCALE /usr/local/bin/wadjet-bench \
        -test.v -test.run "TestTPCHQueriesLarge/Q${PADDED}" \
        -test.timeout 30m -test.count=1 2>&1 | tee -a "$RESULT_FILE"
    done
  done

  log "Stopping coordinator..."
  kill $COORD_PID 2>/dev/null || true
fi

# ---- Phase 3: Summary ----

log ""
log "=== Summary ==="
log "Results written to: $RESULT_FILE"

# Extract timing summary
grep -E "Q[0-9]{2}:" "$RESULT_FILE" | tail -22 | while read -r line; do
  log "  $line"
done

log ""
log "To download results (via SSM):"
INSTANCE_ID=$(curl -sf http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo "INSTANCE_ID")
log "  aws s3 cp ${RESULT_FILE} s3://${BUCKET}/results/ && aws s3 cp s3://${BUCKET}/results/$(basename ${RESULT_FILE}) ."
log "  # Or via SSM port forwarding:"
log "  # aws ssm start-session --target ${INSTANCE_ID} --region ${REGION}"
log ""
log "Done. Remember to terraform destroy when finished!"
