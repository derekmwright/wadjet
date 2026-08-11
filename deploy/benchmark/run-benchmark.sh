#!/usr/bin/env bash
#
# Wadjet benchmark runner for EC2 instances.
# Supports both TPC-H and security benchmarks via BENCHMARK_TYPE.
#
# Usage:
#   ./run-benchmark.sh standalone SF1           # Single-node SF1 (uses S3 data)
#   ./run-benchmark.sh distributed SF1 3        # Distributed SF1, 3 workers
#
# Environment (set by Terraform user data):
#   WADJET_BUCKET   — S3 bucket name
#   WADJET_REGION   — AWS region
#   GENERATE_DATA   — Set to "1" to regenerate data instead of using pre-seeded bucket
#   BENCHMARK_TYPE  — "tpch" (default) or "security"
#
# Outputs results to /root/benchmark-results/

set -euo pipefail

MODE="${1:-standalone}"
SF="${2:-SF1}"
WORKER_COUNT="${3:-3}"
RUNS="${BENCHMARK_RUNS:-3}"
GENERATE="${GENERATE_DATA:-0}"
SKIP="${SKIP_QUERIES:-}"
TIMEOUT="${QUERY_TIMEOUT:-10m}"
# DATA_PREFIX intentionally left unset here so we can distinguish
# "caller explicitly set it" from "use the default". The default ("tables/")
# is applied later, only when no profile is in use.
BENCH_TYPE="${BENCHMARK_TYPE:-tpch}"
PROFILE="${BENCHMARK_PROFILE:-}"

# Select benchmark binary
case "$BENCH_TYPE" in
  tpch)     BENCH_BIN="/usr/local/bin/tpch-bench" ; BENCH_LABEL="TPC-H" ;;
  security) BENCH_BIN="/usr/local/bin/security-bench" ; BENCH_LABEL="Security" ;;
  *) echo "Unknown BENCHMARK_TYPE: $BENCH_TYPE (use tpch or security)"; exit 1 ;;
esac

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
  echo "=== Wadjet ${BENCH_LABEL} Benchmark ==="
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

# Build common flags. When a profile is set, tpch-bench reads bucket/region/
# prefix/scale/timeout from the YAML — only profiling flags are needed here.
# Without a profile, we pass everything explicitly via CLI flags.
PROFILE_FLAG=()
[ -n "$PROFILE" ] && PROFILE_FLAG=(--config="${PROFILE}")

# Catalog snapshot/restore (PR #115): non-empty prefix makes tpch-bench
# restore the post-discovery catalog from s3://<bucket>/<prefix> (or write
# the first snapshot after a fresh discovery+ANALYZE). Profile-file fields
# don't reach tfvars-driven deploys (no --config), so this flows via env.
CATALOG_SNAP_FLAGS=()
[ -n "${TPCH_CATALOG_SNAPSHOT_PREFIX:-}" ] && CATALOG_SNAP_FLAGS=(--catalog-snapshot-prefix="${TPCH_CATALOG_SNAPSHOT_PREFIX}")

# Decide whether to pass --data-prefix on the CLI:
#   - If DATA_PREFIX is explicitly set in the environment, honor it (overrides profile).
#   - Else if no profile is set, fall back to the legacy default "tables/".
#   - Else (profile set, env unset): omit the flag and let the profile's
#     data_prefix win — including the empty-string case for SF100.
DATA_PREFIX_FLAG=()
if [ -n "${DATA_PREFIX+x}" ]; then
  DATA_PREFIX_FLAG=(--data-prefix="${DATA_PREFIX}")
elif [ -z "$PROFILE" ]; then
  DATA_PREFIX_FLAG=(--data-prefix="tables/")
fi

if [ "$MODE" = "standalone" ]; then
  log "Running ${BENCH_LABEL} SF${SCALE} standalone benchmark (${RUNS} runs)..."

  SKIP_FLAGS=()
  [ -n "$SKIP" ] && SKIP_FLAGS=(--skip-queries="${SKIP}")
  [ -n "$TIMEOUT" ] && SKIP_FLAGS+=( --query-timeout="${TIMEOUT}" )

  "$BENCH_BIN" \
    "${PROFILE_FLAG[@]}" \
    --scale="${SCALE}" \
    --runs="${RUNS}" \
    "${DATA_PREFIX_FLAG[@]}" \
    "${S3_FLAGS[@]}" \
    "${LOAD_FLAGS[@]}" \
    "${CATALOG_SNAP_FLAGS[@]}" \
    "${SKIP_FLAGS[@]}" \
    --cpuprofile="${PROF_DIR}/cpu-standalone.prof" \
    --memprofile="${PROF_DIR}/mem-standalone.prof" \
    --profdir="${PROF_DIR}" \
    2>&1 | tee -a "$RESULT_FILE"

elif [ "$MODE" = "distributed" ]; then
  log "Running ${BENCH_LABEL} SF${SCALE} distributed benchmark (${RUNS} runs, ${WORKER_COUNT} workers)..."

  # tpch-bench is self-contained for distributed mode:
  # - Starts embedded NATS on :4222 (workers connect here)
  # - Creates coordinator internally
  # - Registers all TPC-H tables in NATS KV catalog
  # - Waits for --workers=N workers to connect
  # - Runs all 22 queries through the coordinator
  SKIP_FLAGS=()
  [ -n "$SKIP" ] && SKIP_FLAGS=(--skip-queries="${SKIP}")
  [ -n "$TIMEOUT" ] && SKIP_FLAGS+=( --query-timeout="${TIMEOUT}" )

  NATIVE_DAG_FLAGS=()
  [ "${USE_NATIVE_DAG:-0}" = "1" ] && NATIVE_DAG_FLAGS=(--use-native-dag)

  DATA_PLANE_FLAGS=()
  if [ -n "${TPCH_DATA_PLANE:-}" ]; then
    DATA_PLANE_FLAGS=(--data-plane="${TPCH_DATA_PLANE}")
    [ -n "${TPCH_DATA_PLANE_ADDR:-}" ] && DATA_PLANE_FLAGS+=(--data-plane-addr="${TPCH_DATA_PLANE_ADDR}")
  fi

  "$BENCH_BIN" \
    "${PROFILE_FLAG[@]}" \
    --scale="${SCALE}" \
    --runs="${RUNS}" \
    --workers="${WORKER_COUNT}" \
    "${DATA_PREFIX_FLAG[@]}" \
    "${S3_FLAGS[@]}" \
    "${LOAD_FLAGS[@]}" \
    "${CATALOG_SNAP_FLAGS[@]}" \
    "${SKIP_FLAGS[@]}" \
    "${NATIVE_DAG_FLAGS[@]}" \
    "${DATA_PLANE_FLAGS[@]}" \
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

# Coordinator-side logs ride along with the results: the 2026-08-11
# Q21-R2 stall diagnosis had worker logs (grab-worker-logs.sh) but no
# coordinator witness — manifest publish times, retry classification,
# and stage barrier events all live here. benchmark.log is this
# script's own transcript; the journal slice carries the coordinator
# wadjet process output.
aws s3 cp /root/benchmark.log "s3://${BUCKET}/results/${TIMESTAMP}/coord-benchmark.log" 2>/dev/null || true
journalctl --since "24 hours ago" --no-pager 2>/dev/null | gzip -c | \
  aws s3 cp - "s3://${BUCKET}/results/${TIMESTAMP}/coord-journal.gz" 2>/dev/null || true

log ""
log "Results uploaded to: s3://${BUCKET}/results/${TIMESTAMP}/"

# ---- Purge intermediate query scratch ----
#
# Distributed execution materializes every DAG stage's output to
# s3://${BUCKET}/queries/<id>/... (shuffle/repartition + per-stage .wshf).
# The coordinator's async CleanQuery races the post-run teardown and usually
# loses, so each run leaks its full intermediate working set (~1.4 TB at SF100)
# until the 1-day S3 lifecycle rule reclaims it — the dominant S3 storage cost.
# All runs are complete and results are already uploaded above, so this scratch
# is safe to delete now. `|| true` so a cleanup hiccup never blocks shutdown.
log ""
log "=== Purging intermediate query scratch (s3://${BUCKET}/queries/) ==="
aws s3 rm "s3://${BUCKET}/queries/" --recursive 2>&1 | tail -3 | tee -a "$RESULT_FILE" || true

log ""
log "Benchmark complete. Shutting down instance."

# Auto-shutdown to avoid burning compute after benchmark completes.
# Terraform destroy will clean up the instance and associated resources.
sudo shutdown -h now
