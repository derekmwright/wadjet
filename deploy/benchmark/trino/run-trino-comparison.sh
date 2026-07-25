#!/usr/bin/env bash
#
# Trino vs Wadjet TPC-H comparison runner. Runs on the Trino coordinator
# after bootstrap (assets pulled from s3://<bucket>/bin/trino-assets/).
#
# Environment (set by Terraform user data):
#   TRINO_BUCKET   — S3 bucket holding both the parquet tables and results/
#   TRINO_REGION   — AWS region
#   TRINO_RUNS     — suite repetitions (default 2: run 1 cold, run 2 = page-
#                    cache-warm "steady" analog; Trino has no NVMe table cache)
#   TRINO_QUERIES  — comma-separated subset (e.g. "q01,q06") for validation
#
# Output: /root/benchmark-results/trino-comparison-<ts>.txt, uploaded to
# s3://<bucket>/results/trino-<ts>/ alongside the wadjet convention.

set -uo pipefail

BUCKET="${TRINO_BUCKET:?Set TRINO_BUCKET}"
REGION="${TRINO_REGION:?Set TRINO_REGION}"
RUNS="${TRINO_RUNS:-2}"
ASSETS=/opt/trino-assets
RESULTS_DIR=/root/benchmark-results
TS=$(date +%Y%m%d-%H%M%S)
OUT="${RESULTS_DIR}/trino-comparison-${TS}.txt"
CLI=/usr/local/bin/trino-cli

mkdir -p "$RESULTS_DIR"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

# ---- Wait for cluster ready (coordinator + expected workers active) ----
WANT_NODES="${TRINO_WANT_NODES:-1}"
log "Waiting for Trino cluster (${WANT_NODES} active nodes)..."
for i in $(seq 1 120); do
  n=$("$CLI" --output-format TSV --execute "SELECT count(*) FROM system.runtime.nodes WHERE state='active'" 2>/dev/null | tr -d '"') || n=0
  [ "${n:-0}" -ge "$WANT_NODES" ] && break
  sleep 5
done
n=$("$CLI" --output-format TSV --execute "SELECT count(*) FROM system.runtime.nodes WHERE state='active'" 2>/dev/null | tr -d '"') || n=0
log "Active nodes: ${n}"
if [ "${n:-0}" -lt "$WANT_NODES" ]; then
  log "FATAL: cluster did not reach ${WANT_NODES} nodes"
  exit 1
fi
log "Trino version: $("$CLI" --output-format TSV --execute 'SELECT node_version FROM system.runtime.nodes LIMIT 1' | tr -d '"')"

# ---- Register tables (idempotent) ----
log "Registering Glue-backed external tables over s3://${BUCKET}/..."
sed "s/\${BUCKET}/${BUCKET}/g" "$ASSETS/register-tables.sql" > /tmp/register.sql
if ! "$CLI" --file /tmp/register.sql >> "$OUT" 2>&1; then
  log "FATAL: table registration failed"
  exit 1
fi
for t in lineitem orders customer supplier nation region part partsupp; do
  c=$("$CLI" --output-format TSV --execute "SELECT count(*) FROM hive.tpch.$t" 2>>"$OUT" | tr -d '"')
  log "  $t rows: ${c:-ERROR}"
done

# ---- Query list ----
if [ -n "${TRINO_QUERIES:-}" ]; then
  QUERIES=$(echo "$TRINO_QUERIES" | tr ',' ' ')
else
  QUERIES=$(cd "$ASSETS/queries" && ls q*.sql | sed 's/\.sql//' | sort | tr '\n' ' ')
fi
log "Queries: $QUERIES  (runs: $RUNS)"

# ---- Runs ----
for run in $(seq 1 "$RUNS"); do
  log ""
  log "=== Run $run ==="
  total_ms=0
  for q in $QUERIES; do
    f="$ASSETS/queries/${q}.sql"
    [ -f "$f" ] || { log "  $q: MISSING"; continue; }
    start=$(date +%s%3N)
    rows=$("$CLI" --catalog hive --schema tpch --output-format TSV --file "$f" 2>/tmp/qerr.txt | wc -l)
    rc=$?
    end=$(date +%s%3N)
    ms=$((end - start))
    if [ $rc -ne 0 ]; then
      log "  $q  FAILED after ${ms}ms: $(head -2 /tmp/qerr.txt | tr '\n' ' ')"
      continue
    fi
    total_ms=$((total_ms + ms))
    log "  $q  wall_ms=${ms}  rows=${rows}"
  done
  log "  TOTAL run${run}: ${total_ms} ms"
done

# ---- Upload ----
log "Uploading results to s3://${BUCKET}/results/trino-${TS}/"
aws s3 cp "$OUT" "s3://${BUCKET}/results/trino-${TS}/" --region "$REGION" --quiet
log "done"
