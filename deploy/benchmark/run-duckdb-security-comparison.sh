#!/usr/bin/env bash
#
# DuckDB vs Wadjet security benchmark head-to-head comparison.
#
# Runs all 20 security queries through DuckDB against the same S3 Parquet files
# that Wadjet's security-bench writes. Captures timing, row counts, and sample
# results for correctness validation.
#
# Usage:
#   ./run-duckdb-security-comparison.sh                          # Uses env vars
#   ./run-duckdb-security-comparison.sh my-bucket us-east-2      # Explicit
#
# Prerequisites:
#   - Security data already generated in S3 via: security-bench --data-only ...
#   - AWS credentials available (IAM role on EC2, or local ~/.aws/credentials)

set -euo pipefail

BUCKET="${1:-${WADJET_BUCKET:-wadjet-security}}"
REGION="${2:-${WADJET_REGION:-us-east-2}}"
DATA_PREFIX="${DATA_PREFIX:-tables/}"
RESULTS_DIR="${RESULTS_DIR:-${HOME}/benchmark-results}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="${RESULTS_DIR}/duckdb-security-comparison-${TIMESTAMP}.txt"

mkdir -p "$RESULTS_DIR"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULT_FILE"; }

# ---- Install DuckDB if not present ----

install_duckdb() {
  if command -v duckdb &>/dev/null; then
    log "DuckDB already installed: $(duckdb --version)"
    return
  fi

  log "Installing DuckDB..."
  ARCH=$(uname -m)
  case "$ARCH" in
    aarch64|arm64) DUCKDB_ARCH="aarch64" ;;
    x86_64|amd64)  DUCKDB_ARCH="amd64" ;;
    *) log "ERROR: unsupported architecture: $ARCH"; exit 1 ;;
  esac

  DUCKDB_VERSION="v1.2.1"
  DUCKDB_URL="https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-${DUCKDB_ARCH}.zip"

  cd /tmp
  curl -fsSL -o duckdb.zip "$DUCKDB_URL"
  unzip -o duckdb.zip duckdb
  chmod +x duckdb
  mv duckdb /usr/local/bin/duckdb || sudo mv duckdb /usr/local/bin/duckdb
  cd -

  log "Installed DuckDB: $(duckdb --version)"
}

install_duckdb

# ---- System info ----

{
  echo "=== DuckDB vs Wadjet Security Benchmark ==="
  echo "Bucket:  s3://${BUCKET}"
  echo "Region:  ${REGION}"
  echo "Prefix:  ${DATA_PREFIX}"
  echo "Date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo ""
  if command -v lscpu &>/dev/null; then
    echo "--- CPU ---"
    lscpu | grep -E "^(Model name|CPU\(s\)|Thread|Core|Socket)" 2>/dev/null || true
    echo ""
    echo "--- Memory ---"
    free -h 2>/dev/null | head -2 || true
    echo ""
  fi
  echo "DuckDB version: $(duckdb --version)"
  echo ""
} | tee "$RESULT_FILE"

# ---- DuckDB setup ----

S3_PREFIX="s3://${BUCKET}/${DATA_PREFIX}"
DUCKDB_DB="${RESULTS_DIR}/security.duckdb"
rm -f "$DUCKDB_DB"

log "Setting up DuckDB database with S3 views..."

# Wadjet stores timestamps as int64 milliseconds since epoch in Parquet.
# DuckDB auto-detects Parquet TIMESTAMP annotations in most cases, but if
# the files use plain INT64 we cast via epoch_ms(). The views below handle
# both cases: if ts is already TIMESTAMP, the CASE is a no-op identity.
duckdb "$DUCKDB_DB" <<SETUP_EOF
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
CALL load_aws_credentials();
SET s3_region='${REGION}';
SET threads TO $(nproc);
SET memory_limit='$(( $(free -b 2>/dev/null | awk '/Mem:/{print $2}' || echo 8589934592) * 8 / 10 / 1073741824 ))GB';

CREATE VIEW netflow  AS SELECT * FROM read_parquet('${S3_PREFIX}netflow/*.parquet');
CREATE VIEW firewall AS SELECT * FROM read_parquet('${S3_PREFIX}firewall/*.parquet');
CREATE VIEW dns      AS SELECT * FROM read_parquet('${S3_PREFIX}dns/*.parquet');
CREATE VIEW auth     AS SELECT * FROM read_parquet('${S3_PREFIX}auth/*.parquet');
CREATE VIEW alerts   AS SELECT * FROM read_parquet('${S3_PREFIX}alerts/*.parquet');
SETUP_EOF
log "DuckDB setup complete"

# ---- Security queries ----
# Identical to benchmarks/security/queries.go. DuckDB handles all standard SQL
# used in these queries. Network-native types (IPv4, Port, Protocol) are stored
# as their underlying Parquet types (string, int32) — comparisons work as-is.

declare -A QUERY_NAMES
declare -A QUERIES

QUERY_NAMES[1]="Top Talkers by Bytes"
QUERIES[1]="SELECT src_ip, SUM(bytes_sent) as total_bytes, COUNT(*) as flow_count
FROM netflow
GROUP BY src_ip
ORDER BY total_bytes DESC
LIMIT 25"

QUERY_NAMES[2]="Traffic by Protocol"
QUERIES[2]="SELECT protocol, COUNT(*) as flows,
    SUM(packets) as total_packets, SUM(bytes_sent + bytes_recv) as total_bytes
FROM netflow
GROUP BY protocol
ORDER BY total_bytes DESC"

QUERY_NAMES[3]="Outbound Non-Standard Ports"
QUERIES[3]="SELECT dst_ip, dst_port, COUNT(*) as flows, SUM(bytes_sent) as bytes_out
FROM netflow
WHERE direction = 'outbound'
    AND dst_port NOT IN (80, 443, 53, 22, 25, 993, 587)
    AND protocol = 6
GROUP BY dst_ip, dst_port
ORDER BY flows DESC
LIMIT 50"

QUERY_NAMES[4]="Hourly Traffic Volume"
QUERIES[4]="SELECT EXTRACT(HOUR FROM ts) as hour,
    COUNT(*) as flows,
    SUM(bytes_sent + bytes_recv) as total_bytes
FROM netflow
GROUP BY EXTRACT(HOUR FROM ts)
ORDER BY hour"

QUERY_NAMES[5]="Long Duration Flows"
QUERIES[5]="SELECT src_ip, dst_ip, dst_port, protocol, duration_ns, bytes_sent
FROM netflow
WHERE duration_ns > 60000000000
ORDER BY duration_ns DESC
LIMIT 100"

QUERY_NAMES[6]="Top Denied Sources"
QUERIES[6]="SELECT src_ip, COUNT(*) as deny_count, SUM(bytes_sent) as bytes_attempted
FROM firewall
WHERE action IN ('deny', 'drop')
GROUP BY src_ip
ORDER BY deny_count DESC
LIMIT 25"

QUERY_NAMES[7]="Cross-Zone Traffic Matrix"
QUERIES[7]="SELECT zone_src, zone_dst, action, COUNT(*) as events
FROM firewall
GROUP BY zone_src, zone_dst, action
ORDER BY events DESC"

QUERY_NAMES[8]="Hot Firewall Rules"
QUERIES[8]="SELECT rule_id, action, COUNT(*) as hits
FROM firewall
GROUP BY rule_id, action
HAVING COUNT(*) > 10
ORDER BY hits DESC
LIMIT 50"

QUERY_NAMES[9]="NXDOMAIN by Client"
QUERIES[9]="SELECT client_ip, COUNT(*) as nxdomain_count
FROM dns
WHERE response_code = 'NXDOMAIN'
GROUP BY client_ip
ORDER BY nxdomain_count DESC
LIMIT 25"

QUERY_NAMES[10]="Suspicious DNS Queries"
QUERIES[10]="SELECT client_ip, query_name, query_type, COUNT(*) as query_count
FROM dns
WHERE query_name LIKE '%.evil.%'
    OR query_name LIKE '%.badactor.%'
    OR query_name LIKE '%phish%'
    OR query_name LIKE '%malware%'
    OR query_name LIKE '%crypto-mine%'
GROUP BY client_ip, query_name, query_type
ORDER BY query_count DESC"

QUERY_NAMES[11]="DNS Latency by Server"
QUERIES[11]="SELECT server_ip, COUNT(*) as queries,
    AVG(latency_ns) as avg_latency,
    MAX(latency_ns) as max_latency
FROM dns
GROUP BY server_ip
ORDER BY avg_latency DESC"

QUERY_NAMES[12]="Brute Force Candidates"
QUERIES[12]="SELECT username, service, COUNT(*) as failures, COUNT(DISTINCT src_ip) as unique_sources
FROM auth
WHERE action = 'login_failure'
GROUP BY username, service
ORDER BY failures DESC
LIMIT 25"

QUERY_NAMES[13]="Privilege Escalation Events"
QUERIES[13]="SELECT ts, src_ip, username, method, service, device, risk_score
FROM auth
WHERE action = 'privilege_escalation'
ORDER BY risk_score DESC, ts DESC"

QUERY_NAMES[14]="High Risk Auth Events"
QUERIES[14]="SELECT src_ip, username, action, service, risk_score
FROM auth
WHERE risk_score > 70
ORDER BY risk_score DESC
LIMIT 100"

QUERY_NAMES[15]="Alert Severity Distribution"
QUERIES[15]="SELECT severity, category, COUNT(*) as alert_count
FROM alerts
GROUP BY severity, category
ORDER BY alert_count DESC"

QUERY_NAMES[16]="Critical Alerts Timeline"
QUERIES[16]="SELECT ts, src_ip, dst_ip, dst_port, message, action
FROM alerts
WHERE severity = 'critical'
ORDER BY ts DESC"

QUERY_NAMES[17]="Most Targeted Hosts"
QUERIES[17]="SELECT dst_ip, COUNT(*) as alert_count,
    COUNT(DISTINCT signature_id) as unique_sigs
FROM alerts
WHERE severity IN ('critical', 'high')
GROUP BY dst_ip
ORDER BY alert_count DESC
LIMIT 25"

QUERY_NAMES[18]="Denied Sources with Alerts"
QUERIES[18]="SELECT f.src_ip, COUNT(DISTINCT f.dst_port) as ports_tried,
    COUNT(DISTINCT a.signature_id) as alert_types
FROM firewall f
JOIN alerts a ON f.src_ip = a.src_ip
WHERE f.action IN ('deny', 'drop')
GROUP BY f.src_ip
ORDER BY alert_types DESC
LIMIT 25"

QUERY_NAMES[19]="Failed Auth to Suspicious DNS"
QUERIES[19]="SELECT d.client_ip, d.query_name, COUNT(*) as queries
FROM dns d
WHERE d.client_ip IN (
    SELECT DISTINCT src_ip FROM auth WHERE action = 'login_failure'
)
AND (d.query_name LIKE '%.evil.%' OR d.query_name LIKE '%badactor%'
    OR d.query_name LIKE '%phish%' OR d.query_name LIKE '%malware%')
GROUP BY d.client_ip, d.query_name
ORDER BY queries DESC
LIMIT 50"

QUERY_NAMES[20]="Alert Source Firewall Activity"
QUERIES[20]="SELECT f.src_ip, f.action, f.dst_port, COUNT(*) as events
FROM firewall f
WHERE f.src_ip IN (
    SELECT DISTINCT src_ip FROM alerts WHERE severity IN ('critical', 'high')
)
GROUP BY f.src_ip, f.action, f.dst_port
ORDER BY events DESC
LIMIT 50"

# ---- Run queries ----

log "=== DuckDB Security Benchmark ==="
log ""

TOTAL_TIME=0
declare -A DUCK_TIMES
declare -A DUCK_ROWS

for Q in $(seq 1 20); do
  QNAME="${QUERY_NAMES[$Q]}"
  SQL="${QUERIES[$Q]}"

  START=$(date +%s%N)
  OUTPUT=$(duckdb "$DUCKDB_DB" -csv -noheader -c "$SQL;" 2>&1) || {
    ELAPSED_NS=$(( $(date +%s%N) - START ))
    ELAPSED_S=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc)
    log "$(printf 'Q%02d %-35s %8ss  FAIL' "$Q" "$QNAME" "$ELAPSED_S")"
    log "  Error: $(echo "$OUTPUT" | head -3)"
    DUCK_TIMES[$Q]="FAIL"
    DUCK_ROWS[$Q]=0
    continue
  }
  ELAPSED_NS=$(( $(date +%s%N) - START ))
  ELAPSED_S=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc)

  ROW_COUNT=$(echo "$OUTPUT" | grep -c . || true)

  DUCK_TIMES[$Q]="$ELAPSED_S"
  DUCK_ROWS[$Q]="$ROW_COUNT"
  TOTAL_TIME=$(echo "$TOTAL_TIME + $ELAPSED_S" | bc)

  log "$(printf 'Q%02d %-35s %8ss  %5d rows  OK' "$Q" "$QNAME" "$ELAPSED_S" "$ROW_COUNT")"

  # Save first 5 rows for correctness inspection
  duckdb "$DUCKDB_DB" -csv -c "$SQL LIMIT 5;" > "${RESULTS_DIR}/duckdb-security-q${Q}-sample.csv" 2>/dev/null || true
done

log ""
log "$(printf '%-39s %8ss  TOTAL' '' "$TOTAL_TIME")"

# ---- Comparison table ----

log ""
log "=== Comparison Summary ==="
log ""
log "$(printf '%-5s %-35s %10s %10s %8s %8s' 'Query' 'Name' 'DuckDB' 'Wadjet' 'DuckRows' 'WadRows')"
log "$(printf '%-5s %-35s %10s %10s %8s %8s' '-----' '-----' '------' '------' '--------' '-------')"

# Wadjet baseline: populate from a security-bench run, or leave as N/A.
# Update these after your first security-bench run on the same hardware.
declare -A WADJET_TIMES WADJET_ROWS

# SF1 WSL2 baseline (from local run — replace with EC2 numbers after deploy)
WADJET_TIMES[1]=0.604;  WADJET_ROWS[1]=25
WADJET_TIMES[2]=0.078;  WADJET_ROWS[2]=3
WADJET_TIMES[3]=0.131;  WADJET_ROWS[3]=50
WADJET_TIMES[4]=0.219;  WADJET_ROWS[4]=24
WADJET_TIMES[5]=0.060;  WADJET_ROWS[5]=100
WADJET_TIMES[6]=0.070;  WADJET_ROWS[6]=25
WADJET_TIMES[7]=0.030;  WADJET_ROWS[7]=9
WADJET_TIMES[8]=0.026;  WADJET_ROWS[8]=50
WADJET_TIMES[9]=0.033;  WADJET_ROWS[9]=25
WADJET_TIMES[10]=0.061; WADJET_ROWS[10]=22992
WADJET_TIMES[11]=0.014; WADJET_ROWS[11]=3
WADJET_TIMES[12]=0.020; WADJET_ROWS[12]=25
WADJET_TIMES[13]=0.026; WADJET_ROWS[13]=4015
WADJET_TIMES[14]=0.014; WADJET_ROWS[14]=100
WADJET_TIMES[15]=0.006; WADJET_ROWS[15]=10
WADJET_TIMES[16]=0.020; WADJET_ROWS[16]=10089
WADJET_TIMES[17]=0.018; WADJET_ROWS[17]=25
WADJET_TIMES[18]=0.035; WADJET_ROWS[18]=10
WADJET_TIMES[19]=0.061; WADJET_ROWS[19]=50
WADJET_TIMES[20]=0.023; WADJET_ROWS[20]=50

MATCH_COUNT=0
MISMATCH_COUNT=0
TOTAL_QUERIES=0

for Q in $(seq 1 20); do
  DT="${DUCK_TIMES[$Q]:-N/A}"
  DR="${DUCK_ROWS[$Q]:-N/A}"
  WT="${WADJET_TIMES[$Q]:-N/A}"
  WR="${WADJET_ROWS[$Q]:-N/A}"

  ROW_MATCH=""
  if [ "$DR" != "N/A" ] && [ "$WR" != "N/A" ] && [ "$DR" != "0" ] && [ "$DT" != "FAIL" ]; then
    TOTAL_QUERIES=$((TOTAL_QUERIES + 1))
    if [ "$DR" = "$WR" ]; then
      ROW_MATCH=" =="
      MATCH_COUNT=$((MATCH_COUNT + 1))
    else
      ROW_MATCH=" !!"
      MISMATCH_COUNT=$((MISMATCH_COUNT + 1))
    fi
  fi

  log "$(printf 'Q%02d   %-35s %8ss %8ss %8s %8s%s' "$Q" "${QUERY_NAMES[$Q]}" "$DT" "$WT" "$DR" "$WR" "$ROW_MATCH")"
done

log ""
log "Row count matches: ${MATCH_COUNT}/${TOTAL_QUERIES}, mismatches: ${MISMATCH_COUNT}/${TOTAL_QUERIES}"
log "  == = row counts match    !! = row counts differ (investigate)"
log ""
log "Results saved to: ${RESULT_FILE}"
log "Per-query samples in: ${RESULTS_DIR}/duckdb-security-q*-sample.csv"
