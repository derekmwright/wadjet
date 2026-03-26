#!/usr/bin/env bash
#
# DuckDB vs Wadjet TPC-H head-to-head comparison.
#
# Runs all 22 TPC-H queries through DuckDB against the same S3 Parquet files
# that Wadjet benchmarks use. Captures timing, row counts, and result checksums
# for correctness validation.
#
# Usage:
#   ./run-duckdb-comparison.sh                      # Uses WADJET_BUCKET/WADJET_REGION env vars
#   ./run-duckdb-comparison.sh my-bucket us-east-2   # Explicit bucket and region
#
# Outputs:
#   /root/benchmark-results/duckdb-comparison-<timestamp>.txt  (on EC2)
#   ./duckdb-comparison-<timestamp>.txt                        (local)

set -euo pipefail

BUCKET="${1:-${WADJET_BUCKET:-wadjet-bench-sf10}}"
REGION="${2:-${WADJET_REGION:-us-east-2}}"
RESULTS_DIR="${RESULTS_DIR:-${HOME}/benchmark-results}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="${RESULTS_DIR}/duckdb-comparison-${TIMESTAMP}.txt"

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

  # Ensure unzip and bc are available (not always present on AL2023)
  for pkg in unzip bc; do
    command -v "$pkg" &>/dev/null || { dnf install -y "$pkg" 2>/dev/null || sudo dnf install -y "$pkg"; }
  done

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
  echo "=== DuckDB vs Wadjet TPC-H Comparison ==="
  echo "Bucket:  s3://${BUCKET}"
  echo "Region:  ${REGION}"
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

# ---- DuckDB setup SQL ----

# ---- Create persistent DuckDB database with setup ----

DUCKDB_DB="${RESULTS_DIR}/tpch.duckdb"
rm -f "$DUCKDB_DB"

log "Setting up DuckDB database with S3 views..."
duckdb "$DUCKDB_DB" <<SETUP_EOF
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;

-- Persistent secret so each duckdb invocation auto-loads AWS credentials
CREATE PERSISTENT SECRET aws_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN);
SET s3_region='${REGION}';
SET threads TO $(nproc);
SET memory_limit='$(( $(free -b 2>/dev/null | awk '/Mem:/{print $2}' || echo 8589934592) * 8 / 10 / 1073741824 ))GB';
CREATE VIEW region   AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/region/*.parquet');
CREATE VIEW nation   AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/nation/*.parquet');
CREATE VIEW supplier AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/supplier/*.parquet');
CREATE VIEW part     AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/part/*.parquet');
CREATE VIEW partsupp AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/partsupp/*.parquet');
CREATE VIEW customer AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/customer/*.parquet');
CREATE VIEW orders   AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/orders/*.parquet');
CREATE VIEW lineitem AS SELECT * FROM read_parquet('s3://${BUCKET}/tables/lineitem/*.parquet');
SETUP_EOF
log "DuckDB setup complete"

# ---- TPC-H Queries ----
# Same queries as Wadjet's benchmarks/tpch/queries.go, adapted for DuckDB.
# Key differences: none needed — DuckDB handles SUBSTR, string dates, etc.

declare -A QUERY_NAMES
declare -A QUERIES

QUERY_NAMES[1]="Pricing Summary Report"
QUERIES[1]="SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) as sum_qty,
  SUM(l_extendedprice) as sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  AVG(l_quantity) as avg_qty,
  AVG(l_extendedprice) as avg_price,
  AVG(l_discount) as avg_disc,
  COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus"

QUERY_NAMES[2]="Minimum Cost Supplier"
QUERIES[2]="SELECT
  s_acctbal, s_name, n_name, p_partkey, p_mfgr,
  s_address, s_phone, s_comment
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN supplier ON s_suppkey = ps_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE r_name = 'EUROPE'
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp
    JOIN supplier ON s_suppkey = ps_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
    WHERE ps_partkey = p_partkey
      AND r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100"

QUERY_NAMES[3]="Shipping Priority"
QUERIES[3]="SELECT
  l_orderkey,
  SUM(l_extendedprice * (1 - l_discount)) as revenue,
  o_orderdate,
  o_shippriority
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING'
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10"

QUERY_NAMES[4]="Order Priority Checking"
QUERIES[4]="SELECT
  o_orderpriority,
  COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND EXISTS (
    SELECT 1 FROM lineitem
    WHERE l_orderkey = o_orderkey
      AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority"

QUERY_NAMES[5]="Local Supplier Volume"
QUERIES[5]="SELECT
  n_name,
  SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE c_nationkey = s_nationkey
  AND r_name = 'ASIA'
  AND o_orderdate >= '1994-01-01'
  AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC"

QUERY_NAMES[6]="Forecasting Revenue Change"
QUERIES[6]="SELECT
  SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount >= 0.05
  AND l_discount <= 0.07
  AND l_quantity < 24"

QUERY_NAMES[7]="Volume Shipping"
QUERIES[7]="SELECT
  n1.n_name as supp_nation,
  n2.n_name as cust_nation,
  SUBSTR(l_shipdate, 1, 4) as l_year,
  SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier
JOIN lineitem ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN nation n1 ON s_nationkey = n1.n_nationkey
JOIN nation n2 ON c_nationkey = n2.n_nationkey
WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
  OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
  AND l_shipdate >= '1995-01-01'
  AND l_shipdate <= '1996-12-31'
GROUP BY n1.n_name, n2.n_name, SUBSTR(l_shipdate, 1, 4)
ORDER BY supp_nation, cust_nation, l_year"

QUERY_NAMES[8]="National Market Share"
QUERIES[8]="SELECT
  SUBSTR(o_orderdate, 1, 4) as o_year,
  SUM(CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as brazil_revenue,
  SUM(l_extendedprice * (1 - l_discount)) as total_revenue
FROM part
JOIN lineitem ON p_partkey = l_partkey
JOIN supplier ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN nation n1 ON c_nationkey = n1.n_nationkey
JOIN region ON n1.n_regionkey = r_regionkey
JOIN nation n2 ON s_nationkey = n2.n_nationkey
WHERE r_name = 'AMERICA'
  AND o_orderdate >= '1995-01-01'
  AND o_orderdate <= '1996-12-31'
  AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY SUBSTR(o_orderdate, 1, 4)
ORDER BY o_year"

QUERY_NAMES[9]="Product Type Profit Measure"
QUERIES[9]="SELECT
  n_name as nation,
  SUBSTR(o_orderdate, 1, 4) as o_year,
  SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part
JOIN lineitem ON p_partkey = l_partkey
JOIN supplier ON s_suppkey = l_suppkey
JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey
JOIN orders ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE p_name LIKE '%green%'
GROUP BY n_name, SUBSTR(o_orderdate, 1, 4)
ORDER BY nation, o_year DESC"

QUERY_NAMES[10]="Returned Item Reporting"
QUERIES[10]="SELECT
  c_custkey, c_name,
  SUM(l_extendedprice * (1 - l_discount)) as revenue,
  c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= '1993-10-01'
  AND o_orderdate < '1994-01-01'
  AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20"

QUERY_NAMES[11]="Important Stock Identification"
# Q11 uses SF-dependent fraction: 0.0001/SF
# Default to SF10 (0.00001); override with Q11_FRACTION env var
Q11_FRACTION="${Q11_FRACTION:-0.0000100000}"
QUERIES[11]="SELECT
  ps_partkey,
  SUM(ps_supplycost * ps_availqty) as value
FROM partsupp
JOIN supplier ON ps_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
WHERE n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
  SELECT SUM(ps_supplycost * ps_availqty) * ${Q11_FRACTION}
  FROM partsupp
  JOIN supplier ON ps_suppkey = s_suppkey
  JOIN nation ON s_nationkey = n_nationkey
  WHERE n_name = 'GERMANY'
)
ORDER BY value DESC"

QUERY_NAMES[12]="Shipping Modes and Order Priority"
QUERIES[12]="SELECT
  l_shipmode,
  SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count,
  SUM(CASE WHEN o_orderpriority != '1-URGENT' AND o_orderpriority != '2-HIGH' THEN 1 ELSE 0 END) as low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= '1994-01-01'
  AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode"

QUERY_NAMES[13]="Customer Distribution"
QUERIES[13]="SELECT
  c_custkey,
  COUNT(o_orderkey) as c_count
FROM customer
LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
GROUP BY c_custkey
ORDER BY c_count DESC, c_custkey
LIMIT 100"

QUERY_NAMES[14]="Promotion Effect"
QUERIES[14]="SELECT
  SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) as promo_revenue,
  SUM(l_extendedprice * (1 - l_discount)) as total_revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
WHERE l_shipdate >= '1995-09-01'
  AND l_shipdate < '1995-10-01'"

QUERY_NAMES[15]="Top Supplier"
QUERIES[15]="WITH revenue AS (
  SELECT
    l_suppkey as supplier_no,
    SUM(l_extendedprice * (1 - l_discount)) as total_revenue
  FROM lineitem
  WHERE l_shipdate >= '1996-01-01'
    AND l_shipdate < '1996-04-01'
  GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier
JOIN revenue ON s_suppkey = supplier_no
WHERE total_revenue = (
  SELECT MAX(total_revenue) FROM revenue
)
ORDER BY s_suppkey"

QUERY_NAMES[16]="Parts/Supplier Relationship"
QUERIES[16]="SELECT
  p_brand, p_type, p_size,
  COUNT(DISTINCT ps_suppkey) as supplier_cnt
FROM partsupp
JOIN part ON p_partkey = ps_partkey
WHERE p_brand != 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"

QUERY_NAMES[17]="Small-Quantity-Order Revenue"
QUERIES[17]="SELECT
  SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * AVG(l_quantity)
    FROM lineitem
    WHERE l_partkey = p_partkey
  )"

QUERY_NAMES[18]="Large Volume Customer"
QUERIES[18]="SELECT
  c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
  SUM(l_quantity) as total_qty
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE o_orderkey IN (
  SELECT l_orderkey
  FROM lineitem
  GROUP BY l_orderkey
  HAVING SUM(l_quantity) > 300
)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100"

QUERY_NAMES[19]="Discounted Revenue"
QUERIES[19]="SELECT
  SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE (
  p_brand = 'Brand#12'
  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
  AND l_quantity >= 1 AND l_quantity <= 11
  AND p_size >= 1 AND p_size <= 5
  AND l_shipmode IN ('AIR', 'REG AIR')
  AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
  p_brand = 'Brand#23'
  AND p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
  AND l_quantity >= 10 AND l_quantity <= 20
  AND p_size >= 1 AND p_size <= 10
  AND l_shipmode IN ('AIR', 'REG AIR')
  AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
  p_brand = 'Brand#34'
  AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
  AND l_quantity >= 20 AND l_quantity <= 30
  AND p_size >= 1 AND p_size <= 15
  AND l_shipmode IN ('AIR', 'REG AIR')
  AND l_shipinstruct = 'DELIVER IN PERSON'
)"

QUERY_NAMES[20]="Potential Part Promotion"
QUERIES[20]="SELECT s_name, s_address
FROM supplier
JOIN nation ON s_nationkey = n_nationkey
WHERE n_name = 'CANADA'
  AND s_suppkey IN (
    SELECT ps_suppkey
    FROM partsupp
    WHERE ps_partkey IN (
      SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
      SELECT 0.5 * SUM(l_quantity)
      FROM lineitem
      WHERE l_partkey = ps_partkey
        AND l_suppkey = ps_suppkey
        AND l_shipdate >= '1994-01-01'
        AND l_shipdate < '1995-01-01'
    )
  )
ORDER BY s_name"

QUERY_NAMES[21]="Suppliers Who Kept Orders Waiting"
QUERIES[21]="SELECT s_name, COUNT(*) as numwait
FROM supplier
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
JOIN orders ON o_orderkey = l1.l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n_name = 'SAUDI ARABIA'
  AND EXISTS (
    SELECT 1 FROM lineitem l2
    WHERE l2.l_orderkey = l1.l_orderkey
      AND l2.l_suppkey != l1.l_suppkey
  )
  AND NOT EXISTS (
    SELECT 1 FROM lineitem l3
    WHERE l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey != l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
  )
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100"

QUERY_NAMES[22]="Global Sales Opportunity"
QUERIES[22]="SELECT
  SUBSTR(c_phone, 1, 2) as cntrycode,
  COUNT(*) as numcust,
  SUM(c_acctbal) as totacctbal
FROM customer
WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND c_acctbal > (
    SELECT AVG(c_acctbal)
    FROM customer
    WHERE c_acctbal > 0.00
      AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  )
  AND NOT EXISTS (
    SELECT 1 FROM orders WHERE o_custkey = c_custkey
  )
GROUP BY SUBSTR(c_phone, 1, 2)
ORDER BY cntrycode"

# ---- Run queries ----

log "=== DuckDB TPC-H Benchmark ==="
log ""

TOTAL_TIME=0
declare -A DUCK_TIMES
declare -A DUCK_ROWS

for Q in $(seq 1 22); do
  QNAME="${QUERY_NAMES[$Q]}"
  SQL="${QUERIES[$Q]}"

  # Run query against persistent database (no per-query setup overhead)
  START=$(date +%s%N)
  OUTPUT=$(duckdb "$DUCKDB_DB" -csv -noheader -c "$SQL;" 2>&1) || {
    ELAPSED_NS=$(( $(date +%s%N) - START ))
    ELAPSED_S=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc)
    log "$(printf 'Q%02d %-35s %8ss  FAIL' "$Q" "$QNAME" "$ELAPSED_S")"
    log "  Error: $OUTPUT"
    DUCK_TIMES[$Q]="FAIL"
    DUCK_ROWS[$Q]=0
    continue
  }
  ELAPSED_NS=$(( $(date +%s%N) - START ))
  ELAPSED_S=$(echo "scale=3; $ELAPSED_NS / 1000000000" | bc)

  # Count rows (noheader mode: every non-empty line is a data row)
  ROW_COUNT=$(echo "$OUTPUT" | grep -c . || true)

  DUCK_TIMES[$Q]="$ELAPSED_S"
  DUCK_ROWS[$Q]="$ROW_COUNT"
  TOTAL_TIME=$(echo "$TOTAL_TIME + $ELAPSED_S" | bc)

  log "$(printf 'Q%02d %-35s %8ss  %5d rows  OK' "$Q" "$QNAME" "$ELAPSED_S" "$ROW_COUNT")"

  # Save first 5 rows for correctness inspection (re-run with header for sample)
  duckdb "$DUCKDB_DB" -csv -c "$SQL LIMIT 5;" > "${RESULTS_DIR}/duckdb-q${Q}-sample.csv" 2>/dev/null
done

log ""
log "$(printf '%-39s %8ss  TOTAL' '' "$TOTAL_TIME")"

# ---- Comparison table ----
# If Wadjet results exist, build a side-by-side comparison

log ""
log "=== Comparison Summary ==="
log ""
log "$(printf '%-5s %-35s %10s %10s %8s %8s' 'Query' 'Name' 'DuckDB' 'Wadjet' 'DuckRows' 'WadRows')"
log "$(printf '%-5s %-35s %10s %10s %8s %8s' '-----' '-----' '------' '------' '--------' '-------')"

# Parse Wadjet results from the most recent tpch-bench run in the same results dir.
declare -A WADJET_TIMES WADJET_ROWS

WADJET_RESULT=$(ls -t "${RESULTS_DIR}"/*-SF*-*.txt 2>/dev/null | grep -v duckdb | head -1)
if [ -n "$WADJET_RESULT" ]; then
  log "Wadjet results from: $(basename "$WADJET_RESULT")"
  while IFS= read -r line; do
    # Parse: "Q01 Pricing Summary   8.937s   6 rows  heap ..."
    QNUM=$(echo "$line" | grep -oP '^Q\K[0-9]+')
    [ -z "$QNUM" ] && continue
    QNUM=$((10#$QNUM))

    TIME_STR=$(echo "$line" | grep -oP '[0-9.]+(?:ms|s)' | head -1)
    if [[ "$TIME_STR" == *ms ]]; then
      TIME_S=$(echo "scale=3; ${TIME_STR%ms} / 1000" | bc)
    else
      TIME_S="${TIME_STR%s}"
    fi

    ROW_COUNT=$(echo "$line" | grep -oP '[0-9]+(?= rows)')

    WADJET_TIMES[$QNUM]="$TIME_S"
    WADJET_ROWS[$QNUM]="$ROW_COUNT"
  done < <(grep -P '^Q[0-9]+ .* [0-9.]+(ms|s) +[0-9]+ rows' "$WADJET_RESULT" | head -22)
else
  log "WARNING: No Wadjet results file found in ${RESULTS_DIR} — comparison will show N/A"
fi

MATCH_COUNT=0
MISMATCH_COUNT=0

for Q in $(seq 1 22); do
  DT="${DUCK_TIMES[$Q]:-N/A}"
  DR="${DUCK_ROWS[$Q]:-N/A}"
  WT="${WADJET_TIMES[$Q]:-N/A}"
  WR="${WADJET_ROWS[$Q]:-N/A}"

  ROW_MATCH=""
  if [ "$DR" != "N/A" ] && [ "$WR" != "N/A" ] && [ "$DR" != "0" ]; then
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
log "Row count matches: ${MATCH_COUNT}/22, mismatches: ${MISMATCH_COUNT}/22"
log "  == = row counts match    !! = row counts differ (investigate)"
log ""
log "Results saved to: ${RESULT_FILE}"
log "Per-query samples in: ${RESULTS_DIR}/duckdb-q*-sample.csv"
