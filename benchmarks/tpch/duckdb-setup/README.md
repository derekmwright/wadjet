# DuckDB-vs-Wadjet TPC-H baseline setup

This directory contains the setup scripts that generate a shared TPC-H
SF0.01 dataset readable by both DuckDB and Wadjet, used by
`TestDuckDBCompare` (gated by `WADJET_DUCKDB_COMPARE=1`).

## Why a separate dataset?

`benchmarks/tpch/datagen.go` is Wadjet's own TPC-H generator. It produces
data that's shaped right for the spec but distributes values differently
than DuckDB's `tpch.dbgen` (Wadjet's Q01 returns 6 (return_flag,
line_status) combinations at SF0.01; DuckDB returns the canonical 4).
Cross-engine comparison only makes sense on the *same* data.

DuckDB's `tpch.dbgen` is the de-facto reference implementation of TPC-H
data generation — it's what Trino, Presto, ClickHouse, Spark all compare
against. We use it as the source of truth.

## Setup steps

```bash
# 1. Install DuckDB CLI (one-time, not auto-installed by the test):
wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
unzip -d /tmp /tmp/duckdb.zip   # produces /tmp/duckdb

# 2. Generate the shared SF0.01 dataset (~3 MB parquet, ~38 MB JSON):
mkdir -p /tmp/tpch-sf001
/tmp/duckdb < benchmarks/tpch/duckdb-setup/export-wadjet-shape.sql
/tmp/duckdb < benchmarks/tpch/duckdb-setup/export-json.sql

# 3. Run the comparison:
WADJET_DUCKDB_COMPARE=1 go test -run TestDuckDBCompare -v ./benchmarks/tpch/
```

## Output

Each TPC-H query is run on both engines against the *same* parquet files.
Wadjet's typed result rows are normalized to canonical strings and
compared cell-by-cell against DuckDB's CSV output, with float-aware
tolerance (1e-4 relative).

Mismatches surface as test failures with the first 3 divergent cells
shown plus a per-query summary line.

## What the SQL does

- `export-wadjet-shape.sql`: generates SF0.01 in DuckDB's tpch extension,
  exports each table to parquet with Wadjet-compatible types (DOUBLE for
  monetary, INTEGER for keys, strftime'd YYYY-MM-DD for dates).
- `export-json.sql`: same projection, output as JSON arrays for Wadjet's
  ingester to load.

Both scripts emit type-coerced columns (`CAST(x::INTEGER) AS x` etc.)
because DuckDB's tpch extension defaults to BIGINT keys / DECIMAL(15,2)
prices, neither of which Wadjet's TPC-H schema expects.
