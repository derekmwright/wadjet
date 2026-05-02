# DuckDB-vs-Wadjet TPC-H baseline setup

This directory contains the SQL script that regenerates the SF0.01
parquet fixtures committed under `benchmarks/tpch/duckdb-data/`. Those
fixtures are the input to `TestDuckDBCompare` (in
`benchmarks/tpch/duckdb_compare_test.go`).

## Why a separate dataset?

`benchmarks/tpch/datagen.go` is Wadjet's own TPC-H generator. It produces
data that's shaped right for the spec but distributes values differently
than DuckDB's `tpch.dbgen` (Wadjet's Q01 returns 6 (return_flag,
line_status) combinations at SF0.01; DuckDB returns the canonical 4).
Cross-engine comparison only makes sense on the *same* data.

DuckDB's `tpch.dbgen` is the de-facto reference implementation of TPC-H
data generation — it's what Trino, Presto, ClickHouse, Spark all compare
against. We use it as the source of truth.

## How the test runs

The default `TestDuckDBCompare` mode runs Wadjet against the committed
parquet fixtures and gates each query's output checksum against
`benchmarks/tpch/baseline-duckdb-sf001.json` — no DuckDB binary required,
and no environment variables. This is the CI gate for cross-engine
correctness drift.

```bash
# Default — Wadjet vs stored DuckDB checksums (no DuckDB binary needed):
go test -run TestDuckDBCompare ./benchmarks/tpch/

# Live cross-engine compare (also runs DuckDB and verifies cell-by-cell):
WADJET_DUCKDB_COMPARE=1 go test -run TestDuckDBCompare ./benchmarks/tpch/

# Regenerate the stored baseline (after intentional output change OR
# DuckDB version bump). Requires /tmp/duckdb.
WADJET_REGENERATE_DUCKDB_BASELINE=1 go test -run TestDuckDBCompare ./benchmarks/tpch/
```

## Regenerating the parquet fixtures

This is needed only if `tpch.dbgen` data shape changes (rare) or to
upgrade the DuckDB CLI version that produced the fixtures.

```bash
# 1. Install DuckDB CLI (one-time):
wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
unzip -d /tmp /tmp/duckdb.zip   # produces /tmp/duckdb

# 2. Regenerate parquet:
mkdir -p /tmp/tpch-sf001
/tmp/duckdb < benchmarks/tpch/duckdb-setup/export-wadjet-shape.sql

# 3. Replace committed fixtures:
cp /tmp/tpch-sf001/*.parquet benchmarks/tpch/duckdb-data/

# 4. Regenerate the stored DuckDB-output baseline (the parquet shape may
#    have shifted slightly):
WADJET_REGENERATE_DUCKDB_BASELINE=1 go test -run TestDuckDBCompare ./benchmarks/tpch/
```

## What `export-wadjet-shape.sql` does

Generates SF0.01 in DuckDB's tpch extension and exports each table to
parquet with Wadjet-compatible types: DOUBLE for monetary, INTEGER for
keys, `strftime`'d `YYYY-MM-DD` for dates. The `CAST(x::INTEGER) AS x`
projections are required because DuckDB's tpch extension defaults to
BIGINT keys / DECIMAL(15,2) prices, neither of which Wadjet's TPC-H
schema expects.
