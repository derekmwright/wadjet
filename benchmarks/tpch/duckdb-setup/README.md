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

The default `TestDuckDBCompare` mode runs every corpus query on **both**
Wadjet execution paths against the committed parquet fixtures and holds
each answer against a fingerprint of DuckDB's answer stored in
`benchmarks/tpch/baseline-duckdb-sf001.json` — no DuckDB binary required,
and no environment variables. This is the CI gate for cross-engine
correctness drift.

- **arm A** — the single-process engine (`wadjet.DB`, the same planner and
  pipeline the coordinator's local fast path runs)
- **arm B** — the distributed stage DAG (embedded NATS + three workers,
  `LocalFastPathBytes=0` so nothing routes around it)

Arm B exists because the gate used to have only arm A: Q05's ~25x inflated
revenues (#312) were DAG-only, so DuckDB truth existed the whole time and
never saw the bug.

The fingerprint (`internal/oracle.Fingerprint`) covers **every** column,
strings and NULLs included (NULL renders distinctly from the empty
string), and is **order-sensitive exactly when the query has a top-level
ORDER BY**. A query with a trailing `LIMIT` is gated twice: the stripped
form row for row, and the verbatim form by row count, because rows tied at
the cut are interchangeable. A bare `LIMIT` with no `ORDER BY` is
count-only — SQL does not say which rows come back. Floats are digested at
two precisions and a match at either counts, so accumulation-order noise
does not register while real value errors do.

Every stored entry carries `"source": "duckdb"`; the loader refuses the
file if any entry does not, because an expectation that cannot be traced
to DuckDB is exactly how a wrong answer once became the baseline.

```bash
# Default — both arms vs the stored DuckDB fingerprints (no DuckDB binary):
go test -run TestDuckDBCompare ./benchmarks/tpch/

# Live cross-engine compare: also runs DuckDB, verifies the STORED
# fingerprint still equals live DuckDB output, and prints a cell-by-cell
# diff when an arm disagrees.
WADJET_DUCKDB_COMPARE=1 go test -run TestDuckDBCompare ./benchmarks/tpch/

# Regenerate the stored baseline from DuckDB (after a corpus change OR a
# DuckDB version bump). Requires /tmp/duckdb. This is the only writer of
# the file, and it writes only DuckDB output.
WADJET_REGENERATE_DUCKDB_BASELINE=1 go test -run TestDuckDBCompare ./benchmarks/tpch/

# The gate's proof of work: reconstructs each 2026-08-17 wrong-answer bug
# at the level of the result and requires the fingerprint to reject it.
go test -run TestGateCatchesHistoricalBugs ./benchmarks/tpch/
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
