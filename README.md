# Wadjet

A lightweight analytical query engine in pure Go. Columnar storage on Parquet, vectorized execution, full SQL, and optional distributed processing over NATS and S3-compatible object storage.

## Why Wadjet

- **No coordinator bottleneck** — the coordinator plans queries and schedules tasks but never touches data bytes. Workers read from and write results to object storage directly.
- **Lightweight workers** — viable at 512 MB RAM with spill-to-disk. Scale to zero, start in under 2 seconds.
- **Single binary** — run standalone for development or split into coordinator + workers for production.
- **Pure Go** — no JVM, no CGo, no external query engine dependencies. Custom recursive descent SQL parser, vectorized batch execution, typed kernel dispatch.
- **Network-native types** — first-class IPv4, IPv6, CIDR, MAC, Port, and Protocol column types with 80+ network functions covering CIDR math, deep packet inspection, ICMP analysis, IPv6 tunneling, JA3/JA3S TLS fingerprinting, payload search, and GeoIP/ASN enrichment (MaxMind).
- **Nested types** — ARRAY, ROW/STRUCT, and MAP column types with dot-notation field access, array functions, and full Parquet round-trip.
- **Table functions** — `read_json()`, `read_csv()`, `read_parquet()` query local files and HTTP URLs directly from SQL, with glob patterns and named parameters.
- **GeoIP enrichment** — optional MaxMind GeoLite2/GeoIP2 integration with 11 functions for IP geolocation (country, city, subdivision, coordinates, timezone, continent) and ASN lookup (AS number, organization).

## Quick Start

Query files on disk. No server, no object storage, no configuration:

```bash
# Build (-o must not be plain "wadjet" — that's the API package directory)
go build -o wadjet-bin ./cmd/wadjet

# Query a JSON log straight from disk
./wadjet-bin query "SELECT id_orig_h, SUM(orig_bytes) AS total FROM read_json('conn.log') GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

`read_json()`, `read_csv()`, and `read_parquet()` take local paths, `~/` paths, glob patterns, and HTTP URLs. Add `--format table` for human-readable output.

### With object storage

Managed tables live in an S3-compatible store (MinIO, AWS S3, R2). That is the distributed and production path — see [Getting Started](docs/getting-started.md) for MinIO setup:

```bash
# Start standalone (embedded NATS + worker + coordinator)
./wadjet-bin serve --mode=standalone --endpoint=localhost:9000

# Run a query against a managed table
./wadjet-bin query --endpoint=localhost:9000 "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10"

# Interactive shell
./wadjet-bin shell --endpoint=localhost:9000
```

## Features

### SQL

Full analytical SQL via a custom recursive descent parser:

- SELECT, EXPLAIN, DESCRIBE, CREATE TABLE, DROP TABLE
- CTEs (`WITH ... AS`), UNION / INTERSECT / EXCEPT (with ALL variants)
- INNER, LEFT, RIGHT, FULL OUTER, CROSS JOINs
- Subqueries: scalar, IN, EXISTS, correlated subqueries
- Window functions with PARTITION BY, ORDER BY, NULLS FIRST/LAST, and ROWS/RANGE frame specs
- GROUP BY, GROUPING SETS, CUBE, ROLLUP, and ORDER BY with positional references
- CASE, CAST, LIKE, BETWEEN, IN, IS NULL/TRUE/FALSE
- Fixed-point DECIMAL(p,s) type with Int128 arithmetic (DuckDB-style scaled integers)
- Nested types: ARRAY, ROW/STRUCT, MAP with `person.name` dot-notation, `element_at()`, `map_keys()`
- Table functions: `read_json()`, `read_csv()`, `read_parquet()` with glob patterns and named parameters
- VECTOR(N) type for embedding storage with cosine_similarity, l2_distance, dot_product, vector_norm, vector_dims
- `embed()` SQL function — OpenAI, Voyage AI, and Ollama embedding providers with batched API calls (one call per record batch) and LRU cache
- 280+ built-in scalar functions (string, math, trig, date/time, network, UUID, conditional, regex, hash, encoding, bitwise, JSON, URL, deep packet inspection, ICMP, IPv6, JA3 fingerprinting, payload search, GeoIP/ASN, vector distance)
- 23 aggregate functions including approx_distinct, corr, covar, percentile_cont/disc, mode, median, min_by/max_by
- User-defined functions (CREATE FUNCTION)

```sql
-- Query JSON files directly from SQL
SELECT ip, count, geoip_country(ip) AS country
FROM read_json('https://example.com/traffic.json')
WHERE count > 100
ORDER BY count DESC

-- Query CSV with custom delimiter
SELECT * FROM read_csv('logs/*.csv', delimiter='|', header=true)

-- Window functions over CTEs
WITH hourly AS (
    SELECT DATE_TRUNC('hour', timestamp) AS hour, SUM(bytes_in) AS bytes
    FROM flow_logs WHERE date = '2026-03-15'
    GROUP BY 1
)
SELECT hour, bytes,
       SUM(bytes) OVER (ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative,
       RANK() OVER (ORDER BY bytes DESC) AS traffic_rank
FROM hourly
ORDER BY hour
```

```sql
-- Semantic search with embeddings
SELECT alert_id, description,
       cosine_similarity(embed(description), embed('credential theft')) AS score
FROM alerts
ORDER BY score DESC LIMIT 10

-- Store embeddings in VECTOR columns
CREATE TABLE doc_embeddings (doc_id INT64, embedding VECTOR(1536))
```

### Execution Engine

- **Vectorized** — operators process batches of 2048 rows, not row-at-a-time
- **Push-based pipelines** — Source → UnaryOp → Sink with selection vectors instead of data copying
- **Typed kernels** — type dispatch resolved once at query init, no per-row switches in hot loops
- **3-level pushdown** — partition pruning → row-group stats pruning → row-level filtering
- **Cost-based optimization** — DP join reordering with Selinger-style costing over column statistics (`ANALYZE TABLE`: HLL distinct counts, histograms)
- **Spill-to-disk everywhere** — hash join (grace partition-on-arrival), hash aggregate (partial-state k-way merge), sort and window (external sorted-run merge) all degrade gracefully past memory, governed by a byte-true memory ledger
- **Morsel-driven parallelism** (`--morsel-workers=0`) — intra-task parallel pipeline consumers with bounded, self-draining aggregate partials; opt-in, validated at SF100

### Table Functions

Query files directly from SQL without ingestion:

```sql
SELECT * FROM read_json('data.json')                              -- local file
SELECT * FROM read_json('https://api.example.com/events.json')    -- HTTP/HTTPS
SELECT * FROM read_json('logs/*.json')                            -- glob patterns
SELECT * FROM read_csv('data.csv', delimiter='|', header=false)   -- named parameters
SELECT * FROM read_parquet('warehouse/sales.parquet')             -- Parquet files
```

- **read_json** — JSONL and JSON array auto-detection, schema inference (IPv4, timestamp, bool, numeric), custom direct-to-columnar byte scanner (8x faster than `encoding/json`)
- **read_csv** — configurable delimiter, header detection, type inference
- **read_parquet** — column-at-a-time page reading with row-group stats pruning
- **HTTP filesystem** — connection pooling, Range requests, configurable auth headers
- **Glob patterns** — `read_json('data/*.json')` expands and concatenates matching files

### Storage

- **Apache Parquet** on any S3-compatible store (MinIO, AWS S3, R2, SeaweedFS)
- **Apache Iceberg** metadata reading — register external Iceberg tables and query them via the catalog
- **Hive-style partitioning** with automatic time-based partition keys
- **NATS KV catalog** with revision-based optimistic concurrency
- **Micro-batch ingestion** with configurable flush thresholds (size, row count, time)

### Distributed

- **Stage-DAG execution** — distributed queries run as multi-stage DAGs; every stage output is durable in object storage, giving Trino-style fault-tolerant execution with task retry and worker-death recovery
- **Streaming exchange** (default on) — consumers fetch stage outputs directly from producer workers' local disk over gRPC with asynchronous S3 upload; any failure falls back to the durable S3 path (SF100 suite −23% vs S3-only shuffle)
- **Small-query fast path** — queries under a post-pruning size threshold (default 64 MiB) execute in-process on the coordinator, skipping the DAG entirely
- **Broadcast + probe-split joins** — small builds replicate to all workers; the probe side's files split across workers with coordinator merge
- **Split control/data plane** — NATS for heartbeats, cancellation, and the KV catalog; one multiplexed gRPC stream per worker for task dispatch and results
- **Memory-aware scheduling** — per-task byte estimates bin-packed against live worker pool budgets, with admission gating under memory pressure
- **Graceful worker drain** — SIGTERM stops intake, finishes in-flight tasks, flushes uploads, then exits; Kubernetes-ready with `/healthz`, `/readyz`, and `POST /drain`
- **Catalog snapshots** — periodic S3 snapshots of the NATS KV catalog; a rebooted cluster discovers its tables in seconds
- **Federation** across clusters via NATS leaf nodes
- **Embedded NATS** — no external dependencies beyond object storage

### Security

- API key, JWT (HMAC/RSA), and mTLS authentication — enforced on HTTP, pgwire, and gRPC
- RBAC (role-based) and ABAC (attribute-based) access control with deny-overrides combining
- Cell-level policies: column masking, column denial, row filtering via ABAC obligations
- Identity enrichment from JWT claims, mTLS cert fields, and API key attributes
- Hot-reloadable configuration (including ABAC policies)

### Client Connectivity

- **PostgreSQL wire protocol** (pgwire) — connect with `psql`, JDBC, ODBC, or any PostgreSQL client
- **HTTP** REST API for queries, table management, health, and Prometheus metrics
- **gRPC** API with protobuf service definition — generate type-safe clients for Go, Python, Java, TypeScript, Rust, C#, and more
- **MCP** (Model Context Protocol) — AI agent integration for Claude Desktop, Claude Code, Cursor, and other MCP-compatible tools
- gRPC health checking protocol for load balancer integration

### Operations

- Prometheus metrics for queries, scans, workers, cache, and spill
- Kubernetes-compatible probes on every process (`/healthz`, `/readyz`) and graceful worker drain (`SIGTERM` / `POST /drain`)
- Catalog snapshot / restore for fast cluster recovery
- Output in table, JSON, or CSV format

## Benchmarks

All 22 TPC-H queries pass with row-count-validated results at SF0.01 (CI,
~5s), SF10, and SF100 (~600M lineitem rows, distributed with
spill-to-disk). Cross-engine result validation against DuckDB confirms
identical results over the same S3 Parquet data. ClickBench runs the full
43-query suite under the official methodology with cell-exact
cross-validation against DuckDB.

### TPC-H SF100, distributed (4 nodes)

Coordinator `c7g.2xlarge` + 3× `c7gd.4xlarge` workers (16 vCPU / 32 GB /
NVMe each), SF100 Parquet on S3 (us-east-2), NATS control plane, gRPC
streaming exchange with durable S3 fallback. Steady-state suite (mean of
runs 2-4 of 4; caches populated — cold run 1 of the same session was
2m59s). Row counts are validated per run and the answers are additionally
verifiable value-level against a committed DuckDB fingerprint ground
truth (`benchmarks/tpch/fingerprint-sf100.json`, captured in-region).
2026-08-22 at v0.17.0, `results/20260822-055421` (same-window baseline
`results/20260822-053854`, engine `23abd8e` / v0.16.0-correctness).

| Query | Time | | Query | Time |
|---|---:|---|---|---:|
| Q01 | 3.5s | | Q12 | 5.1s |
| Q02 | 4.9s | | Q13 | 5.6s |
| Q03 | 9.2s | | Q14 | 1.9s |
| Q04 | 5.3s | | Q15 | 2.4s |
| Q05 | 6.2s | | Q16 | 5.7s |
| Q06 | 0.8s | | Q17 | 8.7s |
| Q07 | 4.2s | | Q18 | 13.9s |
| Q08 | 17.4s | | Q19 | 3.7s |
| Q09 | 20.7s | | Q20 | 9.2s |
| Q10 | 12.1s | | Q21 | 9.6s |
| Q11 | 3.5s | | Q22 | 2.7s |

**Suite total: 2m36s steady (mean of runs 2-4) / 2m59s cold.** The best
single steady run was 2m33s (152.9s, run 3), beating the prior all-time
record of 2m36s (155.9s, 2026-08-16); the cold run also beat the prior
best cold of 3m21s (201s, 2026-08-16). Same-window baseline on engine
`23abd8e` (v0.16.0-correctness) was 2m56s steady / 3m30s cold — an 11%
steady-state improvement, 12% on suite totals across all 4 runs (648s vs
738s). Row counts and DuckDB fingerprint value signatures are identical
across every arm and run. This is the perf-clawback arc that follows the
v0.16.0 correctness campaign: lazy-resolution guards and a compile-time
boolean protocol taken out of the expression row loop (no kill switch —
results are provably unchanged), a group-index layout decided at sink
construction so bounded aggregates build flat and never convert
(`WADJET_TWO_LEVEL_BORN_FLAT`), stage-sink row accumulation outside the
lock (`WADJET_STAGE_SINK_ACCUM`), decode-ahead admitted as its own
CPU-token class (`WADJET_DECODE_ADMISSION`), the scan prefetcher started
at source Init (`WADJET_PREFETCH_AT_INIT`), geometric backoff and peer
hints on the gather-merge durable wait (`WADJET_DURABLE_WAIT_BACKOFF`,
`WADJET_INTERM_PEER_HINTS`), and vector-backing reuse on the hash-join
probe emit path (`WADJET_VECTOR_REUSE`). Full attribution:
[profile-attribution-2026-08-21.md](docs/benchmarks/profile-attribution-2026-08-21.md),
[sf100-window-analysis-2026-08-22.md](docs/benchmarks/sf100-window-analysis-2026-08-22.md),
[sf100-window2-analysis-2026-08-22.md](docs/benchmarks/sf100-window2-analysis-2026-08-22.md),
and sf100-window3-analysis-2026-08-22.md (landing shortly). On identical
hardware in a same-day paired run (2026-08-14), Wadjet's steady state beat
Trino 470 FTE by 10% on suite wall and 19% on per-query geomean, winning
12 of 22 queries
([full comparison](docs/benchmarks/trino-comparison-2026-08-14.md)).

### ClickBench, single node (official spec)

The full 43-query ClickBench suite on the official listing hardware —
`c6a.4xlarge` (16 vCPU / 32 GB), 500 GB gp2, querying the 100M-row
`hits` Parquet data in place (14.7 GB, no import step). Official
methodology: page-cache drop before each query, cold + 2 hot tries,
one process per query. Every query result is cell-exact against DuckDB
on the same data (`benchmarks/clickbench/`). 2026-08-22 at
v0.17.0, `benchmarks/clickbench/results-c6a-20260822-v0170.json`.

| Query | Cold | Hot | Query | Cold | Hot |
|---|---:|---:|---|---:|---:|
| Q01 | 0.001s | 0.001s | Q23 | 21.5s | 4.37s |
| Q02 | 0.056s | 0.021s | Q24 | 12.3s | 3.11s |
| Q03 | 0.23s | 0.19s | Q25 | 2.64s | 1.12s |
| Q04 | 0.33s | 0.19s | Q26 | 0.99s | 0.92s |
| Q05 | 0.74s | 0.69s | Q27 | 2.64s | 1.13s |
| Q06 | 1.70s | 1.53s | Q28 | 9.75s | 3.96s |
| Q07 | 0.014s | 0.012s | Q29 | 12.3s | 11.7s |
| Q08 | 0.13s | 0.095s | Q30 | 0.15s | 0.11s |
| Q09 | 1.15s | 1.08s | Q31 | 2.07s | 0.96s |
| Q10 | 3.22s | 2.85s | Q32 | 5.70s | 1.39s |
| Q11 | 0.69s | 0.59s | Q33 | 5.88s | 5.25s |
| Q12 | 0.77s | 0.71s | Q34 | 10.7s | 4.70s |
| Q13 | 1.28s | 1.03s | Q35 | 14.7s | 8.41s |
| Q14 | 3.11s | 2.67s | Q36 | 4.29s | 3.81s |
| Q15 | 1.56s | 1.31s | Q37 | 0.31s | 0.19s |
| Q16 | 0.96s | 0.87s | Q38 | 0.20s | 0.11s |
| Q17 | 3.13s | 2.48s | Q39 | 0.15s | 0.067s |
| Q18 | 2.67s | 2.10s | Q40 | 0.55s | 0.41s |
| Q19 | 11.1s | 10.8s | Q41 | 0.071s | 0.042s |
| Q20 | 0.17s | 0.053s | Q42 | 0.071s | 0.046s |
| Q21 | 10.1s | 1.44s | Q43 | 0.19s | 0.15s |
| Q22 | 11.2s | 1.95s |  |  |  |

**Suite sums: 2m42s cold / 1m25s hot (43/43, no failures).** By the
official ClickBench formula (reproducible via
`benchmarks/clickbench/rank.py`) this places Wadjet at combined #41,
hot #66, and cold #17 of the 136 published `c6a.4xlarge` entries
(as of 2026-08-22) — ahead of the Trino, Presto, Impala, Spark,
Daft, GlareDB, and pg_duckdb Parquet entries on the same hardware.
This release's perf-clawback arc targeted the distributed TPC-H path;
ClickBench (single-node) is flat within noise against v0.16.0-correctness
(cold 161.5s vs 162.3s, hot 84.6s vs 85.2s).
The remaining hot spots (Q29, Q33, Q19, Q35 — regex-keyed grouping
and high-cardinality aggregation) are the active optimization arc.
Cold times for early large-read queries vary run-to-run with EBS gp2
burst-credit state (inherent to the official hardware spec); hot times
are stable.

```bash
# SF0.01 correctness (CI, ~5s)
go test -v -run TestTPCHQueries ./benchmarks/tpch/

# ClickBench correctness vs DuckDB (needs a hits part + /tmp/duckdb)
WADJET_HITS_PART=hits_0.parquet WADJET_CLICKBENCH_DUCKDB=1 \
  go test -run TestHitsCorrectness ./benchmarks/clickbench/

# Distributed smoke gate (~20s, spawns a local coordinator + workers)
go run ./cmd/tpch-harness --mode=local

# Full EC2 benchmark matrix (OpenTofu + SSM, no SSH required)
cd deploy/benchmark/terraform && tofu apply -var-file=sf100-distributed.tfvars
cd deploy/benchmark/terraform-clickbench && tofu apply   # official ClickBench run
```

## Deployment Modes

```
wadjet serve --mode=standalone     # All-in-one (dev / small workloads)
wadjet serve --mode=coordinator    # Plans queries, embeds NATS, touches zero data
wadjet serve --mode=worker         # Stateless task executor, scale horizontally
```

## AI Agent Integration (MCP)

Wadjet includes a native [Model Context Protocol](https://modelcontextprotocol.io/) server, enabling AI agents to discover tables, inspect schemas, and execute SQL queries.

The MCP server communicates over **stdio only** — there is no network listener.

```bash
# Local/dev: unauthenticated, direct-to-store (no ABAC enforced)
wadjet mcp

# Secured: enforce row/column ABAC under an authenticated identity
wadjet mcp --config /etc/wadjet/config.yaml --api-key "$WADJET_MCP_API_KEY"
```

**Security:** when `--config` supplies an auth block, MCP enforces the same
ABAC row filters, column masks, and table-access rules as the pgwire and gRPC
paths, under the identity resolved from `--api-key` (or `WADJET_MCP_API_KEY`).
If auth is configured but no valid credential is supplied, the server refuses
to start (fail closed). Without a config, MCP runs unauthenticated against a
direct-to-store DB — appropriate only where the operator already holds the
store credentials.

Configure in Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "wadjet": {
      "command": "wadjet",
      "args": ["mcp", "--config", "/etc/wadjet/config.yaml", "--api-key", "..."]
    }
  }
}
```

The MCP server exposes 5 tools:

| Tool | Description |
|------|-------------|
| `list_tables` | Discover all tables in the catalog |
| `describe_table` | Get schema with column types (including network-native types), nullability, and partition keys |
| `query` | Execute SQL with token-efficient compact JSON output (array-of-arrays, not array-of-objects) |
| `explain` | Show query execution plan without running |
| `list_functions` | List user-defined functions |

AI agents automatically understand network-typed columns (IPv4, CIDR, MAC, Port, Protocol) and receive hints about available network analysis functions.

## Embedding

Use Wadjet as a Go library:

```go
import "github.com/derekmwright/wadjet/wadjet"

db, _ := wadjet.Open(ctx, wadjet.Config{
    StorageEndpoint: "localhost:9000",
    Bucket:          "analytics",
})
defer db.Close()

result, _ := db.Query(ctx, "SELECT src_ip, COUNT(*) FROM flow_logs GROUP BY src_ip LIMIT 10")
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, first table, first query |
| [Architecture](docs/architecture.md) | System internals, execution model, data flow |
| [SQL Reference](docs/sql-reference.md) | Full SQL syntax, functions, operators |
| [Data Types](docs/data-types.md) | Column types including network primitives |
| [HTTP API](docs/api-reference.md) | REST endpoints for queries, tables, health |
| [gRPC API](docs/grpc-api.md) | Protobuf service for multi-language client generation |
| [Configuration](docs/configuration.md) | YAML config, environment variables, CLI flags |
| [Ingestion](docs/ingestion.md) | Micro-batch accumulator, partitioning, Bento pipelines |
| [Embedding](docs/embedding.md) | Using Wadjet as a Go library |
| [Distributed Deployment](docs/distributed.md) | Multi-node setup, federation, cluster routing |
| [Security](docs/security.md) | API keys, JWT, mTLS, RBAC, ABAC, cell-level policies |
| [Performance Tuning](docs/tuning.md) | Memory budgets, spill tuning, environment profiles |
| [Runbook](docs/runbook.md) | Run scenarios, the full flag surface, Kubernetes lifecycle |
| [Operations](docs/operations.md) | Monitoring, Prometheus metrics, troubleshooting |
| [Network Analytics](docs/network-analytics.md) | End-to-end workflow: devices → Bento → Wadjet → app |
| [Disaster Recovery](docs/disaster-recovery.md) | Recovery scenarios, verification procedures, RTO/RPO |

## TPC-H Benchmark Queries

All 22 TPC-H queries pass with row-count validation at SF0.01 (CI), SF10, and SF100. See [Benchmarks](#benchmarks).

```bash
go test -v -run TestTPCHQueries ./benchmarks/tpch/                                    # SF0.01 correctness
TPCH_SCALE=10 go test -v -run TestTPCHQueriesLarge -timeout 120m ./benchmarks/tpch/   # SF10 performance
```

## License

Wadjet is free and open-source software licensed under the
[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

If the AGPL doesn't fit your use case (e.g., embedding Wadjet in a
proprietary product), commercial licenses are available — contact
derekmwright@gmail.com.

Contributions are accepted under the [CLA](CLA.md); see
[CONTRIBUTING.md](CONTRIBUTING.md).
