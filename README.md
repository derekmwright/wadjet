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

```bash
# Build
go build -o wadjet ./cmd/wadjet

# Start standalone (embedded NATS + worker + coordinator)
./wadjet serve --mode=standalone --endpoint=localhost:9000

# Run a query
./wadjet query "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10"

# Interactive shell
./wadjet shell
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
streaming exchange with durable S3 fallback. Steady-state suite (run 4 of
4; caches populated — cold run 1 of the same session was 3m21s).
2026-08-16, `results/20260816-000900`.

| Query | Time | | Query | Time |
|---|---:|---|---|---:|
| Q01 | 3.9s | | Q12 | 5.5s |
| Q02 | 4.5s | | Q13 | 5.9s |
| Q03 | 9.7s | | Q14 | 1.9s |
| Q04 | 7.0s | | Q15 | 1.4s |
| Q05 | 9.2s | | Q16 | 5.5s |
| Q06 | 1.3s | | Q17 | 5.9s |
| Q07 | 8.0s | | Q18 | 11.0s |
| Q08 | 18.1s | | Q19 | 3.9s |
| Q09 | 16.1s | | Q20 | 10.2s |
| Q10 | 11.0s | | Q21 | 10.1s |
| Q11 | 2.6s | | Q22 | 2.9s |

**Suite total: 2m35.9s steady / 3m21s cold.** On identical hardware in a
same-day paired run (2026-08-14), Wadjet's steady state beat Trino 470
FTE by 10% on suite wall and 19% on per-query geomean, winning 12 of 22
queries ([full comparison](docs/benchmarks/trino-comparison-2026-08-14.md));
the numbers above include further improvements landed since that pairing.

### ClickBench, single node (official spec)

The full 43-query ClickBench suite on the official listing hardware —
`c6a.4xlarge` (16 vCPU / 32 GB), 500 GB gp2, querying the 100M-row
`hits` Parquet data in place (14.7 GB, no import step). Official
methodology: page-cache drop before each query, cold + 2 hot tries,
one process per query. Every query result is cell-exact against DuckDB
on the same data (`benchmarks/clickbench/`). 2026-08-17,
`benchmarks/clickbench/results-c6a-20260817-wave6.json`.

| Query | Cold | Hot | Query | Cold | Hot |
|---|---:|---:|---|---:|---:|
| Q01 | 0.001s | 0.001s | Q23 | 21.45s | 4.36s |
| Q02 | 0.093s | 0.054s | Q24 | 12.30s | 3.10s |
| Q03 | 0.22s | 0.18s | Q25 | 0.98s | 0.93s |
| Q04 | 0.30s | 0.19s | Q26 | 1.03s | 0.93s |
| Q05 | 0.77s | 0.71s | Q27 | 1.00s | 0.94s |
| Q06 | 1.72s | 1.59s | Q28 | 9.72s | 5.22s |
| Q07 | 0.015s | 0.011s | Q29 | 13.21s | 13.52s |
| Q08 | 0.16s | 0.12s | Q30 | 0.15s | 0.12s |
| Q09 | 1.22s | 1.15s | Q31 | 2.01s | 0.99s |
| Q10 | 2.80s | 2.60s | Q32 | 5.65s | 1.43s |
| Q11 | 0.70s | 0.63s | Q33 | 17.09s | 16.76s |
| Q12 | 0.80s | 0.71s | Q34 | 10.67s | 4.85s |
| Q13 | 1.36s | 1.13s | Q35 | 14.38s | 8.37s |
| Q14 | 3.12s | 2.65s | Q36 | 4.48s | 4.19s |
| Q15 | 1.57s | 1.33s | Q37 | 0.35s | 0.18s |
| Q16 | 0.96s | 0.89s | Q38 | 0.18s | 0.12s |
| Q17 | 3.05s | 2.45s | Q39 | 0.16s | 0.073s |
| Q18 | 2.54s | 2.15s | Q40 | 0.63s | 0.42s |
| Q19 | 12.06s | 10.65s | Q41 | 0.079s | 0.040s |
| Q20 | 0.15s | 0.056s | Q42 | 0.073s | 0.039s |
| Q21 | 10.11s | 1.46s | Q43 | 0.21s | 0.17s |
| Q22 | 11.15s | 1.95s |  |  |  |

**Suite sums: 2m51s cold / 1m39s hot (43/43, no failures).** By the
official ClickBench formula (reproducible via
`benchmarks/clickbench/rank.py`) this places Wadjet at combined #47,
hot #67, and cold #18 of the 132 published `c6a.4xlarge` entries
(as of 2026-08-17) — ahead of the Trino, Presto, Impala, Spark,
Daft, GlareDB, and pg_duckdb Parquet entries on the same hardware.
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
