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
./wadjet serve --mode=standalone --storage.endpoint=localhost:9000

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
- `embed()` SQL function — OpenAI text-embedding-3-small/large with batched API calls and LRU cache
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
- **Spill-to-disk** — sort, aggregate, and window operators spill under memory pressure

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

- **Pipeline-only execution** — all queries run as full SQL on workers, no inter-stage S3 shuffles
- **Probe-split parallelism** — partition the largest table's files across workers, each runs full SQL on its slice, coordinator merges partials
- **Embedded NATS** for coordination — no external dependencies beyond object storage
- **JetStream task queues** with request/reply result delivery and automatic redelivery
- **Federation** across clusters via NATS leaf nodes
- **Inline fast path** — results under 64 KB bypass S3 entirely

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
- Health endpoint with Kubernetes-compatible probes (HTTP and gRPC)
- Output in table, JSON, or CSV format

## Benchmarks

### TPC-H SF10 Performance

All 22 TPC-H queries at scale factor 10 (~60M lineitem rows, 86.6M total rows across 8 tables). Pure Go, no SIMD, no CGo. Data stored as Parquet on S3 with VPC gateway endpoint.

**Standalone** — AWS c7g.4xlarge (16 vCPU Graviton3, 32 GB RAM), S3 storage:

| Query | Description | Wadjet | DuckDB | Ratio |
|-------|-------------|--------|--------|-------|
| Q01 | Pricing Summary | 9.36s | 15.27s | 1.6x |
| Q02 | Min Cost Supplier | 2.57s | 4.66s | 1.8x |
| Q03 | Shipping Priority | 11.65s | 10.97s | 0.9x |
| Q04 | Order Priority | 12.85s | 8.91s | 0.7x |
| Q05 | Local Supplier Volume | 9.18s | 10.38s | 1.1x |
| Q06 | Revenue Change | 5.85s | 8.04s | 1.4x |
| Q07 | Volume Shipping | 9.54s | 10.88s | 1.1x |
| Q08 | National Market Share | 9.50s | 40.74s | 4.3x |
| Q09 | Product Type Profit | 12.29s | 13.43s | 1.1x |
| Q10 | Returned Item Reporting | 8.72s | 10.26s | 1.2x |
| Q11 | Important Stock | 2.63s | 3.52s | 1.3x |
| Q12 | Shipping Modes | 9.01s | 10.14s | 1.1x |
| Q13 | Customer Distribution | 3.69s | 3.21s | 0.9x |
| Q14 | Promotion Effect | 6.17s | 8.27s | 1.3x |
| Q15 | Top Supplier | 6.16s | 8.29s | 1.3x |
| Q16 | Parts/Supplier | 1.43s | 2.16s | 1.5x |
| Q17 | Small-Quantity Revenue | 8.07s | 11.27s | 1.4x |
| Q18 | Large Volume Customer | 13.94s | 13.20s | 0.9x |
| Q19 | Discounted Revenue | 6.12s | 9.31s | 1.5x |
| Q20 | Potential Part Promotion | 10.91s | 39.06s | 3.6x |
| Q21 | Suppliers Kept Orders Waiting | 18.41s | 20.29s | 1.1x |
| Q22 | Global Sales Opportunity | 2.84s | 2.39s | 0.8x |
| | **Total** | **3m01s** | **4m25s** | **1.5x** |

Wadjet wins 18 of 22 queries. Both engines read the same Parquet files from S3 on the same instance. DuckDB v1.2.1 with httpfs + aws extensions. DuckDB times include per-query credential and view setup (~2s overhead each); adjusted total is ~3m41s (Wadjet still 22% faster).

All 22 queries return correct results with validated row counts at SF0.01 (CI) and SF10 (EC2).

```bash
# Reproduce SF0.01 correctness (CI, ~5s)
go test -v -run TestTPCHQueries ./benchmarks/tpch/

# Reproduce SF10 on EC2 (Terraform + SSM, no SSH required)
cd deploy/benchmark/terraform
tofu apply -var="scale_factor=10" -var="worker_instance_type=c7g.4xlarge" \
  -var="data_bucket=wadjet-bench-sf10-use2" -var="generate_data=true"
```

## Deployment Modes

```
wadjet serve --mode=standalone     # All-in-one (dev / small workloads)
wadjet serve --mode=coordinator    # Plans queries, embeds NATS, touches zero data
wadjet serve --mode=worker         # Stateless task executor, scale horizontally
```

## AI Agent Integration (MCP)

Wadjet includes a native [Model Context Protocol](https://modelcontextprotocol.io/) server, enabling AI agents to discover tables, inspect schemas, and execute SQL queries.

```bash
# Start MCP server on stdio (for Claude Desktop, Claude Code, Cursor)
wadjet mcp --endpoint localhost:9000
```

Configure in Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "wadjet": {
      "command": "wadjet",
      "args": ["mcp", "--endpoint", "localhost:9000"]
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
import "github.com/citc-tech/wadjet/wadjet"

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
| [Operations](docs/operations.md) | Monitoring, Prometheus metrics, troubleshooting |
| [Network Analytics](docs/network-analytics.md) | End-to-end workflow: devices → Bento → Wadjet → app |
| [Disaster Recovery](docs/disaster-recovery.md) | Recovery scenarios, verification procedures, RTO/RPO |

## TPC-H Benchmark Queries

All 22 TPC-H queries pass at SF0.01 (correctness with row count validation) and SF10 (performance). See [benchmarks above](#tpc-h-sf10-performance) for details.

```bash
go test -v -run TestTPCHQueries ./benchmarks/tpch/                                    # SF0.01 correctness
TPCH_SCALE=10 go test -v -run TestTPCHQueriesLarge -timeout 120m ./benchmarks/tpch/   # SF10 performance
```
