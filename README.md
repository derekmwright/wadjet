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
- 273 built-in scalar functions (string, math, trig, date/time, network, UUID, conditional, regex, hash, encoding, bitwise, JSON, URL, deep packet inspection, ICMP, IPv6, JA3 fingerprinting, payload search, GeoIP/ASN)
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

- **Embedded NATS** for coordination — no external dependencies beyond object storage
- **JetStream task queues** with automatic redelivery on worker failure
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

Columnar scan throughput vs row-oriented baseline (`go test -bench`; AMD Ryzen 9 5900X):

| Rows | Columnar | Row-Oriented | Speedup | Allocs/op |
|------|----------|--------------|---------|-----------|
| 1K | 153 MB/s | 13 MB/s | 12x | 221 vs 11K |
| 10K | 180 MB/s | 13 MB/s | 14x | 231 vs 110K |
| 100K | 236 MB/s | 14 MB/s | 17x | 426 vs 1.1M |

With column projection (reading 2 of 5 columns):

| Rows | Columnar | Row-Oriented | Speedup |
|------|----------|--------------|---------|
| 1K | 312 MB/s | 17 MB/s | 18x |
| 10K | 464 MB/s | 17 MB/s | 27x |
| 100K | 603 MB/s | 18 MB/s | 34x |

Operator micro-benchmarks (2048-row batches):

| Operation | Time | Allocs |
|-----------|------|--------|
| Filter (column compare) | 18.1 µs | 0 |
| Hash aggregate (low cardinality) | 67 µs | 72 |
| Sort | 67 µs | 551 |
| Kernel filter (int64) | 6.0 µs | 0 |

JSON reader throughput (10,000 rows, 4 columns; direct-to-columnar byte scanner vs row-oriented `encoding/json`):

| Reader | Time | Allocs | Bytes |
|--------|------|--------|-------|
| Columnar (byte scanner) | 2.8 ms | 6,976 | 546 KB |
| Row-oriented | 23.3 ms | 221,335 | 6.6 MB |
| **Speedup** | **8.4x** | **31.7x fewer** | **12.1x less** |

Run benchmarks locally: `go test -bench=. -benchmem ./internal/engine/...`

### TPC-H SF1 Performance

All 22 TPC-H queries at scale factor 1 (~6M lineitem rows, 8 tables). Pure Go, no SIMD, no CGo. Single-node standalone mode on AMD Ryzen 9 5900X (WSL2).

| Query | Description | Time | Heap Delta | Rows |
|-------|-------------|------|------------|------|
| Q01 | Pricing Summary | 1.7s | +133 MB | 6 |
| Q02 | Min Cost Supplier | 0.7s | +169 MB | 100 |
| Q03 | Shipping Priority | 1.3s | +299 MB | 10 |
| Q04 | Order Priority | 1.5s | +232 MB | 5 |
| Q05 | Local Supplier Volume | 1.0s | +380 MB | 5 |
| Q06 | Revenue Change | 0.8s | +90 MB | 1 |
| Q07 | Volume Shipping | 2.8s | +430 MB | 10 |
| Q08 | National Market Share | 1.0s | +442 MB | 5 |
| Q09 | Product Type Profit | 1.5s | +828 MB | 58 |
| Q10 | Returned Item Reporting | 1.1s | +692 MB | 20 |
| Q11 | Important Stock | 0.2s | +427 MB | 758 |
| Q12 | Shipping Modes | 1.0s | +120 MB | 7 |
| Q13 | Customer Distribution | 0.5s | +152 MB | 100 |
| Q14 | Promotion Effect | 0.9s | +162 MB | 1 |
| Q15 | Top Supplier | 1.4s | +582 MB | 1 |
| Q16 | Parts/Supplier | 0.1s | +250 MB | 14262 |
| Q17 | Small-Quantity Revenue | 2.2s | +199 MB | 1 |
| Q18 | Large Volume Customer | 3.3s | +558 MB | 0 |
| Q19 | Discounted Revenue | 1.0s | +269 MB | 1 |
| Q20 | Potential Part Promotion | 2.6s | +819 MB | 0 |
| Q21 | Suppliers Kept Orders Waiting | 4.0s | +1043 MB | 10 |
| Q22 | Global Sales Opportunity | 0.2s | +382 MB | 1 |
| | **Total** | **30.8s** | | |

Includes cost-based join reordering, subquery decorrelation, common OR predicate extraction, columnar hash joins with int64 fast-path indexing, build-side column pruning, allocation-free aggregate group lookup, Top-K sort materialization, zero-copy semi/anti join output, and 3-level predicate pushdown (partition → row-group → row).

```bash
# Reproduce (requires ~2GB RAM, ~30s for data generation)
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/
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

## TPC-H Benchmark Queries

All 22 TPC-H queries pass at SF0.01 (correctness) and SF1 (performance). See [benchmarks above](#tpc-h-sf1-performance) for details.

```bash
go test -v -run TestTPCHQueries ./benchmarks/tpch/                                    # SF0.01 correctness
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/     # SF1 performance
```
