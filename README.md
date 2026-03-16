# Caelum

A lightweight analytical query engine in pure Go. Columnar storage on Parquet, vectorized execution, full SQL, and optional distributed processing over NATS and S3-compatible object storage.

## Why Caelum

- **No coordinator bottleneck** — the coordinator plans queries and schedules tasks but never touches data bytes. Workers read from and write results to object storage directly.
- **Lightweight workers** — viable at 512 MB RAM with spill-to-disk. Scale to zero, start in under 2 seconds.
- **Single binary** — run standalone for development or split into coordinator + workers for production.
- **Pure Go** — no JVM, no CGo, no external query engine dependencies. Custom recursive descent SQL parser, vectorized batch execution, typed kernel dispatch.
- **Network-native types** — first-class IPv4, IPv6, CIDR, MAC, Port, and Protocol column types with dedicated functions.

## Quick Start

```bash
# Build
go build -o caelum ./cmd/caelum

# Start standalone (embedded NATS + worker + coordinator)
./caelum serve --mode=standalone --storage.endpoint=localhost:9000

# Run a query
./caelum query "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10"

# Interactive shell
./caelum shell
```

## Features

### SQL

Full analytical SQL via a custom recursive descent parser:

- SELECT, EXPLAIN, DESCRIBE, CREATE TABLE, DROP TABLE
- CTEs (`WITH ... AS`), UNION / UNION ALL
- INNER, LEFT, RIGHT, FULL OUTER, CROSS JOINs
- Subqueries (scalar, IN, EXISTS, derived tables)
- Window functions with PARTITION BY, ORDER BY, NULLS FIRST/LAST, and ROWS/RANGE frame specs
- GROUP BY and ORDER BY with positional references
- CASE, CAST, LIKE, BETWEEN, IN, IS NULL/TRUE/FALSE
- 53 built-in scalar functions (string, math, date/time, network, UUID, conditional)
- User-defined functions (CREATE FUNCTION)

```sql
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

### Storage

- **Apache Parquet** on any S3-compatible store (MinIO, AWS S3, R2, SeaweedFS)
- **Hive-style partitioning** with automatic time-based partition keys
- **NATS KV catalog** with revision-based optimistic concurrency
- **Micro-batch ingestion** with configurable flush thresholds (size, row count, time)

### Distributed

- **Embedded NATS** for coordination — no external dependencies beyond object storage
- **JetStream task queues** with automatic redelivery on worker failure
- **Federation** across clusters via NATS leaf nodes
- **Inline fast path** — results under 64 KB bypass S3 entirely

### Security

- API key, JWT (HMAC/RSA), and mTLS authentication
- Role-based table access control
- Cell-level policies (column masking, row filtering)
- Hot-reloadable configuration

### Operations

- Prometheus metrics for queries, scans, workers, cache, and spill
- Health endpoint with Kubernetes-compatible probes
- Output in table, JSON, or CSV format

## Benchmarks

Columnar scan throughput vs row-oriented baseline (`go test -bench`; AMD Ryzen 9 5900X):

| Rows | Columnar | Row-Oriented | Speedup | Allocs/op |
|------|----------|--------------|---------|-----------|
| 1K | 165 MB/s | 15 MB/s | 11x | 221 vs 11K |
| 10K | 193 MB/s | 14 MB/s | 14x | 230 vs 110K |
| 100K | 250 MB/s | 15 MB/s | 17x | 426 vs 1.1M |

With column projection (reading 2 of 5 columns):

| Rows | Columnar | Row-Oriented | Speedup |
|------|----------|--------------|---------|
| 1K | 335 MB/s | 11 MB/s | 31x |
| 10K | 513 MB/s | 11 MB/s | 45x |
| 100K | 661 MB/s | 12 MB/s | 56x |

Operator micro-benchmarks (2048-row batches):

| Operation | Time | Allocs |
|-----------|------|--------|
| Batch SUM (int64) | 616 ns | 1 |
| Filter (column compare) | 17.6 µs | 1 |
| Hash aggregate (low cardinality) | 55 µs | 62 |
| Sort | 50 µs | 40 |
| CASE WHEN | 61 µs | 0 |
| Kernel filter (int64) | 5.1 µs | 0 |

Run benchmarks locally: `go test -bench=. -benchmem ./internal/engine/...`

## Deployment Modes

```
caelum serve --mode=standalone     # All-in-one (dev / small workloads)
caelum serve --mode=coordinator    # Plans queries, embeds NATS, touches zero data
caelum serve --mode=worker         # Stateless task executor, scale horizontally
```

## Embedding

Use Caelum as a Go library:

```go
import "github.com/derekmwright/caelum/pkg/caelum"

db, _ := caelum.Open(ctx, caelum.Config{
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
| [Configuration](docs/configuration.md) | YAML config, environment variables, CLI flags |
| [Ingestion](docs/ingestion.md) | Micro-batch accumulator, partitioning, Bento pipelines |
| [Embedding](docs/embedding.md) | Using Caelum as a Go library |
| [Distributed Deployment](docs/distributed.md) | Multi-node setup, federation, cluster routing |
| [Security](docs/security.md) | API keys, JWT, mTLS, RBAC, cell-level policies |
| [Performance Tuning](docs/tuning.md) | Memory budgets, spill tuning, environment profiles |
| [Operations](docs/operations.md) | Monitoring, Prometheus metrics, troubleshooting |
| [Network Analytics](docs/network-analytics.md) | End-to-end workflow: devices → Bento → Caelum → app |
