# Architecture

Caelum is a columnar analytical query engine designed for high-throughput scan-heavy workloads over S3-compatible object storage. This document covers the system's internals.

## High-Level Architecture

```
                          ┌──────────────────────────────────┐
                          │           CLI / HTTP API          │
                          │   cmd/caelum    internal/server   │
                          └──────────┬───────────────────────┘
                                     │
                          ┌──────────▼───────────────────────┐
                          │          Query Pipeline           │
                          │                                   │
                          │  SQL Parser ──► Logical Plan      │
                          │                    │              │
                          │              Optimizer            │
                          │                    │              │
                          │             Physical Plan         │
                          │                    │              │
                          │          Execution Engine         │
                          └──────────┬───────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
   ┌──────────▼────────┐  ┌─────────▼─────────┐  ┌────────▼────────┐
   │  Storage Layer     │  │  Distributed Layer │  │  Security Layer │
   │  - Object Store    │  │  - NATS/JetStream  │  │  - AuthN        │
   │  - Catalog         │  │  - Coordinator     │  │  - AuthZ        │
   │  - Parquet I/O     │  │  - Worker Pool     │  │  - Cell Policies│
   │  - Ingest          │  │  - Task Dispatch   │  │  - Config Reload│
   └───────────────────┘  └───────────────────┘  └─────────────────┘
```

## Package Layout

```
github.com/derekmwright/caelum/
├── cmd/caelum/             # CLI entry point (cobra commands)
├── pkg/caelum/             # Public embeddable Go API
├── internal/
│   ├── storage/
│   │   ├── objstore/       # S3-compatible object store abstraction
│   │   ├── catalog/        # JSON metadata catalog on object storage
│   │   ├── parquet/        # Parquet reader/writer wrappers
│   │   └── ingest/         # Micro-batch accumulator + partitioner
│   ├── engine/
│   │   ├── batch/          # Record batches, vectors, selection vectors
│   │   ├── exec/           # Push-based pipeline executor
│   │   └── expr/           # Expression compiler (SQL AST → functions)
│   ├── planner/
│   │   ├── sql/            # SQL → SelectInfo parser (vitess-sqlparser)
│   │   ├── logical/        # Logical plan tree + optimizer
│   │   └── physical/       # Physical plan + distributed stages
│   ├── coordinator/        # Distributed query coordinator
│   ├── worker/             # Distributed task executor
│   ├── distributed/        # NATS messaging layer
│   ├── auth/               # Authentication + authorization
│   ├── config/             # YAML config with hot-reload
│   ├── server/             # HTTP API (net/http)
│   └── metrics/            # Prometheus metrics
└── test/                   # Integration tests
```

## Data Model

### Columnar Storage

All data in Caelum is stored in **Apache Parquet** files on S3-compatible object storage. Parquet provides:

- Columnar layout for analytical scan efficiency
- Built-in compression (Snappy default, Zstd, Gzip, LZ4 available)
- Schema-embedded metadata
- Row group organization for parallel reads

### Catalog

The catalog is a JSON metadata layer stored at `_catalog/` in the object store:

```
_catalog/
├── catalog.json              # Top-level: version, table list
├── tables/
│   └── flow_logs/
│       ├── table.json        # Schema, partition keys
│       └── partitions/
│           ├── date=2026-03-14/
│           │   └── manifest.json   # File entries for this partition
│           └── date=2026-03-15/
│               └── manifest.json
```

**Concurrency control**: All catalog updates use S3 ETags for optimistic concurrency — if another writer modifies the file between your read and write, the update fails and retries.

### Record Batches

The in-memory unit of data is the **record batch**: a fixed set of columns (`Vector`s), each holding up to 2048 rows.

```
RecordBatch
├── Vectors[]
│   ├── Vector{Name: "src_ip", Type: IPv4, Data: []uint32, Nulls: bitmap}
│   ├── Vector{Name: "bytes_in", Type: Int64, Data: []int64, Nulls: bitmap}
│   └── ...
├── RowCount: 2048
└── SelectionVector: []uint16  (optional: indices of "active" rows)
```

**Selection vectors** avoid copying data during filtering — instead, only the indices of matching rows are tracked.

## Query Execution Pipeline

### 1. SQL Parsing

The SQL string is parsed into a `SelectInfo` structure via the vitess-sqlparser:

```
"SELECT src_ip, SUM(bytes_in) FROM flow_logs WHERE dst_port = 443 GROUP BY src_ip"
    │
    ▼
SelectInfo{
    Tables:  ["flow_logs"]
    Columns: ["src_ip", "SUM(bytes_in)"]
    Where:   "dst_port = 443"
    GroupBy: ["src_ip"]
}
```

### 2. Logical Planning

The `SelectInfo` is converted into a tree of logical operators:

```
Limit(10)
  └── Sort(total_bytes DESC)
        └── Aggregate(GROUP BY src_ip; SUM(bytes_in))
              └── Filter(dst_port = 443)
                    └── Scan(flow_logs)
```

### 3. Optimization

The optimizer applies rule-based transformations:

- **Predicate pushdown**: Move filters closer to the scan
- **Projection pruning**: Only read columns referenced in the query
- **Constant folding**: Evaluate constant expressions at plan time

### 4. Physical Planning

The logical plan is converted into executable pipeline stages. In distributed mode, stages are assigned task IDs and dependencies for parallel execution.

### 5. Pipeline Execution

Caelum uses a **push-based, streaming pipeline** model:

```
Source (Parquet scan)
  │  produces batches
  ▼
UnaryOperator (Filter)
  │  applies selection vector in-place
  ▼
UnaryOperator (Project)
  │  selects/renames columns
  ▼
SinkSource (Aggregate)
  │  consumes all input, then produces grouped output
  ▼
SinkSource (Sort)
  │  collects all input, sorts, then produces sorted output
  ▼
Sink (Collect)
  │  accumulates final result
  ▼
QueryResult
```

**Pipeline breakers** (aggregates, sorts) consume all input before producing output. Non-breaking operators (filter, project, limit) operate in streaming fashion.

### Vectorized Execution

Operators process entire batches (up to 2048 rows) at a time, not row-by-row. Type dispatch happens once per batch via typed kernel functions, then the inner loop runs without type switches:

```go
// Type switch once
switch vec.Type {
case Int64:
    kernel = sumInt64Kernel
case Float64:
    kernel = sumFloat64Kernel
}

// Hot loop — no branches on type
for _, idx := range selectionVector {
    kernel.Update(vec.Data[idx])
}
```

This enables CPU cache-friendly access patterns and branch prediction.

## Storage Layer

### Object Store Abstraction

The `objstore.Store` interface abstracts S3-compatible storage:

```go
type Store interface {
    Put(ctx, key, reader, size) error
    PutIfMatch(ctx, key, reader, size, etag) error  // optimistic concurrency
    Get(ctx, key) (ReadCloser, error)
    Head(ctx, key) (ObjectInfo, error)
    List(ctx, prefix) ([]ObjectInfo, error)
    Delete(ctx, key) error
}
```

Two implementations:
- **MemStore** — In-memory map for testing
- **MinIOStore** — Production S3-compatible client (MinIO, AWS S3, R2, etc.)

### Ingestion

The ingester is a micro-batch accumulator that:

1. Accepts rows via `Write()`
2. Partitions rows based on configured partition keys (e.g., `date`)
3. Buffers rows in per-partition accumulators
4. Flushes to Parquet when thresholds are exceeded:
   - **Size**: 128 MB default
   - **Row count**: 1,000,000 rows default
   - **Time**: 60 seconds default
5. Updates the catalog manifest atomically

### Parquet I/O

- **Writer**: Configurable row group size (128K rows), page buffer (256KB), compression codec
- **Reader**: Reads full row groups, automatically maps Parquet schema to Caelum types
- **Schema inference**: Parquet physical/logical types are mapped to Caelum types (including network primitives)

## Distributed Execution

```
┌─────────────┐     NATS/JetStream      ┌─────────────┐
│ Coordinator  │◄──────────────────────►│   Worker 1   │
│              │     tasks stream        │              │
│  - Plans     │     results subjects    │  - Executor  │
│  - Dispatches│                         │  - LRU Cache │
│  - Merges    │                         └─────────────┘
│              │                         ┌─────────────┐
│              │◄──────────────────────►│   Worker 2   │
└─────────────┘                         │              │
                                         │  - Executor  │
                                         │  - LRU Cache │
                                         └─────────────┘
```

- **Coordinator**: Receives queries, builds plans, dispatches task messages to NATS, collects results, merges final output
- **Workers**: Pull tasks from a JetStream consumer, execute locally (scan, aggregate, join, sort), write intermediate results to object storage, publish completion notifications
- **Heartbeats**: Workers send heartbeats every 10 seconds; coordinator reaps workers that miss heartbeats
- **Task types**: `scan`, `aggregate`, `join`, `sort`
- **Worker concurrency**: 4 concurrent tasks per worker (default)
- **Worker cache**: 256 MB LRU cache for recently-read Parquet data

## Security Model

See [Security](security.md) for full details.

- **Authentication**: API keys, JWT (HMAC-SHA256 / RSA), mTLS
- **Authorization**: Role-based table access (read / write / admin)
- **Cell-level policies**: Column masking and row filtering applied post-query
- **Hot-reloadable**: Auth config changes are picked up without restart
