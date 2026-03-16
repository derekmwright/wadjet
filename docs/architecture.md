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
│   │   ├── batch/          # Record batches, vectors, selection vectors, BatchPool
│   │   ├── exec/           # Push-based pipeline executor
│   │   ├── expr/           # Expression compiler (SQL AST → functions)
│   │   ├── memory/         # MemoryTracker, SpillManager, spill-to-disk
│   │   └── scan/           # Scanner with 3-level pushdown
│   ├── planner/
│   │   ├── sql/            # Recursive descent SQL parser → SelectInfo
│   │   ├── logical/        # Logical plan tree + optimizer
│   │   └── physical/       # Physical plan + distributed stages
│   ├── coordinator/        # Distributed query coordinator + federated routing
│   ├── worker/             # Distributed task executor + result store
│   ├── distributed/        # NATS messaging layer + cluster-scoped subjects
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
├── catalog.json                        # Top-level: version, table list, timestamps
├── tables/
│   └── flow_logs/
│       ├── schema.json                 # Table schema, partition keys, version
│       └── partitions/
│           └── manifest.json           # All partitions and their file entries
```

The `PartitionManifest` contains an array of `PartitionEntry` objects, each with a `path`, `values` (partition key → value map), and `files` (list of Parquet `FileEntry` objects with path, size, row count, and creation time).

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

### Batch Pool

Record batches are reused via `BatchPool` — a thread-safe, per-schema object pool that avoids allocation on the hot path. Batches are pooled by schema and row count, with up to 16 batches cached per size class.

`GlobalPool` provides cross-operator batch sharing: multiple operators with the same schema share a single pool, improving reuse in multi-operator pipelines.

> **Future: Go Arenas.** When Go arenas reach GA (currently experimental behind `GOEXPERIMENT=arenas`), the BatchPool backing allocator should be swapped to arena-based allocation. Arena lifecycle maps naturally to batch lifecycle: allocate vectors/bitmaps from an arena, free the entire arena when the batch is released. This would eliminate GC pressure on the batch processing path entirely. The pool abstraction makes this swap straightforward — only `Get()` and the underlying allocation need to change.

## Query Execution Pipeline

### 1. SQL Parsing

The SQL string is parsed into a `SelectInfo` structure by the custom recursive descent parser:

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
    Put(ctx, bucket, key, reader, size, contentType) (etag string, err error)
    PutIfMatch(ctx, bucket, key, reader, size, contentType, expectedETag) (etag string, err error)
    Get(ctx, bucket, key) (ReadCloser, ObjectInfo, error)
    Head(ctx, bucket, key) (ObjectInfo, error)
    List(ctx, bucket, opts ListOptions) ([]ObjectInfo, error)
    Delete(ctx, bucket, key) error
    BucketExists(ctx, bucket) (bool, error)
    MakeBucket(ctx, bucket) error
}
```

All methods take an explicit `bucket` parameter. `Put` returns the resulting ETag for subsequent optimistic concurrency checks.

Two implementations:
- **MemStore** — In-memory map for testing
- **MinIOStore** — Production S3-compatible client (MinIO, AWS S3, R2, etc.)

### Ingestion

The ingester is a micro-batch accumulator that:

1. Accepts rows via `Ingest(ctx, []map[string]any)`
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

## Memory Management

### Per-Task Memory Budget

Each task can be assigned a memory budget via `worker.memory_budget`. When an operator (Sort, HashAggregate, Window) exceeds the budget, it spills intermediate state to disk rather than growing memory unboundedly.

```
Task starts → MemoryTracker created with budget
  → Operator allocates → Tracker.Reserve(n)
    → If within budget: proceed in memory
    → If exceeds budget: SpillManager writes to disk
  → Task completes → SpillManager.Cleanup() removes temp files
```

This makes workers viable at 512 MB - 2 GB RAM. Without budgets, a large sort or aggregation could OOM the worker.

### Result Store

The **ResultStore** is an in-memory cache for intermediate stage results, avoiding S3 round-trips when stages execute on the same worker.

```
Without ResultStore:                    With ResultStore:
Stage 1 → write to S3 → Stage 2        Stage 1 → memory → Stage 2
          ~50-200ms per hop                       ~0ms
```

The result store is keyed by S3 path (what the result *would* be stored as), bounded by a configurable capacity (`worker.result_store_bytes`). When full, new results fall back to S3 transparently. Per-query cleanup removes entries after a query completes.

Results smaller than 64 KB bypass both the result store and S3 — they are embedded directly in the NATS result notification message (inline fast path).

### Spill Metrics

Prometheus metrics track spill behavior for tuning:
- `caelum_worker_spill_events_total` — number of spill-to-disk events
- `caelum_worker_spill_bytes_written_total` — bytes written to spill files
- `caelum_worker_memory_budget_bytes` — configured per-task budget
- `caelum_worker_memory_used_bytes` — current tracked memory usage

See [Performance Tuning](tuning.md) for guidance on sizing memory budgets and result stores.

## Distributed Execution

```
┌─────────────┐     NATS/JetStream      ┌─────────────┐
│ Coordinator  │◄──────────────────────►│   Worker 1   │
│              │     tasks stream        │   (central)  │
│  - Plans     │     results subjects    │  - Executor  │
│  - Dispatches│                         │  - LRU Cache │
│  - Merges    │                         │  - ResultStore│
│  - Federation│                         └─────────────┘
│              │                         ┌─────────────┐
│              │◄──────────────────────►│   Worker 2   │
└─────────────┘                         │   (central)  │
       │                                 │  - Executor  │
       │ Leaf Node                       │  - LRU Cache │
       │ Connection                      │  - ResultStore│
       │                                 └─────────────┘
       ▼
┌─────────────┐                         ┌─────────────┐
│ Remote NATS  │◄──────────────────────►│   Worker 3   │
│ (site-east)  │                         │  (site-east) │
└─────────────┘                         └─────────────┘
```

- **Coordinator**: Receives queries, builds plans, dispatches task messages to NATS, collects results, merges final output. For federated queries, splits scan stages per cluster.
- **Workers**: Pull tasks from a JetStream consumer, execute locally (scan, aggregate, join, sort, window), write intermediate results to result store or S3, publish completion notifications. Cluster-scoped: workers only pull tasks for their cluster.
- **Heartbeats**: Workers send heartbeats (with cluster ID) every 10 seconds; coordinator reaps workers that miss heartbeats
- **Task types**: `scan`, `aggregate`, `join`, `sort`, `window`
- **Task routing**: Tasks are published to cluster-scoped NATS subjects (`caelum.tasks.<cluster-id>.<type>.<query-id>.<stage-id>`). Workers subscribe to their cluster's filter (`caelum.tasks.<cluster-id>.>`)
- **Worker concurrency**: 4 concurrent tasks per worker (default)
- **Worker cache**: 256 MB LRU cache for recently-read Parquet data

### Federation

In federated deployments, data lives on multiple clusters (e.g., a central data center and regional sites). The coordinator automatically detects multi-cluster tables and splits scan stages per cluster, routing each to workers at the cluster where the data resides. Downstream aggregation and merge stages run on the central cluster.

See [Distributed Deployment](distributed.md) for federation setup details.

## Security Model

See [Security](security.md) for full details.

- **Authentication**: API keys, JWT (HMAC-SHA256 / RSA), mTLS
- **Authorization**: Role-based table access (read / write / admin)
- **Cell-level policies**: Column masking and row filtering applied post-query
- **Hot-reloadable**: Auth config changes are picked up without restart
