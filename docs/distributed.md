# Distributed Deployment

Caelum supports distributed query execution across multiple worker nodes, coordinated via NATS JetStream and backed by shared S3-compatible object storage.

## Architecture

```
                     Clients
                       │
                       ▼
              ┌─────────────────┐
              │   Coordinator    │
              │                  │
              │  - HTTP API      │
              │  - Query planner │
              │  - NATS embed    │
              │  - Result merger │
              └────────┬────────┘
                       │ NATS JetStream
          ┌────────────┼────────────┐
          │            │            │
    ┌─────▼─────┐ ┌───▼───────┐ ┌─▼─────────┐
    │  Worker 1  │ │  Worker 2  │ │  Worker 3  │
    │            │ │            │ │            │
    │ - Executor │ │ - Executor │ │ - Executor │
    │ - LRU Cache│ │ - LRU Cache│ │ - LRU Cache│
    └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
          │             │             │
          └─────────────┼─────────────┘
                        │
                 ┌──────▼──────┐
                 │  S3 / MinIO  │
                 │  (shared)    │
                 └─────────────┘
```

## Components

### Coordinator

The coordinator is the single entry point for queries:

1. Receives SQL via HTTP API
2. Parses and plans the query
3. Breaks the physical plan into distributed stages
4. For federated queries, expands scan stages per-cluster via `ExpandFederatedScans`
5. Publishes tasks to NATS with cluster-scoped subjects (`caelum.tasks.<cluster-id>.<type>.<query-id>.<stage-id>`)
6. Waits for result notifications from workers
7. Merges partial results into a final response

The coordinator also embeds a NATS server (no external NATS required).

### Workers

Workers are stateless compute nodes that:

1. Connect to the coordinator's NATS server
2. Subscribe to the `tasks` JetStream consumer, filtered by cluster ID
3. Pull and execute tasks (scan, aggregate, join, sort, window)
4. Read data from S3 (checking in-memory result store, then LRU cache, then S3)
5. Write intermediate results to result store (if enabled and capacity available) or S3
6. Publish completion notifications (results < 64 KB are inlined in the notification)
7. Send heartbeats every 10 seconds (with cluster ID)

### NATS JetStream

NATS provides the messaging backbone:

- **`tasks` stream** — Durable stream for task distribution. Tasks are pulled by workers via a JetStream consumer with at-least-once delivery.
- **Result subjects** — Per-query subjects for result notifications. The coordinator subscribes to these to collect task completions.
- **Heartbeat subjects** — Workers publish periodic heartbeats; the coordinator monitors them and reaps dead workers.

## Deployment

### Start the Coordinator

```bash
./caelum serve \
  --mode coordinator \
  --http-addr :8080 \
  --nats-port 4222 \
  --cluster-id central \
  --endpoint minio.internal:9000 \
  --access-key $S3_ACCESS_KEY \
  --secret-key $S3_SECRET_KEY \
  --bucket caelum \
  --config caelum.yaml
```

### Start Workers

On each worker node:

```bash
./caelum serve \
  --mode worker \
  --nats-url nats://coordinator.internal:4222 \
  --cluster-id central \
  --memory-budget 268435456 \
  --spill-dir /var/caelum/spill \
  --result-store 134217728 \
  --endpoint minio.internal:9000 \
  --access-key $S3_ACCESS_KEY \
  --secret-key $S3_SECRET_KEY \
  --bucket caelum
```

Workers automatically register with the coordinator and begin pulling tasks. The `--cluster-id` determines which tasks a worker pulls — it only processes tasks targeted at its cluster. See [Performance Tuning](tuning.md) for guidance on `--memory-budget`, `--spill-dir`, and `--result-store` sizing.

### Docker Compose Example

```yaml
version: "3.8"

services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data

  coordinator:
    build: .
    command: >
      serve --mode coordinator
      --http-addr :8080 --nats-port 4222
      --cluster-id central
      --endpoint minio:9000
      --access-key minioadmin --secret-key minioadmin
      --bucket caelum
    ports:
      - "8080:8080"
      - "4222:4222"
    depends_on:
      - minio

  worker-1:
    build: .
    command: >
      serve --mode worker
      --nats-url nats://coordinator:4222
      --cluster-id central
      --memory-budget 268435456
      --result-store 134217728
      --spill-dir /tmp/caelum-spill
      --endpoint minio:9000
      --access-key minioadmin --secret-key minioadmin
      --bucket caelum
    depends_on:
      - coordinator

  worker-2:
    build: .
    command: >
      serve --mode worker
      --nats-url nats://coordinator:4222
      --cluster-id central
      --memory-budget 268435456
      --result-store 134217728
      --spill-dir /tmp/caelum-spill
      --endpoint minio:9000
      --access-key minioadmin --secret-key minioadmin
      --bucket caelum
    depends_on:
      - coordinator

volumes:
  minio-data:
```

```bash
docker-compose up -d
# Scale workers dynamically
docker-compose up -d --scale worker-1=5
```

### Kubernetes Deployment

```yaml
# coordinator-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: caelum-coordinator
spec:
  replicas: 1  # Single coordinator
  selector:
    matchLabels:
      app: caelum-coordinator
  template:
    metadata:
      labels:
        app: caelum-coordinator
    spec:
      containers:
        - name: coordinator
          image: ghcr.io/derekmwright/caelum:latest
          args:
            - serve
            - --mode=coordinator
            - --http-addr=:8080
            - --nats-port=4222
            - --cluster-id=central
            - --endpoint=$(S3_ENDPOINT)
            - --access-key=$(S3_ACCESS_KEY)
            - --secret-key=$(S3_SECRET_KEY)
            - --bucket=caelum
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 4222
              name: nats
          env:
            - name: S3_ENDPOINT
              valueFrom:
                configMapKeyRef:
                  name: caelum-config
                  key: s3-endpoint
            - name: S3_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: caelum-s3
                  key: access-key
            - name: S3_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: caelum-s3
                  key: secret-key
---
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: caelum-worker
spec:
  replicas: 3  # Scale as needed
  selector:
    matchLabels:
      app: caelum-worker
  template:
    metadata:
      labels:
        app: caelum-worker
    spec:
      containers:
        - name: worker
          image: ghcr.io/derekmwright/caelum:latest
          args:
            - serve
            - --mode=worker
            - --nats-url=nats://caelum-coordinator:4222
            - --cluster-id=central
            - --memory-budget=268435456
            - --result-store=134217728
            - --spill-dir=/tmp/caelum-spill
            - --endpoint=$(S3_ENDPOINT)
            - --access-key=$(S3_ACCESS_KEY)
            - --secret-key=$(S3_SECRET_KEY)
            - --bucket=caelum
          resources:
            requests:
              cpu: "2"
              memory: 2Gi
            limits:
              cpu: "4"
              memory: 4Gi
          env:
            - name: S3_ENDPOINT
              valueFrom:
                configMapKeyRef:
                  name: caelum-config
                  key: s3-endpoint
            - name: S3_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: caelum-s3
                  key: access-key
            - name: S3_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: caelum-s3
                  key: secret-key
---
# coordinator-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: caelum-coordinator
spec:
  selector:
    app: caelum-coordinator
  ports:
    - name: http
      port: 8080
    - name: nats
      port: 4222
```

## Distributed Query Execution Flow

```
1. Client sends SQL to coordinator
   │
2. Coordinator parses SQL → logical plan → physical plan
   │
3. For federated tables: ExpandFederatedScans splits scans per cluster
   │
4. Physical plan is split into stages with dependencies:
   │
   │  Stage 1a: Scan (cluster=central, partitions from central)
   │    ├── Task 1a: Scan partition date=2026-03-14  → Central Worker 1
   │    └── Task 1b: Scan partition date=2026-03-15  → Central Worker 2
   │
   │  Stage 1b: Scan (cluster=site-east, partitions from site-east)
   │    └── Task 1c: Scan partition date=2026-03-15  → Site-East Worker 1
   │
   │  Stage 2: Aggregate (depends on Stage 1a + 1b)
   │    ├── Task 2a: Partial aggregate chunk 1       → Central Worker 1
   │    └── Task 2b: Partial aggregate chunk 2       → Central Worker 2
   │
   │  Stage 3: Final merge (depends on Stage 2)
   │    └── Task 3a: Merge + sort + limit            → Central Worker 1
   │
5. Workers write intermediate results to result store (or S3 if store full)
   │
6. Coordinator reads final result and returns to client
```

## Task Types

| Type | Description | Parallelism |
|------|-------------|-------------|
| `scan` | Read Parquet files, apply filters | Per-partition or per-file |
| `aggregate` | Group-by with aggregate functions | Per-chunk of scan results |
| `join` | Hash join between two datasets | Per-partition of probe side |
| `sort` | Sort intermediate results | Single task (pipeline breaker) |
| `window` | Window functions (ROW_NUMBER, RANK, etc.) | Per-partition |

All task types respect per-task memory budgets. Sort, aggregate, and window tasks spill to disk when their memory budget is exceeded.

## Worker Tuning

### Concurrency

Each worker defaults to processing 4 tasks concurrently. For CPU-heavy workloads (complex aggregates), reduce this. For I/O-heavy workloads (large scans from S3), increase it.

### Memory Budget and Spill-to-Disk

Workers can be configured with a per-task memory budget (`--memory-budget`). When operators (Sort, HashAggregate, Window) exceed the budget, they spill intermediate state to disk instead of growing memory unboundedly. This makes workers viable at 512 MB - 2 GB RAM.

Set `--spill-dir` to a fast local disk (SSD/NVMe preferred) for best spill performance. Spill files are cleaned up automatically after each task completes.

### In-Memory Result Store

Enable `--result-store` to cache intermediate stage results in memory, avoiding S3 round-trips between stages that execute on the same worker. This is the single biggest optimization for multi-stage query latency.

Results below 64 KB are always passed inline via NATS messages regardless of result store configuration.

### LRU Cache

Workers cache recently-read Parquet file data in an LRU cache (256 MB default). This benefits:

- Repeated queries against the same time range
- Join probes that reference the same build-side data
- Interactive exploration (user refining queries)

### Resource Sizing

| Workload | CPU | Memory | Workers | Recommended Flags |
|----------|-----|--------|---------|-------------------|
| Small (< 10 GB/day) | 2 cores | 1 GB | 1–2 | `--memory-budget 67108864 --result-store 16777216` |
| Medium (10–100 GB/day) | 4 cores | 2–4 GB | 3–5 | `--memory-budget 268435456 --result-store 134217728` |
| Large (100+ GB/day) | 8 cores | 8+ GB | 5–10+ | `--memory-budget 1073741824 --result-store 1073741824` |

For detailed tuning guidance, see [Performance Tuning](tuning.md).

## Fault Tolerance

- **Worker crash**: The coordinator detects missed heartbeats and reaps the worker. Tasks assigned to the dead worker time out and are redelivered by JetStream to surviving workers.
- **Coordinator crash**: All in-flight queries fail. Restart the coordinator; workers reconnect automatically. No data is lost (all state is on S3).
- **S3 unavailability**: Queries and ingestion fail with storage errors. Resume when S3 recovers — catalog uses ETags so no corruption occurs.
- **NATS partition**: Workers lose connection and stop receiving tasks. They reconnect automatically when the partition heals.

## Federation

Federation allows Caelum to query data spread across multiple clusters — for example, a central data center and multiple regional sites.

### Architecture

```
                     ┌─────────────────┐
                     │   Coordinator    │
                     │   (central)      │
                     │                  │
                     │  Embedded NATS   │
                     └───────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────────┐
        │ Worker 1  │  │ Worker 2  │  │ Remote NATS   │
        │ (central) │  │ (central) │  │ (site-east)   │
        │           │  │           │  │  Leaf Node     │
        └──────────┘  └──────────┘  └──────┬───────┘
                                            │
                                     ┌──────▼───────┐
                                     │   Worker 3    │
                                     │  (site-east)  │
                                     └──────────────┘
```

### How It Works

1. Each cluster has a unique `--cluster-id` (e.g., `central`, `site-east`)
2. Workers subscribe to cluster-scoped NATS subjects: `caelum.tasks.<cluster-id>.>`
3. The catalog stores per-cluster partition manifests
4. When a table exists on multiple clusters, the coordinator's `ExpandFederatedScans` splits scan stages per-cluster
5. Each cluster's workers scan their local data; results flow back for centralized aggregation

### Setup

**Central coordinator:**
```bash
./caelum serve --mode coordinator \
  --cluster-id central \
  --nats-port 4222 \
  --endpoint minio-central:9000 \
  --access-key $S3_KEY --secret-key $S3_SECRET --bucket caelum
```

**Central workers:**
```bash
./caelum serve --mode worker \
  --cluster-id central \
  --nats-url nats://coordinator:4222 \
  --memory-budget 268435456 --result-store 134217728 \
  --endpoint minio-central:9000 \
  --access-key $S3_KEY --secret-key $S3_SECRET --bucket caelum
```

**Remote site workers (with leaf node connection):**
```bash
./caelum serve --mode worker \
  --cluster-id site-east \
  --nats-url nats://coordinator:4222 \
  --leaf-remote nats://coordinator:4222 \
  --memory-budget 67108864 --result-store 16777216 \
  --endpoint minio-east:9000 \
  --access-key $S3_KEY --secret-key $S3_SECRET --bucket caelum
```

### NATS Subject Routing

Tasks are published to cluster-scoped subjects:

```
caelum.tasks.<cluster-id>.<type>.<query-id>.<stage-id>
```

Workers subscribe to their cluster's wildcard filter:

```
caelum.tasks.<cluster-id>.>
```

This ensures workers only pull tasks for data in their local object store, avoiding cross-cluster data transfers for scan operations.

## Standalone vs. Distributed

| Aspect | Standalone | Distributed | Federated |
|--------|-----------|-------------|-----------|
| Processes | 1 | 1 coordinator + N workers | 1 coordinator + N workers across clusters |
| NATS | Embedded (in-process) | Embedded in coordinator | Coordinator + leaf nodes |
| Query parallelism | Single-process pipeline | Multi-node task distribution | Multi-cluster task routing |
| Best for | Development, small datasets | Production, large datasets | Geo-distributed data |
| Scaling | Vertical only | Horizontal (add workers) | Horizontal + geographic |
