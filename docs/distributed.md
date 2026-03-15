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
4. Publishes tasks to the NATS `tasks` stream
5. Waits for result notifications from workers
6. Merges partial results into a final response

The coordinator also embeds a NATS server (no external NATS required).

### Workers

Workers are stateless compute nodes that:

1. Connect to the coordinator's NATS server
2. Subscribe to the `tasks` JetStream consumer
3. Pull and execute tasks (scan, aggregate, join, sort)
4. Read data from S3 (with LRU caching)
5. Write intermediate results to S3
6. Publish completion notifications
7. Send heartbeats every 10 seconds

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
  --listen :8080 \
  --nats-port 4222 \
  --s3-endpoint minio.internal:9000 \
  --s3-access-key $S3_ACCESS_KEY \
  --s3-secret-key $S3_SECRET_KEY \
  --s3-bucket caelum \
  --config caelum.yaml
```

### Start Workers

On each worker node:

```bash
./caelum serve \
  --mode worker \
  --nats-url nats://coordinator.internal:4222 \
  --s3-endpoint minio.internal:9000 \
  --s3-access-key $S3_ACCESS_KEY \
  --s3-secret-key $S3_SECRET_KEY \
  --s3-bucket caelum
```

Workers automatically register with the coordinator and begin pulling tasks.

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
      --listen :8080 --nats-port 4222
      --s3-endpoint minio:9000
      --s3-access-key minioadmin --s3-secret-key minioadmin
      --s3-bucket caelum
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
      --s3-endpoint minio:9000
      --s3-access-key minioadmin --s3-secret-key minioadmin
      --s3-bucket caelum
    depends_on:
      - coordinator

  worker-2:
    build: .
    command: >
      serve --mode worker
      --nats-url nats://coordinator:4222
      --s3-endpoint minio:9000
      --s3-access-key minioadmin --s3-secret-key minioadmin
      --s3-bucket caelum
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
            - --listen=:8080
            - --nats-port=4222
            - --s3-endpoint=$(S3_ENDPOINT)
            - --s3-access-key=$(S3_ACCESS_KEY)
            - --s3-secret-key=$(S3_SECRET_KEY)
            - --s3-bucket=caelum
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
            - --s3-endpoint=$(S3_ENDPOINT)
            - --s3-access-key=$(S3_ACCESS_KEY)
            - --s3-secret-key=$(S3_SECRET_KEY)
            - --s3-bucket=caelum
          resources:
            requests:
              cpu: "2"
              memory: 1Gi
            limits:
              cpu: "4"
              memory: 2Gi
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
3. Physical plan is split into stages with dependencies:
   │
   │  Stage 1: Scan (parallel across partitions)
   │    ├── Task 1a: Scan partition date=2026-03-14  → Worker 1
   │    ├── Task 1b: Scan partition date=2026-03-15  → Worker 2
   │    └── Task 1c: Scan partition date=2026-03-16  → Worker 3
   │
   │  Stage 2: Aggregate (depends on Stage 1)
   │    ├── Task 2a: Partial aggregate chunk 1       → Worker 1
   │    └── Task 2b: Partial aggregate chunk 2       → Worker 2
   │
   │  Stage 3: Final merge (depends on Stage 2)
   │    └── Task 3a: Merge + sort + limit            → Worker 1
   │
4. Workers write intermediate results to S3
   │
5. Coordinator reads final result from S3 and returns to client
```

## Task Types

| Type | Description | Parallelism |
|------|-------------|-------------|
| `scan` | Read Parquet files, apply filters | Per-partition or per-file |
| `aggregate` | Group-by with aggregate functions | Per-chunk of scan results |
| `join` | Hash join between two datasets | Per-partition of probe side |
| `sort` | Sort intermediate results | Single task (pipeline breaker) |

## Worker Tuning

### Concurrency

Each worker defaults to processing 4 tasks concurrently. For CPU-heavy workloads (complex aggregates), reduce this. For I/O-heavy workloads (large scans from S3), increase it.

### LRU Cache

Workers cache recently-read Parquet file data in an LRU cache (256 MB default). This benefits:

- Repeated queries against the same time range
- Join probes that reference the same build-side data
- Interactive exploration (user refining queries)

### Resource Sizing

| Workload | CPU | Memory | Workers |
|----------|-----|--------|---------|
| Small (< 10 GB/day) | 2 cores | 1 GB | 1–2 |
| Medium (10–100 GB/day) | 4 cores | 2 GB | 3–5 |
| Large (100+ GB/day) | 8 cores | 4 GB | 5–10+ |

## Fault Tolerance

- **Worker crash**: The coordinator detects missed heartbeats and reaps the worker. Tasks assigned to the dead worker time out and are redelivered by JetStream to surviving workers.
- **Coordinator crash**: All in-flight queries fail. Restart the coordinator; workers reconnect automatically. No data is lost (all state is on S3).
- **S3 unavailability**: Queries and ingestion fail with storage errors. Resume when S3 recovers — catalog uses ETags so no corruption occurs.
- **NATS partition**: Workers lose connection and stop receiving tasks. They reconnect automatically when the partition heals.

## Standalone vs. Distributed

| Aspect | Standalone | Distributed |
|--------|-----------|-------------|
| Processes | 1 | 1 coordinator + N workers |
| NATS | Embedded (in-process) | Embedded in coordinator |
| Query parallelism | Single-process pipeline | Multi-node task distribution |
| Best for | Development, small datasets | Production, large datasets |
| Scaling | Vertical only | Horizontal (add workers) |
