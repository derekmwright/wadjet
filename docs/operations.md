# Operations

This guide covers monitoring, metrics, troubleshooting, and operational best practices for running Wadjet in production.

## Prometheus Metrics

Wadjet exposes Prometheus metrics at `GET /metrics`.

### Available Metrics

**Query metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `wadjet_queries_total` | Counter | `status` | Total queries executed, by status |
| `wadjet_query_duration_seconds` | Histogram | — | Query duration (buckets: 10ms–120s) |
| `wadjet_query_rows_scanned` | Counter | `table` | Rows scanned, by table |
| `wadjet_query_bytes_read` | Counter | — | Total bytes read from S3 |
| `wadjet_active_queries` | Gauge | — | Currently executing queries |

**Scanner metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `wadjet_files_scanned` | Counter | Parquet files read |
| `wadjet_files_pruned` | Counter | Parquet files skipped (pruning) |
| `wadjet_row_groups_scanned` | Counter | Row groups read |
| `wadjet_row_groups_pruned` | Counter | Row groups skipped |
| `wadjet_partitions_scanned` | Counter | Partitions read |
| `wadjet_partitions_pruned` | Counter | Partitions skipped (partition pruning) |

**Worker metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `wadjet_worker_tasks_total` | Counter | `type`, `status` | Tasks processed by type and outcome |
| `wadjet_worker_task_duration_seconds` | Histogram | `type` | Task duration by type (buckets: 10ms–60s) |
| `wadjet_worker_active_tasks` | Gauge | — | Currently active task slots |
| `wadjet_worker_memory_bytes` | Gauge | — | Worker memory usage |
| `wadjet_worker_spill_events_total` | Counter | — | Spill-to-disk events (Sort, Aggregate, Window) |
| `wadjet_worker_spill_bytes_written_total` | Counter | — | Total bytes written to spill files |
| `wadjet_worker_memory_budget_bytes` | Gauge | — | Configured per-task memory budget |
| `wadjet_worker_memory_used_bytes` | Gauge | — | Current tracked memory usage |

**Coordinator metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `wadjet_registered_workers` | Gauge | Workers with recent heartbeats |

**Pipeline metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `wadjet_batches_processed` | Counter | Record batches processed through pipeline |
| `wadjet_rows_output` | Counter | Total rows returned to clients |

**Cache metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `wadjet_cache_hits` | Counter | LRU cache hits |
| `wadjet_cache_misses` | Counter | LRU cache misses |
| `wadjet_cache_bytes` | Gauge | Current cache size in bytes |

### Prometheus Scrape Config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: wadjet
    scrape_interval: 15s
    static_targets:
      - targets:
          - wadjet.internal:8080
    metrics_path: /metrics
```

### Grafana Dashboard Queries

**Query rate (queries/sec):**
```promql
rate(wadjet_queries_total[5m])
```

**Average query latency:**
```promql
rate(wadjet_query_duration_seconds_sum[5m]) / rate(wadjet_query_duration_seconds_count[5m])
```

**P99 query latency:**
```promql
histogram_quantile(0.99, rate(wadjet_query_duration_seconds_bucket[5m]))
```

**Scan throughput (rows/sec):**
```promql
rate(wadjet_query_rows_scanned_total[5m])
```

**Spill rate (events/sec):**
```promql
rate(wadjet_worker_spill_events_total[5m])
```

**Spill volume (bytes/sec):**
```promql
rate(wadjet_worker_spill_bytes_written_total[5m])
```

**Memory utilization vs budget:**
```promql
wadjet_worker_memory_used_bytes / wadjet_worker_memory_budget_bytes
```

**Cache hit ratio:**
```promql
rate(wadjet_cache_hits_total[5m]) / (rate(wadjet_cache_hits_total[5m]) + rate(wadjet_cache_misses_total[5m]))
```

For a complete tuning methodology using these metrics, see [Performance Tuning](tuning.md).

## Health Checking

### HTTP Health Endpoint

```bash
curl http://localhost:8080/v1/health
# {"status": "ok"}
```

For load balancers and orchestrators, configure health checks against this endpoint:

**Kubernetes liveness probe:**
```yaml
livenessProbe:
  httpGet:
    path: /v1/health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15
```

**Kubernetes readiness probe:**
```yaml
readinessProbe:
  httpGet:
    path: /v1/health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Troubleshooting

### Query Returns No Results

1. **Check the table exists:**
   ```bash
   curl http://localhost:8080/v1/tables | jq .
   ```

2. **Check the table schema matches your query:**
   ```bash
   curl http://localhost:8080/v1/tables/flow_logs | jq .
   ```

3. **Check partitions have data:** Verify Parquet files exist on S3 for the partition you're querying:
   ```bash
   mc ls local/wadjet/data/flow_logs/date=2026-03-15/
   ```

4. **Check your WHERE clause:** Partition pruning requires exact matches on partition keys. If you're filtering by `date = '2026-03-15'`, ensure data exists for that date.

### Query Is Slow

1. **Check rows scanned:** The `stats.rows_scanned` field in the query response tells you how much data was read. If it's unexpectedly high:
   - Add a partition key filter (e.g., `WHERE date = '...'`) to enable partition pruning
   - Narrow your time range
   - Add more selective WHERE conditions

2. **Check for excessive spilling:** If `wadjet_worker_spill_events_total` is growing rapidly, the memory budget is too tight. Increase `--memory-budget` or reduce `--max-concurrent` to give each task more headroom.

3. **Check result store usage:** If multi-stage queries are slow, enable the in-memory result store with `--result-store` to avoid S3 round-trips between stages. See [Performance Tuning](tuning.md) for sizing guidance.

4. **Check file sizes:** Many small files (< 1 MB) cause high S3 LIST/GET overhead. Re-tune Bento's batch size to produce larger files (64–256 MB target).

5. **Check cache hit ratio:** A low cache hit ratio means workers are re-reading data from S3. Increase `worker.cache_bytes` to hold more of the working set.

6. **Check worker count:** In distributed mode, add more workers for parallelism:
   ```bash
   # Check worker count
   curl http://localhost:8080/v1/health | jq .
   ```

7. **Check join order:** For JOINs, place the smaller table on the right side (it becomes the hash table build side).

### Ingestion Not Appearing in Queries

1. **Flush delay:** The ingester buffers data for up to 60 seconds before flushing. Wait for a flush cycle or call `FlushAll()` explicitly.

2. **Catalog update:** After Bento writes new Parquet files, the catalog manifest must include them. If using Bento's direct S3 output, you need to manually update the catalog or use Wadjet's ingester which handles this automatically.

3. **Schema mismatch:** If the Parquet schema doesn't match the registered table schema, reads may fail silently. Verify schemas match:
   ```bash
   # Check what Bento writes
   mc cat local/wadjet/data/flow_logs/date=2026-03-15/part-xyz.parquet | parquet-tools schema
   ```

### Worker Not Connecting

1. **Check NATS connectivity:**
   ```bash
   # From worker node
   nats server check connection --server nats://coordinator:4222
   ```

2. **Check firewall rules:** NATS uses port 4222 by default. Ensure it's open between coordinator and workers.

3. **Check logs:** Worker logs will show connection errors and retry attempts.

### Catalog Issues

The catalog uses NATS KV with revision-based optimistic concurrency, so corruption from concurrent writes shouldn't happen. However, if catalog data is lost (e.g., NATS storage wiped):

1. **Inspect the catalog via the API:**
   ```bash
   curl http://localhost:8080/v1/tables | jq .
   curl http://localhost:8080/v1/tables/flow_logs | jq .
   ```

2. **Rebuild from Parquet files:** The Parquet files on S3 are the source of truth. If the catalog is lost, re-register the tables and rebuild manifests by listing the Parquet files.

## Data Retention

Wadjet is append-only — there's no built-in DELETE or UPDATE. Manage data lifecycle through S3 policies:

### MinIO Lifecycle Policy

```json
{
  "Rules": [
    {
      "ID": "expire-old-data",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "data/"
      },
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
```

Apply it:

```bash
mc ilm import local/wadjet < lifecycle.json
```

### AWS S3 Lifecycle Policy

```json
{
  "Rules": [
    {
      "ID": "archive-old-data",
      "Status": "Enabled",
      "Filter": {"Prefix": "data/"},
      "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 90, "StorageClass": "GLACIER"}
      ],
      "Expiration": {"Days": 365}
    }
  ]
}
```

After S3 deletes old Parquet files, remove stale catalog entries by re-running table registration or building a cleanup script that prunes manifests referencing deleted files.

## Backup and Recovery

### What to Back Up

| Component | Location | Criticality |
|-----------|----------|-------------|
| Catalog (NATS KV) | NATS JetStream storage dir | High — needed to query data |
| Parquet data | `s3://wadjet/data/` | High — the actual data |
| Config YAML | `wadjet.yaml` | Medium — security/auth settings |
| Bento configs | `bento-*.yaml` | Medium — pipeline definitions |

### Catalog Backup

The catalog lives in NATS JetStream storage. Back up the NATS store directory:

```bash
# Snapshot the NATS JetStream data (includes catalog KV bucket)
tar czf /backup/wadjet-nats-$(date +%Y%m%d).tar.gz /var/wadjet/nats-store/
```

### Recovery

1. Restore the NATS JetStream store directory and restart the coordinator
2. Alternatively, re-register tables from Parquet files on S3 (the data is the source of truth)
3. Verify tables are visible: `curl http://localhost:8080/v1/tables`
4. Run a test query to confirm data access

## Resource Planning

### Object Storage

| Data Volume | Daily Ingest | 90-Day Storage (Snappy) |
|-------------|-------------|------------------------|
| Small | 1 GB/day | ~60 GB |
| Medium | 10 GB/day | ~600 GB |
| Large | 100 GB/day | ~6 TB |
| Very Large | 1 TB/day | ~60 TB |

Snappy compression typically achieves 2–4x ratio on network log data. Zstd achieves 4–8x.

### Compute

| Role | CPU | Memory | Storage | Key Flags |
|------|-----|--------|---------|-----------|
| Coordinator | 2 cores | 1 GB | Minimal (stateless) | — |
| Worker (constrained) | 1–2 cores | 512 MB–1 GB | 10 GB (spill) | `--memory-budget 67108864 --result-store 16777216` |
| Worker (moderate) | 2–4 cores | 2–4 GB | 20 GB (spill+cache) | `--memory-budget 268435456 --result-store 134217728` |
| Worker (unconstrained) | 4–8 cores | 8–16 GB | 50 GB (spill+cache) | `--memory-budget 1073741824 --result-store 1073741824` |

For detailed tuning by hardware profile, see [Performance Tuning](tuning.md).

### Network

Workers read from S3 during query execution. Size network bandwidth based on query patterns:

| Query Pattern | Network Requirement |
|---------------|-------------------|
| Small, filtered scans | 100 Mbps per worker |
| Large aggregations over days of data | 1 Gbps per worker |
| Joins on large tables | 1 Gbps per worker |

## Logging

Wadjet logs to stderr. In production, redirect to your log aggregation system:

```bash
# systemd
[Service]
ExecStart=/usr/local/bin/wadjet serve ...
StandardError=journal

# Docker
docker run ... wadjet serve ... 2>&1 | tee /var/log/wadjet/server.log

# Kubernetes
# Logs are automatically captured by the kubelet
kubectl logs -f deployment/wadjet-coordinator
```
