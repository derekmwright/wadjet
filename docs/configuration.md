# Configuration

Wadjet is configured through CLI flags, environment variables, and an optional YAML configuration file.

## CLI Flags

### `serve` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--mode` | Deployment mode: `standalone`, `coordinator`, `worker` | `standalone` |
| `--http-addr` | HTTP API listen address | `:8080` |
| `--grpc-addr` | gRPC API listen address | `:9090` |
| `--storage-type` | Storage backend: `s3` or `file` | `s3` |
| `--data-dir` | Local directory for `--storage-type file` | none |
| `--endpoint` | S3-compatible endpoint (host:port) | `localhost:9000` |
| `--access-key` | S3 access key | required for S3 |
| `--secret-key` | S3 secret key | required for S3 |
| `--bucket` | S3 bucket name | `wadjet` |
| `--nats-port` | Embedded NATS port (standalone/coordinator mode) | `4222` |
| `--nats-url` | NATS server URL (worker mode) | none |
| `--cluster-id` | Unique cluster identifier for federated routing | `local` |
| `--leaf-remote` | Remote NATS URLs for leaf node federation (repeatable) | none |
| `--memory-budget` | Per-task memory budget in bytes (0 = auto-detect from cgroup, else unlimited) | `0` |
| `--spill-dir` | Directory for spill-to-disk files | OS temp dir |
| `--result-store` | In-memory result store capacity in bytes (0 = disabled) | `512 MiB` |
| `--pg-addr` | PostgreSQL wire protocol listen address | `:5433` |
| `--metrics-addr` | Prometheus metrics listen address (worker mode only) | `:9100` |
| `--max-concurrent` | Maximum concurrent tasks per worker | `4` |
| `--cache-bytes` | LRU file cache size in bytes (0 = auto: 20% of memory) | `0` |
| `--query-timeout` | Default query timeout (`30s`, `5m`, `0` = unlimited) | `0` |
| `--morsel-workers` | Intra-fragment parallel consumers (0 = auto, 1 = serial) | `0` |
| `--local-fastpath-bytes` | Queries under this post-pruning scan size run in-process on the coordinator (0 = disabled) | `64 MiB` |
| `--shuffle-durability` | Stage-output durability: `eager`, `lazy`, `off` | `eager` |
| `--skew-split` | Adaptive skew-aware shuffle layout | `true` |
| `--drain-timeout` | Bound on graceful worker drain (0 = unbounded) | `0` |
| `--config` | Path to YAML config file | none |

### `query` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--endpoint` | S3-compatible endpoint | `localhost:9000` |
| `--access-key` | S3 access key | required |
| `--secret-key` | S3 secret key | required |
| `--bucket` | S3 bucket name | `wadjet` |
| `--format` | Output format: `json`, `table`, `csv` | `json` |

Usage: `wadjet query [flags] "SQL STATEMENT"`

### `tables` Command

Same S3 flags as `query`. Lists all tables in the catalog.

### `shell` Command

Same S3 flags as `query`. Opens an interactive SQL REPL.

| Flag | Description | Default |
|------|-------------|---------|
| `--format` | Output format: `table`, `json`, `csv` | `table` |

## YAML Configuration File

The YAML config file controls all aspects of Wadjet's configuration. Security settings are hot-reloadable — changes take effect without restarting the server.

```yaml
# wadjet.yaml

mode: standalone  # standalone, coordinator, worker

storage:
  type: s3                    # s3 or file
  data_dir: ""                # local directory for type=file
  endpoint: "localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  bucket: "wadjet"
  use_ssl: false
  region: ""

nats:
  port: 4222
  url: ""                     # worker mode: coordinator's NATS URL
  store_dir: "/tmp/wadjet-nats"
  cluster_id: "local"         # unique cluster ID for federation
  leaf_remotes: []            # remote NATS URLs for leaf node connections

http:
  addr: ":8080"

grpc:
  addr: ":9090"

worker:
  max_concurrent: 4
  cache_bytes: 268435456      # 256 MB LRU cache
  memory_budget: 0            # per-task memory budget (0 = unlimited, no spill)
  spill_dir: ""               # spill directory (default: OS temp dir)
  result_store_bytes: 0       # in-memory result store (0 = disabled, use S3)

parquet:
  compression: "snappy"       # snappy, zstd, gzip, lz4, none
  row_group_size: 131072      # 128K rows per row group
  page_buffer_size: 262144    # 256 KB page buffer

auth:
  enabled: true

  # API key definitions
  api_keys:
    - key: "wadjet-key-abc123"
      name: "ingest-service"
      role: writer
    - key: "wadjet-key-xyz789"
      name: "analytics-dashboard"
      role: reader

  # JWT settings
  jwt:
    enabled: true
    secret: "your-256-bit-secret"       # HMAC secret
    # public_key_file: /path/to/pub.pem # RSA public key (alternative to secret)
    role_claim: role                     # JWT claim containing role name
    issuer: ""                           # Expected issuer (optional)

  # mTLS settings
  mtls:
    enabled: false
    ca_file: /path/to/ca.pem
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    role_map:                            # Map certificate CN to role
      "CN=ingest-service": writer
      "CN=dashboard": reader
    default_role: reader                 # Fallback role if CN not in map

  # Role definitions
  roles:
    - name: reader
      tables: ["*"]
      allow: [read]
    - name: writer
      tables: [flow_logs, syslog, device_inventory]
      allow: [read, write]
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]

  # Cell-level security policies (legacy RBAC)
  policies:
    - table: flow_logs
      role: reader
      columns:
        src_ip: mask                     # action: allow | mask | deny — anything
                                         # else and the server refuses to start
      row_filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%'"

  # ABAC policies (attribute-based access control)
  # When defined, these take precedence over RBAC roles.
  # When omitted, RBAC roles are auto-migrated to ABAC at startup.
  abac_policies:
    - name: classified-access
      description: "Clearance-based access to classified tables"
      priority: 10                       # lower = evaluated first
      # enabled: true                    # default true; set false to disable
      rules:
        - effect: allow
          conditions:
            - attribute: subject.clearance
              operator: in
              value: "TOP_SECRET,SECRET"
            - attribute: resource.name
              operator: eq
              value: classified_events
          obligations:
            - type: row_filter
              target: classified_events
              value: "classification_level <= 2"
            - type: mask_column
              target: ssn
              value: "***REDACTED***"
        - effect: deny
          conditions:
            - attribute: subject.role
              operator: eq
              value: contractor
            - attribute: resource.name
              operator: eq
              value: classified_events
```

## Hot Reload

The configuration file is watched for changes via filesystem notifications. When the file is modified:

1. The new config is parsed and validated
2. Auth providers are updated atomically
3. Active connections continue with their existing credentials
4. New requests use the updated configuration

You can subscribe to config changes programmatically:

```go
cfg := config.LoadOrDefault("/path/to/wadjet.yaml")
// Config is loaded once at startup; the auth Provider handles hot-reload
// by watching the file and swapping credentials atomically.
```

## Storage Tuning

These constants are compiled into the binary and represent the default ingestion and query tuning parameters. For production workloads, consider building with modified values:

### Ingestion Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| Flush size threshold | 128 MB | Flush partition buffer when accumulated data exceeds this |
| Flush row threshold | 1,000,000 | Flush partition buffer when row count exceeds this |
| Flush interval | 60 seconds | Maximum time before a non-empty buffer is flushed |
| Parquet row group size | 131,072 rows (128 K) | Rows per row group in written Parquet files |
| Parquet page buffer | 256 KB | Page buffer size for Parquet writer |
| Default compression | Snappy | Compression codec for Parquet files |

### Query Execution Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| Batch size | 2,048 rows | Rows per record batch during execution |
| Worker concurrency | 4 tasks | Max concurrent tasks per worker |
| Worker cache size | 256 MB | LRU cache for recently-read Parquet data |
| Memory budget | 0 (auto-detect from cgroup, else unlimited) | Per-task memory limit before spilling to disk |
| Spill directory | OS temp dir | Where HashJoin/Aggregate/Sort/Window spill files are written |
| Result store | 512 MiB | In-memory result cache for intermediate stage results (0 = disabled) |
| Inline result threshold | 512 KB | Results below this are embedded in NATS messages |
| Heartbeat interval | 10 seconds | Worker heartbeat frequency |
| Batch pool max per class | `NumCPU() * 4`, clamped to 32-256 | Maximum pooled RecordBatches per schema/size class |

For detailed tuning guidance by hardware profile, see [Performance Tuning](tuning.md).

## Query Limits

Cost-based query guards prevent expensive queries from consuming excessive resources. Limits are checked at plan time using manifest metadata — no I/O occurs before validation.

### Global Limits

```yaml
query_limits:
  max_scan_bytes: 107374182400       # 100 GB
  max_scan_rows: 1000000000          # 1 billion rows
  max_scan_files: 10000              # 10,000 files
  require_filter_above_bytes: 10737418240  # Require WHERE for scans > 10 GB
  require_limit_above_rows: 100000000      # Require LIMIT for scans > 100M rows
```

### Per-Role Limits

Override global limits for specific roles by adding `query_limits` to the role definition:

```yaml
auth:
  roles:
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]
      # No query_limits = unlimited

    - name: analyst
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_bytes: 10737418240   # 10 GB per query
        max_scan_rows: 100000000      # 100M rows per query

    - name: viewer
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_bytes: 1073741824    # 1 GB per query
        require_filter_above_bytes: 0 # Always require WHERE clause
```

Per-role limits fully override global limits when defined. See [Security](security.md#query-cost-estimation-and-guards) for details.

## Environment Variables

A subset of the configuration can be overridden via `WADJET_*` environment variables — the ones listed below, and no others (`internal/config/config.go`, `applyEnvOverrides`). This is useful for container and cloud deployments where config files may not be practical.

### S3/Storage

| Variable | Maps To | Description |
|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `--access-key` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | `--secret-key` | S3 secret key |
| `WADJET_STORAGE_ENDPOINT` | `--endpoint` | S3 endpoint |
| `WADJET_STORAGE_BUCKET` | `--bucket` | S3 bucket name |
| `WADJET_STORAGE_TYPE` | `--storage-type` | `s3` or `file` |
| `WADJET_STORAGE_ACCESS_KEY` | `--access-key` | S3 access key |
| `WADJET_STORAGE_SECRET_KEY` | `--secret-key` | S3 secret key |
| `WADJET_STORAGE_REGION` | `--region` | S3 region |
| `WADJET_STORAGE_USE_SSL` | `--ssl` | TLS for S3 connections |

### Server

| Variable | Description |
|----------|-------------|
| `WADJET_HTTP_ADDR` | HTTP listen address |
| `WADJET_GRPC_ADDR` | gRPC listen address |
| `WADJET_MODE` | Deployment mode (`standalone`, `coordinator`, `worker`) |

There is no environment variable for the connection cap, the slow-query threshold or the shutdown timeout; use `--drain-timeout` for the drain bound.

### NATS

| Variable | Description |
|----------|-------------|
| `WADJET_NATS_PORT` | Embedded NATS port |
| `WADJET_NATS_URL` | NATS server URL (worker mode) |
| `WADJET_NATS_CLUSTER_ID` | Cluster identifier for federation |
| `WADJET_NATS_LEAF_REMOTES` | Comma-separated remote NATS URLs for leaf nodes |
| `WADJET_NATS_TLS_CERT` / `_KEY` / `_CA` | NATS mTLS material |

### Worker

| Variable | Description |
|----------|-------------|
| `WADJET_WORKER_MAX_CONCURRENT` | Max concurrent tasks per worker |
| `WADJET_WORKER_MEMORY_BUDGET` | Per-task memory budget |
| `WADJET_WORKER_SPILL_DIR` | Spill directory |

The LRU cache size and the result-store capacity have no environment variable; use `--cache-bytes` and `--result-store`.

### Query Limits

| Variable | Description |
|----------|-------------|
| `WADJET_QUERY_MAX_SCAN_BYTES` | Max bytes to scan per query |
| `WADJET_QUERY_MAX_SCAN_ROWS` | Max rows to scan per query |
| `WADJET_QUERY_MAX_SCAN_FILES` | Max files to scan per query |

### Telemetry and GeoIP

| Variable | Description |
|----------|-------------|
| `WADJET_OTEL_ENDPOINT` | OTLP gRPC endpoint for trace export |
| `WADJET_OTEL_INSECURE` | Plaintext gRPC for the OTLP exporter |
| `WADJET_OTEL_SAMPLE_RATE` | Trace sampling rate (0.0-1.0) |
| `WADJET_GEOIP_CITY_DB` | Path to GeoLite2-City.mmdb |
| `WADJET_GEOIP_ASN_DB` | Path to GeoLite2-ASN.mmdb |

Wadjet has no built-in rate limiter; place a reverse proxy in front of it to
enforce per-client limits.
