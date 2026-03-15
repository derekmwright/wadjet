# Configuration

Caelum is configured through CLI flags, environment variables, and an optional YAML configuration file.

## CLI Flags

### `serve` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--mode` | Deployment mode: `standalone`, `coordinator`, `worker` | `standalone` |
| `--listen` | HTTP listen address | `:8080` |
| `--s3-endpoint` | S3-compatible endpoint (host:port) | required |
| `--s3-access-key` | S3 access key | required |
| `--s3-secret-key` | S3 secret key | required |
| `--s3-bucket` | S3 bucket name | required |
| `--nats-port` | Embedded NATS port (standalone/coordinator mode) | `4222` |
| `--nats-url` | NATS server URL (worker mode) | `nats://localhost:4222` |
| `--config` | Path to YAML config file | none |

### `query` Command

| Flag | Description |
|------|-------------|
| `--s3-endpoint` | S3-compatible endpoint |
| `--s3-access-key` | S3 access key |
| `--s3-secret-key` | S3 secret key |
| `--s3-bucket` | S3 bucket name |

Usage: `caelum query [flags] "SQL STATEMENT"`

### `tables` Command

Same S3 flags as `query`. Lists all tables in the catalog.

### `shell` Command

Same S3 flags as `query`. Opens an interactive SQL REPL.

## YAML Configuration File

The YAML config file is used primarily for security settings and is hot-reloadable — changes take effect without restarting the server.

```yaml
# caelum.yaml

auth:
  # Authentication method: "apikey", "jwt", or "mtls"
  method: jwt

  jwt:
    # Signing method: "hmac" or "rsa"
    signing_method: hmac
    # HMAC secret (for hmac signing)
    secret: "your-256-bit-secret"
    # RSA public key path (for rsa signing)
    # public_key: /path/to/public.pem
    # Claims field containing the subject identity
    subject_claim: sub

  # API key definitions (for apikey auth)
  api_keys:
    - key: "caelum-key-abc123"
      identity: "ingest-service"
      roles:
        - writer
    - key: "caelum-key-xyz789"
      identity: "analytics-dashboard"
      roles:
        - reader

  # mTLS settings (for mtls auth)
  mtls:
    ca_cert: /path/to/ca.pem
    # Extract identity from certificate field
    identity_field: common_name  # or "serial", "dns_san"

# Role-based access control
roles:
  reader:
    tables:
      "*":
        permissions:
          - read
  writer:
    tables:
      flow_logs:
        permissions:
          - read
          - write
      device_inventory:
        permissions:
          - read
          - write
  admin:
    tables:
      "*":
        permissions:
          - read
          - write
          - admin

# Cell-level security policies
policies:
  - name: mask-source-ip
    description: "Mask source IPs for non-admin users"
    table: flow_logs
    type: column_mask
    column: src_ip
    mask_value: "***MASKED***"
    applies_to:
      roles:
        - reader

  - name: filter-internal-only
    description: "Readers can only see internal network traffic"
    table: flow_logs
    type: row_filter
    filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%'"
    applies_to:
      roles:
        - reader
```

## Hot Reload

The configuration file is watched for changes via filesystem notifications. When the file is modified:

1. The new config is parsed and validated
2. Auth providers are updated atomically
3. Active connections continue with their existing credentials
4. New requests use the updated configuration

You can subscribe to config changes programmatically:

```go
configMgr := config.NewManager("/path/to/caelum.yaml")
configMgr.Subscribe(func(cfg *config.Config) {
    log.Println("Config reloaded")
})
```

## Storage Tuning

These constants are compiled into the binary and represent the default ingestion and query tuning parameters. For production workloads, consider building with modified values:

### Ingestion Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| Flush size threshold | 128 MB | Flush partition buffer when accumulated data exceeds this |
| Flush row threshold | 1,000,000 | Flush partition buffer when row count exceeds this |
| Flush interval | 60 seconds | Maximum time before a non-empty buffer is flushed |
| Parquet row group size | 128,000 rows | Rows per row group in written Parquet files |
| Parquet page buffer | 256 KB | Page buffer size for Parquet writer |
| Default compression | Snappy | Compression codec for Parquet files |

### Query Execution Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| Batch size | 2,048 rows | Rows per record batch during execution |
| Worker concurrency | 4 tasks | Max concurrent tasks per worker |
| Worker cache size | 256 MB | LRU cache for recently-read Parquet data |
| Heartbeat interval | 10 seconds | Worker heartbeat frequency |

## Environment Variables

S3 credentials can also be inherited from standard AWS environment variables when using AWS S3:

| Variable | Maps To |
|----------|---------|
| `AWS_ACCESS_KEY_ID` | `--s3-access-key` |
| `AWS_SECRET_ACCESS_KEY` | `--s3-secret-key` |
| `AWS_ENDPOINT_URL_S3` | `--s3-endpoint` |
