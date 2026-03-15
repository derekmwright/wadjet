# Configuration

Caelum is configured through CLI flags, environment variables, and an optional YAML configuration file.

## CLI Flags

### `serve` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--mode` | Deployment mode: `standalone`, `coordinator`, `worker` | `standalone` |
| `--http-addr` | HTTP API listen address | `:8080` |
| `--endpoint` | S3-compatible endpoint (host:port) | `localhost:9000` |
| `--access-key` | S3 access key | required |
| `--secret-key` | S3 secret key | required |
| `--bucket` | S3 bucket name | `caelum` |
| `--nats-port` | Embedded NATS port (standalone/coordinator mode) | `4222` |
| `--nats-url` | NATS server URL (worker mode) | `nats://localhost:4222` |
| `--config` | Path to YAML config file | none |

### `query` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--endpoint` | S3-compatible endpoint | `localhost:9000` |
| `--access-key` | S3 access key | required |
| `--secret-key` | S3 secret key | required |
| `--bucket` | S3 bucket name | `caelum` |
| `--format` | Output format: `json`, `table`, `csv` | `json` |

Usage: `caelum query [flags] "SQL STATEMENT"`

### `tables` Command

Same S3 flags as `query`. Lists all tables in the catalog.

### `shell` Command

Same S3 flags as `query`. Opens an interactive SQL REPL.

| Flag | Description | Default |
|------|-------------|---------|
| `--format` | Output format: `table`, `json`, `csv` | `table` |

## YAML Configuration File

The YAML config file is used primarily for security settings and is hot-reloadable — changes take effect without restarting the server.

```yaml
# caelum.yaml

auth:
  enabled: true

  # API key definitions
  api_keys:
    - key: "caelum-key-abc123"
      name: "ingest-service"
      role: writer
    - key: "caelum-key-xyz789"
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

  # Cell-level security policies
  policies:
    - table: flow_logs
      role: reader
      columns:
        src_ip: "***MASKED***"           # Column masking: column -> mask value
      row_filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%'"
```

## Hot Reload

The configuration file is watched for changes via filesystem notifications. When the file is modified:

1. The new config is parsed and validated
2. Auth providers are updated atomically
3. Active connections continue with their existing credentials
4. New requests use the updated configuration

You can subscribe to config changes programmatically:

```go
cfg := config.LoadOrDefault("/path/to/caelum.yaml")
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
| `AWS_ACCESS_KEY_ID` | `--access-key` |
| `AWS_SECRET_ACCESS_KEY` | `--secret-key` |
| `AWS_ENDPOINT_URL_S3` | `--endpoint` |
