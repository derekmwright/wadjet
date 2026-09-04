# Configuration

Wadjet is configured through CLI flags, environment variables, and an optional YAML configuration file.

## Precedence

**An explicit flag beats an environment variable, which beats the config file, which beats the default.**

```
--flag  >  WADJET_*  >  wadjet.yaml  >  built-in default
```

A flag counts only when you actually type it. A flag's *default* never beats
an environment variable or a config-file value — that is PostgreSQL's rule
(`postgresql.conf` loses to `PGOPTIONS` loses to a session `SET`), and
[ADR-0029](adr/0029-configuration-precedence.md) records why Wadjet takes it.

Every setting is resolved once, before any command runs, and every part of
the process reads that one resolution. `GET /v1/admin/config` reports the
resolved value of every key together with the tier it came from.

Two conventions follow from the order:

- **An empty value never overrides a lower tier**, on any tier.
  `--nats-tls-cert=""` and `WADJET_NATS_TLS_CERT=` both read as *unset*, not
  as *explicitly blank*.
- **A config-file key written with its type's zero value reads as absent.**
  `bucket: ""` does not override; omit the key or give it a value.

An unreadable or unparseable `--config` file stops the process with the
reason, on every command. It is never skipped.

**An unrecognised key is an error too.** The config file is decoded strictly:
a key the schema does not define stops the process with that key's name.
`storage.buckett` and a misspelled `stroage:` section used to be ignored in
silence, which — now that the file tier actually reaches runtime — is the
difference between reading the right bucket and the wrong one with nothing
said at startup. PostgreSQL refuses an unrecognised parameter in
`postgresql.conf` for the same reason. The cost is forward compatibility: an
older binary refuses a file written for a newer one.

**A key with no runtime consumer is an error as well.** The `parquet:`
section is parsed and reported but reaches no writer (see below); setting it
stops the process naming the key, rather than accepting a value nothing
reads.

### What changed when the loader landed

Before this release the real order was *flag — even at its default — beats
the config file, which reached only `auth`, `query_limits` and `geoip`, with
environment variables reaching nothing at all*. Ten settings have a flag
whose default is not the zero value, so that default used to win silently.
They now lose to an environment variable or a config file that sets them:

| Setting | Flag (default) | Lower tier that now wins | Before | After |
|---|---|---|---|---|
| `mode` | `--mode` (`standalone`) | `WADJET_MODE`, `mode:` | flag default | env, else file |
| `http.addr` | `--http-addr` (`:8080`) | `WADJET_HTTP_ADDR`, `http.addr` | flag default | env, else file |
| `grpc.addr` | `--grpc-addr` (`:9090`) | `WADJET_GRPC_ADDR`, `grpc.addr` | flag default | env, else file |
| `storage.type` | `--storage-type` (`s3`) | `WADJET_STORAGE_TYPE`, `storage.type` | flag default | env, else file |
| `storage.endpoint` | `--endpoint` (`localhost:9000`) | `WADJET_STORAGE_ENDPOINT`, `storage.endpoint` | flag default | env, else file |
| `storage.bucket` | `--bucket` (`wadjet`) | `WADJET_STORAGE_BUCKET`, `storage.bucket` | flag default | env, else file |
| `nats.port` | `--nats-port` (`4222`) | `WADJET_NATS_PORT`, `nats.port` | flag default | env, else file |
| `nats.cluster_id` | `--cluster-id` (`local`) | `WADJET_NATS_CLUSTER_ID`, `nats.cluster_id` | flag default | env, else file |
| `worker.max_concurrent` | `--max-concurrent` (`4`) | `WADJET_WORKER_MAX_CONCURRENT`, `worker.max_concurrent` | flag default | env, else file |
| `worker.result_store_bytes` | `--result-store` (`512 MiB`) | `worker.result_store_bytes` | flag default | file |

Concretely: a unit that exports `WADJET_STORAGE_BUCKET=prod` without passing
`--bucket` moves from reading bucket `wadjet` to reading bucket `prod`. If
you set the same setting two ways today and relied on the flag default
winning, pass the flag explicitly.

`WADJET_ENABLE_ALERTS` changes in the other direction: it used to override
`--enable-alerts` unconditionally, and now an explicitly typed
`--enable-alerts=false` wins over it. Setting only the variable behaves as
before.

Fifteen more settings had a flag whose default *is* the zero value; for those
nothing changes except that the environment variable and the config file
start working. The `storage:`, `nats:`, `http:`, `grpc:`, `worker:` and
`telemetry:` sections of the config file reach runtime as of that
change.

`worker.result_store_bytes` gained one more behaviour: the automatic clamp to
15% of the memory envelope now applies only when the result store was left at
its default *everywhere*. Setting it in the config file is as explicit as
passing `--result-store`.

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
| `--cache-bytes` | LRU file cache size in bytes (0 = auto: 10% of the Go memory limit, ~7.5% of detected memory) | `0` |
| `--query-timeout` | Default query timeout (`30s`, `5m`, `0` = unlimited) | `0` |
| `--morsel-workers` | Intra-fragment parallel consumers (0 = auto, 1 = serial) | `0` |
| `--local-fastpath-bytes` | Queries under this post-pruning scan size run in-process on the coordinator (0 = disabled) | `64 MiB` |
| `--shuffle-durability` | Stage-output durability: `eager`, `lazy`, `off` | `eager` |
| `--skew-split` | Adaptive skew-aware shuffle layout | `true` |
| `--drain-timeout` | Bound on graceful worker drain (0 = unbounded) | `0` |
| `--storage-circuit-threshold` | Consecutive object-store failures **in one operation class** (read / write / delete) before that class's circuit breaker opens | `5` |
| `--storage-circuit-reset` | How long an open object-store breaker stays open before admitting one half-open probe | `30s` |
| `--storage-circuit-request-timeout` | Per-request object-store timeout the breaker applies to non-streaming operations (Head/List/Delete/BucketExists/MakeBucket) | `10s` |
| `--query-intermediate-ttl` | Age at which the periodic sweep reclaims a `queries/<id>/*` prefix the per-query cleanup missed (`serve` modes only — see below) | `1h` |
| `--query-intermediate-sweep` | How often that sweep runs | `10m` |
| `--config` | Path to YAML config file | none |

#### Object-store circuit breaker

The breaker keeps a dead object store from turning every query into a pile
of timeouts. It is scoped **by operation class** — reads (`Get`,
`GetReaderAt`, `Head`, `List`, `BucketExists`), writes (`Put`,
`PutIfMatch`, `MakeBucket`) and deletes each carry their own consecutive
failure counter and their own open/half-open state — so a failing scratch
cleanup or a throttled upload burst can never fast-fail a base-table read
(ADR-0028). A NotFound is a healthy answer and clears the counter; a
client-side `context.Canceled` is neutral.

`wadjet_circuit_breaker_opened_total{class}` counts transitions into the
open state, labelled `read`, `write` or `delete`. An operator seeing
`class="delete"` climbing is seeing scratch reclamation struggling, not a
read outage.

`--query-intermediate-ttl` and `--query-intermediate-sweep` reach the two
`serve` modes that build a coordinator (`standalone` and `coordinator`). A
coordinator constructed by the embedded API keeps the built-in 1-hour TTL
and 10-minute sweep; there is no library setting for them.

**These five settings are flags and defaults only.** The YAML file does not
reach `storage.*` or the coordinator's cleanup settings today (see
[Environment Variables](#environment-variables) for the same limitation on
the env layer); putting a `storage.circuit` block in the config file has no
effect.

### `query` Command

| Flag | Description | Default |
|------|-------------|---------|
| `--endpoint` | S3-compatible endpoint | `localhost:9000` |
| `--access-key` | S3 access key | required |
| `--secret-key` | S3 secret key | required |
| `--bucket` | S3 bucket name | `wadjet` |
| `--storage-type` | `s3` or `file` | `s3` |
| `--data-dir` | Local data directory (with `--storage-type=file`) | — |
| `--nats-url` / `--nats-port` | Where to find the catalog, if a server is running | `nats://127.0.0.1:4222` |
| `--nats-store-dir` | Catalog store directory when no server is running | `~/.wadjet/nats` |
| `--format` | Output format: `json`, `table`, `csv` | `json` |

Usage: `wadjet query [flags] "SQL STATEMENT"`

`query`, `create-table`, `drop-table`, `shell` and `tables` share ONE
persisted catalog with `serve`: they connect to a reachable server when there
is one, and otherwise open `--nats-store-dir` themselves for the length of the
command. One process holds that directory at a time; a second is refused with
a message naming `--nats-url`. See [Getting Started](getting-started.md#start-the-server).

### `tables` Command

Same flags as `query` (no `--format`). Lists all tables in the catalog.

### `shell` Command

Same flags as `query`. Opens an interactive SQL REPL.

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
  circuit:                    # object-store circuit breaker (ADR-0028)
    failure_threshold: 5      # consecutive failures in one class before it opens
    reset_timeout: 30s        # how long an open breaker stays open
    request_timeout: 10s      # per-request timeout for non-streaming operations

nats:
  port: 4222
  url: ""                     # worker mode: coordinator's NATS URL
  store_dir: "/tmp/wadjet-nats"
  cluster_id: "local"         # unique cluster ID for federation
  leaf_remotes: []            # remote NATS URLs for leaf node connections
  tls_cert: ""                # NATS mTLS certificate (see the note below)
  tls_key: ""                 # NATS mTLS private key
  tls_ca: ""                  # CA that verifies the peer

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

alerts:
  enabled: false              # CREATE ALERT DDL and the scheduler

query:
  intermediate_ttl: 1h        # reclaim age for a leftover queries/<id>/ prefix
  intermediate_sweep: 10m     # how often the coordinator sweeps queries/

# The `parquet:` section is DEFERRED and REFUSED. It has no runtime
# consumer: every ingest writer is built from ingest.DefaultConfig(), and
# reaching the writer needs wadjet.Config -> ingest.Config ->
# parquet.WriterConfig plumbing at seven call sites. Setting any of these
# keys stops the process naming the key, rather than accepting a value
# nothing reads. They stay in the schema (and in GET /v1/admin/config, with
# "reaches_runtime": false) so the refusal can name them; uncommenting this
# block will NOT start.
#
# parquet:
#   compression: "snappy"       # snappy, zstd, gzip, lz4, none
#   row_group_size: 131072      # 128K rows per row group
#   page_buffer_size: 262144    # 256 KB page buffer

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

The configuration file is watched for changes (modtime + size polling). When
the file is modified:

1. The new config is parsed and validated
2. Auth providers are updated atomically
3. Active connections continue with their existing credentials
4. New requests use the updated configuration

**`auth` is the hot-reloadable section.** A key is hot-reloadable only when
something in the running process re-reads it, and the auth provider is the
one subscriber there is. Everything else — `mode`, `storage`, `nats`, `http`,
`grpc`, `worker`, `parquet`, `telemetry`, `query`, `query_limits` — takes
effect at startup and is *preserved* across a reload, so the running
configuration and the reported configuration never disagree.

`PUT /v1/admin/config` and `PUT /v1/admin/tuning` follow the same rule: a
write to a key nothing consumes is refused with HTTP 409 naming the key,
rather than accepted with `{"status":"applied"}` while nothing changes.
Restart with the flag, the environment variable or the config file to change
those.

`GET /v1/admin/config` reports every key as

```json
"storage.bucket": {
  "value": "prod",
  "source": "env",
  "env": "WADJET_STORAGE_BUCKET",
  "flag": "--bucket",
  "hot_reloadable": false
}
```

`source` is one of `flag`, `env`, `file`, `default` or `admin`. Credentials
(`storage.access_key`, `storage.secret_key`) report their source with
`"redacted": true` and no value.

`POST /v1/admin/config/reload` re-reads the file. Keys it could not apply —
the startup-only ones — come back in the response as `not_applied`, and are
logged as a warning, rather than being dropped behind a bare `"reloaded"`.

**The admin API exists only when the server was started with `--config`**:
it is constructed alongside the auth provider, which the config file
defines. A process configured purely by flags and environment variables has
no `/v1/admin/config` to read its own resolution from.

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

These are read by the loader on every command, and they beat the config file
and the flag defaults (see [Precedence](#precedence)). An empty value reads
as unset. The tables below are the complete set — a test asserts that they
match the configuration registry exactly, so a variable that works is listed
here and a variable listed here works.

(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are read by the MinIO
credential chain rather than by this loader; they apply when
`storage.access_key` / `storage.secret_key` resolve to empty.)

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
| `WADJET_STORAGE_CIRCUIT_THRESHOLD` | `--storage-circuit-threshold` | Consecutive failures in one operation class before its breaker opens |
| `WADJET_STORAGE_CIRCUIT_RESET` | `--storage-circuit-reset` | How long an open breaker stays open (duration) |
| `WADJET_STORAGE_CIRCUIT_REQUEST_TIMEOUT` | `--storage-circuit-request-timeout` | Per-request object-store timeout (duration) |

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
| `WADJET_NATS_TLS_CERT` | NATS mTLS certificate file |
| `WADJET_NATS_TLS_KEY` | NATS mTLS private key file |
| `WADJET_NATS_TLS_CA` | CA certificate that verifies the peer (enables mTLS) |

#### NATS mTLS

`nats.tls_cert` / `nats.tls_key` / `nats.tls_ca` are resolved per field like
every other key: the CLI flag (`--nats-tls-cert` and friends) first, then the
environment variable, then the config file. A deployment may take the
certificate from a flag, the key from the environment and the CA from the
file.

**All three are required together.** The NATS connection is secured only
when the certificate, the private key AND the CA are all present, so naming
one or two of them used to disable TLS silently. A partial set is a startup
error naming what is missing, rather than a plaintext connection the
operator never hears about.

The rest of the `nats:` section — and `storage:`, `http:`, `grpc:`,
`worker:`, `telemetry:`, `query:`, `query_limits:`, `geoip:`, `alerts:` and
`auth:` — reaches runtime since the loader landed, in every run mode that
has the consumer. `telemetry:` reaches all three (`standalone` did not call
the OTLP initializer at all before this arc); `query:` is coordinator-side,
so it does nothing in `worker` mode.

The one exception is `parquet:`, which has no runtime consumer and is
REFUSED at startup rather than accepted — see the note in the YAML sample
above.

### Worker

| Variable | Description |
|----------|-------------|
| `WADJET_WORKER_MAX_CONCURRENT` | Max concurrent tasks per worker |
| `WADJET_WORKER_MEMORY_BUDGET` | Per-task memory budget |
| `WADJET_WORKER_SPILL_DIR` | Spill directory |

The LRU cache size and the result-store capacity have no environment
variable; use `--cache-bytes` / `--result-store`, or the `worker.cache_bytes`
/ `worker.result_store_bytes` config-file keys.

### Query Limits

| Variable | Description |
|----------|-------------|
| `WADJET_QUERY_MAX_SCAN_BYTES` | Max bytes to scan per query |
| `WADJET_QUERY_MAX_SCAN_ROWS` | Max rows to scan per query |
| `WADJET_QUERY_MAX_SCAN_FILES` | Max files to scan per query |
| `WADJET_QUERY_INTERMEDIATE_TTL` | Reclaim age for a leftover `queries/<id>/` prefix (duration) |
| `WADJET_QUERY_INTERMEDIATE_SWEEP` | How often the coordinator sweeps `queries/` (duration) |

### Telemetry and GeoIP

| Variable | Description |
|----------|-------------|
| `WADJET_OTEL_ENDPOINT` | OTLP gRPC endpoint for trace export |
| `WADJET_OTEL_INSECURE` | Plaintext gRPC for the OTLP exporter |
| `WADJET_OTEL_SAMPLE_RATE` | Trace sampling rate (0.0-1.0) |
| `WADJET_ENABLE_ALERTS` | Enable `CREATE ALERT` DDL and the scheduler |
| `WADJET_GEOIP_CITY_DB` | Path to GeoLite2-City.mmdb |
| `WADJET_GEOIP_ASN_DB` | Path to GeoLite2-ASN.mmdb |

Wadjet has no built-in rate limiter; place a reverse proxy in front of it to
enforce per-client limits.
