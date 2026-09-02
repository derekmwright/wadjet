# Ingestion

This guide covers writing data into Wadjet — from the built-in micro-batch ingester to production pipelines using Bento.

## Ingestion Overview

Wadjet ingests data by:

1. Accepting rows into per-partition memory buffers
2. Automatically flushing buffers to Parquet files on object storage
3. Updating the catalog manifest atomically

Data lands in Parquet format on your S3-compatible store, organized by partition keys.

## Built-in Micro-Batch Ingester

### How It Works

```mermaid
graph LR
    R["Rows<br/>([]map)"] -- "Ingest()" --> P["Route by<br/>partition key"]
    P -- "Buffer" --> A["Accumulate<br/>per partition"]
    A -- "FlushAll" --> S["Parquet<br/>to S3"]
    S --> C["Update catalog<br/>manifest (NATS KV)"]
```

The ingester flushes a partition's buffer automatically when that PARTITION's
buffer crosses a threshold — the limits are per-partition, not totals across
the table:

| Trigger | Default | Description |
|---------|---------|-------------|
| Size | 128 MB | One partition's estimated buffered bytes (`MaxBufferSize`) |
| Row count | 1,000,000 | One partition's buffered row count (`MaxBufferRows`) |
| Time | 60 seconds | Background ticker (`FlushInterval`). It flushes only partitions holding at least `MinFlushRows` rows — default 100 — so a trickling partition is not flushed into tiny files. |

Two further `ingest.Config` fields carry defaults: `RowGroupSize` (128 K rows
per Parquet row group) and `MinFlushRows` (100). `FlushAll` ignores the
size/row thresholds and `MinFlushRows` and writes everything buffered.

### Usage (Go API)

```go
import (
    "github.com/derekmwright/wadjet/internal/storage/ingest"
    "github.com/derekmwright/wadjet/internal/storage/parquet"
)

schema := parquet.Schema{
    Columns: []parquet.Column{
        {Name: "timestamp", Type: parquet.TypeTimestamp},
        {Name: "device",    Type: parquet.TypeString},
        {Name: "severity",  Type: parquet.TypeString},
        {Name: "message",   Type: parquet.TypeString},
        {Name: "src_ip",    Type: parquet.TypeIPv4},
    },
}

// NewIngester returns *ingest.Ingester (no error)
ingester := db.NewIngester("syslog", schema, []string{"day", "device"}, ingest.Config{
    FlushInterval: 30 * time.Second,
    MaxBufferRows: 500000,
})

// Start background flush goroutine
ingester.Start()

// Ingest rows as batches — ingester handles partitioning and buffering.
// Every partition key must be present in EVERY row, or Ingest returns
// `missing partition key "day" in row`.
batch := make([]map[string]any, 0, len(events))
for _, event := range events {
    batch = append(batch, map[string]any{
        "timestamp": event.Time,
        "day":       event.Time.Format("2006-01-02"),
        "device":    event.Hostname,
        "severity":  event.Severity,
        "message":   event.Message,
        "src_ip":    event.SourceIP,
    })
}
err := ingester.Ingest(ctx, batch)

// Explicit flush (also happens automatically on thresholds)
ingester.FlushAll(ctx)

// Stop when done
ingester.Stop(ctx)
```

### Partitioning Strategy

Partition keys determine how data is organized on storage:

```
s3://wadjet/tables/syslog/day=2026-03-15/device=fw-01/chunk_0198f2c1-....parquet
s3://wadjet/tables/syslog/day=2026-03-15/device=sw-02/chunk_0198f2c2-....parquet
s3://wadjet/tables/syslog/day=2026-03-14/device=fw-01/chunk_0198f2c3-....parquet
```

Data files always live under `tables/<table>/`, in Hive-style `key=value`
directories, named `chunk_<uuidv7>.parquet` by the ingester.

Partitioning enables **partition pruning** — the planner drops whole partitions
before the scan — but only for the four Hive-standard key names `year`,
`month`, `day` and `hour`. A key with any other name still organizes the files
on storage, and still requires an equality filter to be evaluated row by row,
but prunes nothing. A partition key must also be declared in the table's schema
`Columns` to be referenceable in SQL at all; otherwise a `WHERE` on it is
rejected with SQLSTATE 42703.

**Recommendations:**

| Workload | Partition Keys | Why |
|----------|---------------|-----|
| Time-series logs | `day` | Most queries filter by time range, and `day` is one of the four prunable names |
| Multi-tenant | `tenant_id`, `day` | Isolates tenant data on storage; only `day` prunes |
| Network monitoring | `day`, `device` | Filter by device and time; only `day` prunes |
| Per-region analytics | `region`, `day` | Region-scoped queries; only `day` prunes |

Only `year`, `month`, `day` and `hour` are pruned. A second key such as
`device`, `tenant_id` or `region` buys storage layout and smaller files, not a
skipped scan — budget for that.

Avoid over-partitioning (too many small files) or under-partitioning (single giant partitions). A good target is partition files between 64 MB and 256 MB.

## Bento Integration

[Bento](https://warpstreamlabs.github.io/bento/) (formerly Benthos) is a stream processing tool that fits naturally as the ingestion layer in front of Wadjet. Bento handles parsing, enrichment, and batching, then writes Parquet directly to S3.

### Why Bento + Wadjet

```
Device Logs ──► Bento (parse, enrich, partition) ──► S3 (Parquet) ──► Wadjet (query)
```

- **Bento** handles the messy real-world ingestion: syslog parsing, JSON decoding, field extraction, enrichment, batching, retries
- **Wadjet** handles the analytical query layer: SQL, aggregation, joins, API serving

This separation keeps each component focused and independently scalable.

### Syslog to Wadjet via Bento

```yaml
# bento-syslog.yaml
input:
  socket_server:
    network: udp
    address: 0.0.0.0:5514
    codec: lines

pipeline:
  processors:
    # Parse RFC5424 syslog
    - mapping: |
        let parsed = this.parse_log("syslog_rfc5424")
        root.timestamp = $parsed.timestamp
        root.hostname = $parsed.hostname
        root.app_name = $parsed.app_name
        root.severity = $parsed.severity
        root.facility = $parsed.facility
        root.message = $parsed.message
        root.day = $parsed.timestamp.format_timestamp("2006-01-02")

    # Extract source IP from message if present
    - mapping: |
        root = this
        let ip_match = this.message.re_find("SRC=([0-9.]+)")
        root.src_ip = if $ip_match != "" { $ip_match } else { "0.0.0.0" }

output:
  aws_s3:
    bucket: wadjet
    path: 'tables/syslog/day=${! this.day }/part-${! uuid_v4() }.parquet'
    batching:
      count: 100000
      period: 30s
    codec: parquet
    parquet_encoding:
      schema:
        - name: timestamp
          type: INT64
          annotation: TIMESTAMP_MILLIS
        - name: hostname
          type: UTF8
        - name: app_name
          type: UTF8
        - name: severity
          type: UTF8
        - name: facility
          type: UTF8
        - name: message
          type: UTF8
        - name: src_ip
          type: UTF8
        - name: day
          type: UTF8
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

Run it:

```bash
bento -c bento-syslog.yaml
```

### SNMP Trap Collection

```yaml
# bento-snmp.yaml
input:
  socket_server:
    network: udp
    address: 0.0.0.0:1162
    codec: lines

pipeline:
  processors:
    - mapping: |
        root.timestamp = now()
        root.day = now().format_timestamp("2006-01-02")
        let fields = this.split(" ")
        root.device = $fields.index(0)
        root.oid = $fields.index(1)
        root.value = $fields.index(2)
        root.trap_type = match $fields.index(1) {
          ".1.3.6.1.6.3.1.1.5.3" => "linkDown",
          ".1.3.6.1.6.3.1.1.5.4" => "linkUp",
          ".1.3.6.1.4.1.*" => "vendor",
          _ => "unknown",
        }

output:
  aws_s3:
    bucket: wadjet
    path: 'tables/snmp_traps/day=${! this.day }/part-${! uuid_v4() }.parquet'
    batching:
      count: 50000
      period: 60s
    codec: parquet
    parquet_encoding:
      schema:
        - name: timestamp
          type: INT64
          annotation: TIMESTAMP_MILLIS
        - name: device
          type: UTF8
        - name: oid
          type: UTF8
        - name: value
          type: UTF8
        - name: trap_type
          type: UTF8
        - name: day
          type: UTF8
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

### NetFlow/IPFIX via Bento

```yaml
# bento-netflow.yaml
input:
  # Use a NetFlow collector that outputs JSON (e.g., goflow2)
  # goflow2 -transport.type kafka -transport.kafka.brokers localhost:9092
  kafka:
    addresses:
      - localhost:9092
    topics:
      - netflow
    consumer_group: wadjet-ingest

pipeline:
  processors:
    - mapping: |
        root.timestamp = this.TimeReceived
        root.day = (this.TimeReceived / 1000).format_timestamp("2006-01-02")
        root.src_ip = this.SrcAddr
        root.dst_ip = this.DstAddr
        root.src_port = this.SrcPort
        root.dst_port = this.DstPort
        root.protocol = match this.Proto {
          6 => "TCP",
          17 => "UDP",
          1 => "ICMP",
          _ => this.Proto.string(),
        }
        root.bytes = this.Bytes
        root.packets = this.Packets
        root.tcp_flags = this.TCPFlags
        root.exporter = this.SamplerAddress

output:
  aws_s3:
    bucket: wadjet
    path: 'tables/netflow/day=${! this.day }/part-${! uuid_v4() }.parquet'
    batching:
      count: 250000
      period: 30s
    codec: parquet
    parquet_encoding:
      schema:
        - name: timestamp
          type: INT64
          annotation: TIMESTAMP_MILLIS
        - name: src_ip
          type: UTF8
        - name: dst_ip
          type: UTF8
        - name: src_port
          type: INT32
        - name: dst_port
          type: INT32
        - name: protocol
          type: UTF8
        - name: bytes
          type: INT64
        - name: packets
          type: INT64
        - name: tcp_flags
          type: INT32
        - name: exporter
          type: UTF8
        - name: day
          type: UTF8
    region: us-east-1
    endpoint: http://localhost:9000
    credentials:
      id: minioadmin
      secret: minioadmin
    force_path_style_urls: true
```

### Registering Bento-Written Tables in Wadjet

After Bento starts writing Parquet files to S3, register the table schema in Wadjet so it can be queried:

```go
import (
    "github.com/derekmwright/wadjet/internal/storage/objstore"
    "github.com/derekmwright/wadjet/internal/storage/parquet"
    "github.com/derekmwright/wadjet/wadjet"
)

store, _ := objstore.NewMinIOStore(objstore.MinIOConfig{
    Endpoint: "localhost:9000", AccessKey: "minioadmin", SecretKey: "minioadmin",
})
db, _ := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "wadjet"})

// Register the syslog table
db.CreateTable(ctx, "syslog", parquet.Schema{
    Columns: []parquet.Column{
        {Name: "timestamp", Type: parquet.TypeTimestamp},
        {Name: "hostname",  Type: parquet.TypeString},
        {Name: "app_name",  Type: parquet.TypeString},
        {Name: "severity",  Type: parquet.TypeString},
        {Name: "facility",  Type: parquet.TypeString},
        {Name: "message",   Type: parquet.TypeString},
        {Name: "src_ip",    Type: parquet.TypeString},  // String since Bento writes UTF8
    },
}, []string{"day"})

// Register the netflow table
db.CreateTable(ctx, "netflow", parquet.Schema{
    Columns: []parquet.Column{
        {Name: "timestamp", Type: parquet.TypeTimestamp},
        {Name: "src_ip",    Type: parquet.TypeString},
        {Name: "dst_ip",    Type: parquet.TypeString},
        {Name: "src_port",  Type: parquet.TypeInt32},
        {Name: "dst_port",  Type: parquet.TypeInt32},
        {Name: "protocol",  Type: parquet.TypeString},
        {Name: "bytes",     Type: parquet.TypeInt64},
        {Name: "packets",   Type: parquet.TypeInt64},
        {Name: "tcp_flags", Type: parquet.TypeInt32},
        {Name: "exporter",  Type: parquet.TypeString},
    },
}, []string{"day"})
```

Or via the HTTP API, create tables, then start querying.

## Ingestion Best Practices

### File Sizing

| File Size | Outcome |
|-----------|---------|
| < 16 MB | Too many small files — high S3 LIST overhead, slow scans |
| 64–256 MB | Ideal range — balanced between parallelism and I/O efficiency |
| > 512 MB | Too large — limits parallelism, slow retries on failure |

Tune Bento's `batching.count` and `batching.period` to hit the sweet spot.

### Compression Selection

| Use Case | Codec | Bento Config |
|----------|-------|-------------|
| Real-time dashboards (low latency) | Snappy or LZ4 | Default |
| Archival / cold storage | Zstd | `compression: zstd` |
| Cross-tool compatibility | Gzip | `compression: gzip` |

### Partition Key Selection

- Always include a time dimension, and name it `year`, `month`, `day` or `hour` — those four names are the only ones the planner prunes on. A key called `date` or `ts` organizes storage but skips nothing.
- Declare every partition key in the table's schema `Columns` as well; an undeclared key cannot be referenced in SQL (SQLSTATE 42703).
- Add a second key only if queries consistently filter on it (e.g., `device`, `region`) — it buys file layout, not pruning
- Avoid high-cardinality partition keys (e.g., `user_id` with millions of users) — creates millions of tiny partitions

### Exactly-Once Considerations

Wadjet's catalog uses revision-based optimistic concurrency (CAS) on its metadata KV — NATS JetStream KV under `wadjet serve`, an in-process map when an embedded `wadjet.Config` leaves `MetaKV` nil — so concurrent writers won't corrupt the catalog. However, `flushBuffer` uploads the Parquet object before it updates the manifest, so if an ingestion process crashes in that window a Parquet file may exist on S3 without a corresponding catalog entry. The file becomes orphaned; it does not affect queries, since the scan resolves files from the manifest. Wadjet ships no orphan-Parquet reaper (the coordinator's sweeper only cleans the `queries/` prefix), so periodic cleanup is your job in production deployments.
