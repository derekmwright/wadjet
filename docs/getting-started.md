# Getting Started

This guide walks you through installing Caelum, creating a table, ingesting data, and running your first query.

## Prerequisites

- **Go 1.25.5+**
- **S3-compatible object storage** (MinIO for local development, AWS S3 or similar for production)
- **NATS** (only required for distributed mode)

## Installation

### From Source

```bash
git clone https://github.com/derekmwright/caelum.git
cd caelum
go build -o caelum ./cmd/caelum
```

### As a Go Library

```bash
go get github.com/derekmwright/caelum/pkg/caelum
```

## Start MinIO (Local Development)

If you don't have an S3-compatible store running:

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create a bucket:

```bash
# Using mc (MinIO client)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/caelum
```

## Start the Server

### Standalone Mode (Single Process)

```bash
./caelum serve \
  --mode standalone \
  --s3-endpoint localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-bucket caelum \
  --listen :8080
```

This starts an embedded coordinator and worker in a single process — ideal for development and single-node deployments.

### One-Off Query

```bash
./caelum query \
  --s3-endpoint localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-bucket caelum \
  "SELECT * FROM my_table LIMIT 10"
```

### Interactive Shell

```bash
./caelum shell \
  --s3-endpoint localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-bucket caelum
```

### List Tables

```bash
./caelum tables \
  --s3-endpoint localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-bucket caelum
```

## Your First Table (Embedded Go)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/derekmwright/caelum/pkg/caelum"
)

func main() {
    ctx := context.Background()

    db, err := caelum.Open(ctx, caelum.Config{
        S3Endpoint:  "localhost:9000",
        S3AccessKey: "minioadmin",
        S3SecretKey: "minioadmin",
        S3Bucket:    "caelum",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Define a schema for network flow logs
    schema := caelum.Schema{
        Columns: []caelum.Column{
            {Name: "timestamp",  Type: caelum.Timestamp},
            {Name: "src_ip",    Type: caelum.IPv4},
            {Name: "dst_ip",    Type: caelum.IPv4},
            {Name: "src_port",  Type: caelum.Int32},
            {Name: "dst_port",  Type: caelum.Int32},
            {Name: "protocol",  Type: caelum.String},
            {Name: "bytes_in",  Type: caelum.Int64},
            {Name: "bytes_out", Type: caelum.Int64},
        },
    }

    // Create the table, partitioned by date
    err = db.CreateTable(ctx, "flow_logs", schema, []string{"date"})
    if err != nil {
        log.Fatal(err)
    }

    // Set up an ingester
    ingester, err := db.NewIngester("flow_logs", schema, []string{"date"}, caelum.IngestConfig{
        FlushInterval: 10 * time.Second,
        MaxRows:       100000,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Write some rows
    err = ingester.Write(ctx, map[string]any{
        "timestamp": time.Now(),
        "src_ip":    "10.0.1.50",
        "dst_ip":    "10.0.2.100",
        "src_port":  int32(54321),
        "dst_port":  int32(443),
        "protocol":  "TCP",
        "bytes_in":  int64(2048),
        "bytes_out": int64(512),
    })
    if err != nil {
        log.Fatal(err)
    }

    // Flush to ensure data is written
    ingester.Flush(ctx)

    // Query it
    result, err := db.Query(ctx, "SELECT src_ip, dst_ip, bytes_in FROM flow_logs LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }

    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

## Your First Query (HTTP API)

With the server running:

```bash
curl -s -X POST http://localhost:8080/v1/queries \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT src_ip, dst_port, SUM(bytes_in) as total_bytes FROM flow_logs GROUP BY src_ip, dst_port ORDER BY total_bytes DESC LIMIT 10"}' \
  | jq .
```

Response:

```json
{
  "query_id": "q-abc123",
  "columns": ["src_ip", "dst_port", "total_bytes"],
  "rows": [
    {"src_ip": "10.0.1.50", "dst_port": 443, "total_bytes": 1048576}
  ],
  "stats": {
    "elapsed": "12ms",
    "rows_scanned": 50000
  }
}
```

## Next Steps

- [Ingestion Guide](ingestion.md) — Bento pipelines, partitioning strategies, tuning flush thresholds
- [SQL Reference](sql-reference.md) — Supported syntax, aggregates, joins
- [Network Analytics Workflow](network-analytics.md) — Full pipeline from device logs to dashboards
