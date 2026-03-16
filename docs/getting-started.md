# Getting Started

This guide walks you through installing Caelum, creating a table, ingesting data, and running your first query.

## Prerequisites

- **Go 1.26+**
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
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket caelum \
  --http-addr :8080
```

This starts an embedded coordinator and worker in a single process — ideal for development and single-node deployments.

### One-Off Query

```bash
./caelum query \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket caelum \
  "SELECT * FROM my_table LIMIT 10"
```

Supports `--format` flag: `json` (default), `table`, or `csv`.

### Interactive Shell

```bash
./caelum shell \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket caelum
```

Supports `--format` flag: `table` (default), `json`, or `csv`.

### List Tables

```bash
./caelum tables \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket caelum
```

## Your First Table (Embedded Go)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/derekmwright/caelum/internal/storage/ingest"
    "github.com/derekmwright/caelum/internal/storage/objstore"
    "github.com/derekmwright/caelum/internal/storage/parquet"
    "github.com/derekmwright/caelum/pkg/caelum"
)

func main() {
    ctx := context.Background()

    // Create an S3-compatible object store client
    store, err := objstore.NewMinIOStore(ctx, objstore.MinIOConfig{
        Endpoint:  "localhost:9000",
        AccessKey: "minioadmin",
        SecretKey: "minioadmin",
        UseSSL:    false,
    })
    if err != nil {
        log.Fatal(err)
    }

    db, err := caelum.Open(ctx, caelum.Config{
        Store:  store,
        Bucket: "caelum",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Define a schema for network flow logs
    schema := parquet.Schema{
        Columns: []parquet.Column{
            {Name: "timestamp",  Type: parquet.TypeTimestamp},
            {Name: "src_ip",    Type: parquet.TypeIPv4},
            {Name: "dst_ip",    Type: parquet.TypeIPv4},
            {Name: "src_port",  Type: parquet.TypeInt32},
            {Name: "dst_port",  Type: parquet.TypeInt32},
            {Name: "protocol",  Type: parquet.TypeString},
            {Name: "bytes_in",  Type: parquet.TypeInt64},
            {Name: "bytes_out", Type: parquet.TypeInt64},
        },
    }

    // Create the table, partitioned by date
    err = db.CreateTable(ctx, "flow_logs", schema, []string{"date"})
    if err != nil {
        log.Fatal(err)
    }

    // Set up an ingester — returns *ingest.Ingester (no error)
    ingester := db.NewIngester("flow_logs", schema, []string{"date"}, ingest.Config{
        FlushInterval: 10 * time.Second,
        MaxBufferRows: 100000,
    })
    ingester.Start() // start background flush goroutine

    // Ingest rows as a batch (takes a slice of row maps)
    err = ingester.Ingest(ctx, []map[string]any{
        {
            "timestamp": time.Now(),
            "src_ip":    "10.0.1.50",
            "dst_ip":    "10.0.2.100",
            "src_port":  int32(54321),
            "dst_port":  int32(443),
            "protocol":  "TCP",
            "bytes_in":  int64(2048),
            "bytes_out": int64(512),
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Flush to ensure data is written, then stop the ingester
    ingester.FlushAll(ctx)
    ingester.Stop(ctx)

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
    "rows_scanned": 50000,
    "plan": "Scan(flow_logs) → Aggregate(src_ip, dst_port) → Sort(total_bytes DESC) → Limit(10)"
  }
}
```

## Next Steps

- [Ingestion Guide](ingestion.md) — Bento pipelines, partitioning strategies, tuning flush thresholds
- [SQL Reference](sql-reference.md) — Supported syntax, aggregates, joins
- [Network Analytics Workflow](network-analytics.md) — Full pipeline from device logs to dashboards
