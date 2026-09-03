# Getting Started

Wadjet is a distributed SQL analytics engine for Go: embed it directly in
your Go application, or deploy it as a standalone server (or a coordinator +
worker cluster) speaking the PostgreSQL wire protocol, HTTP, and gRPC. Both
paths run the same SQL engine over Apache Parquet and Apache Iceberg tables
— on local disk when embedding, or S3-compatible object storage for the
server — with first-class network-telemetry types (IPv4, IPv6, CIDR, MAC,
Port, Protocol) on top. This guide walks through installing it, querying
files on disk, the embedded Go API, and then running the server: creating a
table, ingesting data, and querying managed storage.

## Prerequisites

- **Go 1.26+**

Nothing else is required to query local files. Managed tables need object
storage — local disk (`objstore.FileStore`, no server) when embedding, or an
S3-compatible store (MinIO for local development, AWS S3 or similar for
production) for the standalone/distributed server. Distributed mode
(coordinator + worker split) additionally needs:

- **NATS** (only required for distributed mode)

## Installation

### From Source

```bash
git clone https://github.com/derekmwright/wadjet.git
cd wadjet
# NOTE: -o must not be plain "wadjet" — that's the API package
# directory, and Go would drop the binary inside it.
go build -o wadjet-bin ./cmd/wadjet
```

### As a Go Library

```bash
go get github.com/derekmwright/wadjet/wadjet
```

## Your First Query (No Setup)

Table functions read files directly, so a query whose only sources are `read_json()`, `read_csv()`, or `read_parquet()` needs no object storage, no server, and no configuration:

```bash
cat > conn.log <<'JSON'
{"id_orig_h":"10.0.0.1","orig_bytes":1024}
{"id_orig_h":"10.0.0.2","orig_bytes":512}
{"id_orig_h":"10.0.0.1","orig_bytes":2048}
JSON

./wadjet-bin query --format table \
  "SELECT id_orig_h, SUM(orig_bytes) AS total
   FROM read_json('conn.log')
   GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

Paths may be local files, `~/` home-relative paths, glob patterns (`logs/*.json`), or HTTP URLs.

## Your First Table (Embedded Go)

Wadjet is also a Go library — no server process required. Point it at local
disk or an S3-compatible store; the rest of the API is identical either way:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/derekmwright/wadjet/internal/storage/ingest"
    "github.com/derekmwright/wadjet/internal/storage/objstore"
    "github.com/derekmwright/wadjet/internal/storage/parquet"
    "github.com/derekmwright/wadjet/wadjet"
)

func main() {
    ctx := context.Background()

    // Local disk needs no server. Swap in objstore.NewMinIOStore(...) to
    // point at an S3-compatible store instead — the rest of this program
    // is unchanged either way.
    store, err := objstore.NewFileStore("./wadjet-data")
    if err != nil {
        log.Fatal(err)
    }

    db, err := wadjet.Open(ctx, wadjet.Config{
        Store:  store,
        Bucket: "wadjet",
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
            {Name: "date",      Type: parquet.TypeDate},
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

    // Ingest rows as a batch (takes a slice of row maps). Every partition key
    // must be present in every row, or Ingest returns
    // `missing partition key "date" in row`.
    now := time.Now()
    err = ingester.Ingest(ctx, []map[string]any{
        {
            "timestamp": now,
            "src_ip":    "10.0.1.50",
            "dst_ip":    "10.0.2.100",
            "src_port":  int32(54321),
            "dst_port":  int32(443),
            "protocol":  "TCP",
            "bytes_in":  int64(2048),
            "bytes_out": int64(512),
            "date":      now,
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

The rest of this guide covers the **server** deployment — the same engine
behind `wadjet serve`, speaking the PostgreSQL wire protocol, HTTP, and
gRPC, backed by managed object storage.

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
mc mb local/wadjet
```

## Start the Server

### Standalone Mode (Single Process)

```bash
./wadjet-bin serve \
  --mode standalone \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket wadjet \
  --http-addr :8080
```

This starts an embedded coordinator and worker in a single process — ideal for development and single-node deployments.

### One-Off Query

```bash
./wadjet-bin query \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket wadjet \
  "SELECT * FROM my_table LIMIT 10"
```

Supports `--format` flag: `json` (default), `table`, or `csv`.

### Interactive Shell

```bash
./wadjet-bin shell \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket wadjet
```

Supports `--format` flag: `table` (default), `json`, or `csv`.

### List Tables

```bash
./wadjet-bin tables \
  --endpoint localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket wadjet
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
    "rows_scanned": 1,
    "plan": "Scan(flow_logs) → Aggregate(src_ip, dst_port) → Sort(total_bytes DESC) → Limit(10)"
  }
}
```

## Your First Query (gRPC)

Wadjet also exposes a gRPC API on `:9090` (default). Clients can be generated for any language from the proto definition at `proto/wadjet/v1/wadjet.proto`.

**Using grpcurl** — the server does not register gRPC reflection, so point
grpcurl at the proto file. Run these from the repo root:

```bash
# List tables
grpcurl -plaintext -import-path proto -proto wadjet/v1/wadjet.proto localhost:9090 wadjet.v1.WadjetService/ListTables

# Execute a query
grpcurl -plaintext -import-path proto -proto wadjet/v1/wadjet.proto \
  -d '{"sql": "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip LIMIT 5"}' \
  localhost:9090 wadjet.v1.WadjetService/Query
```

**Generate a client (e.g., Python):**

```bash
pip install grpcio-tools
python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. wadjet/v1/wadjet.proto
```

See [gRPC API](grpc-api.md) for the full service reference.

## Next Steps

- [Ingestion Guide](ingestion.md) — Bento pipelines, partitioning strategies, tuning flush thresholds
- [SQL Reference](sql-reference.md) — Supported syntax, aggregates, joins
- [gRPC API](grpc-api.md) — Generate type-safe clients for any language
- [Network Analytics Workflow](network-analytics.md) — Full pipeline from device logs to dashboards
