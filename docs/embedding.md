# Embedding Caelum

Caelum can be embedded directly in Go applications via the `pkg/caelum` package, giving you a programmatic analytical query engine without running a separate server.

## Installation

```bash
go get github.com/derekmwright/caelum/pkg/caelum
```

## Core API

### Opening a Database

```go
import "github.com/derekmwright/caelum/pkg/caelum"

db, err := caelum.Open(ctx, caelum.Config{
    S3Endpoint:  "localhost:9000",
    S3AccessKey: "minioadmin",
    S3SecretKey: "minioadmin",
    S3Bucket:    "caelum",
})
```

The `Open` call initializes the object store client and loads the catalog from storage.

### Table Management

```go
// Create a table
err := db.CreateTable(ctx, "flow_logs", schema, []string{"date"})

// Drop a table (removes metadata only — Parquet files remain on S3)
err := db.DropTable(ctx, "flow_logs")

// List all tables
tables, err := db.ListTables(ctx)
// tables: ["flow_logs", "syslog", "device_inventory"]
```

### Ingestion

```go
ingester, err := db.NewIngester("flow_logs", schema, []string{"date"}, caelum.IngestConfig{
    FlushInterval: 30 * time.Second,
    MaxRows:       500000,
})

// Write individual rows
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

// Force flush all buffered data
ingester.Flush(ctx)
```

The ingester automatically:
- Partitions rows based on partition keys
- Buffers rows in memory
- Flushes to Parquet on S3 when thresholds are hit
- Updates the catalog manifest

### Querying

```go
result, err := db.Query(ctx, "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10")
if err != nil {
    log.Fatal(err)
}

// Access result metadata
fmt.Println("Columns:", result.Columns)
fmt.Println("Rows returned:", len(result.Rows))

// Iterate rows
for _, row := range result.Rows {
    srcIP := row["src_ip"].(string)
    total := row["total"].(int64)
    fmt.Printf("%s: %d bytes\n", srcIP, total)
}
```

## Full Example: Network Monitoring Service

This example shows a complete Go service that ingests flow data and exposes custom analytics endpoints.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/derekmwright/caelum/pkg/caelum"
)

var db *caelum.DB

func main() {
    ctx := context.Background()

    var err error
    db, err = caelum.Open(ctx, caelum.Config{
        S3Endpoint:  "minio.internal:9000",
        S3AccessKey: "prod-access-key",
        S3SecretKey: "prod-secret-key",
        S3Bucket:    "network-analytics",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Ensure tables exist
    ensureTables(ctx)

    // Start background ingestion from a message queue
    go ingestFromQueue(ctx)

    // Serve analytics API
    http.HandleFunc("/api/top-talkers", handleTopTalkers)
    http.HandleFunc("/api/port-distribution", handlePortDistribution)
    http.HandleFunc("/api/anomalies", handleAnomalies)
    http.HandleFunc("/api/custom-query", handleCustomQuery)

    log.Println("Analytics API listening on :9090")
    log.Fatal(http.ListenAndServe(":9090", nil))
}

func ensureTables(ctx context.Context) {
    tables, _ := db.ListTables(ctx)
    tableSet := make(map[string]bool)
    for _, t := range tables {
        tableSet[t] = true
    }

    if !tableSet["flow_logs"] {
        db.CreateTable(ctx, "flow_logs", caelum.Schema{
            Columns: []caelum.Column{
                {Name: "timestamp", Type: caelum.Timestamp},
                {Name: "src_ip", Type: caelum.IPv4},
                {Name: "dst_ip", Type: caelum.IPv4},
                {Name: "src_port", Type: caelum.Int32},
                {Name: "dst_port", Type: caelum.Int32},
                {Name: "protocol", Type: caelum.String},
                {Name: "bytes_in", Type: caelum.Int64},
                {Name: "bytes_out", Type: caelum.Int64},
            },
        }, []string{"date"})
    }
}

func handleTopTalkers(w http.ResponseWriter, r *http.Request) {
    date := r.URL.Query().Get("date")
    if date == "" {
        date = time.Now().Format("2006-01-02")
    }
    limit := r.URL.Query().Get("limit")
    if limit == "" {
        limit = "25"
    }

    result, err := db.Query(r.Context(), fmt.Sprintf(`
        SELECT
            src_ip,
            SUM(bytes_in) AS ingress,
            SUM(bytes_out) AS egress,
            SUM(bytes_in + bytes_out) AS total,
            COUNT(*) AS flows
        FROM flow_logs
        WHERE date = '%s'
        GROUP BY src_ip
        ORDER BY total DESC
        LIMIT %s
    `, date, limit))
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result.Rows)
}

func handlePortDistribution(w http.ResponseWriter, r *http.Request) {
    date := r.URL.Query().Get("date")
    if date == "" {
        date = time.Now().Format("2006-01-02")
    }

    result, err := db.Query(r.Context(), fmt.Sprintf(`
        SELECT
            dst_port,
            protocol,
            COUNT(*) AS connections,
            SUM(bytes_in) AS total_bytes
        FROM flow_logs
        WHERE date = '%s'
        GROUP BY dst_port, protocol
        ORDER BY connections DESC
        LIMIT 50
    `, date))
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result.Rows)
}

func handleAnomalies(w http.ResponseWriter, r *http.Request) {
    date := r.URL.Query().Get("date")
    if date == "" {
        date = time.Now().Format("2006-01-02")
    }

    // Find IPs hitting an unusual number of unique ports (potential scan)
    result, err := db.Query(r.Context(), fmt.Sprintf(`
        SELECT
            src_ip,
            COUNT(DISTINCT dst_port) AS unique_ports,
            COUNT(*) AS total_flows,
            SUM(bytes_in) AS bytes
        FROM flow_logs
        WHERE date = '%s' AND protocol = 'TCP'
        GROUP BY src_ip
        HAVING COUNT(DISTINCT dst_port) > 50
        ORDER BY unique_ports DESC
    `, date))
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result.Rows)
}

func handleCustomQuery(w http.ResponseWriter, r *http.Request) {
    var req struct {
        SQL string `json:"sql"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid request body", 400)
        return
    }

    result, err := db.Query(r.Context(), req.SQL)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]any{
        "columns": result.Columns,
        "rows":    result.Rows,
    })
}

func ingestFromQueue(ctx context.Context) {
    // Placeholder: in production, consume from Kafka, NATS, or similar
    // The key point is that ingestion and querying coexist in the same process
    log.Println("Background ingestion started")
}
```

## When to Embed vs. Run the Server

| Use Case | Recommendation |
|----------|---------------|
| Single Go application needs analytics | Embed |
| Multiple services need to query the same data | Run the server |
| Need SQL access from non-Go languages | Run the server |
| Want to add custom business logic around queries | Embed |
| CI/CD pipeline analytics | Embed (one-shot queries) |
| Interactive exploration | Run the server + shell |

## Thread Safety

- `DB.Query()` is safe to call concurrently from multiple goroutines
- `DB.CreateTable()` and `DB.DropTable()` use optimistic concurrency on the catalog
- `Ingester.Write()` is safe for concurrent writes from multiple goroutines within the same ingester
- Do not share an `Ingester` across multiple processes writing to the same table — use separate ingesters (catalog ETag concurrency will prevent corruption but may cause flush retries)
