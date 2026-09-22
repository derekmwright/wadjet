# Embedding Wadjet

Wadjet's `wadjet` package is the in-process query engine used by `cmd/wadjet`
and by the test suites in this repository, giving a programmatic analytical
query engine without running a separate server.

It is **MIT-licensed**, as is everything it links: embed it, ship it, modify
it, keep your changes. The AGPL-3.0 half of this repository is the
distributed engine — the coordinator, the workers and the exchange — and an
embedded program links none of it, which is a gate rather than a claim
(`tools/licensecheck`, and [LICENSING.md](../LICENSING.md)).

## What the `wadjet` package exposes

One import — `github.com/derekmwright/wadjet/wadjet` — is everything an
out-of-tree program needs to open a database, declare a table, ingest rows and
query them:

| Name | What it is |
|---|---|
| `Open`, `Config`, `DB` | the database |
| `Config.DataDir`, `Config.CatalogDir`, `ErrCatalogHeld` | a catalog that survives a restart (see [Persistence](#persistence-a-catalog-that-survives-a-restart)) |
| `NewMemStore()`, `NewFileStore(dir)`, `NewS3Store(S3Config)` | the object store `Config.Store` takes |
| `Schema`, `Column`, `ColumnType`, the 22 `Type*` constants | a table's columns |
| `IngestConfig`, `DefaultIngestConfig()` | the ingester's flush policy |
| `QueryResult`, `ColumnMeta`, `ExecResult` | what a query and a DML statement return |

`test/embed/` in this repository is a separate Go module that imports only
that package and runs the guide's program; the test suite builds and runs it,
so this table is checked rather than asserted.

**Deliberately not exposed.** `Config.MetaKV` (the catalog KV a process
that already runs one hands over — the servers in this repository) and
`Config.AuthProvider` (in-process ABAC) name types under `internal/` and have
no public constructor. Neither is needed out of tree: a persistent catalog is
`Config.DataDir` or `Config.CatalogDir`, and access policy is enforced at the
server doors — the PostgreSQL wire protocol, HTTP or gRPC.

## Installation

```bash
go get github.com/derekmwright/wadjet/wadjet
```

## Core API

### Opening a Database

```go
import "github.com/derekmwright/wadjet/wadjet"

// One local directory for the data and the catalog: the tables survive a
// restart, and `wadjet serve --storage-type=file --data-dir=./wadjet-data`
// opens the same ones.
db, err := wadjet.Open(ctx, wadjet.Config{DataDir: "./wadjet-data"})
```

Over an S3-compatible store, the store is built first and the catalog
directory is named separately:

```go
store, err := wadjet.NewS3Store(wadjet.S3Config{
    Endpoint:  "localhost:9000",
    AccessKey: "minioadmin",
    SecretKey: "minioadmin",
    UseSSL:    false,
})
if err != nil {
    log.Fatal(err)
}

db, err := wadjet.Open(ctx, wadjet.Config{
    Store:      store,
    Bucket:     "wadjet",
    CatalogDir: "/var/lib/wadjet/catalog", // local disk; the tables survive a restart
})
```

The `Config` struct accepts:
- `DataDir` — the zero-configuration persistent database: a local directory holding the Parquet objects (a file store, under `Bucket`, default `wadjet`) and the catalog (under `<DataDir>/_catalog`). Leave `Store` nil.
- `CatalogDir` — the directory the catalog persists in, beside any `Store`. `DataDir` defaults it.
- `Store` — the object store, from `wadjet.NewS3Store` (production), `wadjet.NewFileStore` (local dev) or `wadjet.NewMemStore` (testing)
- `Bucket` — S3 bucket name
- `MetaKV` — the catalog KV of a process that already holds one (the servers in this repository pass theirs). **With no `DataDir`, no `CatalogDir` and a nil `MetaKV` the catalog is in memory**: every table you create is process-local and gone at exit, and a `serve` process cannot see it. It names an internal type; an out-of-tree program uses `DataDir` or `CatalogDir`.
- `Logger` — Optional `*slog.Logger` (defaults to slog.Default)
- `MemoryBudget` — per-query memory budget in bytes (0 = unlimited); pipeline breakers spill past it
- `SpillDir` — directory for spill-to-disk files (empty = OS temp dir)
- `AuthProvider` — optional `*auth.Provider`, enables ABAC enforcement at query level
- `SortMergeJoinBytes`, `LateMaterialization`, `BushyJoinReorder` — planner knobs (0/false = off).
  They configure THIS database: two `wadjet.DB`s open in one process plan by their
  own settings, and `Close` takes a database's settings with it.
- `EnableAlerts` — turns on the CREATE ALERT scheduler; call `db.Close()` to stop it.
  It also gates the alert DDL: `CREATE`/`ALTER`/`DROP ALERT` are refused on a `DB`
  that does not carry a scheduler. That coupling is why `wadjetd serve` does not give
  its PostgreSQL wire door the alert statements — the coordinator in that process
  already runs a scheduler, and a second one would fire every alert twice. See
  [SQL reference](sql-reference.md#parsed-and-not-executed).

### Table Management

```go
// Create a table (schema is wadjet.Schema)
// A partition key must ALSO appear in schema.Columns to be referenceable in
// SQL, and only the names year/month/day/hour are pruned by the planner.
err := db.CreateTable(ctx, "flow_logs", schema, []string{"day"})

// Drop a table (removes metadata only — Parquet files remain on S3)
err := db.DropTable(ctx, "flow_logs")

// List all tables
tables, err := db.ListTables(ctx)
// tables: ["flow_logs", "syslog", "device_inventory"]

// Access underlying catalog and store directly
catalog := db.Catalog()
store := db.Store()
```

### Ingestion

```go
// NewIngester returns an ingester (no error return)
ingester := db.NewIngester("flow_logs", schema, []string{"day"}, wadjet.IngestConfig{
    FlushInterval: 30 * time.Second,
    MaxBufferRows: 500000,
})

// Start the background flush goroutine
ingester.Start()

// Ingest rows as a batch (takes a slice of row maps)
// EVERY partition key must be present in EVERY row, or Ingest returns
// `missing partition key "day" in row`.
now := time.Now()
err = ingester.Ingest(ctx, []map[string]any{
    {
        "timestamp": now,
        "day":       now.Format("2006-01-02"),
        "src_ip":    "10.0.1.50",
        "dst_ip":    "10.0.2.100",
        "src_port":  int32(54321),
        "dst_port":  int32(443),
        "protocol":  "TCP",
        "bytes_in":  int64(2048),
        "bytes_out": int64(512),
    },
})

// Force flush all buffered data
ingester.FlushAll(ctx)

// Stop the ingester when done (flushes remaining data)
ingester.Stop(ctx)
```

The ingester automatically:
- Partitions rows based on partition keys
- Buffers rows in per-partition accumulators
- Flushes to Parquet on S3 when thresholds are hit (size, rows, or time)
- Updates the catalog manifest atomically via revision-based CAS on the catalog KV (JetStream KV under `DataDir` / `CatalogDir` and under the servers; an in-process map when no catalog directory is named)

### Querying

```go
result, err := db.Query(ctx, "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10")
if err != nil {
    log.Fatal(err)
}

// Access result metadata
fmt.Println("Columns:", result.Columns)
fmt.Println("Rows returned:", len(result.Rows))

// Iterate rows. Mind the box: an aggregate's Go type follows its DECLARED
// output type, not its input's. SUM over an INT64 column declares DECIMAL, and
// a DECIMAL is boxed as its formatted STRING. COUNT is int64; SUM over INT32
// is int64.
for _, row := range result.Rows {
    srcIP := row["src_ip"].(string)
    total := row["total"].(string) // SUM(bytes_in) over an INT64 column
    fmt.Printf("%s: %s bytes\n", srcIP, total)
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

    "github.com/derekmwright/wadjet/wadjet"
)

var db *wadjet.DB

func main() {
    ctx := context.Background()

    store, err := wadjet.NewS3Store(wadjet.S3Config{
        Endpoint:  "minio.internal:9000",
        AccessKey: "prod-access-key",
        SecretKey: "prod-secret-key",
    })
    if err != nil {
        log.Fatal(err)
    }

    db, err = wadjet.Open(ctx, wadjet.Config{
        Store:  store,
        Bucket: "network-analytics",
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
        db.CreateTable(ctx, "flow_logs", wadjet.Schema{
            Columns: []wadjet.Column{
                {Name: "timestamp", Type: wadjet.TypeTimestamp},
                {Name: "src_ip", Type: wadjet.TypeIPv4},
                {Name: "dst_ip", Type: wadjet.TypeIPv4},
                {Name: "src_port", Type: wadjet.TypeInt32},
                {Name: "dst_port", Type: wadjet.TypeInt32},
                {Name: "protocol", Type: wadjet.TypeString},
                {Name: "bytes_in", Type: wadjet.TypeInt64},
                {Name: "bytes_out", Type: wadjet.TypeInt64},
                {Name: "day", Type: wadjet.TypeString},
            },
        }, []string{"day"})
    }
}

func handleTopTalkers(w http.ResponseWriter, r *http.Request) {
    day := r.URL.Query().Get("day")
    if day == "" {
        day = time.Now().Format("2006-01-02")
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
        WHERE day = '%s'
        GROUP BY src_ip
        ORDER BY total DESC
        LIMIT %s
    `, day, limit))
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result.Rows)
}

func handlePortDistribution(w http.ResponseWriter, r *http.Request) {
    day := r.URL.Query().Get("day")
    if day == "" {
        day = time.Now().Format("2006-01-02")
    }

    result, err := db.Query(r.Context(), fmt.Sprintf(`
        SELECT
            dst_port,
            protocol,
            COUNT(*) AS connections,
            SUM(bytes_in) AS total_bytes
        FROM flow_logs
        WHERE day = '%s'
        GROUP BY dst_port, protocol
        ORDER BY connections DESC
        LIMIT 50
    `, day))
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result.Rows)
}

func handleAnomalies(w http.ResponseWriter, r *http.Request) {
    day := r.URL.Query().Get("day")
    if day == "" {
        day = time.Now().Format("2006-01-02")
    }

    // Find IPs hitting an unusual number of unique ports (potential scan)
    result, err := db.Query(r.Context(), fmt.Sprintf(`
        SELECT
            src_ip,
            COUNT(DISTINCT dst_port) AS unique_ports,
            COUNT(*) AS total_flows,
            SUM(bytes_in) AS bytes
        FROM flow_logs
        WHERE day = '%s' AND protocol = 'TCP'
        GROUP BY src_ip
        HAVING COUNT(DISTINCT dst_port) > 50
        ORDER BY unique_ports DESC
    `, day))
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

## Persistence: a catalog that survives a restart

A table is its Parquet objects plus its catalog entry, and the entry is what
decides whether the table is there after a restart. `Config.DataDir` keeps
both in one local directory:

```
./wadjet-data/
  _catalog/            the catalog: a JetStream file store, and wadjet.lock
  wadjet/tables/<t>/   the Parquet objects (the bucket is Config.Bucket, default "wadjet")
```

That is byte for byte the layout `wadjet --storage-type=file
--data-dir=./wadjet-data` and `wadjet serve` use, so a program, the CLI
commands and a server over one directory hold ONE set of tables: run the
program, stop it, start `wadjet serve --storage-type=file
--data-dir=./wadjet-data`, and the server answers the program's tables on the
wire. `Config.CatalogDir` names the catalog directory alone, for a persistent
catalog beside an S3 store. With neither, the catalog is in memory and every
table is process-local — a `NewFileStore` opened that way logs a warning,
because the Parquet objects it received stay behind with nothing naming them.

**One holder at a time.** A directory is held exclusively by the `DB` that
opened it, for the life of that `DB`; `Close` releases it. A second `Open` of
a held directory — from the same process or another, including a `wadjet
serve` — is refused with `wadjet.ErrCatalogHeld`, naming the directory and
the holder's pid. It is a refusal rather than a second opener because two
processes writing one JetStream store write over each other's metadata. The
short-lived CLI commands (`wadjet tables`, `query`, …) are the exception in
the other direction: they reach a running holder's catalog live, through the
address it publishes in `wadjet.lock`, so `wadjet tables
--data-dir=./wadjet-data` beside your running program lists its tables. A
process that exits without `Close` leaves the directory to the kernel: the
lock drops with the process, and the catalog store recovers on the next open
(JetStream logs a rebuild). Nothing committed is lost.

**A kill during a flush.** The ingester writes the Parquet object before it
commits the manifest, so a process killed in that window leaves the table
with no entry for that file and the object behind as an orphan — bytes, not
rows; the scan resolves files from the manifest and never reads it, and
nothing reclaims it (see [Ingestion](ingestion.md#exactly-once-considerations)).
A kill after the commit leaves the table with its rows. The catalog never
names a file that is not there. Across a power loss (not a process kill) the
order is not guaranteed — neither the object nor the catalog write is fsynced.

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
- `DB.CreateTable()` and `DB.DropTable()` use revision-based optimistic concurrency (CAS) on the catalog KV — JetStream KV under `DataDir` / `CatalogDir` and under the servers, an in-process map when no catalog directory is named
- `Ingester.Ingest()` is safe for concurrent calls from multiple goroutines within the same ingester
- Do not share an `Ingester` across multiple processes writing to the same table — use separate ingesters (catalog revision concurrency will prevent corruption but may cause flush retries)
