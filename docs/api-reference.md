# HTTP API Reference

Wadjet exposes a REST API for executing queries, managing tables, and monitoring health.

## Base URL

```
http://<host>:<port>
```

Default listen address: `:8080`

## Authentication

If authentication is configured (see [Security](security.md)), include credentials in requests:

**API Key:**
```
Authorization: Bearer wadjet-key-abc123
```

**JWT:**
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIs...
```

**mTLS:**
```bash
curl --cert client.pem --key client-key.pem --cacert ca.pem https://...
```

## Endpoints

---

### Execute Query

```
POST /v1/queries
```

Execute a SQL query and return results.

**Request:**

```json
{
  "sql": "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10"
}
```

**Headers:**

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Recommended | `application/json` (the server does not check it — the body is decoded as JSON regardless) |
| `Authorization` | If auth configured | Bearer token |

**Response (200 OK):**

```json
{
  "query_id": "q-7f3a2b1c",
  "columns": ["src_ip", "total"],
  "rows": [
    {"src_ip": "10.0.1.50", "total": 104857600},
    {"src_ip": "10.0.2.30", "total": 52428800},
    {"src_ip": "10.0.3.10", "total": 26214400}
  ],
  "stats": {
    "elapsed": "45ms",
    "rows_scanned": 2500000,
    "plan": "Scan(flow_logs) → Filter → Aggregate(src_ip) → Sort(total DESC) → Limit(10)"
  }
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `query_id` | string | Unique identifier for this query execution |
| `columns` | []string | Ordered list of result column names |
| `rows` | []object | Array of row objects (column name → value) |
| `stats.elapsed` | string | Wall-clock execution time |
| `stats.rows_scanned` | int | Rows read from storage on the embedded (no-coordinator) path. On the coordinator path — every `wadjet serve` mode — this carries the **result** row count instead. |
| `stats.plan` | string | Human-readable execution plan |

**Error Response (400/401/403/500):**

```json
{
  "error": "parse error: syntax error at position 42 near 'FORM'"
}
```

**cURL Example:**

```bash
curl -s -X POST http://localhost:8080/v1/queries \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer wadjet-key-abc123" \
  -d '{"sql": "SELECT * FROM flow_logs LIMIT 5"}' | jq .
```

---

### List Tables

```
GET /v1/tables
```

Return all tables registered in the catalog.

**Response (200 OK):**

```json
{
  "tables": ["flow_logs", "syslog", "snmp_traps", "device_inventory"]
}
```

**cURL Example:**

```bash
curl -s http://localhost:8080/v1/tables | jq .
```

---

### Get Table Schema

```
GET /v1/tables/{name}
```

Return the schema and partition keys for a specific table.

**Response (200 OK):**

```json
{
  "name": "flow_logs",
  "schema": {
    "columns": [
      {"name": "timestamp", "type": "Timestamp"},
      {"name": "src_ip", "type": "IPv4"},
      {"name": "dst_ip", "type": "IPv4"},
      {"name": "src_port", "type": "Int32"},
      {"name": "dst_port", "type": "Int32"},
      {"name": "protocol", "type": "String"},
      {"name": "bytes_in", "type": "Int64"},
      {"name": "bytes_out", "type": "Int64"}
    ]
  },
  "partition_keys": ["date"]
}
```

**Error Response (404):**

```json
{
  "error": "table not found: nonexistent_table"
}
```

---

### List Active Queries

```
GET /v1/queries
```

Returns a list of currently active and recently completed queries.

**Response (200 OK):**

```json
{
  "queries": [
    {
      "query_id": "q-7f3a2b1c",
      "state": "running",
      "sql": "SELECT ...",
      "elapsed": "2s"
    }
  ]
}
```

---

### Submit Async Query

```
POST /v1/queries/async
```

Submit a query for asynchronous execution. Returns immediately with a query ID that can be polled for results.

> **Note:** The async endpoints (`POST /v1/queries/async`, `GET /v1/queries`, `GET /v1/queries/{queryID}`, `GET /v1/queries/{queryID}/results`, `DELETE /v1/queries/{queryID}`) need a coordinator. Every `wadjet serve` mode has one — standalone embeds a coordinator, a worker and NATS in one process — so these work there too. They return `503 Service Unavailable` only when the HTTP server is constructed without a coordinator, which is the embedded-library path.

**Request:**

```json
{
  "sql": "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC"
}
```

**Response (202 Accepted):**

```json
{
  "query_id": "q-9e8d7c6b"
}
```

---

### Get Query Status

```
GET /v1/queries/{queryID}
```

Check the status of an async query.

**Response (200 OK):**

```json
{
  "query_id": "q-9e8d7c6b",
  "state": "completed",
  "total_rows": 150,
  "elapsed": "340ms"
}
```

States: `pending`, `running`, `completed`, `failed`, `cancelled`

---

### Get Query Results

```
GET /v1/queries/{queryID}/results
```

Retrieve the results of a completed async query.

**Response (200 OK):**

Same format as the synchronous `POST /v1/queries` response.

---

### Cancel Query

```
DELETE /v1/queries/{queryID}
```

Cancel a running query.

**Response (200 OK):**

```json
{
  "query_id": "q-9e8d7c6b",
  "state": "cancelled"
}
```

---

### Health Check

```
GET /v1/health
```

Returns server health status.

**Response (200 OK):**

```json
{
  "status": "ok"
}
```

---

### Prometheus Metrics

```
GET /metrics
```

Returns Prometheus-formatted metrics. See [Operations](operations.md) for details on available metrics.

**Response (200 OK):**

```
# HELP wadjet_queries_total Total number of queries executed
# TYPE wadjet_queries_total counter
wadjet_queries_total 1523
# HELP wadjet_query_rows_scanned_total Total rows scanned across all queries
# TYPE wadjet_query_rows_scanned_total counter
wadjet_query_rows_scanned_total{table="flow_logs"} 45000000
# HELP wadjet_query_duration_seconds Query execution time
# TYPE wadjet_query_duration_seconds histogram
wadjet_query_duration_seconds_bucket{le="0.01"} 892
wadjet_query_duration_seconds_bucket{le="0.1"} 1400
wadjet_query_duration_seconds_bucket{le="1"} 1510
wadjet_query_duration_seconds_bucket{le="10"} 1523
```

---

## Client Integration Examples

### Python

```python
import requests

WADJET_URL = "http://localhost:8080"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer wadjet-key-abc123",
}

def query(sql: str) -> dict:
    resp = requests.post(
        f"{WADJET_URL}/v1/queries",
        json={"sql": sql},
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()

# Top talkers
result = query("""
    SELECT src_ip, SUM(bytes_in) AS total
    FROM flow_logs
    WHERE date = '2026-03-15'
    GROUP BY src_ip
    ORDER BY total DESC
    LIMIT 10
""")

for row in result["rows"]:
    print(f"{row['src_ip']}: {row['total']:,} bytes")
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
)

type QueryRequest struct {
    SQL string `json:"sql"`
}

type QueryResponse struct {
    QueryID string              `json:"query_id"`
    Columns []string            `json:"columns"`
    Rows    []map[string]any    `json:"rows"`
    Stats   map[string]any      `json:"stats"`
}

func query(sql string) (*QueryResponse, error) {
    body, _ := json.Marshal(QueryRequest{SQL: sql})
    req, _ := http.NewRequest("POST", "http://localhost:8080/v1/queries", bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Authorization", "Bearer wadjet-key-abc123")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var result QueryResponse
    json.NewDecoder(resp.Body).Decode(&result)
    return &result, nil
}

func main() {
    result, _ := query("SELECT src_ip, COUNT(*) AS n FROM flow_logs GROUP BY src_ip LIMIT 10")
    for _, row := range result.Rows {
        fmt.Printf("%s: %v\n", row["src_ip"], row["n"])
    }
}
```

### JavaScript / TypeScript

```typescript
const WADJET_URL = "http://localhost:8080";
const API_KEY = "wadjet-key-abc123";

interface QueryResult {
  query_id: string;
  columns: string[];
  rows: Record<string, unknown>[];
  stats: { elapsed: string; rows_scanned: number };
}

async function query(sql: string): Promise<QueryResult> {
  const res = await fetch(`${WADJET_URL}/v1/queries`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_KEY}`,
    },
    body: JSON.stringify({ sql }),
  });
  if (!res.ok) throw new Error(`Query failed: ${res.statusText}`);
  return res.json();
}

// Usage
const result = await query(`
  SELECT src_ip, SUM(bytes_in) AS total
  FROM flow_logs
  WHERE date = '2026-03-15'
  GROUP BY src_ip
  ORDER BY total DESC
  LIMIT 10
`);
console.table(result.rows);
```

### Shell (cURL + jq)

```bash
# One-liner for quick queries
wadjet_query() {
  curl -s -X POST http://localhost:8080/v1/queries \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${WADJET_API_KEY}" \
    -d "{\"sql\": \"$1\"}" | jq .
}

# Usage
wadjet_query "SELECT COUNT(*) as total FROM flow_logs WHERE date = '2026-03-15'"
```

## Rate Limiting and Concurrency

The HTTP server processes queries concurrently with no built-in rate limiting. For production deployments, place a reverse proxy (e.g., nginx, Caddy, Envoy) in front of Wadjet to enforce rate limits, connection limits, and request timeouts.

## Content Types

| Endpoint | Request | Response |
|----------|---------|----------|
| `POST /v1/queries` | `application/json` | `application/json` |
| `GET /v1/queries` | — | `application/json` |
| `POST /v1/queries/async` | `application/json` | `application/json` |
| `GET /v1/queries/{id}` | — | `application/json` |
| `GET /v1/queries/{id}/results` | — | `application/json` |
| `DELETE /v1/queries/{id}` | — | `application/json` |
| `GET /v1/tables` | — | `application/json` |
| `GET /v1/tables/{name}` | — | `application/json` |
| `GET /v1/health` | — | `application/json` |
| `GET /metrics` | — | `text/plain` (Prometheus) |
