# gRPC API Reference

Caelum exposes a gRPC API alongside the HTTP API, enabling type-safe client generation for any language with protobuf support.

## Service Definition

The proto file is at `proto/caelum/v1/caelum.proto`. Generated Go code is in `gen/caelum/v1/`.

Default listen address: `:9090` (configurable via `--grpc-addr` or `grpc.addr` in YAML).

## RPCs

### Query (unary)

Execute a SQL query and return all results in a single response.

```protobuf
rpc Query(QueryRequest) returns (QueryResponse);
```

**Request:**
```json
{ "sql": "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip LIMIT 10" }
```

**Response:**
```json
{
  "query_id": "q-7f3a2b1c",
  "columns": ["src_ip", "total"],
  "rows": [
    { "fields": { "src_ip": "10.0.1.50", "total": 104857600 } }
  ],
  "stats": {
    "total_rows": 1,
    "elapsed": "0.045s",
    "plan": "Scan(flow_logs) -> Aggregate(src_ip) -> Sort(total DESC) -> Limit(10)"
  }
}
```

---

### QueryStream (server streaming)

Execute a SQL query and stream results in batches of 1000 rows.

```protobuf
rpc QueryStream(QueryRequest) returns (stream QueryStreamResponse);
```

The first batch includes `columns`. The last batch includes `stats` and `is_last = true`. For empty results, a single response is sent with `is_last = true`.

---

### SubmitQuery (async, distributed mode only)

Submit a query for asynchronous execution. Returns immediately with a query ID.

```protobuf
rpc SubmitQuery(QueryRequest) returns (SubmitQueryResponse);
```

Returns `UNAVAILABLE` in standalone mode.

---

### GetQueryStatus

Poll the status of an async query.

```protobuf
rpc GetQueryStatus(GetQueryStatusRequest) returns (GetQueryStatusResponse);
```

States: `pending`, `running`, `completed`, `failed`, `cancelled`.

The response includes per-stage progress (`StageStatus`) with task counts.

---

### CancelQuery

Cancel a running async query.

```protobuf
rpc CancelQuery(CancelQueryRequest) returns (CancelQueryResponse);
```

---

### ListTables

List all tables in the catalog.

```protobuf
rpc ListTables(ListTablesRequest) returns (ListTablesResponse);
```

---

### DescribeTable

Return a table's schema and partition keys.

```protobuf
rpc DescribeTable(DescribeTableRequest) returns (DescribeTableResponse);
```

---

### CreateTable

Create a new table with a schema and optional partition keys.

```protobuf
rpc CreateTable(CreateTableRequest) returns (CreateTableResponse);
```

Column types: `BIGINT`, `INT`, `DOUBLE`, `FLOAT`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP`, `IPV4`, `IPV6`, `CIDR`, `MAC`, `PORT`, `PROTOCOL`.

---

### DropTable

Drop a table. Set `if_exists = true` to suppress errors if the table doesn't exist.

```protobuf
rpc DropTable(DropTableRequest) returns (DropTableResponse);
```

---

## Health Checking

The server registers the standard [gRPC health checking protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md):

```bash
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

Service names:
- `""` (empty) — overall server health
- `caelum.v1.CaelumService` — query service health

## Client Generation

### Go

Generated Go code is already included at `gen/caelum/v1/`. Import it directly:

```go
import caelumv1 "github.com/derekmwright/caelum/gen/caelum/v1"

conn, _ := grpc.NewClient("localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := caelumv1.NewCaelumServiceClient(conn)

resp, _ := client.Query(ctx, &caelumv1.QueryRequest{Sql: "SELECT COUNT(*) AS n FROM flow_logs"})
fmt.Println(resp.Rows[0].Fields["n"].GetNumberValue())
```

### Python

```bash
pip install grpcio-tools
python -m grpc_tools.protoc \
  -Iproto \
  --python_out=./gen \
  --grpc_python_out=./gen \
  proto/caelum/v1/caelum.proto
```

```python
import grpc
from caelum.v1 import caelum_pb2, caelum_pb2_grpc

channel = grpc.insecure_channel("localhost:9090")
client = caelum_pb2_grpc.CaelumServiceStub(channel)

resp = client.Query(caelum_pb2.QueryRequest(sql="SELECT COUNT(*) AS n FROM flow_logs"))
for row in resp.rows:
    print(row.fields["n"].number_value)
```

### TypeScript / Node.js

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

```typescript
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

const packageDef = protoLoader.loadSync("proto/caelum/v1/caelum.proto");
const proto = grpc.loadPackageDefinition(packageDef) as any;

const client = new proto.caelum.v1.CaelumService(
  "localhost:9090",
  grpc.credentials.createInsecure()
);

client.Query({ sql: "SELECT COUNT(*) AS n FROM flow_logs" }, (err, resp) => {
  console.log(resp.rows[0].fields.n.numberValue);
});
```

### Java

Use the [protobuf-gradle-plugin](https://github.com/google/protobuf-gradle-plugin) or `protoc` directly:

```bash
protoc --java_out=src/main/java --grpc-java_out=src/main/java \
  -Iproto proto/caelum/v1/caelum.proto
```

### Rust

Use [tonic-build](https://docs.rs/tonic-build) in your `build.rs`:

```rust
tonic_build::compile_protos("proto/caelum/v1/caelum.proto")?;
```

## Regenerating Code

```bash
# Using Taskfile
task proto

# Or manually
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/caelum/v1/caelum.proto
```

Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`.

## Error Codes

| gRPC Code | When |
|-----------|------|
| `INVALID_ARGUMENT` | Empty SQL, missing table name, invalid column type |
| `NOT_FOUND` | Table does not exist, query ID not found |
| `UNAVAILABLE` | No query engine configured, async RPCs in standalone mode |
| `INTERNAL` | Query execution error, storage error |
