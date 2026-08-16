# Data Types

Wadjet supports a focused set of column types optimized for analytical workloads, with first-class support for network primitives.

## Type Reference

### Numeric Types

| Type | Go Backing | Size | Range | Use Cases |
|------|-----------|------|-------|-----------|
| `Int32` | `int32` | 4 bytes | -2^31 to 2^31-1 | Ports, counters, protocol numbers |
| `Int64` | `int64` | 8 bytes | -2^63 to 2^63-1 | Byte counts, large counters, IDs |
| `Float32` | `float32` | 4 bytes | IEEE 754 | Ratios, percentages |
| `Float64` | `float64` | 8 bytes | IEEE 754 | Latency, jitter, precise measurements |
| `Decimal(p,s)` | `Int128` | 16 bytes | Up to 38 digits | Financial amounts, exact arithmetic |
| `Bool` | `bool` | 1 bit | true/false | Flags, states |

#### DECIMAL Type

`DECIMAL(precision, scale)` stores exact fixed-point numbers using 128-bit scaled integers (the same approach used by DuckDB). The value `123.45` with `DECIMAL(10,2)` is stored internally as `12345` with scale 2.

```sql
CREATE TABLE transactions (
    amount DECIMAL(18,2),
    tax_rate DECIMAL(5,4)
);
```

- **precision**: Total number of digits (1–38, default 38)
- **scale**: Digits after the decimal point (default 0)
- **Arithmetic**: SUM, AVG, MIN, MAX all use exact Int128 arithmetic through the aggregate pipeline
- **Parquet storage**: Written as Parquet DECIMAL logical type for interoperability

### String and Binary Types

| Type | Go Backing | Storage | Use Cases |
|------|-----------|---------|-----------|
| `String` | `string` | Variable-length (offset/data layout) | Hostnames, messages, labels |
| `Bytes` | `[]byte` | Variable-length (offset/data layout) | Raw payloads, binary data |

Variable-length types use an **offset/data** columnar layout: a contiguous data buffer with a parallel offset array indexing into it. This avoids per-row heap allocation.

### Network Types

| Type | Go Backing | Size | Format | Use Cases |
|------|-----------|------|--------|-----------|
| `IPv4` | `uint32` | 4 bytes | Dotted-quad string on input ("10.0.1.1") | Source/destination addresses |
| `IPv6` | `[16]byte` | 16 bytes | Standard IPv6 notation on input | IPv6 addresses |
| `CIDR` | `string` | Variable | CIDR notation ("10.0.0.0/8") | Subnet definitions, ACLs |
| `MAC` | `uint64` | 8 bytes | Colon-separated hex on input ("aa:bb:cc:dd:ee:ff") | Interface identification |
| `Port` | `uint16` (in `Int32Data`) | 2 bytes | Integer 0–65535 | Transport-layer ports |
| `Protocol` | `uint8` (in `Int32Data`) | 1 byte | IANA protocol number | IP protocol (6=TCP, 17=UDP) |

Network types are stored in their compact binary representations (not as strings), enabling efficient comparison and aggregation while maintaining human-readable input/output formats.

### Temporal Types

| Type | Go Backing | Precision | Use Cases |
|------|-----------|-----------|-----------|
| `Timestamp` | `int64` | Milliseconds since epoch | Event times, log timestamps |
| `Date` | `int32` | Days since 1970-01-01 | Calendar dates, partition keys |
| `Duration` | `int64` | Nanoseconds | Time intervals, latency measurements |

### Identifier Types

| Type | Go Backing | Size | Use Cases |
|------|-----------|------|-----------|
| `UUID` | `[16]byte` (ByteArray) | 16 bytes | Unique identifiers, trace IDs, correlation IDs |

### Nested Types

| Type | Storage | Access | Use Cases |
|------|---------|--------|-----------|
| `ARRAY(T)` | Offsets + child vector | `element_at(col, i)`, `ARRAY[1,2,3]` | Tags, IP lists, port lists, DNS answers |
| `ROW(f1 T1, f2 T2, ...)` | Child vector per field | `col.field` dot notation, `row_field()` | Geo (lat/lng/country), enrichment metadata |
| `MAP(K, V)` | Key/value child vectors | `element_at(col, 'key')`, `map_keys()` | HTTP headers, flow labels, key-value metadata |

ARRAY uses an offset-based layout: row `i`'s elements are `child[offsets[i]..offsets[i+1]]`, the same model as Arrow and DuckDB.

```sql
-- Array literal and access
SELECT ARRAY[1, 2, 3] AS nums, element_at(ARRAY[10, 20, 30], 2) AS second

-- ROW field access via dot notation
SELECT person.name, person.age FROM events

-- Array functions
SELECT cardinality(tags) AS num_tags,
       array_contains(tags, 'critical') AS is_critical,
       array_join(tags, ', ') AS tag_list
FROM alerts

-- Map functions
SELECT map_keys(headers) AS header_names,
       element_at(headers, 'Content-Type') AS content_type
FROM http_logs
```

**Array functions:** `cardinality`, `element_at`, `array_contains`, `array_join`, `array_min`, `array_max`, `array_length`

**Map functions:** `map_keys`, `map_values`, `map_entries`, `map_from_entries`

**JSON extraction:** `json_extract`, `json_extract_scalar`, `json_array_length`, `json_valid`

Nested types are fully supported in Parquet read (LIST/MAP/STRUCT pattern detection) and display output.

### Vector Type

| Type | Storage | Parquet | Use Cases |
|------|---------|---------|-----------|
| `VECTOR(N)` | N x float32 per row | FIXED_LEN_BYTE_ARRAY | Embeddings, similarity search |

`VECTOR(N)` stores fixed-dimension float32 vectors for embedding-based workflows. Each row occupies exactly N x 4 bytes with zero overhead.

```sql
-- Create a table with embedding column
CREATE TABLE doc_embeddings (
    doc_id INT64,
    title STRING,
    embedding VECTOR(1536)
)

-- Generate embeddings from text (requires WADJET_OPENAI_API_KEY)
SELECT embed('lateral movement detected') AS vec

-- Semantic similarity search
SELECT doc_id, title,
       cosine_similarity(embedding, embed('credential theft')) AS score
FROM doc_embeddings
ORDER BY score DESC LIMIT 10
```

**Vector functions:** `cosine_similarity(a, b)`, `l2_distance(a, b)`, `dot_product(a, b)`, `vector_norm(a)`, `vector_dims(a)`

**Embedding functions:** `embed(text)`, `embed_model()`, `embed_dim()`

Configure the embedding provider via environment variables:
```bash
export WADJET_OPENAI_API_KEY=sk-...
export WADJET_EMBED_MODEL=text-embedding-3-small  # default
```

Supported models: `text-embedding-3-small` (1536-dim), `text-embedding-3-large` (3072-dim). Embeddings are cached in an LRU cache (50K entries) to avoid repeat API calls.

## Nullability

Every column supports null values via a **null bitmap** — one bit per row indicating presence or absence. This has minimal storage overhead (1 bit per row) and enables three-valued logic in expressions.

Null handling in expressions:
- Comparisons with NULL yield NULL (not true or false)
- `COALESCE(a, b)` returns the first non-null argument
- Aggregates (SUM, COUNT, MIN, MAX, AVG) skip null values
- `COUNT(*)` counts all rows; `COUNT(column)` counts non-null values

## Schema Definition

### In Go (Embedded API)

```go
import "github.com/derekmwright/wadjet/internal/storage/parquet"

schema := parquet.Schema{
    Columns: []parquet.Column{
        {Name: "timestamp",    Type: parquet.TypeTimestamp},
        {Name: "src_ip",      Type: parquet.TypeIPv4},
        {Name: "dst_ip",      Type: parquet.TypeIPv4},
        {Name: "src_port",    Type: parquet.TypePort},
        {Name: "dst_port",    Type: parquet.TypePort},
        {Name: "protocol",    Type: parquet.TypeProtocol},
        {Name: "bytes_in",    Type: parquet.TypeInt64},
        {Name: "bytes_out",   Type: parquet.TypeInt64},
        {Name: "src_mac",     Type: parquet.TypeMAC},
        {Name: "vlan_id",     Type: parquet.TypeInt32},
        {Name: "is_encrypted", Type: parquet.TypeBool},
        {Name: "flow_id",     Type: parquet.TypeUUID},
        {Name: "duration",    Type: parquet.TypeDuration},
    },
}
```

Column types are referenced as `parquet.TypeXxx` constants (e.g., `parquet.TypeIPv4`, `parquet.TypeTimestamp`). The `Nullable` field on `Column` defaults to `false`; set it to `true` to allow nulls.

### In Parquet (Automatic Mapping)

When reading Parquet files written by external tools (e.g., Bento), Wadjet automatically infers types from the Parquet schema:

| Parquet Physical Type | Parquet Logical Annotation | Wadjet Type |
|----------------------|---------------------------|-------------|
| INT32 | none | Int32 |
| INT64 | none | Int64 |
| INT64 | TIMESTAMP_MILLIS | Timestamp |
| FLOAT | none | Float32 |
| DOUBLE | none | Float64 |
| BOOLEAN | none | Bool |
| BYTE_ARRAY | UTF8 | String |
| BYTE_ARRAY | none | Bytes |
| INT64 | DECIMAL | Decimal |

Network types (`IPv4`, `IPv6`, `CIDR`, `MAC`) require explicit schema registration since Parquet has no native representation for these. When creating a table via the API, specify the schema with the correct network types, and the ingester will handle conversion.

## Type Coercion in Expressions

The expression compiler handles implicit type promotion in arithmetic and comparisons:

| Left Type | Right Type | Result Type |
|-----------|-----------|-------------|
| Int32 | Int64 | Int64 |
| Int32 | Float64 | Float64 |
| Int64 | Float64 | Float64 |
| Float32 | Float64 | Float64 |

Explicit casting is available via `CAST(column AS type)`:

```sql
SELECT CAST(src_port AS Int64) FROM flow_logs
```

## Compression

Parquet files written by the ingester support multiple compression codecs:

| Codec | Trade-off | When to Use |
|-------|-----------|-------------|
| **Snappy** (default) | Fast compression/decompression, moderate ratio | General purpose, low-latency queries |
| **Zstd** | Better ratio than Snappy, still fast | Cold storage, archival, bandwidth-constrained |
| **Gzip** | Good ratio, slower | Compatibility with external tools |
| **LZ4** | Fastest decompression | Latency-critical queries |
| **None** | No compression | Debugging, already-compressed data |
