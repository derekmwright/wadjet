# Data Types

Caelum supports a focused set of column types optimized for analytical workloads, with first-class support for network primitives.

## Type Reference

### Numeric Types

| Type | Go Backing | Size | Range | Use Cases |
|------|-----------|------|-------|-----------|
| `Int32` | `int32` | 4 bytes | -2^31 to 2^31-1 | Ports, counters, protocol numbers |
| `Int64` | `int64` | 8 bytes | -2^63 to 2^63-1 | Byte counts, large counters, IDs |
| `Float32` | `float32` | 4 bytes | IEEE 754 | Ratios, percentages |
| `Float64` | `float64` | 8 bytes | IEEE 754 | Latency, jitter, precise measurements |
| `Bool` | `bool` | 1 bit | true/false | Flags, states |

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

Network types are stored in their compact binary representations (not as strings), enabling efficient comparison and aggregation while maintaining human-readable input/output formats.

### Temporal Types

| Type | Go Backing | Precision | Use Cases |
|------|-----------|-----------|-----------|
| `Timestamp` | `int64` | Milliseconds since epoch | Event times, log timestamps |

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
schema := caelum.Schema{
    Columns: []caelum.Column{
        {Name: "timestamp",   Type: caelum.Timestamp},
        {Name: "src_ip",     Type: caelum.IPv4},
        {Name: "dst_ip",     Type: caelum.IPv4},
        {Name: "src_port",   Type: caelum.Int32},
        {Name: "dst_port",   Type: caelum.Int32},
        {Name: "protocol",   Type: caelum.String},
        {Name: "bytes_in",   Type: caelum.Int64},
        {Name: "bytes_out",  Type: caelum.Int64},
        {Name: "src_mac",    Type: caelum.MAC},
        {Name: "vlan_id",    Type: caelum.Int32},
        {Name: "is_encrypted", Type: caelum.Bool},
    },
}
```

### In Parquet (Automatic Mapping)

When reading Parquet files written by external tools (e.g., Bento), Caelum automatically infers types from the Parquet schema:

| Parquet Physical Type | Parquet Logical Annotation | Caelum Type |
|----------------------|---------------------------|-------------|
| INT32 | none | Int32 |
| INT64 | none | Int64 |
| INT64 | TIMESTAMP_MILLIS | Timestamp |
| FLOAT | none | Float32 |
| DOUBLE | none | Float64 |
| BOOLEAN | none | Bool |
| BYTE_ARRAY | UTF8 | String |
| BYTE_ARRAY | none | Bytes |

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
