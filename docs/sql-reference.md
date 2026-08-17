# SQL Reference

Wadjet supports a broad subset of SQL for analytical queries, parsed by a custom recursive descent parser with precedence-climbing expression parsing.

## Supported Statement Types

| Statement | Description |
|-----------|-------------|
| `SELECT` | Query data from tables |
| `EXPLAIN [VERBOSE]` | Show the query execution plan without running it |
| `DESCRIBE table_name` | Show the schema of a table |
| `SHOW COLUMNS FROM table_name` | Alias for DESCRIBE |
| `SHOW TABLES` | List all tables |
| `SHOW FUNCTIONS` | List registered user-defined functions |
| `CREATE TABLE` | Create a table with schema and optional partitioning |
| `DROP TABLE [IF EXISTS]` | Remove a table |
| `CREATE [OR REPLACE] FUNCTION` | Register a user-defined function |
| `DROP FUNCTION [IF EXISTS]` | Remove a user-defined function |

## SELECT Statement

```sql
SELECT [DISTINCT] [columns | expressions | aggregates | window_functions]
FROM table_name [alias]
[JOIN other_table [alias] ON condition]
[WHERE condition]
[GROUP BY columns | positions]
[HAVING condition]
[ORDER BY columns [ASC|DESC] [NULLS FIRST|LAST]]
[LIMIT n [OFFSET m]]
```

## Table Functions

Query files directly from SQL without prior ingestion. Table functions appear in the `FROM` clause and support positional arguments, named parameters (`key=value`), and glob patterns.

### read_json

Reads JSON files (JSONL or JSON array) with automatic schema inference and a custom direct-to-columnar byte scanner.

```sql
-- Local file (JSONL or JSON array auto-detected)
SELECT * FROM read_json('/path/to/data.json')

-- HTTP/HTTPS URL
SELECT * FROM read_json('https://api.example.com/events.json')

-- Glob pattern (concatenates matching files)
SELECT * FROM read_json('logs/2026-03-*.json')

-- With alias
SELECT j.src_ip, j.bytes FROM read_json('traffic.json') AS j WHERE j.bytes > 1000
```

Type inference detects: integers, floats, booleans, IPv4 addresses, timestamps (RFC 3339, ISO 8601, date-only), and strings.

### read_csv

Reads CSV files with configurable parsing and type inference.

```sql
-- Default (comma-delimited, first row is header)
SELECT * FROM read_csv('data.csv')

-- Custom delimiter
SELECT * FROM read_csv('data.tsv', delimiter='\t')

-- Pipe-delimited, no header
SELECT * FROM read_csv('data.txt', delimiter='|', header=false)

-- Glob across partitioned files
SELECT * FROM read_csv('export/part_*.csv')
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `delimiter` / `delim` / `sep` | `,` | Field separator character |
| `header` | `true` | Whether the first row contains column names |

### read_parquet

Reads Parquet files with column-at-a-time page reading and row-group stats pruning.

```sql
SELECT * FROM read_parquet('warehouse/sales.parquet')
SELECT * FROM read_parquet('https://storage.example.com/data.parquet')
```

All table functions support local file paths and HTTP/HTTPS URLs with connection pooling and configurable auth headers.

### Streaming I/O

Local CSV files are read in streaming mode — only the current batch of rows is held in memory at a time, making it possible to query files larger than available RAM. Schema is inferred from the first 100 rows.

Local Parquet files are opened as file handles (`io.ReaderAt`), enabling page-level random access without reading the entire file into memory. HTTP sources and glob patterns still require a full download.

### postgres_scan / postgres_query

Query external PostgreSQL databases directly from SQL. Uses `database/sql` with the `lib/pq` driver.

```sql
-- Scan an entire table
SELECT * FROM postgres_scan('host=pghost dbname=mydb user=readonly', 'customers')

-- Run an arbitrary query with pushdown
SELECT name, total
FROM postgres_query('host=pghost dbname=mydb sslmode=require', 'SELECT name, SUM(amount) AS total FROM orders GROUP BY name')
WHERE total > 1000
```

Connection strings use the standard PostgreSQL `libpq` format (`host=... dbname=... user=... password=... sslmode=...`).

### mysql_scan / mysql_query

Query external MySQL databases directly from SQL. Uses the `go-sql-driver/mysql` driver.

```sql
-- Scan an entire table
SELECT * FROM mysql_scan('user:password@tcp(mysqlhost:3306)/mydb', 'products')

-- Run an arbitrary query
SELECT * FROM mysql_query('user:password@tcp(mysqlhost:3306)/mydb', 'SELECT * FROM logs WHERE created_at > NOW() - INTERVAL 1 HOUR')
```

Connection strings use the standard MySQL DSN format (`user:password@tcp(host:port)/dbname`).

### Database Connector Type Mapping

| Database Type | Wadjet Type |
|---|---|
| BOOL, BOOLEAN | BOOL |
| SMALLINT, TINYINT | INT32 |
| INT, INTEGER, BIGINT | INT64 |
| FLOAT, REAL, DOUBLE | FLOAT64 |
| NUMERIC, DECIMAL | FLOAT64 |
| DATE | DATE |
| TIMESTAMP, DATETIME | TIMESTAMP |
| TEXT, VARCHAR, CHAR | STRING |
| BYTEA, BLOB | BYTES |
| JSON, JSONB | STRING |
| UUID, INET, CIDR | STRING |

## Common Table Expressions (CTEs)

CTEs define named temporary result sets for use in the main query:

```sql
-- Basic CTE
WITH recent_flows AS (
    SELECT * FROM flow_logs WHERE date = '2026-03-15'
)
SELECT src_ip, SUM(bytes_in) AS total
FROM recent_flows
GROUP BY src_ip

-- Multiple CTEs
WITH
    high_traffic AS (
        SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip HAVING SUM(bytes_in) > 1000000
    ),
    devices AS (
        SELECT ip_address, hostname FROM device_inventory
    )
SELECT d.hostname, h.total
FROM high_traffic h
JOIN devices d ON h.src_ip = d.ip_address

-- CTE with column list
WITH traffic(ip, total_bytes) AS (
    SELECT src_ip, SUM(bytes_in) FROM flow_logs GROUP BY src_ip
)
SELECT ip, total_bytes FROM traffic ORDER BY total_bytes DESC LIMIT 10
```

## Set Operations (UNION, INTERSECT, EXCEPT)

Combine results from multiple queries:

```sql
-- UNION: all rows from both sides, deduplicated
SELECT src_ip AS ip FROM flow_logs
UNION
SELECT ip_address AS ip FROM device_inventory

-- UNION ALL: all rows, including duplicates
SELECT src_ip, bytes_in FROM flow_logs WHERE date = '2026-03-14'
UNION ALL
SELECT src_ip, bytes_in FROM flow_logs WHERE date = '2026-03-15'
ORDER BY bytes_in DESC
LIMIT 100

-- INTERSECT: only rows that appear in both sides
SELECT user_id FROM purchases
INTERSECT
SELECT user_id FROM refunds

-- INTERSECT ALL: preserves duplicate counts (min of left/right occurrences)
SELECT user_id FROM purchases
INTERSECT ALL
SELECT user_id FROM refunds

-- EXCEPT: rows from the left side that do not appear in the right side
SELECT src_ip FROM flow_logs
EXCEPT
SELECT ip_address FROM blocklist

-- EXCEPT ALL: each right occurrence removes one left occurrence
SELECT src_ip FROM flow_logs
EXCEPT ALL
SELECT ip_address FROM blocklist
```

All set operations support ORDER BY and LIMIT on the combined result. Operations are left-associative when chained (e.g., `A UNION B EXCEPT C` is `(A UNION B) EXCEPT C`).

## EXPLAIN

View the query plan without executing:

```sql
EXPLAIN SELECT src_ip, SUM(bytes_in) FROM flow_logs GROUP BY src_ip

-- Verbose plan with more detail
EXPLAIN VERBOSE SELECT src_ip, SUM(bytes_in) FROM flow_logs GROUP BY src_ip
```

## DESCRIBE

Inspect a table's schema:

```sql
DESCRIBE flow_logs
-- or
SHOW COLUMNS FROM flow_logs

-- Output: column names, types, nullable
```

## CREATE TABLE

```sql
CREATE TABLE flow_logs (
    src_ip    IPv4 NOT NULL,
    dst_ip    IPv4 NOT NULL,
    src_port  Int32,
    dst_port  Int32,
    bytes_in  Int64,
    timestamp Timestamp NOT NULL
) PARTITION BY (date)
```

## Column Selection

```sql
-- All columns
SELECT * FROM flow_logs

-- Table-qualified wildcard
SELECT f.* FROM flow_logs f

-- Specific columns
SELECT src_ip, dst_ip, bytes_in FROM flow_logs

-- Aliased columns
SELECT src_ip AS source, dst_ip AS destination FROM flow_logs
```

## WHERE Clause

### Comparison Operators

```sql
SELECT * FROM flow_logs WHERE dst_port = 443
SELECT * FROM flow_logs WHERE bytes_in > 1000000
SELECT * FROM flow_logs WHERE protocol != 'UDP'
SELECT * FROM flow_logs WHERE protocol <> 'UDP'    -- alternate syntax
SELECT * FROM flow_logs WHERE src_port >= 1024
SELECT * FROM flow_logs WHERE bytes_out <= 512
```

### Logical Operators

```sql
SELECT * FROM flow_logs WHERE dst_port = 443 AND protocol = 'TCP'
SELECT * FROM flow_logs WHERE dst_port = 80 OR dst_port = 443
SELECT * FROM flow_logs WHERE NOT (protocol = 'ICMP')
```

### Complex Predicates

```sql
SELECT * FROM flow_logs
WHERE (dst_port = 443 OR dst_port = 8443)
  AND bytes_in > 10000
  AND protocol = 'TCP'
```

### String Matching

```sql
SELECT * FROM syslog WHERE message LIKE '%error%'
SELECT * FROM syslog WHERE hostname LIKE 'fw-%'
SELECT * FROM syslog WHERE message NOT LIKE '%debug%'
```

### NULL Handling

```sql
SELECT * FROM flow_logs WHERE src_ip IS NULL
SELECT * FROM flow_logs WHERE src_ip IS NOT NULL
SELECT * FROM flow_logs WHERE active IS TRUE
SELECT * FROM flow_logs WHERE active IS NOT TRUE
SELECT * FROM flow_logs WHERE active IS FALSE
SELECT * FROM flow_logs WHERE active IS NOT FALSE
```

### IN Predicate

```sql
SELECT * FROM flow_logs WHERE dst_port IN (80, 443, 8080, 8443)
SELECT * FROM syslog WHERE severity IN ('error', 'critical')
SELECT * FROM flow_logs WHERE dst_port NOT IN (22, 23)
```

### BETWEEN Predicate

```sql
SELECT * FROM flow_logs WHERE dst_port BETWEEN 1024 AND 65535
SELECT * FROM flow_logs WHERE bytes_in NOT BETWEEN 0 AND 100
```

### EXISTS Predicate

```sql
SELECT * FROM flow_logs f
WHERE EXISTS (SELECT 1 FROM blocked_ips b WHERE b.ip = f.src_ip)

SELECT * FROM flow_logs f
WHERE NOT EXISTS (SELECT 1 FROM device_inventory d WHERE d.ip_address = f.src_ip)
```

## Subqueries

### Scalar Subqueries

```sql
SELECT src_ip, bytes_in,
       bytes_in - (SELECT AVG(bytes_in) FROM flow_logs) AS diff_from_avg
FROM flow_logs
```

### IN Subqueries

```sql
SELECT * FROM flow_logs
WHERE src_ip IN (SELECT ip_address FROM device_inventory WHERE role = 'server')
```

### Correlated Subqueries

Subqueries that reference columns from the outer query. Re-executed per outer row:

```sql
-- EXISTS with correlation
SELECT s.s_name FROM supplier s
WHERE EXISTS (
    SELECT 1 FROM lineitem l WHERE l.l_suppkey = s.s_suppkey
)

-- Correlated scalar subquery
SELECT o.o_orderkey, (
    SELECT SUM(l.l_extendedprice) FROM lineitem l
    WHERE l.l_orderkey = o.o_orderkey
) AS total
FROM orders o

-- NOT EXISTS
SELECT c.c_name FROM customer c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.o_custkey = c.c_custkey
)
```

### Derived Tables

```sql
SELECT t.src_ip, t.total
FROM (
    SELECT src_ip, SUM(bytes_in) AS total
    FROM flow_logs
    GROUP BY src_ip
) AS t
WHERE t.total > 1000000
```

## Aggregate Functions

| Function | Description | Null Handling |
|----------|-------------|---------------|
| `COUNT(*)` | Count all rows | Counts nulls |
| `COUNT(column)` | Count non-null values | Skips nulls |
| `COUNT(DISTINCT column)` | Count distinct non-null values | Skips nulls |
| `SUM(column)` | Sum of values | Skips nulls |
| `MIN(column)` | Minimum value | Skips nulls |
| `MAX(column)` | Maximum value | Skips nulls |
| `AVG(column)` | Average value | Skips nulls |
| `STRING_AGG(column, separator)` | Concatenate values with separator | Skips nulls |
| `BOOL_AND(column)` / `EVERY(column)` | True if all values are true | Skips nulls |
| `BOOL_OR(column)` | True if any value is true | Skips nulls |
| `STDDEV(column)` / `STDDEV_SAMP(column)` | Sample standard deviation | Skips nulls |
| `STDDEV_POP(column)` | Population standard deviation | Skips nulls |
| `VARIANCE(column)` / `VAR_SAMP(column)` | Sample variance | Skips nulls |
| `VAR_POP(column)` | Population variance | Skips nulls |
| `APPROX_DISTINCT(column)` | Approximate distinct count | Skips nulls |
| `CORR(y, x)` | Pearson correlation coefficient | Skips nulls |
| `COVAR_SAMP(y, x)` | Sample covariance | Skips nulls |
| `COVAR_POP(y, x)` | Population covariance | Skips nulls |
| `PERCENTILE_CONT(p, column)` | Continuous percentile (interpolated) | Skips nulls |
| `PERCENTILE_DISC(p, column)` | Discrete percentile (nearest rank) | Skips nulls |
| `MODE(column)` | Most frequent value | Skips nulls |
| `MEDIAN(column)` | Median value (= percentile_cont(0.5)) | Skips nulls |
| `MIN_BY(return_col, sort_col)` | Value at row where sort_col is minimum | Skips nulls |
| `MAX_BY(return_col, sort_col)` | Value at row where sort_col is maximum | Skips nulls |

### Examples

```sql
-- Total traffic by source IP
SELECT src_ip, SUM(bytes_in) AS total_bytes, COUNT(*) AS flow_count
FROM flow_logs
GROUP BY src_ip
ORDER BY total_bytes DESC
LIMIT 20

-- Count distinct destinations per source
SELECT src_ip, COUNT(DISTINCT dst_ip) AS unique_destinations
FROM flow_logs
GROUP BY src_ip

-- Top talkers by destination port
SELECT dst_port, SUM(bytes_in + bytes_out) AS total_traffic
FROM flow_logs
GROUP BY dst_port
HAVING SUM(bytes_in + bytes_out) > 1000000
ORDER BY total_traffic DESC
```

## GROUP BY

Groups rows sharing specified column values and applies aggregate functions:

```sql
SELECT device, severity, COUNT(*) AS event_count
FROM syslog
GROUP BY device, severity
ORDER BY event_count DESC
```

### Positional References

Use column positions (1-indexed) instead of repeating expressions:

```sql
-- These are equivalent:
SELECT src_ip, dst_port, COUNT(*) FROM flow_logs GROUP BY src_ip, dst_port
SELECT src_ip, dst_port, COUNT(*) FROM flow_logs GROUP BY 1, 2
```

### GROUPING SETS

Generate multiple levels of aggregation in a single query:

```sql
-- Explicit grouping sets
SELECT region, product, SUM(sales)
FROM orders
GROUP BY GROUPING SETS ((region, product), (region), ())

-- ROLLUP: hierarchical subtotals
SELECT year, month, SUM(revenue)
FROM sales
GROUP BY ROLLUP (year, month)
-- Equivalent to: GROUPING SETS ((year, month), (year), ())

-- CUBE: all possible combinations
SELECT region, product, SUM(sales)
FROM orders
GROUP BY CUBE (region, product)
-- Equivalent to: GROUPING SETS ((region, product), (region), (product), ())
```

Each grouping set produces its own aggregation level. Columns not in a given set are NULL in the output.

## HAVING

Filters groups after aggregation (whereas WHERE filters rows before aggregation):

```sql
SELECT src_ip, COUNT(*) AS conn_count
FROM flow_logs
GROUP BY src_ip
HAVING COUNT(*) > 1000
```

## ORDER BY

```sql
-- Ascending (default)
SELECT * FROM flow_logs ORDER BY timestamp ASC

-- Descending
SELECT * FROM flow_logs ORDER BY bytes_in DESC

-- Multi-column sort
SELECT * FROM flow_logs ORDER BY src_ip ASC, bytes_in DESC

-- Positional references
SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY 1 ORDER BY 2 DESC

-- Null ordering
SELECT * FROM flow_logs ORDER BY src_ip ASC NULLS FIRST
SELECT * FROM flow_logs ORDER BY bytes_in DESC NULLS LAST
```

## LIMIT and OFFSET

```sql
-- First 100 rows
SELECT * FROM flow_logs LIMIT 100

-- Pagination: skip 200, return next 100
SELECT * FROM flow_logs ORDER BY timestamp DESC LIMIT 100 OFFSET 200
```

## JOIN

Wadjet supports multiple join types using a hash join strategy.

### Inner Join

```sql
SELECT f.src_ip, f.bytes_in, d.hostname, d.location
FROM flow_logs f
JOIN device_inventory d ON f.src_ip = d.ip_address
WHERE f.bytes_in > 1000000
```

### Left Join

```sql
SELECT f.src_ip, f.bytes_in, d.hostname
FROM flow_logs f
LEFT JOIN device_inventory d ON f.src_ip = d.ip_address
```

### Right Join

```sql
SELECT f.src_ip, d.hostname, d.location
FROM flow_logs f
RIGHT JOIN device_inventory d ON f.src_ip = d.ip_address
```

### Full Outer Join

```sql
SELECT f.src_ip, d.hostname
FROM flow_logs f
FULL OUTER JOIN device_inventory d ON f.src_ip = d.ip_address
```

### Cross Join

```sql
SELECT f.src_ip, p.name AS protocol_name
FROM flow_logs f
CROSS JOIN protocols p
```

The join implementation uses a **hash join** strategy: the right side is loaded into a hash table (build phase), then the left side is probed against it (probe phase). Place the smaller table on the right side of the JOIN for best performance.

## Arithmetic Expressions

```sql
SELECT
    src_ip,
    bytes_in + bytes_out AS total_bytes,
    bytes_in * 8 AS bits_in,
    bytes_in % 1024 AS remainder,
    CAST(bytes_in AS Float64) / CAST(packets AS Float64) AS avg_packet_size
FROM flow_logs
```

### Supported Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction (binary and unary) |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |
| `\|\|` | String concatenation |

## DISTINCT

Deduplicate result rows:

```sql
SELECT DISTINCT protocol FROM flow_logs
SELECT DISTINCT src_ip, dst_port FROM flow_logs WHERE date = '2026-03-15'
```

## CASE Expressions

### Searched CASE

```sql
SELECT
    src_ip,
    CASE
        WHEN dst_port = 443 THEN 'HTTPS'
        WHEN dst_port = 80 THEN 'HTTP'
        WHEN dst_port = 22 THEN 'SSH'
        ELSE 'OTHER'
    END AS traffic_type,
    bytes_in
FROM flow_logs
```

### Simple CASE

```sql
SELECT
    src_ip,
    CASE protocol
        WHEN 'TCP' THEN 'Transmission Control'
        WHEN 'UDP' THEN 'User Datagram'
        ELSE 'Other'
    END AS protocol_name
FROM flow_logs
```

## CAST

```sql
SELECT CAST(dst_port AS Int64) FROM flow_logs
SELECT CAST(bytes_in AS Float64) / CAST(packets AS Float64) AS avg_size FROM flow_logs
```

## Window Functions

Window functions compute values across sets of rows related to the current row without collapsing them into groups.

### Supported Window Functions

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Sequential row number within partition |
| `RANK()` | Rank with gaps for ties |
| `DENSE_RANK()` | Rank without gaps for ties |
| `SUM(expr)` | Running or partition sum |
| `COUNT(expr)` | Running or partition count |
| `AVG(expr)` | Running or partition average |
| `MIN(expr)` | Running or partition minimum |
| `MAX(expr)` | Running or partition maximum |
| `LAG(expr [, offset [, default]])` | Value from a preceding row |
| `LEAD(expr [, offset [, default]])` | Value from a following row |
| `FIRST_VALUE(expr)` | First value in the partition |
| `LAST_VALUE(expr)` | Last value in the partition |
| `NTH_VALUE(expr, n)` | Value at the nth row in the partition |
| `NTILE(n)` | Distribute rows into n buckets |
| `PERCENT_RANK()` | Relative rank: (rank - 1) / (total - 1) |
| `CUME_DIST()` | Cumulative distribution |

### Basic Window Functions

```sql
SELECT
    timestamp,
    src_ip,
    bytes_in,
    SUM(bytes_in) OVER (PARTITION BY src_ip ORDER BY timestamp) AS running_total,
    ROW_NUMBER() OVER (PARTITION BY src_ip ORDER BY bytes_in DESC) AS rank
FROM flow_logs
WHERE date = '2026-03-15'
```

### PARTITION BY and ORDER BY

```sql
-- Partition by multiple columns
SELECT
    dept, team, salary,
    RANK() OVER (PARTITION BY dept, team ORDER BY salary DESC) AS team_rank
FROM employees

-- Order with null placement
SELECT
    src_ip, bytes_in,
    ROW_NUMBER() OVER (ORDER BY bytes_in DESC NULLS LAST) AS rank
FROM flow_logs
```

### Window Frame Specifications

Frame specifications control which rows within the partition are included in the window calculation:

```sql
-- Running sum (all rows from start to current)
SELECT
    timestamp, bytes_in,
    SUM(bytes_in) OVER (
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM flow_logs

-- Sliding window (3-row moving average)
SELECT
    timestamp, bytes_in,
    AVG(bytes_in) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS moving_avg
FROM flow_logs

-- Range-based frame
SELECT
    timestamp, bytes_in,
    SUM(bytes_in) OVER (
        ORDER BY timestamp
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative
FROM flow_logs
```

#### Frame Bound Options

| Bound | Description |
|-------|-------------|
| `UNBOUNDED PRECEDING` | From the first row of the partition |
| `N PRECEDING` | N rows/values before current row |
| `CURRENT ROW` | The current row |
| `N FOLLOWING` | N rows/values after current row |
| `UNBOUNDED FOLLOWING` | To the last row of the partition |

#### Frame Modes

| Mode | Description |
|------|-------------|
| `ROWS` | Physical row-based window boundaries |
| `RANGE` | Logical value-based window boundaries |

## Built-in Functions

Wadjet includes 273 built-in scalar functions across several categories.

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER(protocol)` |
| `LOWER(s)` | Lowercase | `LOWER(hostname)` |
| `CONCAT(a, b, ...)` | Concatenate strings | `CONCAT(src_ip, ':', src_port)` |
| `LENGTH(s)` / `LEN(s)` | String length | `LENGTH(message)` |
| `SUBSTR(s, start, len)` | Extract substring | `SUBSTR(message, 1, 50)` |
| `TRIM(s)` | Remove leading/trailing whitespace | `TRIM(hostname)` |
| `LTRIM(s)` | Remove leading whitespace | `LTRIM(message)` |
| `RTRIM(s)` | Remove trailing whitespace | `RTRIM(message)` |
| `REPLACE(s, old, new)` | Replace occurrences | `REPLACE(message, 'error', 'ERROR')` |
| `REVERSE(s)` | Reverse string | `REVERSE(hostname)` |
| `LEFT(s, n)` | First n characters | `LEFT(hostname, 3)` |
| `RIGHT(s, n)` | Last n characters | `RIGHT(hostname, 2)` |
| `STARTS_WITH(s, prefix)` | Test if string starts with prefix | `STARTS_WITH(hostname, 'web')` |
| `ENDS_WITH(s, suffix)` | Test if string ends with suffix | `ENDS_WITH(hostname, '.com')` |
| `CONTAINS(s, sub)` | Test if string contains substring | `CONTAINS(message, 'error')` |
| `REPEAT(s, n)` | Repeat string n times | `REPEAT('*', 10)` |
| `SPLIT_PART(s, delim, n)` | Extract nth part from delimited string (1-based) | `SPLIT_PART(url, '/', 3)` |
| `STRPOS(s, sub)` / `POSITION(sub IN s)` | Position of substring (1-based, 0 if not found) | `STRPOS(message, 'error')` |
| `REGEXP_LIKE(s, pattern)` | Test if string matches regex | `REGEXP_LIKE(src_ip, '^\d+\.\d+')` |
| `REGEXP_EXTRACT(s, pattern [, group])` | Extract regex match or capture group | `REGEXP_EXTRACT(url, '(\w+)://(\w+)', 2)` |
| `REGEXP_REPLACE(s, pattern, repl)` | Replace regex matches | `REGEXP_REPLACE(message, '\s+', ' ')` |
| `REGEXP_COUNT(s, pattern)` | Count regex matches | `REGEXP_COUNT(path, '/')` → `3` |
| `REGEXP_EXTRACT_ALL(s, pattern)` | Extract all regex matches (JSON array) | `REGEXP_EXTRACT_ALL(log, '\d+')` → `'["123","456"]'` |
| `REGEXP_SPLIT(s, pattern)` | Split by regex (JSON array) | `REGEXP_SPLIT(csv, ',\s*')` |
| `SPLIT(s, delim)` | Split by delimiter (JSON array) | `SPLIT('a.b.c', '.')` → `'["a","b","c"]'` |
| `LPAD(s, n [, pad])` | Left-pad to length n | `LPAD(port, 5, '0')` |
| `RPAD(s, n [, pad])` | Right-pad to length n | `RPAD(name, 20)` |
| `CHR(n)` | Character from code point | `CHR(65)` → `'A'` |
| `CODEPOINT(s)` | Code point of first character | `CODEPOINT('A')` → `65` |
| `CONCAT_WS(sep, a, b, ...)` | Concatenate with separator (skips NULLs) | `CONCAT_WS(',', a, b, c)` |
| `CHAR_LENGTH(s)` | Character length (Unicode-aware) | `CHAR_LENGTH('日本語')` → `3` |
| `TRANSLATE(s, from, to)` | Character-by-character translation | `TRANSLATE('abc', 'abc', 'xyz')` |
| `SOUNDEX(s)` | Phonetic code | `SOUNDEX('Robert')` → `'R163'` |
| `LEVENSHTEIN_DISTANCE(a, b)` | Edit distance between strings | `LEVENSHTEIN_DISTANCE('kitten', 'sitting')` → `3` |
| `HAMMING_DISTANCE(a, b)` | Number of differing characters | `HAMMING_DISTANCE('abc', 'axc')` → `1` |
| `NORMALIZE(s)` | Unicode NFC normalization | `NORMALIZE(text)` |
| `FORMAT(fmt, args...)` | Go-style sprintf formatting | `FORMAT('%s:%d', host, port)` |
| `LCASE(s)` / `UCASE(s)` | Aliases for LOWER/UPPER | `LCASE(name)` |
| `TO_UTF8(s)` | String to UTF-8 byte representation (hex) | `TO_UTF8('hello')` |
| `FROM_UTF8(bytes)` | UTF-8 bytes (hex) to string | `FROM_UTF8(data)` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(n)` | Absolute value | `ABS(bytes_in - bytes_out)` |
| `CEIL(n)` | Round up | `CEIL(avg_latency)` |
| `FLOOR(n)` | Round down | `FLOOR(avg_latency)` |
| `ROUND(n)` | Round to nearest | `ROUND(ratio)` |
| `POW(base, exp)` / `POWER(base, exp)` | Exponentiation | `POW(2, 10)` |
| `SQRT(n)` | Square root | `SQRT(variance)` |
| `MOD(a, b)` | Modulo | `MOD(timestamp, 3600000)` |
| `LOG(n)` | Base-10 logarithm | `LOG(bytes_in)` |
| `LN(n)` | Natural logarithm | `LN(bytes_in)` |
| `EXP(n)` | Exponential (e^n) | `EXP(rate)` |
| `SIGN(n)` | Sign of number (-1, 0, 1) | `SIGN(profit)` |
| `GREATEST(a, b, ...)` | Largest value | `GREATEST(bytes_in, bytes_out)` |
| `LEAST(a, b, ...)` | Smallest value | `LEAST(bytes_in, bytes_out)` |
| `BITWISE_AND(a, b)` | Bitwise AND | `BITWISE_AND(flags, 0xFF)` |
| `BITWISE_OR(a, b)` | Bitwise OR | `BITWISE_OR(flags, 0x01)` |
| `BITWISE_XOR(a, b)` | Bitwise XOR | `BITWISE_XOR(a, b)` |
| `BITWISE_NOT(a)` | Bitwise NOT | `BITWISE_NOT(mask)` |
| `BITWISE_LEFT_SHIFT(a, n)` | Shift bits left by n positions | `BITWISE_LEFT_SHIFT(1, 4)` → `16` |
| `BITWISE_RIGHT_SHIFT(a, n)` | Logical shift bits right by n positions | `BITWISE_RIGHT_SHIFT(16, 4)` → `1` |
| `BITWISE_ARITHMETIC_SHIFT_RIGHT(a, n)` | Arithmetic right shift (sign-preserving) | `BITWISE_ARITHMETIC_SHIFT_RIGHT(-16, 2)` → `-4` |
| `PI()` | Pi constant | `PI()` → `3.14159...` |
| `DEGREES(rad)` | Radians to degrees | `DEGREES(PI())` → `180` |
| `RADIANS(deg)` | Degrees to radians | `RADIANS(180)` → `3.14159...` |
| `SIN(x)` | Sine | `SIN(RADIANS(30))` |
| `COS(x)` | Cosine | `COS(0)` → `1` |
| `TAN(x)` | Tangent | `TAN(RADIANS(45))` → `1` |
| `ASIN(x)` | Arcsine | `ASIN(1)` → `1.5707...` |
| `ACOS(x)` | Arccosine | `ACOS(1)` → `0` |
| `ATAN(x)` | Arctangent | `ATAN(1)` → `0.7853...` |
| `ATAN2(y, x)` | Two-argument arctangent | `ATAN2(1, 1)` |
| `CBRT(n)` | Cube root | `CBRT(27)` → `3` |
| `LOG2(n)` | Base-2 logarithm | `LOG2(8)` → `3` |
| `TRUNCATE(n [, d])` | Truncate to d decimal places | `TRUNCATE(3.789, 2)` → `3.78` |
| `RANDOM()` / `RAND()` | Random float in [0, 1) | `RANDOM()` |
| `E()` | Euler's number (2.71828...) | `E()` |
| `LOG10(n)` | Base-10 logarithm | `LOG10(1000)` → `3` |
| `INFINITY()` | Positive infinity | `INFINITY()` |
| `NAN()` | Not-a-Number | `NAN()` |
| `IS_NAN(n)` | Test if value is NaN | `IS_NAN(result)` |
| `IS_FINITE(n)` | Test if value is finite | `IS_FINITE(result)` |
| `IS_INFINITE(n)` | Test if value is infinite | `IS_INFINITE(result)` |
| `WIDTH_BUCKET(val, min, max, buckets)` | Assign value to histogram bucket | `WIDTH_BUCKET(latency, 0, 100, 10)` |
| `FROM_BASE(s, base)` | Convert string in given base to int | `FROM_BASE('ff', 16)` → `255` |
| `TO_BASE(n, base)` | Convert int to string in given base | `TO_BASE(255, 16)` → `'ff'` |
| `BIT_COUNT(n)` | Count set bits (popcount) | `BIT_COUNT(255)` → `8` |

### Conditional Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COALESCE(a, b, ...)` | First non-null value | `COALESCE(hostname, 'unknown')` |
| `NULLIF(a, b)` | Returns NULL if a = b | `NULLIF(bytes_in, 0)` |
| `IFNULL(a, b)` | Returns b if a is NULL | `IFNULL(src_ip, '0.0.0.0')` |
| `IF(cond, then, else)` | Conditional value | `IF(bytes_in > 1000000, 'large', 'small')` |

### Type Casting Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CAST(expr AS type)` | SQL-standard type cast | `CAST(port AS Int64)` |
| `CAST_INT(s)` | Cast to integer | `CAST_INT('443')` |
| `CAST_FLOAT(s)` | Cast to float | `CAST_FLOAT('3.14')` |
| `CAST_STRING(n)` | Cast to string | `CAST_STRING(dst_port)` |

### Network Functions

| Function | Description | Example |
|----------|-------------|---------|
| `IP_TO_STRING(ip)` | Convert binary IP to string | `IP_TO_STRING(src_ip)` |
| `CIDR_CONTAINS(cidr, ip)` | Test if IP is in CIDR range | `CIDR_CONTAINS('10.0.0.0/8', src_ip)` |
| `IP_VERSION(ip)` | Return IP version (4 or 6) | `IP_VERSION(src_ip)` |
| `MASK_IP(ip)` | Mask an IP address | `MASK_IP(src_ip)` |
| `MAC_TO_STRING(mac)` | Convert binary MAC to string | `MAC_TO_STRING(src_mac)` |
| `IP_SUBNET(ip)` | Extract subnet from IP | `IP_SUBNET(src_ip)` |
| `IP_NETMASK(cidr)` | Extract netmask from CIDR | `IP_NETMASK(src_cidr)` |
| `IS_PRIVATE_IP(ip)` | Test if IP is in RFC 1918 private range | `IS_PRIVATE_IP('192.168.1.1')` → `true` |
| `IS_LOOPBACK_IP(ip)` | Test if IP is loopback | `IS_LOOPBACK_IP('127.0.0.1')` → `true` |
| `IP_TO_INT(ip)` | Convert IPv4 to integer | `IP_TO_INT('10.0.0.1')` → `167772161` |
| `INT_TO_IP(n)` | Convert integer to IPv4 string | `INT_TO_IP(167772161)` → `'10.0.0.1'` |
| `IS_IPV4(s)` | Test if string is valid IPv4 | `IS_IPV4('192.168.1.1')` → `true` |
| `IS_IPV6(s)` | Test if string is valid IPv6 | `IS_IPV6('::1')` → `true` |
| `NETWORK_ADDRESS(cidr)` | Extract network address from CIDR | `NETWORK_ADDRESS('192.168.1.100/24')` → `'192.168.1.0'` |
| `BROADCAST_ADDRESS(cidr)` | Compute broadcast address from CIDR | `BROADCAST_ADDRESS('192.168.1.0/24')` → `'192.168.1.255'` |
| `PREFIX_LENGTH(cidr)` | Extract prefix length from CIDR | `PREFIX_LENGTH('10.0.0.0/8')` → `8` |
| `CIDR_TO_RANGE(cidr)` | Return first and last IP as range string | `CIDR_TO_RANGE('192.168.1.0/30')` → `'192.168.1.0-192.168.1.3'` |
| `HOSTS_IN_CIDR(cidr)` | Count usable host addresses | `HOSTS_IN_CIDR('10.0.0.0/24')` → `254` |
| `CIDR_OVERLAP(cidr1, cidr2)` | Test if two CIDRs overlap | `CIDR_OVERLAP('10.0.0.0/8', '10.1.0.0/16')` → `true` |
| `IP_IN_RANGE(ip, start, end)` | Test if IP is between two IPs (inclusive) | `IP_IN_RANGE('192.168.1.50', '192.168.1.0', '192.168.1.255')` |
| `SAME_SUBNET(ip1, ip2, prefix)` | Test if two IPs share a subnet | `SAME_SUBNET('192.168.1.10', '192.168.1.20', 24)` → `true` |
| `IP_ADD(ip, offset)` | Add integer offset to IP | `IP_ADD('192.168.1.1', 10)` → `'192.168.1.11'` |
| `IP_SUBTRACT(ip, offset)` | Subtract integer offset from IP | `IP_SUBTRACT('192.168.1.10', 5)` → `'192.168.1.5'` |
| `IP_DIFF(ip1, ip2)` | Integer difference between two IPs | `IP_DIFF('192.168.1.10', '192.168.1.1')` → `9` |
| `IP_BETWEEN(ip, start, end)` | Test if IP is between two others | `IP_BETWEEN('10.0.0.50', '10.0.0.1', '10.0.0.100')` → `true` |
| `REVERSE_DNS(ip)` | Generate reverse DNS name | `REVERSE_DNS('192.168.1.1')` → `'1.1.168.192.in-addr.arpa'` |
| `IS_MULTICAST_IP(ip)` | Test if IP is multicast | `IS_MULTICAST_IP('224.0.0.1')` → `true` |
| `IS_LINK_LOCAL_IP(ip)` | Test if IP is link-local | `IS_LINK_LOCAL_IP('169.254.1.1')` → `true` |
| `IS_RESERVED_IP(ip)` | Test if IP is in any reserved range | `IS_RESERVED_IP('127.0.0.1')` → `true` |
| `IP_TO_HEX(ip)` | Convert IP to hex string | `IP_TO_HEX('192.168.1.1')` → `'c0a80101'` |

### MAC Functions

| Function | Description | Example |
|----------|-------------|---------|
| `MAC_TO_STRING(mac)` | Convert binary MAC to string | `MAC_TO_STRING(src_mac)` |
| `MAC_VENDOR_OUI(mac)` | Extract vendor OUI (first 3 bytes) | `MAC_VENDOR_OUI('aa:bb:cc:dd:ee:ff')` → `'AA:BB:CC'` |
| `MAC_IS_UNICAST(mac)` | Test if MAC is unicast | `MAC_IS_UNICAST('00:11:22:33:44:55')` → `true` |
| `MAC_IS_LOCAL(mac)` | Test if MAC is locally administered | `MAC_IS_LOCAL('02:11:22:33:44:55')` → `true` |
| `MAC_FORMAT(mac, sep)` | Format MAC with custom separator | `MAC_FORMAT('aabbccddeeff', '-')` → `'aa-bb-cc-dd-ee-ff'` |

### Port Functions

| Function | Description | Example |
|----------|-------------|---------|
| `PORT_NAME(port)` | Map port number to service name | `PORT_NAME(443)` → `'https'` |
| `IS_WELL_KNOWN_PORT(port)` | Test if port is 0-1023 | `IS_WELL_KNOWN_PORT(22)` → `true` |
| `IS_REGISTERED_PORT(port)` | Test if port is 1024-49151 | `IS_REGISTERED_PORT(3306)` → `true` |
| `IS_EPHEMERAL_PORT(port)` | Test if port is 49152-65535 | `IS_EPHEMERAL_PORT(50000)` → `true` |
| `PORT_CLASS(port)` | Classify port range | `PORT_CLASS(443)` → `'well-known'` |

### Protocol Functions

| Function | Description | Example |
|----------|-------------|---------|
| `PROTOCOL_NAME(num)` | Map protocol number to name | `PROTOCOL_NAME(6)` → `'tcp'` |
| `PROTOCOL_NUMBER(name)` | Map protocol name to number | `PROTOCOL_NUMBER('tcp')` → `6` |

### TCP Inspection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TCP_FLAGS_TO_STRING(flags)` | Convert TCP flags bitmask to names | `TCP_FLAGS_TO_STRING(0x12)` → `'SYN,ACK'` |
| `HAS_TCP_FLAG(flags, name)` | Test if a TCP flag is set | `HAS_TCP_FLAG(flags, 'SYN')` → `true` |
| `TCP_FLAGS_FROM_STRING(names)` | Convert flag names to bitmask | `TCP_FLAGS_FROM_STRING('SYN,ACK')` → `18` |
| `IS_TCP_HANDSHAKE(flags)` | Test for SYN-only (connection init) | `IS_TCP_HANDSHAKE(flags)` |
| `IS_TCP_RESET(flags)` | Test for RST flag | `IS_TCP_RESET(flags)` |
| `TCP_SESSION_ID(src, dst, sport, dport, proto)` | Canonical 5-tuple session key | `TCP_SESSION_ID(src_ip, dst_ip, src_port, dst_port, protocol)` |
| `FLOW_DIRECTION(src_ip, dst_ip)` | Classify as inbound/outbound/internal/transit | `FLOW_DIRECTION(src_ip, dst_ip)` → `'outbound'` |

### DNS Inspection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `DNS_QUERY_NAME(payload)` | Extract query domain from DNS packet | `DNS_QUERY_NAME(dns_payload)` → `'example.com'` |
| `DNS_QUERY_TYPE(payload)` | Extract query type (A, AAAA, MX, etc.) | `DNS_QUERY_TYPE(dns_payload)` → `'A'` |
| `DNS_IS_RESPONSE(payload)` | Test if DNS packet is a response | `DNS_IS_RESPONSE(dns_payload)` |
| `DNS_RESPONSE_CODE(payload)` | Extract RCODE (NOERROR, NXDOMAIN, etc.) | `DNS_RESPONSE_CODE(dns_payload)` → `'NOERROR'` |
| `DNS_QUESTION_COUNT(payload)` | Number of questions in DNS packet | `DNS_QUESTION_COUNT(dns_payload)` → `1` |
| `DNS_ANSWER_COUNT(payload)` | Number of answers in DNS packet | `DNS_ANSWER_COUNT(dns_payload)` → `0` |
| `DNS_TRANSACTION_ID(payload)` | Extract 16-bit transaction ID | `DNS_TRANSACTION_ID(dns_payload)` |

### TLS Inspection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TLS_SNI(payload)` | Extract Server Name Indication from ClientHello | `TLS_SNI(tls_payload)` → `'example.com'` |
| `TLS_VERSION(payload)` | Extract TLS version from record header | `TLS_VERSION(tls_payload)` → `'TLS 1.2'` |
| `TLS_RECORD_TYPE(payload)` | Identify TLS record type | `TLS_RECORD_TYPE(payload)` → `'Handshake'` |
| `IS_TLS_CLIENT_HELLO(payload)` | Test if payload is a TLS ClientHello | `IS_TLS_CLIENT_HELLO(payload)` |
| `TLS_HANDSHAKE_TYPE(payload)` | Identify handshake message type | `TLS_HANDSHAKE_TYPE(payload)` → `'ClientHello'` |

### HTTP Inspection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `HTTP_METHOD(payload)` | Extract HTTP method | `HTTP_METHOD(payload)` → `'GET'` |
| `HTTP_PATH(payload)` | Extract request path | `HTTP_PATH(payload)` → `'/api/v1/users'` |
| `HTTP_HOST(payload)` | Extract Host header | `HTTP_HOST(payload)` → `'api.example.com'` |
| `HTTP_STATUS_CODE(payload)` | Extract response status code | `HTTP_STATUS_CODE(payload)` → `200` |
| `HTTP_STATUS_CLASS(code)` | Classify status code | `HTTP_STATUS_CLASS(404)` → `'4xx'` |
| `HTTP_CONTENT_TYPE(payload)` | Extract Content-Type header | `HTTP_CONTENT_TYPE(payload)` → `'text/html'` |
| `HTTP_CONTENT_LENGTH(payload)` | Extract Content-Length as integer | `HTTP_CONTENT_LENGTH(payload)` → `1234` |
| `HTTP_USER_AGENT(payload)` | Extract User-Agent header | `HTTP_USER_AGENT(payload)` |
| `HTTP_HEADER(payload, name)` | Extract any header by name | `HTTP_HEADER(payload, 'X-Request-ID')` |
| `HTTP_VERSION(payload)` | Extract HTTP version | `HTTP_VERSION(payload)` → `'HTTP/1.1'` |
| `IS_HTTP_REQUEST(payload)` | Test if payload is an HTTP request | `IS_HTTP_REQUEST(payload)` |
| `IS_HTTP_RESPONSE(payload)` | Test if payload is an HTTP response | `IS_HTTP_RESPONSE(payload)` |

### Packet Header Functions

| Function | Description | Example |
|----------|-------------|---------|
| `IP_HEADER_LENGTH(payload)` | IPv4 header length in bytes | `IP_HEADER_LENGTH(ip_header)` → `20` |
| `IP_TTL(payload)` | Extract TTL from IPv4 header | `IP_TTL(ip_header)` → `64` |
| `IP_TOTAL_LENGTH(payload)` | Extract total length from IPv4 header | `IP_TOTAL_LENGTH(ip_header)` → `60` |
| `IP_DSCP(payload)` | Extract DSCP value from IPv4 TOS byte | `IP_DSCP(ip_header)` → `46` |
| `ETHER_TYPE(frame)` | Identify EtherType from Ethernet header | `ETHER_TYPE(frame)` → `'IPv4'` |
| `VLAN_ID(frame)` | Extract VLAN ID from 802.1Q tagged frame | `VLAN_ID(frame)` → `100` |

### Payload Analysis Functions

| Function | Description | Example |
|----------|-------------|---------|
| `PAYLOAD_ENTROPY(data)` | Shannon entropy of payload (0-8 bits) | `PAYLOAD_ENTROPY(payload)` → `7.2` |
| `PAYLOAD_HEX_DUMP(data, n)` | First N bytes as hex dump | `PAYLOAD_HEX_DUMP(payload, 16)` → `'48 65 6c 6c ...'` |

### ICMP Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ICMP_TYPE_NAME(type)` | ICMP type code to human name | `ICMP_TYPE_NAME(8)` → `'Echo Request'` |
| `ICMP_CODE_NAME(type, code)` | ICMP code to human name | `ICMP_CODE_NAME(3, 3)` → `'Port Unreachable'` |
| `IS_ICMP_ECHO(type)` | True if Echo Request (8) or Reply (0) | `IS_ICMP_ECHO(8)` → `true` |
| `ICMP_PARSE(data)` | Parse raw ICMP header as "type:code" | `ICMP_PARSE(header)` → `'8:0'` |
| `ICMP_TYPE(data)` | Extract ICMP type from raw header | `ICMP_TYPE(header)` → `8` |
| `ICMP_CODE(data)` | Extract ICMP code from raw header | `ICMP_CODE(header)` → `0` |

### IPv6 Functions

| Function | Description | Example |
|----------|-------------|---------|
| `IPV6_SCOPE(addr)` | Classify IPv6 address scope | `IPV6_SCOPE('fe80::1')` → `'link-local'` |
| `IPV6_EXPAND(addr)` | Expand abbreviated IPv6 to full form | `IPV6_EXPAND('::1')` → `'0000:0000:0000:0000:0000:0000:0000:0001'` |
| `IPV6_COMPRESS(addr)` | Compress full IPv6 to short form | `IPV6_COMPRESS('0000:...:0001')` → `'::1'` |
| `IPV6_TO_EUI64(mac)` | Convert MAC to EUI-64 interface identifier | `IPV6_TO_EUI64('00:11:22:33:44:55')` → `'0211:22ff:fe33:4455'` |
| `IS_6TO4(addr)` | True if 6to4 tunneled address (2002::/16) | `IS_6TO4('2002:c0a8:0101::1')` → `true` |
| `IS_TEREDO(addr)` | True if Teredo tunneled address (2001:0000::/32) | `IS_TEREDO(addr)` |
| `TEREDO_SERVER(addr)` | Extract Teredo server IPv4 address | `TEREDO_SERVER(addr)` → `'65.54.227.120'` |
| `TEREDO_CLIENT(addr)` | Extract Teredo client IPv4 address | `TEREDO_CLIENT(addr)` → `'192.0.2.45'` |
| `SIXTO4_GATEWAY(addr)` | Extract embedded IPv4 from 6to4 address | `SIXTO4_GATEWAY('2002:c0a8:0101::1')` → `'192.168.1.1'` |

### JA3 TLS Fingerprinting Functions

| Function | Description | Example |
|----------|-------------|---------|
| `JA3_FINGERPRINT(tls_data)` | MD5 hash of JA3 string from ClientHello | `JA3_FINGERPRINT(payload)` → `'e7d7...a3b2'` |
| `JA3_STRING(tls_data)` | Raw JA3 string (version,ciphers,extensions,curves,formats) | `JA3_STRING(payload)` |
| `JA3S_FINGERPRINT(tls_data)` | MD5 hash of JA3S string from ServerHello | `JA3S_FINGERPRINT(payload)` |
| `JA3S_STRING(tls_data)` | Raw JA3S string (version,cipher,extensions) | `JA3S_STRING(payload)` |

### Payload Search Functions

| Function | Description | Example |
|----------|-------------|---------|
| `PAYLOAD_CONTAINS(data, pattern)` | Test if payload contains byte pattern | `PAYLOAD_CONTAINS(payload, 'GET ')` → `true` |
| `PAYLOAD_MATCHES(data, regex)` | Test if payload matches regex | `PAYLOAD_MATCHES(payload, 'GET /api/.*')` |
| `PAYLOAD_OFFSET(data, offset, length)` | Extract bytes at offset as hex string | `PAYLOAD_OFFSET(payload, 0, 4)` → `'47455420'` |
| `PAYLOAD_LENGTH(data)` | Length of payload in bytes | `PAYLOAD_LENGTH(payload)` → `1500` |

### GeoIP / ASN Functions

GeoIP functions require [MaxMind GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data) or GeoIP2 MMDB databases. Configure with `--geoip-city` and `--geoip-asn` CLI flags, or in YAML config under `geoip.city_db` and `geoip.asn_db`. Functions return NULL if no database is loaded or the IP is not found.

```bash
# Start with GeoIP databases
wadjet serve --geoip-city /path/to/GeoLite2-City.mmdb --geoip-asn /path/to/GeoLite2-ASN.mmdb
```

```yaml
# Or in config file
geoip:
  city_db: /path/to/GeoLite2-City.mmdb
  asn_db: /path/to/GeoLite2-ASN.mmdb
```

| Function | Description | Example |
|----------|-------------|---------|
| `GEOIP_COUNTRY(ip)` | ISO 3166-1 alpha-2 country code | `GEOIP_COUNTRY('8.8.8.8')` → `'US'` |
| `GEOIP_COUNTRY_NAME(ip)` | Full country name (English) | `GEOIP_COUNTRY_NAME('8.8.8.8')` → `'United States'` |
| `GEOIP_CITY(ip)` | City name (English) | `GEOIP_CITY('8.8.8.8')` → `'Mountain View'` |
| `GEOIP_SUBDIVISION(ip)` | State/province name or ISO code | `GEOIP_SUBDIVISION('8.8.8.8')` → `'California'` |
| `GEOIP_POSTAL_CODE(ip)` | Postal/ZIP code | `GEOIP_POSTAL_CODE('8.8.8.8')` → `'94043'` |
| `GEOIP_LATITUDE(ip)` | Latitude (float64) | `GEOIP_LATITUDE('8.8.8.8')` → `37.386` |
| `GEOIP_LONGITUDE(ip)` | Longitude (float64) | `GEOIP_LONGITUDE('8.8.8.8')` → `-122.0838` |
| `GEOIP_TIMEZONE(ip)` | IANA timezone | `GEOIP_TIMEZONE('8.8.8.8')` → `'America/Los_Angeles'` |
| `GEOIP_CONTINENT(ip)` | Continent code | `GEOIP_CONTINENT('8.8.8.8')` → `'NA'` |
| `GEOIP_ASN(ip)` | Autonomous System number | `GEOIP_ASN('8.8.8.8')` → `15169` |
| `GEOIP_ORG(ip)` | AS organization name | `GEOIP_ORG('8.8.8.8')` → `'Google LLC'` |

**Example queries:**

```sql
-- Geographic distribution of traffic sources
SELECT GEOIP_COUNTRY(src_ip) AS country,
       GEOIP_CITY(src_ip) AS city,
       COUNT(*) AS connections,
       SUM(bytes_in) AS total_bytes
FROM flow_logs
GROUP BY 1, 2
ORDER BY total_bytes DESC
LIMIT 20

-- Foreign traffic analysis
SELECT src_ip,
       GEOIP_COUNTRY_NAME(src_ip) AS country,
       GEOIP_ORG(src_ip) AS organization,
       GEOIP_ASN(src_ip) AS asn,
       COUNT(*) AS flows
FROM flow_logs
WHERE GEOIP_COUNTRY(src_ip) != 'US'
GROUP BY src_ip
ORDER BY flows DESC

-- Traffic by AS organization
SELECT GEOIP_ORG(src_ip) AS org,
       GEOIP_ASN(src_ip) AS asn,
       SUM(bytes_in + bytes_out) AS total_bytes
FROM flow_logs
GROUP BY 1, 2
ORDER BY total_bytes DESC
LIMIT 10
```

### Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `NOW()` | Current timestamp | `NOW()` |
| `CURRENT_DATE()` | Current date | `CURRENT_DATE()` |
| `YEAR(ts)` | Extract year | `YEAR(timestamp)` |
| `MONTH(ts)` | Extract month | `MONTH(timestamp)` |
| `DAY(ts)` | Extract day | `DAY(timestamp)` |
| `HOUR(ts)` | Extract hour | `HOUR(timestamp)` |
| `MINUTE(ts)` | Extract minute | `MINUTE(timestamp)` |
| `SECOND(ts)` | Extract second | `SECOND(timestamp)` |
| `EXTRACT(part FROM ts)` | Extract date part | `EXTRACT(hour FROM timestamp)` |
| `DATE_TRUNC(part, ts)` | Truncate to precision | `DATE_TRUNC('hour', timestamp)` |
| `DATE_DIFF(unit, a, b)` | Difference between timestamps | `DATE_DIFF('second', start_ts, end_ts)` |
| `DATE_ADD(ts, interval)` | Add interval to timestamp | `DATE_ADD(timestamp, 3600000)` |
| `TO_DATE(s)` | Parse string to date | `TO_DATE('2026-03-15')` |
| `FROM_UNIXTIME(epoch)` | Convert unix timestamp to datetime string | `FROM_UNIXTIME(1700000000)` |
| `TO_UNIXTIME(ts)` | Convert datetime to unix epoch | `TO_UNIXTIME(timestamp)` |
| `DATE_FORMAT(ts, fmt)` | Format timestamp with SQL format specifiers | `DATE_FORMAT(ts, '%Y-%m-%d')` |
| `DATE_PARSE(s, fmt)` | Parse string to timestamp using format | `DATE_PARSE('2026-03-15', '%Y-%m-%d')` |
| `QUARTER(ts)` | Extract quarter (1-4) | `QUARTER(timestamp)` |
| `WEEK(ts)` | Extract ISO week number | `WEEK(timestamp)` |
| `DAY_OF_WEEK(ts)` | Day of week (1=Monday, 7=Sunday) | `DAY_OF_WEEK(timestamp)` |
| `DAY_OF_YEAR(ts)` | Day of year (1-366) | `DAY_OF_YEAR(timestamp)` |
| `LAST_DAY_OF_MONTH(ts)` | Last day of the month | `LAST_DAY_OF_MONTH(timestamp)` |
| `CURRENT_TIMESTAMP()` | Current timestamp (alias for NOW) | `CURRENT_TIMESTAMP()` |
| `FROM_ISO8601_TIMESTAMP(s)` | Parse ISO 8601 timestamp to epoch millis | `FROM_ISO8601_TIMESTAMP('2026-03-15T10:30:00Z')` |
| `FROM_ISO8601_DATE(s)` | Parse and validate ISO 8601 date | `FROM_ISO8601_DATE('2026-03-15')` |
| `TO_ISO8601(epoch_ms)` | Convert epoch millis to ISO 8601 string | `TO_ISO8601(1773570600000)` → `'2026-03-15T10:30:00Z'` |
| `TO_MILLISECONDS(ts)` | Convert timestamp/string to epoch milliseconds | `TO_MILLISECONDS('2026-03-15T10:30:00Z')` |
| `TIMEZONE_HOUR(epoch_ms)` | Extract timezone hour offset | `TIMEZONE_HOUR(ts)` → `0` |
| `TIMEZONE_MINUTE(epoch_ms)` | Extract timezone minute offset | `TIMEZONE_MINUTE(ts)` → `0` |
| `AT_TIMEZONE(ts, tz)` | Convert timestamp to timezone | `AT_TIMEZONE(ts, 'America/New_York')` |
| `HUMAN_READABLE_SECONDS(n)` | Format seconds as human string | `HUMAN_READABLE_SECONDS(3661)` → `'1 hour, 1 minute, 1 second'` |

### Hash Functions

| Function | Description | Example |
|----------|-------------|---------|
| `MD5(s)` | MD5 hash as hex string | `MD5(payload)` |
| `SHA256(s)` | SHA-256 hash as hex string | `SHA256(password)` |
| `SHA512(s)` | SHA-512 hash as hex string | `SHA512(token)` |
| `SHA1(s)` | SHA-1 hash as hex string | `SHA1(data)` |
| `CRC32(s)` | CRC-32 checksum | `CRC32(payload)` |
| `HMAC_SHA256(data, key)` | HMAC-SHA256 | `HMAC_SHA256(msg, secret)` |
| `HMAC_SHA512(data, key)` | HMAC-SHA512 | `HMAC_SHA512(msg, secret)` |
| `XXHASH64(s)` | XXHash64 hash as hex string | `XXHASH64(payload)` |
| `MURMUR3(s)` | MurmurHash3 x64_128 as hex string | `MURMUR3(key)` |

### Encoding Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TO_HEX(n)` | Convert integer to hex string | `TO_HEX(255)` → `'ff'` |
| `FROM_HEX(s)` | Convert hex string to integer | `FROM_HEX('ff')` → `255` |
| `TO_BASE64(s)` | Encode string to Base64 | `TO_BASE64('hello')` |
| `FROM_BASE64(s)` | Decode Base64 string | `FROM_BASE64('aGVsbG8=')` |
| `FROM_BASE(s, base)` | Convert string in given base to int | `FROM_BASE('ff', 16)` → `255` |
| `TO_BASE(n, base)` | Convert int to string in given base | `TO_BASE(255, 16)` → `'ff'` |
| `TO_BASE32(s)` | Encode string to Base32 | `TO_BASE32('hello')` → `'NBSWY3DP'` |
| `FROM_BASE32(s)` | Decode Base32 string | `FROM_BASE32('NBSWY3DP')` → `'hello'` |

### JSON Functions

| Function | Description | Example |
|----------|-------------|---------|
| `JSON_EXTRACT(json, path)` | Extract value from JSON by path | `JSON_EXTRACT(data, '$.user.name')` |
| `JSON_EXTRACT_SCALAR(json, path)` | Extract scalar value (returns NULL for objects/arrays) | `JSON_EXTRACT_SCALAR(data, '$.id')` |
| `JSON_ARRAY_LENGTH(json)` | Length of a JSON array | `JSON_ARRAY_LENGTH('[1,2,3]')` → `3` |
| `JSON_VALID(s)` | Test if string is valid JSON | `JSON_VALID(payload)` |

### URL Functions

| Function | Description | Example |
|----------|-------------|---------|
| `URL_EXTRACT_HOST(url)` | Extract hostname | `URL_EXTRACT_HOST('https://example.com:8080/path')` → `'example.com'` |
| `URL_EXTRACT_PORT(url)` | Extract port number | `URL_EXTRACT_PORT('https://example.com:8080/')` → `8080` |
| `URL_EXTRACT_PATH(url)` | Extract path | `URL_EXTRACT_PATH('https://example.com/api/v1')` → `'/api/v1'` |
| `URL_EXTRACT_PROTOCOL(url)` | Extract protocol/scheme | `URL_EXTRACT_PROTOCOL('https://example.com')` → `'https'` |
| `URL_EXTRACT_QUERY(url)` | Extract query string | `URL_EXTRACT_QUERY('https://x.com?a=1&b=2')` → `'a=1&b=2'` |
| `URL_EXTRACT_PARAMETER(url, key)` | Extract query parameter value | `URL_EXTRACT_PARAMETER(url, 'limit')` |

### Session Information Functions

PostgreSQL clients ask who and where they are before they ask for data, so
these answer the standard session functions. `CURRENT_USER`, `SESSION_USER`,
`USER`, `CURRENT_ROLE`, `CURRENT_CATALOG` and `CURRENT_SCHEMA` are niladic:
standard SQL spells them without parentheses, and both spellings work. The
values are server constants — the scalar function registry is process-global
and cannot see the calling connection.

| Function | Description | Example |
|----------|-------------|---------|
| `CURRENT_USER` | Session user name | `SELECT CURRENT_USER` → `'wadjet'` |
| `SESSION_USER` | Alias for `CURRENT_USER` | `SELECT SESSION_USER` → `'wadjet'` |
| `USER` | Alias for `CURRENT_USER` | `SELECT USER` → `'wadjet'` |
| `CURRENT_ROLE` | Alias for `CURRENT_USER` | `SELECT CURRENT_ROLE` → `'wadjet'` |
| `CURRENT_CATALOG` | Database name | `SELECT CURRENT_CATALOG` → `'wadjet'` |
| `CURRENT_DATABASE()` | Database name | `SELECT CURRENT_DATABASE()` → `'wadjet'` |
| `CURRENT_SCHEMA` | Current schema | `SELECT CURRENT_SCHEMA` → `'public'` |
| `CURRENT_SCHEMAS(implicit)` | Search path as a text array | `CURRENT_SCHEMAS(false)` → `'{public}'` |
| `VERSION()` | Server version string | `SELECT VERSION()` → `'PostgreSQL 15.0 (Wadjet analytical query engine)'` |

These names are reserved, as they are in PostgreSQL: a table column that is
literally called `user` is referenced with the double-quoted spelling
(`SELECT "user" FROM audit`), which is a plain column reference.

### Type Introspection

| Function | Description | Example |
|----------|-------------|---------|
| `TYPEOF(expr)` | Return SQL type name of expression | `TYPEOF(42)` → `'bigint'` |
| `FORMAT_NUMBER(n [, decimals])` | Format number with comma separators | `FORMAT_NUMBER(1234567)` → `'1,234,567'` |

### Array Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CARDINALITY(array)` | Number of elements | `CARDINALITY(ARRAY[1,2,3])` → `3` |
| `ARRAY_LENGTH(array)` | Alias for CARDINALITY | `ARRAY_LENGTH(tags)` |
| `ELEMENT_AT(array, index)` | 1-based element access (negative indexes from end) | `ELEMENT_AT(ips, 1)` → first element |
| `ARRAY_CONTAINS(array, value)` | Test membership | `ARRAY_CONTAINS(tags, 'critical')` |
| `ARRAY_JOIN(array, delimiter)` | Concatenate elements with delimiter | `ARRAY_JOIN(tags, ', ')` |
| `ARRAY_MIN(array)` | Minimum element | `ARRAY_MIN(scores)` |
| `ARRAY_MAX(array)` | Maximum element | `ARRAY_MAX(scores)` |

Array literal syntax: `ARRAY[1, 2, 3]`

### ROW/STRUCT Functions

| Function | Description | Example |
|----------|-------------|---------|
| `col.field` | Dot-notation field access | `person.name` |
| `ROW_FIELD(row, 'name')` | Extract field by name | `ROW_FIELD(geo, 'country')` |
| `STRUCT_FIELD(row, 'name')` | Alias for ROW_FIELD | `STRUCT_FIELD(meta, 'source')` |

### MAP Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ELEMENT_AT(map, key)` | Lookup value by key | `ELEMENT_AT(headers, 'Host')` |
| `MAP_KEYS(map)` | Extract all keys as ARRAY | `MAP_KEYS(labels)` |
| `MAP_VALUES(map)` | Extract all values as ARRAY | `MAP_VALUES(labels)` |
| `MAP_ENTRIES(map)` | Convert to ARRAY(ROW(key, value)) | `MAP_ENTRIES(headers)` |
| `MAP_FROM_ENTRIES(entries)` | Construct MAP from entry array | `MAP_FROM_ENTRIES(pairs)` |

### Vector Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COSINE_SIMILARITY(a, b)` | Cosine similarity between vectors, returns FLOAT64 in [-1, 1] | `COSINE_SIMILARITY(embed('cat'), embed('dog'))` |
| `L2_DISTANCE(a, b)` | Euclidean distance between vectors | `L2_DISTANCE(v1, v2)` |
| `DOT_PRODUCT(a, b)` | Dot product of two vectors | `DOT_PRODUCT(v1, v2)` |
| `VECTOR_NORM(a)` | L2 norm of a vector | `VECTOR_NORM(embedding)` |
| `VECTOR_DIMS(a)` | Number of dimensions in a vector | `VECTOR_DIMS(embedding)` → `1536` |

### Embedding Functions

Requires `WADJET_OPENAI_API_KEY` environment variable.

| Function | Description | Example |
|----------|-------------|---------|
| `EMBED(text)` | Generate embedding vector from text | `EMBED('lateral movement')` |
| `EMBED_MODEL()` | Current embedding model name | `EMBED_MODEL()` → `'text-embedding-3-small'` |
| `EMBED_DIM()` | Current embedding dimension | `EMBED_DIM()` → `1536` |

```sql
-- Semantic search
SELECT alert_id, description,
       COSINE_SIMILARITY(EMBED(description), EMBED('credential theft')) AS score
FROM alerts
ORDER BY score DESC LIMIT 10

-- Store pre-computed embeddings
CREATE TABLE doc_embeddings (doc_id INT64, embedding VECTOR(1536))
```

### UUID Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UUID_VERSION(uuid)` | Extract UUID version | `UUID_VERSION(flow_id)` |
| `UUID()` | Generate random UUID v4 | `UUID()` → `'a1b2c3d4-...'` |
| `UUID_TO_STRING(uuid)` | Convert binary UUID to string | `UUID_TO_STRING(flow_id)` |

## User-Defined Functions (UDFs)

Register custom SQL expression functions:

```sql
-- Create a UDF
CREATE FUNCTION classify_port(p) AS
  CASE WHEN p < 1024 THEN 'well-known'
       WHEN p < 49152 THEN 'registered'
       ELSE 'dynamic' END

-- Create or replace
CREATE OR REPLACE FUNCTION classify_port(p) AS
  CASE WHEN p < 1024 THEN 'system'
       WHEN p < 49152 THEN 'registered'
       ELSE 'ephemeral' END

-- Use it in queries
SELECT src_ip, classify_port(dst_port) AS port_class FROM flow_logs

-- List all UDFs
SHOW FUNCTIONS

-- Remove a UDF
DROP FUNCTION classify_port
DROP FUNCTION IF EXISTS classify_port
```

UDFs can be locked by their owner so only the creator (or an admin) can modify or remove them.

## Query Examples for Network Analytics

### Top Sources by Bandwidth

```sql
SELECT
    src_ip,
    SUM(bytes_in) AS ingress,
    SUM(bytes_out) AS egress,
    SUM(bytes_in + bytes_out) AS total,
    COUNT(*) AS flows
FROM flow_logs
WHERE date = '2026-03-15'
GROUP BY src_ip
ORDER BY total DESC
LIMIT 25
```

### Port Scan Detection

```sql
SELECT
    src_ip,
    COUNT(DISTINCT dst_port) AS unique_ports,
    COUNT(*) AS attempts
FROM flow_logs
WHERE date = '2026-03-15'
  AND bytes_in < 100
  AND protocol = 'TCP'
GROUP BY src_ip
HAVING COUNT(DISTINCT dst_port) > 100
ORDER BY unique_ports DESC
```

### Error Rate by Device

```sql
SELECT
    hostname,
    severity,
    COUNT(*) AS count
FROM syslog
WHERE date = '2026-03-15'
  AND severity IN ('error', 'critical', 'alert', 'emergency')
GROUP BY hostname, severity
ORDER BY count DESC
```

### Traffic by Hour

```sql
SELECT
    CAST(timestamp / 3600000 * 3600000 AS Timestamp) AS hour,
    SUM(bytes_in) AS total_ingress,
    SUM(bytes_out) AS total_egress,
    COUNT(*) AS flow_count
FROM flow_logs
WHERE date = '2026-03-15'
GROUP BY CAST(timestamp / 3600000 * 3600000 AS Timestamp)
ORDER BY hour
```

### Join Flow Data with Device Inventory

```sql
SELECT
    d.hostname,
    d.location,
    d.role,
    SUM(f.bytes_in) AS total_ingress,
    COUNT(*) AS flow_count
FROM flow_logs f
JOIN device_inventory d ON f.src_ip = d.ip_address
WHERE f.date = '2026-03-15'
GROUP BY d.hostname, d.location, d.role
ORDER BY total_ingress DESC
LIMIT 50
```

### Running Totals with Window Functions

```sql
WITH hourly AS (
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour,
        SUM(bytes_in) AS hourly_bytes
    FROM flow_logs
    WHERE date = '2026-03-15'
    GROUP BY DATE_TRUNC('hour', timestamp)
)
SELECT
    hour,
    hourly_bytes,
    SUM(hourly_bytes) OVER (ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_bytes,
    RANK() OVER (ORDER BY hourly_bytes DESC) AS traffic_rank
FROM hourly
ORDER BY hour
```

### Unmatched Devices (Full Outer Join)

```sql
SELECT
    COALESCE(f.src_ip, d.ip_address) AS ip,
    d.hostname,
    SUM(f.bytes_in) AS total_bytes
FROM flow_logs f
FULL OUTER JOIN device_inventory d ON f.src_ip = d.ip_address
GROUP BY 1, 2
ORDER BY total_bytes DESC NULLS LAST
```

## Operator Precedence

From lowest to highest:

| Precedence | Operators |
|------------|-----------|
| 1 (lowest) | `OR` |
| 2 | `AND` |
| 3 | `NOT` |
| 4 | `IS`, `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `LIKE` |
| 5 | `+`, `-`, `\|\|` |
| 6 | `*`, `/`, `%` |
| 7 (highest) | Unary `-`, `+` |

## Data Manipulation (DML)

Wadjet supports INSERT, UPDATE, and DELETE via merge-on-read semantics. Deleted rows are tracked as markers in the manifest and filtered out at scan time. Updated rows are implemented as DELETE + INSERT of the modified values.

### INSERT

```sql
INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...) [, (val3, val4, ...) ...]
```

If column list is omitted, values are matched to schema column order.

### DELETE

```sql
DELETE FROM table_name [WHERE condition]
```

Without WHERE, deletes all rows. Delete markers are stored in the table manifest and applied during scans.

### UPDATE

```sql
UPDATE table_name SET col1 = val1 [, col2 = val2 ...] [WHERE condition]
```

Internally executes as DELETE of matching rows + INSERT of modified rows.

### Type Coercion

Values in INSERT/UPDATE are automatically coerced to the target column type:

| Column Type | Accepted Formats |
|---|---|
| INT32, INT64 | Integer literals: `42` |
| FLOAT32, FLOAT64 | Numeric literals: `3.14` |
| BOOL | `true`, `false` |
| STRING | Quoted: `'hello'` |
| TIMESTAMP | `'2026-03-17T10:00:00Z'`, `'2026-03-17 10:00:00'` |
| DATE | `'2026-03-17'` |

## Limitations

- No lateral joins
- No recursive CTEs
