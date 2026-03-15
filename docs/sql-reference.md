# SQL Reference

Caelum supports a focused subset of SQL for analytical queries, parsed via the vitess-sqlparser.

## Supported Statement Types

| Statement | Description |
|-----------|-------------|
| `SELECT` | Query data from tables |
| `EXPLAIN` | Show the query execution plan without running it |
| `DESCRIBE table_name` | Show the schema of a table |
| `CREATE FUNCTION` | Register a user-defined function |
| `DROP FUNCTION` | Remove a user-defined function |
| `SHOW FUNCTIONS` | List registered user-defined functions |

## SELECT Statement

```sql
SELECT [DISTINCT] [columns | expressions | aggregates | window_functions]
FROM table_name
[JOIN other_table ON condition]
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY columns [ASC|DESC]]
[LIMIT n [OFFSET m]]
```

## EXPLAIN

View the query plan without executing:

```sql
EXPLAIN SELECT src_ip, SUM(bytes_in) FROM flow_logs GROUP BY src_ip

-- Output shows: Scan → Filter → Aggregate → Sort → Limit
```

## DESCRIBE

Inspect a table's schema:

```sql
DESCRIBE flow_logs

-- Output: column names, types, nullable
```

## Column Selection

```sql
-- All columns
SELECT * FROM flow_logs

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
```

### NULL Handling

```sql
SELECT * FROM flow_logs WHERE src_ip IS NULL
SELECT * FROM flow_logs WHERE src_ip IS NOT NULL
```

## Aggregate Functions

| Function | Description | Null Handling |
|----------|-------------|---------------|
| `COUNT(*)` | Count all rows | Counts nulls |
| `COUNT(column)` | Count non-null values | Skips nulls |
| `SUM(column)` | Sum of values | Skips nulls |
| `MIN(column)` | Minimum value | Skips nulls |
| `MAX(column)` | Maximum value | Skips nulls |
| `AVG(column)` | Average value | Skips nulls |

### Examples

```sql
-- Total traffic by source IP
SELECT src_ip, SUM(bytes_in) AS total_bytes, COUNT(*) AS flow_count
FROM flow_logs
GROUP BY src_ip
ORDER BY total_bytes DESC
LIMIT 20

-- Average packet size by protocol
SELECT protocol, AVG(bytes_in) AS avg_size, COUNT(*) AS count
FROM flow_logs
GROUP BY protocol

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
```

## LIMIT and OFFSET

```sql
-- First 100 rows
SELECT * FROM flow_logs LIMIT 100

-- Pagination: skip 200, return next 100
SELECT * FROM flow_logs ORDER BY timestamp DESC LIMIT 100 OFFSET 200
```

## JOIN

Caelum supports inner joins and left joins:

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

The join implementation uses a **hash join** strategy: the right side is loaded into a hash table (build phase), then the left side is probed against it (probe phase). Place the smaller table on the right side of the JOIN for best performance.

## Arithmetic Expressions

```sql
SELECT
    src_ip,
    bytes_in + bytes_out AS total_bytes,
    bytes_in * 8 AS bits_in,
    CAST(bytes_in AS Float64) / CAST(packets AS Float64) AS avg_packet_size
FROM flow_logs
```

### Supported Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |

## DISTINCT

Deduplicate result rows:

```sql
SELECT DISTINCT protocol FROM flow_logs
SELECT DISTINCT src_ip, dst_port FROM flow_logs WHERE date = '2026-03-15'
```

## Additional Predicate Expressions

### IN

```sql
SELECT * FROM flow_logs WHERE dst_port IN (80, 443, 8080, 8443)
SELECT * FROM syslog WHERE severity IN ('error', 'critical')
```

### BETWEEN

```sql
SELECT * FROM flow_logs WHERE dst_port BETWEEN 1024 AND 65535
SELECT * FROM flow_logs WHERE bytes_in BETWEEN 1000 AND 1000000
```

### CASE

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

## Built-in Functions

Caelum includes 53 built-in scalar functions across several categories.

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER(protocol)` → `"TCP"` |
| `LOWER(s)` | Lowercase | `LOWER(hostname)` → `"fw-01"` |
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
| `EXTRACT(part FROM ts)` | Extract date part | `EXTRACT(hour FROM timestamp)` |
| `DATE_TRUNC(part, ts)` | Truncate to precision | `DATE_TRUNC('hour', timestamp)` |
| `DATE_DIFF(unit, a, b)` | Difference between timestamps | `DATE_DIFF('second', start_ts, end_ts)` |
| `DATE_ADD(ts, interval)` | Add interval to timestamp | `DATE_ADD(timestamp, 3600000)` |
| `TO_DATE(s)` | Parse string to date | `TO_DATE('2026-03-15')` |

### UUID Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UUID_VERSION(uuid)` | Extract UUID version | `UUID_VERSION(flow_id)` |
| `UUID_TO_STRING(uuid)` | Convert binary UUID to string | `UUID_TO_STRING(flow_id)` |

## User-Defined Functions (UDFs)

Register custom SQL expression functions:

```sql
-- Create a UDF
CREATE FUNCTION classify_port(p) AS
  CASE WHEN p < 1024 THEN 'well-known'
       WHEN p < 49152 THEN 'registered'
       ELSE 'dynamic' END

-- Use it in queries
SELECT src_ip, classify_port(dst_port) AS port_class FROM flow_logs

-- List all UDFs
SHOW FUNCTIONS

-- Remove a UDF
DROP FUNCTION classify_port
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

## Window Functions

Caelum supports window functions for running calculations across row sets:

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

Window specifications support `PARTITION BY` and `ORDER BY` clauses.

## Limitations

- No subqueries (yet)
- No UNION / INTERSECT / EXCEPT
- No CREATE TABLE / INSERT via SQL (use the Go API or Bento for ingestion)
- No UPDATE / DELETE (append-only analytical store)
