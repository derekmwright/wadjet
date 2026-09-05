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
| `INSERT` / `UPDATE` / `DELETE` | Modify table data (merge-on-read) — see [Data Manipulation](#data-manipulation-dml) |
| `MERGE INTO ... USING ... WHEN MATCHED` | Conditional upsert; target and source must have different exposed names (SQLSTATE 42712 otherwise); `WHEN NOT MATCHED BY SOURCE/TARGET` is refused with SQLSTATE 0A000 |
| `ANALYZE [TABLE] table_name` | Collect column statistics for the cost-based planner |
| `CREATE ALERT` / `ALTER ALERT` / `DROP ALERT` | Manage saved alert definitions. Runs on the embedded API with `Config.EnableAlerts`; on a server, through the gRPC `Query` RPC against a coordinator started with `--enable-alerts`. `psql` and the HTTP query endpoint do not reach a handler — see below |

### Parsed and not executed

`ALTER TABLE`, `CREATE VIEW` and `DROP VIEW` are read by the parser and run by
nothing. Each is refused with SQLSTATE `0A000` (`feature_not_supported`) and a
message naming the statement — `ALTER TABLE is not supported` — on every door:
the embedded API, the PostgreSQL wire protocol and the HTTP query endpoint.

`CREATE SNAPSHOT` is in the same position on every door a person types SQL
into. Its one handler is `Coordinator.ExecuteSQL`, and the two doors a client
reaches — the PostgreSQL wire protocol and the HTTP query endpoint — both send
a DDL statement to the embedded engine instead, whose type switch has no case
for it. So `psql`, `POST /v1/queries` and the embedded API all refuse it
`0A000`. It reaches its handler only through the gRPC `Query` RPC against a
coordinator. Take a snapshot on a cadence instead
(`--catalog-snapshot-interval`; see
[Disaster recovery](disaster-recovery.md)).

**The `ALERT` statements are reachable the same way on a server.** The routing
above is per statement and not per deployment: the wire protocol sends only
`SELECT` and `WITH` to the coordinator, so a `CREATE ALERT` typed into `psql`
goes to the embedded engine behind that door — which `wadjet serve` opens
WITHOUT `EnableAlerts`, because in standalone mode the coordinator owns the
alert scheduler and a second one in the same process would evaluate and fire
every alert twice. So `--enable-alerts` makes the three statements reachable
through the gRPC `Query` RPC, and `psql` answers `alerts are disabled on this
connection`. Embedding the engine directly, `Config.EnableAlerts` makes them
run on that `DB`, scheduler included.
`EXPLAIN` over one of them is refused the same way (`EXPLAIN ALTER TABLE is not
supported`); PostgreSQL refuses that spelling as a syntax error (`42601`)
because its grammar does not accept it at all.

There is no schema evolution: change a table's columns by creating a new table.

## Identifiers

An **unquoted** identifier is folded to lower case, exactly as PostgreSQL folds
one, so `SELECT G`, `SELECT g` and `SELECT G` inside a `GROUP BY` are one name.
A **delimited** identifier — one written between double quotes — keeps its
bytes, so `"id.orig_h"` is a single column name with a dot in it and `"Kk"` is
a different name from `kk`.

```sql
SELECT WatchID FROM hits          -- the column watchid, published as `watchid`
SELECT g AS Foo FROM t            -- published as `foo`
SELECT g AS "Foo" FROM t          -- published as `Foo`
SELECT "id.orig_h" FROM conn      -- one column, not conn.orig_h
```

The fold is ASCII `A`–`Z` only, which is what PostgreSQL does in a UTF-8
database: `Ä` is not folded to `ä`.

**Resolving a folded name against a mixed-case schema.** Parquet files and
ingested data routinely carry CamelCase column names, so a folded reference
that matches no column byte for byte resolves case-insensitively when exactly
one column matches — `SELECT WatchID`, `SELECT watchid` and `SELECT "WatchID"`
all read a column stored as `WatchID`. If two columns in scope match only
case-insensitively the reference is ambiguous, SQLSTATE `42702`. A column
stored under two spellings that differ only by case cannot exist: `CREATE
TABLE` refuses such a schema.

A **delimited** reference carrying an upper-case letter does not get that
concession — it is matched byte for byte, so `SELECT "G"` over a column named
`g` is SQLSTATE `42703` (`column "G" does not exist`), which is PostgreSQL's
answer too. An all-lower-case delimited reference is indistinguishable from an
unquoted one below the parser and does get it: `SELECT "watchid"` reads a
column stored `WatchID` here, where PostgreSQL is `42703`.

A **qualifier** — a table name or alias before the dot — is always matched byte
for byte, in either case. A delimited alias `"T"` and an unquoted `t` are two
different relations, and `SELECT "T".x FROM t` is SQLSTATE `42P01`
(`missing FROM-clause entry for table "T"`), as it is in PostgreSQL.

A **table name** takes the same concession a column name does, and for the same
reason: wadjet's tables come from parquet and ingest, where a user-chosen
mixed-case name is ordinary and nothing quoted it at creation. An unquoted
reference folds, and if the folded name matches no table byte for byte it
resolves to the one registered table that matches it case-insensitively — so
`SELECT * FROM MyTab`, `FROM mytab` and `FROM MYTAB` all read a table stored as
`MyTab`. PostgreSQL answers `42P01` for the first and third, because it matches
its catalog exactly after folding; this is the relation-name half of the same
recorded divergence (ADR-0012).

The boundary is the same too:

- a **delimited** reference carrying an upper-case letter takes no concession —
  `FROM "MYTAB"` is `42P01` here as in PostgreSQL, and `FROM "MyTab"` is
  byte-exact;
- a **byte-exact match always wins**, so with both `MyTab` and `mytab`
  registered, `FROM MyTab` folds to `mytab` and reads `mytab` — which is what
  PostgreSQL does as well;
- **two tables differing only by case** and neither matching byte-exact resolve
  to NOTHING. The refusal is `42P01` and names both candidates, because
  choosing one would be a silently wrong table;
- **CREATE TABLE does not concede.** Minting a name is not referencing one, so
  creating `mytab` beside `MyTab` makes a second table. Every statement that
  REFERENCES an existing table — `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
  `MERGE`, `COPY` — resolves it the same way, and the write lands on the table
  the read resolved.

Table **aliases**, CTE names and function names are matched byte for byte, as
qualifiers are. `SELECT * FROM t` publishes each column under the schema's own
spelling.

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

Reads Parquet files with column-at-a-time page reading. Column projection and row-group statistics pruning apply to catalog tables, not to the `read_parquet()` table function, which decodes every column of every row group.

```sql
SELECT * FROM read_parquet('warehouse/sales.parquet')
SELECT * FROM read_parquet('https://storage.example.com/data.parquet')
```

All table functions support local file paths and HTTP/HTTPS URLs, fetched through a pooled HTTP client. Custom auth headers are not configurable from SQL.

### Streaming I/O

CSV and JSON files are read in streaming mode from every source — local paths, glob patterns (expanded lazily, one file open at a time) and HTTP/HTTPS URLs — so only the current batch of rows is held in memory and files larger than available RAM are queryable. Schema is inferred from the first 100 rows.

Local Parquet files are opened as file handles (`io.ReaderAt`), enabling page-level random access without reading the entire file into memory. For `read_parquet()` only, HTTP sources and glob patterns are still buffered in full, because Parquet needs random access.

### postgres_scan / postgres_query

Query external PostgreSQL databases directly from SQL. Uses `database/sql` with the `lib/pq` driver.

```sql
-- Scan an entire table
SELECT * FROM postgres_scan('host=pghost dbname=mydb user=readonly', 'customers')

-- Run an arbitrary query on the remote server (the outer WHERE is applied locally, not pushed down)
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

### Quantified Comparison (ANY / SOME / ALL)

```sql
SELECT * FROM flow_logs WHERE dst_port = ANY(ARRAY[80, 443, 8080])
SELECT * FROM flow_logs WHERE dst_port <> ALL(ARRAY[22, 23])
SELECT * FROM flow_logs WHERE bytes_in > ANY(ARRAY[1000, 5000])
SELECT * FROM flow_logs WHERE src_ip = ANY(SELECT ip FROM watchlist)
```

The candidate list is an `ARRAY[...]` literal, a comma-separated value list, or
a subquery. Over a subquery only the equality forms are supported: `= ANY` /
`= SOME` means `IN` and `<> ALL` means `NOT IN`; an ordering quantifier over a
subquery is SQLSTATE 0A000.

NULL follows PostgreSQL: `ANY` is TRUE if any comparison is TRUE, NULL if none
is TRUE and any is NULL, FALSE otherwise; `ALL` is FALSE if any comparison is
FALSE, NULL if none is FALSE and any is NULL, TRUE otherwise.

### Row Values

```sql
SELECT * FROM flow_logs WHERE (src_ip, dst_port) = ('10.0.0.1', 443)
SELECT * FROM flow_logs WHERE (src_ip, dst_port) IN (('10.0.0.1', 443), ('10.0.0.2', 80))
SELECT * FROM events WHERE (day, seq) > (17, 100)
```

A row comparison compares field by field. `=` and `<>` look at every field;
the ordering operators stop at the first field pair that is not equal and
answer from it, so `(1, 2) < (1, 3)` is true and `(2, 0) < (1, 9)` is false.
A NULL in a field the comparison has to look at makes the whole comparison
NULL. Both sides must have the same number of fields (otherwise 42601).

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

Subqueries that reference columns from the outer query. The optimizer decorrelates them where it can — EXISTS / NOT EXISTS and IN become semi/anti joins, and a correlated scalar subquery becomes a join against a grouped aggregate — so they are not re-executed per outer row. Either side may be a CTE, a derived table, a comma-joined list or a base table: the subquery's own FROM clause is planned the way a top-level FROM clause is.

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

-- Over a CTE, correlated on a column the CTE renames
WITH recent AS (SELECT o_custkey AS cust FROM orders WHERE o_orderdate > DATE '1998-01-01')
SELECT COUNT(*) FROM recent r
WHERE EXISTS (SELECT 1 FROM customer c WHERE c.c_custkey = r.cust)
```

Two shapes are deliberately NOT turned into a join, and run as a per-row
subquery instead — a slower right answer:

- **A correlated `NOT IN`.** `NOT IN` is three-valued and its third value is
  per correlation group (a NULL in *that group's* list makes the predicate
  UNKNOWN), which an anti join cannot express. An UNCORRELATED `NOT IN` still
  becomes a null-aware anti join.
- **A subquery whose correlation is not a simple equality of two columns**,
  whose own FROM reads a **RECURSIVE** CTE, or whose own FROM JOINS a derived
  table or a CTE reference to another relation.

Both stay correct on every execution path; on a distributed cluster the query
runs on the coordinator rather than across workers.

A derived table, an ordinary CTE reference and a comma-joined FROM list in the
subquery are decorrelated like a base table. They used to run as a per-row
subquery, which read the inner relation once for every outer row.

A correlated subquery inside an **aggregate argument** —
`SUM(CASE WHEN EXISTS (…) THEN 1 ELSE 0 END)` — is evaluated per row, not
decorrelated.

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

### LATERAL Joins

A `LATERAL` subquery may reference columns of the FROM items to its left, and
is evaluated once per outer row:

```sql
SELECT o.customer, s.item_count, s.total_amount
FROM orders o
JOIN LATERAL (
    SELECT COUNT(*) AS item_count, SUM(amount) AS total_amount
    FROM line_items WHERE order_id = o.id
) s ON true
```

An UNGROUPED aggregate over an empty input still yields one row, so an outer
row the lateral matches nothing for **survives even an inner join**, with
`COUNT` reading 0 and every other aggregate NULL — the order with no line
items above comes back at `item_count = 0, total_amount = NULL`. A lateral
subquery that writes its own `GROUP BY` follows the ordinary rule instead: an
empty input yields no row, and the outer row is dropped by an inner join and
NULL-padded by a `LEFT JOIN LATERAL`.

**The join's own `ON` still decides.** The lateral produces its row first —
including the defaulted one for an outer row it matched nothing for — and the
`ON` is applied to that pair afterwards, so `ON s.item_count > 0` drops the
empty order and `ON s.item_count = 0` keeps only the empty ones. Writing
`ON true` is what makes the empty-input row unconditional; it is not the only
supported spelling.

A `RIGHT` or `FULL` join written after the lateral turns the empty-input rule
off for that query: such a join can produce rows in which the lateral's
columns are NULL for a reason of its own, and those NULLs are left alone
rather than defaulted. The rest of the join behaves normally. A `LEFT` join
after the lateral is unaffected.

One case is not yet right, and it is a VALUE rather than a row: a written `ON`
on a `LEFT JOIN LATERAL` that the DEFAULT row would PASS —
`LEFT JOIN LATERAL (…) s ON s.item_count = 0`. Every outer row is kept, as
PostgreSQL keeps them, and the `ON` correctly nulls the lateral's columns for
the rows it rejects; what differs is the row the `ON` ACCEPTS. For the order
with no line items, PostgreSQL reads `item_count = 0` and this engine reads
NULL:

```
LEFT JOIN LATERAL (SELECT COUNT(*) AS item_count FROM line_items
                    WHERE order_id = o.id) s ON s.item_count = 0

PostgreSQL   Alice NULL, Bob NULL, Carol 0
this engine  Alice NULL, Bob NULL, Carol NULL      <- one cell differs
```

Every other combination of join kind and `ON` matches PostgreSQL.

`SELECT *` over an aggregated lateral is a second gap of the same kind: the
star expands after the default is applied, so a `COUNT` column reached through
it reads NULL rather than 0. Name the columns to get the default.

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

`DISTINCT` is accepted by every aggregate in this table, not only `COUNT`:
`SUM(DISTINCT x)`, `AVG(DISTINCT x)` and `STRING_AGG(DISTINCT x, ',')` each
de-duplicate their input at the value's exact type before aggregating.
`MIN`/`MAX` are unaffected by de-duplication and answer the same either way.

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

### Which name a bare GROUP BY, HAVING or ORDER BY term binds

A bare name in these three clauses can mean an INPUT column of the FROM
sources or an OUTPUT column of the SELECT list, and the three clauses answer
differently — PostgreSQL's rules, which wadjet follows:

| clause | a name that is BOTH an input column and an output alias binds |
|---|---|
| `GROUP BY` | the **input column** |
| `HAVING` | the **input column** (an output alias is not visible at all: `HAVING k > 2` over `SELECT g*0 AS k` is `42703`) |
| `ORDER BY` | the **output column** |

```sql
-- `g` is a column of t AND the alias of `g*0`.
SELECT g*0 AS g, COUNT(*) AS n FROM t WHERE id < 6 GROUP BY g
-- 6 rows: GROUP BY binds the input column, and the alias is projected per group.

SELECT -g AS g, COUNT(*) AS n FROM t WHERE id < 6 GROUP BY g ORDER BY g
-- 6 rows ordered -5, -4, -3, -2, -1, 0: ORDER BY binds the OUTPUT column.

SELECT -g AS g, COUNT(*) AS n FROM t WHERE id < 6 GROUP BY g HAVING g > 2
-- 3 rows: HAVING binds the input column, so the groups kept are input g = 3, 4, 5.
```

A name that is NOT an input column still binds the output alias in `GROUP BY`:
`SELECT g*0 AS kk, COUNT(*) FROM t GROUP BY kk` is one group. The rules apply
inside a derived table and a CTE exactly as at the top level.

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

### GROUPING

`GROUPING(a[, b, ...])` returns an integer bitmask saying which of its
arguments were **not** grouped in the row's grouping set. It is the only way
to tell a super-aggregate NULL apart from a NULL that was in the data — for a
nullable column, `a IS NULL` is true for both.

```sql
SELECT region, GROUPING(region) AS is_total, SUM(sales)
FROM orders
GROUP BY ROLLUP (region)
-- is_total = 0 on a per-region row (including one whose region IS NULL),
-- 1 on the grand-total row.
```

The leftmost argument is the **most significant** bit, so argument order
matters: over `CUBE (a, b)`, on a row that groups `b` but not `a`,
`GROUPING(a, b)` is `2` and `GROUPING(b, a)` is `1`. Every argument must be a
GROUP BY term of the same query level; anything else is SQLSTATE 42803. With a
plain `GROUP BY` every key is grouped in every row, so the result is always
`0`. An unaliased call reports the column name `grouping`. All of this follows
PostgreSQL.

A `GROUPING` call is legal only where an aggregate's output is in scope — the
SELECT list and `HAVING`. In a `WHERE` clause or a `JOIN ... ON` condition,
which run before grouping, it is SQLSTATE 42803 (`grouping operations are not
allowed in WHERE`), and inside another aggregate's arguments it is 42803
(`aggregate function calls cannot be nested`) — the same rules PostgreSQL
applies, and the same ones it applies to `SUM`, `COUNT` and every other
aggregate.

### When two spellings are one GROUP BY key

A `SELECT` item and a `GROUP BY` term are the same key when they are the same
EXPRESSION, not when they are the same text. Parentheses, identifier case and
whitespace are spelling, and so are two more:

```sql
-- A table qualifier, when the FROM has ONE relation
SELECT flow_logs.bytes_in + 1, COUNT(*) FROM flow_logs GROUP BY bytes_in + 1

-- A CAST type synonym: INT/INTEGER/INT4, BIGINT/INT8, SMALLINT/INT2,
-- REAL/FLOAT4, DOUBLE PRECISION/FLOAT8, DEC/DECIMAL/NUMERIC, BOOL/BOOLEAN,
-- VARCHAR/CHARACTER VARYING
SELECT CAST(b AS DEC(9,2)), COUNT(*) FROM t GROUP BY CAST(b AS DECIMAL(9,2))
```

`VARCHAR` and `TEXT` are **not** synonyms, and PostgreSQL does not treat them
as one either — that pair is SQLSTATE `42803` on both.

Two shapes are refused that PostgreSQL answers, both loudly with `42803`:

- a qualifier over a **join**, where `a.x` and `b.x` are different columns and
  erasing the qualifier would group by the wrong one;
- a **qualified GROUP BY term with an unqualified select item** — the mirror of
  the first example above. The erasure applies to the select item, not to the
  key.

**Two deliberate divergences from PostgreSQL, both loud:**

- `ORDER BY GROUPING(a)` is **not accepted** (PostgreSQL accepts it). Select
  the call and order by its alias instead:
  `SELECT GROUPING(a) AS g, ... ORDER BY g`.
- `GROUP BY GROUPING(a)` reports a syntax error (42601) where PostgreSQL
  reports 42803. The statement is rejected either way; only the code differs.

Queries using `GROUPING SETS`, `ROLLUP` or `CUBE` — with or without
`GROUPING(...)` — execute on the single-process pipeline: the distributed
stage DAG has no grouping-set stage and refuses them explicitly, so the
coordinator routes them local rather than answering a plain `GROUP BY`.

## HAVING

Filters groups after aggregation (whereas WHERE filters rows before aggregation):

```sql
SELECT src_ip, COUNT(*) AS conn_count
FROM flow_logs
GROUP BY src_ip
HAVING COUNT(*) > 1000
```

An aggregate a `HAVING` names is normally computed once and shared with the
SELECT list. Where the SELECT list ALIASES that aggregate like one of the group
keys the sharing is DECLINED and the predicate gets its own copy, because the
shared name would answer to two columns: `SELECT COUNT(*) AS g, g AS x FROM t
GROUP BY g HAVING COUNT(*) > 0` filters on the COUNT, not on the key that
shares its name.

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

-- Positional references over SELECT * — the star's columns count one position
-- each, in schema order, so position 2 is the table's second column
SELECT * FROM flow_logs ORDER BY 1
SELECT *, 1 AS marker FROM flow_logs ORDER BY 2 DESC

-- Null ordering
SELECT * FROM flow_logs ORDER BY src_ip ASC NULLS FIRST
SELECT * FROM flow_logs ORDER BY bytes_in DESC NULLS LAST
```

A position past the end of the select list is SQLSTATE `42P10`
(`ORDER BY position N is not in select list`).

### ORDER BY an aggregate the SELECT list does not carry

```sql
-- The aggregate is computed for the ordering and never returned
SELECT src_ip FROM flow_logs GROUP BY src_ip ORDER BY MAX(bytes_in) DESC
SELECT src_ip FROM flow_logs GROUP BY src_ip ORDER BY COUNT(*), MIN(bytes_in)
```

A bare aggregate CALL in `ORDER BY` is answered whether or not the SELECT list
carries it, as PostgreSQL does. An expression COMPUTED from an aggregate's
output (`ORDER BY COUNT(*) * 2`) is refused with SQLSTATE `0A000`: select the
expression and order by its alias.

One shape is refused that PostgreSQL answers: a positional reference over a
`SELECT *` whose FROM clause is a **join**, or a derived table whose **own**
FROM is a join, where the star is left unexpanded because its column set is not
resolvable from the catalog alone. That is `42P10` with a message saying so;
name the columns, or sort by the column itself. A star over an ordinary derived
table is not affected: `SELECT * FROM (SELECT * FROM t) x ORDER BY 1` answers,
as do an explicit column list, aliased columns, and a nested derived table
inside it (measured on all three execution paths).

## Derived tables and CTE column lists

A derived table or a CTE may rename its output columns positionally with a
column-alias list:

```sql
SELECT kk, nn FROM (SELECT s, n FROM t) AS b(kk, nn)
WITH c(kk, nn) AS (SELECT s, n FROM t) SELECT kk FROM c
```

Fewer aliases than columns rename a **prefix** — `AS b(kk)` over a two-column
subquery publishes `kk` and the second column's own name. More aliases than the
subquery has columns is SQLSTATE `42P10` (`table "b" has 2 columns available
but 3 columns specified`). Both are PostgreSQL's rules.

A list written over a subquery whose SELECT list contains `*` is not applied:
the star's width is not resolvable at that point, and renaming the wrong
columns would be a wrong answer rather than a missing one. PostgreSQL does
apply it there, so that spelling publishes the inner column names here and the
aliases there — a divergence in the published NAMES only, recorded in
ADR-0012.

### CTE scope

A non-recursive CTE's own name is not in scope inside its own body. A CTE may
therefore shadow a base table and still read it:

```sql
-- the body's FROM events reads the TABLE; the outer query reads the CTE
WITH events AS (SELECT id, bytes_in * 2 AS dv FROM events)
SELECT id, dv FROM events
```

A CTE name is visible to SIBLING and LATER CTEs and to the main query. A
reference to a WITH item that is not yet defined — its own name inside its
body, or a forward reference to a later item — is SQLSTATE `42P01`
(`relation "..." does not exist`), as it is in PostgreSQL. `WITH RECURSIVE` is
the exception: a recursive CTE's name IS visible inside its own body.

A `WITH` may also be written INSIDE a nested query block — a derived table, a
CTE body, a `LATERAL` subquery — and its items are in scope for that block; the
enclosing query's items stay visible there too:

```sql
WITH o AS (SELECT id, dx FROM b)
SELECT v FROM (WITH c AS (SELECT id, dx FROM o) SELECT dx AS v FROM c) t
```

One divergence: where a block's own item has the SAME NAME as one of the
enclosing query's, PostgreSQL reads the inner definition and wadjet reads the
outer one. A `WITH` written inside a scalar or `IN` subquery is not parsed at
all (SQLSTATE `42601`).

## Output column names

An item with an explicit `AS` alias is published under it, byte for byte. An
item WITHOUT one takes PostgreSQL's name for it:

| the item | the column's name |
|---|---|
| a column reference — `g`, `t.g`, `(g)` | `g` (unqualified) |
| a ROW field path — `(c_row).b` | `b` (the field) |
| a function or aggregate call — `abs(g)`, `count(*)`, `sum(g) OVER ()` | `abs`, `count`, `sum` |
| `CASE`, `COALESCE`, `NULLIF`, `GREATEST` | `case`, `coalesce`, `nullif`, `greatest` |
| `EXISTS (…)`, `ARRAY[…]`, `EXTRACT(…)` | `exists`, `array`, `extract` |
| `TRIM(x)` | `btrim` — PostgreSQL names the column after the function it RESOLVED to |
| `CAST(g AS bigint)`, `g::int` | `g` — a cast is named after its ARGUMENT |
| `CAST('2020-01-01' AS date)` | `date` — the TYPE, and only when the argument has no name |
| a scalar subquery — `(SELECT g FROM t LIMIT 1)` | `g` |
| anything with no natural name — `g + 1`, `-g`, `1`, `g IS NULL`, `g IN (1,2)` | `?column?` |

**One exception: a SET OPERATION.** `SELECT g + 1 FROM t UNION ALL SELECT g + 2
FROM t` publishes `g + 1` here where PostgreSQL publishes `?column?`, on every
execution path and both doors. A set operation's output columns are named by its
LEFTMOST arm in PostgreSQL; this engine names them from the arm's own resolution
spelling, and the rule above is not applied there. Alias the arm's items to
choose the name.

Several columns may be called `?column?`, or `count`, in one result — a slot's
identity is its POSITION (see below), so that is not a collision. A JSON result object cannot represent two
such columns, so the HTTP door sends a positional `values` array beside `rows`
whenever the names are not unique, and the gRPC door fills `Row.values` for the
same case.

The name a query publishes is not the name it RESOLVES by, and one place shows
the difference. An enclosing query refers to an unnamed derived column by the
inner block's own spelling, not by `?column?`:

```sql
SELECT "g + 1" FROM (SELECT g + 1 FROM t) s   -- answers
SELECT "?column?" FROM (SELECT g + 1 FROM t) s -- 42703, naming the column that exists
```

PostgreSQL is the other way round on both lines. It is a name-only divergence
and it is loud in the direction that matters: a reference wadjet cannot resolve
is an error, never a different column.

## Duplicate output column names

A result may carry two columns of the same name — `SELECT abs(a), abs(b)` is
two columns called `abs`, and an explicit `AS u` twice is two called `u`. Both
keep their own values, on every execution path and over a set operation.

A slot's identity is its POSITION, so `ORDER BY 2` sorts by the second output
column whatever it is called. Sorting by an ambiguous NAME (`ORDER BY u` where
two columns are called `u`) is answered here — it binds the first — where
PostgreSQL refuses it with SQLSTATE `42702`.

Reading a result by column name cannot represent both columns; the embedded
API exposes the positional form (`QueryResult.Cells`) for exactly this case,
and the wire protocol sends every column regardless.

## Set operations

`UNION`, `UNION ALL`, `INTERSECT` and `EXCEPT` combine two queries. Either arm
may be parenthesised, and the parentheses scope that arm's own `ORDER BY` and
`LIMIT`:

```sql
(SELECT id FROM t ORDER BY id LIMIT 1)
UNION ALL
(SELECT id FROM t ORDER BY id DESC LIMIT 1)
```

A whole set operation may be parenthesised too, including as one arm of
another one and as a derived table:

```sql
SELECT COUNT(*) FROM ((SELECT id FROM t) UNION ALL (SELECT id FROM t)) u
```

An `ORDER BY` or `LIMIT` written after the last arm without parentheses
applies to the WHOLE result, as it does in PostgreSQL.

The result's column NAMES are the LEFTMOST arm's, and each one is that SELECT
item's own output name: its alias if it has one, otherwise the column's name
UNQUALIFIED, otherwise the expression as written. `SELECT x.id, x.w FROM …
UNION ALL …` publishes `id | w`, not `x.id | x.w`, and a delimited alias keeps
its bytes.

Corresponding columns must have a COMMON TYPE. The numeric types widen into
one another (`integer` → `bigint` → `numeric` → `real` → `double precision`)
and nothing else does, so a pair with no common type is refused at PLAN time
with SQLSTATE `42804`, in either arm order and for `UNION`, `INTERSECT` and
`EXCEPT` alike:

```
wadjet=> SELECT bytes_in FROM flow_logs UNION ALL SELECT proto_name FROM flow_logs;
ERROR:  UNION types bigint and text cannot be matched: result column "bytes_in"
```

"Common type" is PostgreSQL's rule, so a quoted literal or `NULL` has no type
of its own and takes the other arm's — `SELECT addr FROM t UNION ALL SELECT
'10.0.0.9'` is an `IPV4` union, not a type error — and `PORT`, `PROTOCOL` and
`DURATION` are integers, which is what they declare on the wire.

A few set operations PostgreSQL resolves are not yet CARRIED here. Each is
refused on every path, before execution, with SQLSTATE `0A000` and a message
saying what is missing:

- `DATE` beside `TIMESTAMP`;
- two members of the `IPV4` / `IPV6` / `CIDR` family;
- `PORT`, `PROTOCOL` or `DURATION` beside a `DECIMAL`. Beside an integer or a
  float they resolve and answer.

  For those three, `CAST` both arms to one type.

- a QUOTED literal whose resolved type is `BOOLEAN`, an integer, a float,
  `TIMESTAMP`, `PORT`, `PROTOCOL` or `DURATION`: the literal reaches the result
  column as text, and those columns are not built from text here. Write the
  literal unquoted, or `CAST` it. `NULL` is unaffected, and the remaining types
  (`TEXT`, `BYTEA`, the address types, `UUID`, `DATE`, `NUMERIC`) resolve and
  answer.

An arm is read through what its OWN plan publishes, so an arm's SELECT list may
name anything a standalone `SELECT` may — a window function included, aliased
or not, and whether or not its alias also names a column of the arm's input:

```sql
-- `s` here is the window's value, not the `s` column of decpair
SELECT id, SUM(a) OVER () AS s FROM decpair
UNION ALL
SELECT id, b AS s FROM decpair
```

## Numeric literals

Integer literals take PostgreSQL 16+'s grammar, unquoted and as text read into
an integer type alike:

```sql
SELECT 0x1A, 0o17, 0b101, 1_000, 007   -- 26, 15, 5, 1000, 7
SELECT CAST('0x1A' AS BIGINT)          -- 26
SELECT id FROM t WHERE k = '1_000'     -- the same grammar in a predicate
```

`0x`/`0X`, `0o`/`0O` and `0b`/`0B` are radix prefixes; `_` separates digits and
must sit between them; and a leading zero is DECIMAL, so `017` is seventeen.
`0x_1A` is legal, because the prefix stands in front of the underscore.

Where an underscore is misplaced depends on the door, as it does in
PostgreSQL. Written as an unquoted literal, `100_` and `1__0` are SQLSTATE
`42601` (`trailing junk after numeric literal`) — the number does not simply
stop at the underscore and hand the rest to the parser as a column alias. Read
as TEXT into an integer type, `'_100'`, `'100_'`, `'1__0'` and `'0x'` are
SQLSTATE `22P02`.

An unquoted radix literal has no width of its own and is read exactly, however
long: `0x8000000000000000` is 9223372036854775808. Reading text INTO a type is
where a range applies — `CAST('0x8000000000000000' AS BIGINT)` is SQLSTATE
`22003`, never a wrapped number.

An error message quotes the offending text byte for byte, as PostgreSQL does —
a literal containing a non-ASCII byte is echoed, not escaped.

## Predicates must be boolean

`WHERE`, `HAVING`, a `JOIN ... ON` condition, the operands of `NOT`/`AND`/`OR`
and a searched `CASE`'s `WHEN` all require a boolean. A non-boolean there is
SQLSTATE `42804`, naming the site and the type:

```
wadjet=> SELECT id FROM t WHERE bytes_in;
ERROR:  argument of WHERE must be type boolean, not type bigint
wadjet=> SELECT id FROM t WHERE NOT bytes_in;
ERROR:  argument of NOT must be type boolean, not type bigint
```

A quoted literal is the exception, as it is in PostgreSQL: it has no type of
its own, so a boolean context reads it with the boolean input function.
`WHERE 'true'`, `'t'`, `'yes'`, `'on'`, `'1'` and any non-empty prefix of those
words are TRUE; `'false'`, `'no'`, `'off'`, `'0'` are FALSE; `WHERE NULL`
returns no rows; and a string naming no boolean is SQLSTATE `22P02`
(`invalid input syntax for type boolean: "abc"`).

The refusal is made where the type is PROVABLE — a column whose declaration
the planner carries, a numeric literal, arithmetic, or `COUNT`. A predicate
whose type the planner cannot prove (a scalar function's result, a column of a
derived table or CTE) is not refused.

## LIMIT and OFFSET

```sql
-- First 100 rows
SELECT * FROM flow_logs LIMIT 100

-- Pagination: skip 200, return next 100
SELECT * FROM flow_logs ORDER BY timestamp DESC LIMIT 100 OFFSET 200
```

## JOIN

Wadjet supports multiple join types using a hash join strategy.

### Join conditions

A join names its condition with `ON`, or with `USING (col, ...)` when both
sides carry the same column name:

```sql
-- These two are the same join
FROM flow_logs f JOIN device_inventory d ON f.device_id = d.device_id
FROM flow_logs f JOIN device_inventory d USING (device_id)
```

`USING` accepts several columns — `USING (device_id, day)` — and both sides
remain addressable by their qualified names (`f.device_id`, `d.device_id`).
See **Limitations** for the two `USING` shapes that are refused.

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

The join implementation uses a **hash join** strategy: the right side is loaded into a hash table (build phase), then the left side is probed against it (probe phase). For inner joins the optimizer picks the build side itself from cardinality estimates — the smaller relation builds, the larger probes — and cost-reorders chains of three or more relations. Only outer joins keep the order you wrote, because their order is semantically significant.

A join whose `ON` clause equates two **expressions** rather than two columns — `ON UPPER(a.name) = UPPER(b.name)`, `ON a.id + 1 = b.id + 1`, `ON CONCAT('x', a.g) = CONCAT('x', b.g)` — has no equi-key for the hash table, so it is executed as a cross join with the condition applied to each pair. That is correct but quadratic, and it is also the one join shape whose build side cannot spill: a cross join's every probe row needs every build row, so the build must fit the task's memory budget. Under a budget it fails with `memory budget exceeded` naming that reason. Where the expression can be computed as a column before the join — a stored or projected column joined on directly — the hash path is available and both limits go away.

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
| `\|\|` | String concatenation — NULL in either operand makes the result NULL (use `CONCAT` to ignore NULLs) |

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

`CAST(<col> AS STRING)` renders the value's own printed form — the text the
column projects and the text `LIKE` matches against, which for a TIMESTAMP is
`2006-01-02 15:04:05` (UTC, with milliseconds only when non-zero), for a DATE
`2006-01-02`, for IPv4/IPv6/MAC/CIDR/UUID the address or identifier, and for
BYTES `\x` plus lowercase hex.

Two of those renderings differ from PostgreSQL's, and they differ everywhere —
on the wire, in a projection and in a CAST alike, so a query never disagrees
with itself. A sub-second TIMESTAMP is padded to three fractional digits
(`2023-11-14 22:13:20.500`) where PostgreSQL prints the minimal fraction
(`…20.5`). A DURATION renders its raw nanosecond count where PostgreSQL's
`interval` prints `00:00:00.001`.

### Casts and errors

A conversion that cannot produce the value **raises**; it does not answer NULL
and it does not answer a zero. The SQLSTATE is PostgreSQL's own, and the codes
are different answers — a client branches on them:

| Expression | SQLSTATE | Message |
|---|---|---|
| `CAST('not-a-date' AS DATE)` | `22007` | invalid input syntax for type date: "not-a-date" |
| `CAST('2020-02-30' AS DATE)` | `22008` | date/time field value out of range: "2020-02-30" |
| `CAST('x' AS TIMESTAMP)` | `22007` | invalid input syntax for type timestamp: "x" |
| `CAST('2020-02-30 12:00' AS TIMESTAMP)` | `22008` | date/time field value out of range: … |
| `CAST('abc' AS UUID)` | `22P02` | invalid input syntax for type uuid: "abc" |
| `CAST('abc' AS INTEGER \| BIGINT \| REAL \| DOUBLE PRECISION \| NUMERIC \| BOOLEAN)` | `22P02` | invalid input syntax for type … |
| `CAST('1e400' AS DOUBLE PRECISION)` | `22003` | "1e400" is out of range for type double precision |
| `CAST(1e40 AS REAL)` | `22003` | … is out of range for type real |
| `CAST('abcdef' AS VARCHAR(0))` / `CHAR(0)` | `22023` | length for type varchar \| char must be at least 1 |
| `CAST('abcdef' AS VARCHAR(10485761))` | `22023` | length for type varchar cannot exceed 10485760 |
| `CAST('abcdef' AS VARCHAR(abc))` / `VARCHAR(-1)` | `42601` | syntax error at or near "abc" \| "-" |
| `CAST('abcdef' AS TEXT(5))` | `42601` | type modifier is not allowed for type "text" |
| `CAST(x AS FLOAT(0))` / `FLOAT(54)` | `22023` | precision for type float must be at least 1 bit / less than 54 bits |
| `CAST(x AS DECIMAL(p,s))` past the carrier | `22003` | numeric field overflow |
| `bigint` arithmetic past its range — including under a `CAST`, `ABS`/`MOD`, or a `CASE`/`COALESCE`/`NULLIF`/`GREATEST`/`LEAST` over integer branches | `22003` | bigint out of range |
| `ABS(<int4 column>)` at `-2147483648` | `22003` | integer out of range |
| `ABS(<int8 column>)` at `-9223372036854775808` | `22003` | bigint out of range |
| `DATE_TRUNC('<unrecognized>', ts)` | `22023` | unit "…" not recognized for type timestamp without time zone |
| `SPLIT_PART(s, d, 0)` | `22023` | field position must not be zero |
| `WIDTH_BUCKET(v, lo, hi, n)` with `n <= 0` | `2201G` | count must be greater than zero |
| `WIDTH_BUCKET(v, b, b, n)` | `2201G` | lower bound cannot equal upper bound |
| `CHR(0)` | `54000` | null character not permitted |
| `CHR(<negative>)` | `22023` | character number must be positive |
| `CHR(n)` above U+10FFFF | `54000` | requested character too large for encoding: n |
| `SUBSTRING(s, start, len)` with `len < 0` | `22011` | negative substring length not allowed |
| `1/0`, `x % 0`, `MOD(x, 0)`, `LOG(1, x)` | `22012` | division by zero |
| `LN(0)`, `LOG(0)`, `LOG2(0)` | `2201E` | cannot take logarithm of zero |
| `LN(-1)`, `LOG(-1)` | `2201E` | cannot take logarithm of a negative number |
| `SQRT(-1)` | `2201F` | cannot take square root of a negative number |
| `POWER(0, -1)` | `2201F` | zero raised to a negative power is undefined |
| `POWER(-1, 0.5)` | `2201F` | a negative number raised to a non-integer power yields a complex result |
| `POWER(2, 10000)`, `EXP(1000)` | `22003` | value out of range: overflow |
| `EXP(-1000)` | `22003` | value out of range: underflow |
| `ASIN(2)`, `ACOS(2)` | `22003` | input is out of range |

The four string-modifier refusals and the two `FLOAT(n)` ones are read by ONE
function, so **`CREATE TABLE` refuses exactly what a `CAST` refuses**, with the
same code and the same message (the DDL door adds a `column "v": ` prefix, and
folds an unquoted non-numeric modifier to upper case before quoting it):

```sql
SELECT CAST('abcdef' AS VARCHAR(0));  -- 22023 length for type varchar must be at least 1
CREATE TABLE t (v VARCHAR(0));        -- 22023 column "v": length for type varchar must be at least 1
CREATE TABLE t (v TEXT(5));           -- 42601 column "v": type modifier is not allowed for type "text"
CREATE TABLE t (v VARCHAR(255));      -- accepted; the 255 is not stored
```

Wadjet's own `ABS` refusals are the two-complement asymmetry, not an
arithmetic limit: `|min|` has no value in the type it came from, so it fails
rather than answering the same negative number back. `-2147483647` and every
value above it answer normally. Note that wadjet computes every integer
expression in 64 bits, so `<int4 column> * 2` and `-<int4 column>` ANSWER where
PostgreSQL raises `integer out of range` — a deliberate superset (ADR-0012).

NaN and the infinities are **values**, not failures, and pass through the way
PostgreSQL passes them: `SQRT('NaN')` is NaN, `LN('Infinity')` is Infinity,
`SQRT(-0.0)` is `-0`, `ASIN('NaN')` is NaN, `EXP('-Infinity')` is `0`,
`EXP('Infinity')` is `Infinity`, and `POWER(2, 'Infinity')` is `Infinity`. An
infinite operand is never an overflow — the value was already there.

`POWER` that UNDERFLOWS to zero — `POWER(0.5, 2000)`, `POWER(1e-200, 3)` — is a
**value**, `0`, not an error. PostgreSQL resolves that spelling to
`power(numeric, numeric)`, which has no range check; its `float8` overload does
raise `22003`, and wadjet has one float path, so this answers where an explicit
`power(x::float8, y::float8)` would be refused on the server.

A `CAST` whose destination names no type this engine has is SQLSTATE `42704`
(`type "bogustype" does not exist`), the same code and message the `CREATE
TABLE` door gives for the same name. That includes three PostgreSQL type names
this engine has no type for and gets the VALUE wrong for — `bytea`, `money`,
`inet` — so those are refused where the server answers; the alternative was
returning the operand under a `text` declaration, and that text is not the
server's (`abc` for `\x616263`, `1.5` for `$1.50`, `192.168.1.1` for
`192.168.1.1/32`). Recorded in ADR-0012's divergence list.

`time`, `json` and `xml` are accepted and pass their operand's text through
unchanged, which is what PostgreSQL answers for each: `CAST('12:34:56' AS
time)` is `12:34:56` on both. They are described as `text` on the wire where
PostgreSQL describes them as their own types.

A cast to a destination this engine HAS but does not convert — the network
types, `DURATION`, `BYTES`, `VECTOR(n)`, the containers — still returns its
operand unchanged, and a cast of non-address text to `IPV4`, `IPV6`, `CIDR` or
`MACADDR` does the same rather than raising. Also in that list.

## Window Functions

Window functions compute values across sets of rows related to the current row without collapsing them into groups.

### Supported Window Functions

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Sequential row number within partition |
| `RANK()` | Rank with gaps for ties |
| `DENSE_RANK()` | Rank without gaps for ties |
| `SUM(expr)` | Running or partition sum |
| `COUNT(expr)` | Running or partition count — `COUNT(col)` counts the frame's NON-NULL values, `COUNT(*)` counts its rows; both answer 0 (never NULL) over an empty frame |
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
| `N PRECEDING` | N rows before the current row (`ROWS` mode only) |
| `CURRENT ROW` | The current row |
| `N FOLLOWING` | N rows after the current row (`ROWS` mode only) |
| `UNBOUNDED FOLLOWING` | To the last row of the partition |

#### Frame Modes

| Mode | Description |
|------|-------------|
| `ROWS` | Physical row-based window boundaries |
| `RANGE` | Peer-group boundaries over the ORDER BY key. Only `UNBOUNDED PRECEDING`, `CURRENT ROW` and `UNBOUNDED FOLLOWING` are accepted — a value offset (`RANGE BETWEEN 5 PRECEDING ...`) is rejected rather than silently evaluated as a row count. The `GROUPS` frame mode is not supported. |

## Built-in Functions

Wadjet includes 359 built-in scalar functions across several categories.

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER(protocol)` |
| `LOWER(s)` | Lowercase | `LOWER(hostname)` |
| `CONCAT(a, b, ...)` | Concatenate strings; **NULL arguments are ignored** (all-NULL gives `''`, never NULL) — `\|\|` propagates NULL instead | `CONCAT(src_ip, ':', src_port)` |
| `LENGTH(s)` / `LEN(s)` | String length in **characters**, the synonym of `CHAR_LENGTH` (use `OCTET_LENGTH` for bytes) | `LENGTH(message)` |
| `SUBSTR(s, start, len)` / `SUBSTRING` | Extract substring; `start` and `len` count **characters**, and a negative `len` is SQLSTATE 22011 | `SUBSTR(message, 1, 50)` |
| `TRIM(s)` | Remove leading/trailing whitespace | `TRIM(hostname)` |
| `LTRIM(s)` | Remove leading whitespace | `LTRIM(message)` |
| `RTRIM(s)` | Remove trailing whitespace | `RTRIM(message)` |
| `REPLACE(s, old, new)` | Replace occurrences | `REPLACE(message, 'error', 'ERROR')` |
| `REVERSE(s)` | Reverse string, by character | `REVERSE(hostname)` |
| `LEFT(s, n)` | First n characters | `LEFT(hostname, 3)` |
| `RIGHT(s, n)` | Last n characters | `RIGHT(hostname, 2)` |
| `STARTS_WITH(s, prefix)` | Test if string starts with prefix | `STARTS_WITH(hostname, 'web')` |
| `ENDS_WITH(s, suffix)` | Test if string ends with suffix | `ENDS_WITH(hostname, '.com')` |
| `CONTAINS(s, sub)` | Test if string contains substring | `CONTAINS(message, 'error')` |
| `REPEAT(s, n)` | Repeat string n times | `REPEAT('*', 10)` |
| `SPLIT_PART(s, delim, n)` | Extract nth part from delimited string; 1-based, and a NEGATIVE n counts from the end. Position 0 is SQLSTATE 22023; a position past either end is the empty string | `SPLIT_PART(url, '/', 3)`, `SPLIT_PART(url, '/', -1)` |
| `STRPOS(s, sub)` / `POSITION(sub IN s)` | Position of substring in **characters** (1-based, 0 if not found) | `STRPOS(message, 'error')` |
| `REGEXP_LIKE(s, pattern)` | Test if string matches regex | `REGEXP_LIKE(src_ip, '^\d+\.\d+')` |
| `REGEXP_EXTRACT(s, pattern [, group])` | Extract regex match or capture group | `REGEXP_EXTRACT(url, '(\w+)://(\w+)', 2)` |
| `REGEXP_REPLACE(s, pattern, repl)` | Replace regex matches | `REGEXP_REPLACE(message, '\s+', ' ')` |
| `REGEXP_COUNT(s, pattern)` | Count regex matches | `REGEXP_COUNT(path, '/')` → `3` |
| `REGEXP_EXTRACT_ALL(s, pattern)` | Extract all regex matches (JSON array) | `REGEXP_EXTRACT_ALL(log, '\d+')` → `'["123","456"]'` |
| `REGEXP_SPLIT(s, pattern)` | Split by regex (JSON array) | `REGEXP_SPLIT(csv, ',\s*')` |
| `SPLIT(s, delim)` | Split by delimiter (JSON array) | `SPLIT('a.b.c', '.')` → `'["a","b","c"]'` |
| `LPAD(s, n [, pad])` | Left-pad to n **characters**, truncating to n when longer | `LPAD(port, 5, '0')` |
| `RPAD(s, n [, pad])` | Right-pad to n **characters**, truncating to n when longer | `RPAD(name, 20)` |
| `CHR(n)` | Character from code point. `CHR(0)` is SQLSTATE 54000 (a NUL cannot travel in a text DataRow), a negative code is 22023, and a code past U+10FFFF is 54000 | `CHR(65)` → `'A'` |
| `CODEPOINT(s)` | Code point of first character | `CODEPOINT('A')` → `65` |
| `CONCAT_WS(sep, a, b, ...)` | Concatenate with separator (skips NULLs) | `CONCAT_WS(',', a, b, c)` |
| `CHAR_LENGTH(s)` / `CHARACTER_LENGTH(s)` | Character length (Unicode-aware); the synonym of `LENGTH` | `CHAR_LENGTH('日本語')` → `3` |
| `TRANSLATE(s, from, to)` | Character-by-character translation | `TRANSLATE('abc', 'abc', 'xyz')` |
| `SOUNDEX(s)` | Phonetic code | `SOUNDEX('Robert')` → `'R163'` |
| `LEVENSHTEIN_DISTANCE(a, b)` | Edit distance between strings | `LEVENSHTEIN_DISTANCE('kitten', 'sitting')` → `3` |
| `HAMMING_DISTANCE(a, b)` | Number of differing characters | `HAMMING_DISTANCE('abc', 'axc')` → `1` |
| `NORMALIZE(s)` | Unicode NFC normalization | `NORMALIZE(text)` |
| `FORMAT(fmt, args...)` | Go-style sprintf formatting | `FORMAT('%s:%d', host, port)` |
| `LCASE(s)` / `UCASE(s)` | Aliases for LOWER/UPPER | `LCASE(name)` |
| `TO_UTF8(s)` | String to its raw UTF-8 bytes (BYTES) | `TO_UTF8('hello')` |
| `FROM_UTF8(b)` | BYTES back to a string; NULL when the bytes are not valid UTF-8 | `FROM_UTF8(data)` |

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
| `WIDTH_BUCKET(val, min, max, buckets)` | Assign value to histogram bucket. A bucket count of zero or less, and equal bounds, are SQLSTATE 2201G | `WIDTH_BUCKET(latency, 0, 100, 10)` |
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
| `DATE_TRUNC(part, ts)` | Truncate to precision. `part` is one of `microseconds`, `milliseconds`, `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, `decade`, `century`, `millennium` (case-insensitive); anything else is SQLSTATE 22023 | `DATE_TRUNC('hour', timestamp)` |
| `DATE_DIFF(a, b)` | Whole days between two instants (a - b), truncated toward the past | `DATE_DIFF(end_ts, start_ts)` |
| `DATE_ADD(ts, n)` / `DATE_ADD(ts, INTERVAL)` | Add n **days**, or an INTERVAL in its own unit, preserving time-of-day | `DATE_ADD(ts, 7)` |
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

A function that RETURNS an instant renders it the one way this engine renders
a timestamp — `DATE_TRUNC('day', ts)` is `2023-11-14 00:00:00`, the same text
the column it read produces and the text PostgreSQL produces. `TO_ISO8601` and
`AT_TIMEZONE` are the exceptions: the first is named for its format, and the
second returns a wall clock in another zone whose offset is part of the value.
See [data-types.md](data-types.md) §Timestamp, "One rendering".

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
| `CARDINALITY(array)` | Number of elements; `0` for an empty array | `CARDINALITY(ARRAY[1,2,3])` → `3` |
| `ARRAY_LENGTH(array, dim)` | Length along dimension `dim`, or NULL when that dimension does not exist | `ARRAY_LENGTH(ARRAY[1,2,3], 1)` → `3`; `ARRAY_LENGTH(ARRAY[1,2,3], 2)` → `NULL` |
| `ARRAY_LENGTH(array)` | One-argument form (not in PostgreSQL): same as CARDINALITY | `ARRAY_LENGTH(tags)` |
| `ELEMENT_AT(array, index)` | 1-based element access (negative indexes from end) | `ELEMENT_AT(ips, 1)` → first element |
| `ARRAY_CONTAINS(array, value)` | Test membership | `ARRAY_CONTAINS(tags, 'critical')` |
| `ARRAY_JOIN(array, delimiter)` | Concatenate elements with delimiter | `ARRAY_JOIN(tags, ', ')` |
| `ARRAY_MIN(array)` | Minimum element | `ARRAY_MIN(scores)` |
| `ARRAY_MAX(array)` | Maximum element | `ARRAY_MAX(scores)` |

Array literal syntax: `ARRAY[1, 2, 3]`

`ARRAY_LENGTH` and `CARDINALITY` differ on an EMPTY array, exactly as they do
in PostgreSQL: `CARDINALITY(ARRAY[])` is `0` because there are no elements, and
`ARRAY_LENGTH(ARRAY[], 1)` is `NULL` because there is no dimension 1. Wadjet's
`ARRAY` is one-dimensional, so any `dim` other than `1` is `NULL`.

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

Requires an embedding provider. `WADJET_EMBED_PROVIDER` selects `openai` (default, needs `WADJET_OPENAI_API_KEY`), `voyage` (needs `WADJET_VOYAGE_API_KEY`) or `ollama` (local, keyless, `WADJET_OLLAMA_URL`). With no provider configured, `embed()` is not registered at all.

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

Over the PostgreSQL wire protocol a DML statement completes with PostgreSQL's
command tag — `INSERT 0 <n>`, `UPDATE <n>`, `DELETE <n>`, `MERGE <n>` — on
both the simple protocol (psql) and the extended protocol (pgx, JDBC,
psycopg, and every ORM), so a driver's "rows affected" is the number of rows
the statement actually affected.

### INSERT

```sql
INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...) [, (val3, val4, ...) ...]
```

If column list is omitted, values are matched to schema column order.

The column list is resolved against the table before anything is written, and
the refusals carry PostgreSQL's classes:

| Statement | SQLSTATE |
|---|---|
| a column the table does not have | 42703 `column "x" of relation "t" does not exist` |
| the same column named twice | 42701 `column "x" specified more than once` |
| a row with the wrong number of values | 42601 |
| a NOT NULL column left without a value | 23502 |

Column names are matched case-insensitively, as they are in `UPDATE ... SET`
and in a `WHERE` clause.

### DELETE

```sql
DELETE FROM table_name [WHERE condition]
```

Without WHERE, deletes all rows. Delete markers are stored in the table manifest and applied during scans.

#### A subquery in a DML predicate

`DELETE`, `UPDATE` and a `MERGE ... WHEN ... AND` condition all accept a
subquery in the predicate — `IN (SELECT ...)`, `NOT IN (SELECT ...)`, a scalar
subquery, and `EXISTS` / `NOT EXISTS` correlated to the target:

```sql
DELETE FROM orders     WHERE id IN (SELECT order_id FROM cancelled);
DELETE FROM orders o   WHERE EXISTS (SELECT 1 FROM cancelled c WHERE c.order_id = o.id);
UPDATE orders SET n = 0 WHERE n < (SELECT max(n) FROM archive);
```

A `MERGE` takes one in every position it has: a `WHEN MATCHED AND` condition, a
`WHEN NOT MATCHED AND` condition, a `SET` assignment and an `INSERT ... VALUES`
item. A condition's subquery may be correlated to the TARGET or to the SOURCE —
its scope is the merged row. The `ON` condition is the one place a `MERGE`
takes none, and not because of the subquery: `ON` accepts only equality between
a target column and a source column (see the unsupported list), so any non-key
term refuses.

```sql
MERGE INTO orders t USING batch s ON t.id = s.id
  WHEN MATCHED AND EXISTS (SELECT 1 FROM cancelled c WHERE c.order_id = t.id) THEN DELETE
  WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, (SELECT max(n) FROM archive));
```

Three properties are worth knowing:

- The subquery reads the table's state **as the statement found it**. A
  subquery over the target table itself — `DELETE FROM t WHERE id IN (SELECT id
  FROM t WHERE ...)` — sees no row this statement has removed, which is what
  PostgreSQL does.
- An UNCORRELATED subquery runs once for the statement. A CORRELATED one runs
  once per candidate row, with the outer values substituted as literals, so an
  outer column with no literal spelling (`ARRAY`, `ROW`, `MAP`, `VECTOR`) is
  SQLSTATE `0A000`.
- A subquery that cannot run **fails the statement and writes nothing** —
  never a row set decided by a failure.
- A **scalar** subquery is at most ONE row. Zero rows is SQL `NULL`; more than
  one is SQLSTATE `21000`, `more than one row returned by a subquery used as an
  expression`, and nothing is written. The same rule applies in a `SELECT`.
- An `IN` / `NOT IN` subquery's **set** is bounded by `WADJET_IN_SET_MAX`
  (default 10,000 rows; `0` disables the bound). Past it the statement is
  refused with SQLSTATE `54000` and writes nothing — a set short by one row
  would delete the wrong rows, so the bound refuses rather than truncating.
  The bound is on the SET, so it does not reach the other constructs:
  `EXISTS` asks whether there is A row and reads one, and a scalar subquery
  reads two — enough for the `21000` above, which is the code it raises past
  the bound as well, because what is wrong there is the query's meaning and
  not its size.

### UPDATE

```sql
UPDATE table_name SET col1 = val1 [, col2 = val2 ...] [WHERE condition]
```

Internally executes as DELETE of matching rows + INSERT of modified rows.

### MERGE

```sql
MERGE INTO target [AS alias] USING source [AS alias] ON condition
  WHEN MATCHED [AND cond] THEN UPDATE SET ... | DELETE
  WHEN NOT MATCHED [AND cond] THEN INSERT (cols) VALUES (...)
```

The target and the source must have **different exposed names** — the alias
where one is written, the relation's own name otherwise. `MERGE INTO t USING s
AS t`, `MERGE INTO t USING t` and `MERGE INTO t AS x USING s AS x` are all
SQLSTATE `42712` (`name "t" specified more than once`), refused before anything
is written, as PostgreSQL refuses them. The rule is over exposed names and not
over relations, so a table may be merged into itself under two different
aliases (`MERGE INTO t AS a USING t AS b ON a.id = b.id`), and a source may be
aliased with the target's *table* name when the target itself is aliased to
something else (`MERGE INTO t AS x USING s AS t`).

A target row may be affected at most once. Two source rows matching one target
row is SQLSTATE `21000` (`MERGE command cannot affect row a second time`).

### Several statements in one message

A SQL string may carry several statements separated by semicolons, and where
it is accepted depends on the door, exactly as it does in PostgreSQL:

| Door | A multi-statement string |
|---|---|
| pgwire **simple** query protocol (`psql`, `PQexec`) | runs them **in sequence**, one command tag per statement |
| pgwire **extended** protocol (pgx, JDBC, psycopg, every ORM) | SQLSTATE `42601`, `cannot insert multiple commands into a prepared statement` |
| embedded `wadjet.DB.Execute` / `Query`, the CLI | SQLSTATE `42601`, the same — they answer with one result |
| the HTTP API | HTTP 400 with `"sqlstate": "42601"` in the body |

On the simple protocol the **whole string is parsed before any statement
runs**, so `INSERT ...; ZZZ NOT SQL` runs nothing and reports the syntax
error. The statements then run in order, each sending its own
`CommandComplete`; an error stops the sequence, and the message ends with a
single `ReadyForQuery`.

A semicolon is a separator only when it is one at the top level. Semicolons
inside string literals (`'a;b'`), quoted identifiers (`"a;b"`), dollar-quoted
strings (`$$a;b$$`), line and block comments, and parentheses are text.

A piece with **nothing to run** in it is not a statement, so neither a trailing
semicolon nor a trailing COMMENT makes a second one: `DELETE ... WHERE id = 1;
-- audit note` and `... ; /* banner */` are one statement, as they are in
PostgreSQL. The same holds for a comment-only piece between two statements, and
a string that is only comments is an empty query.

**An error does not undo the statements before it.** PostgreSQL wraps a simple
query string in an implicit transaction and rolls the whole string back;
wadjet has no transactions — `BEGIN` and `COMMIT` are accepted and ignored —
so each statement commits on its own and the ones that already ran stay. This
is the engine's transaction scope, not a property of the sequencing.

### Concurrency

A DELETE, UPDATE or MERGE reads the table's manifest, scans the files it
names, and commits its change — the replacement rows and the delete markers
that supersede what they replace — in a **single compare-and-swap** against
that manifest. The commit is refused if anything the statement read has moved
since it read it:

- **compaction rewrote the files it scanned**, or
- **another statement already superseded a row it is superseding**.

Either way the statement is redone whole against the manifest that replaced
the one it read, so its outcome is one of the serial orders the two statements
could have produced. **Two statements updating the same row leave the key
present once**, with one of the two values; the second `DELETE` of a row the
first already removed reports `DELETE 0`, as PostgreSQL does; and an `UPDATE`
whose row a concurrent `DELETE` removed reports `UPDATE 0`. Statements over
*different* rows never conflict — the rule is over rows, not over files or
tables — so concurrent writers to one table proceed in parallel.

A statement that keeps losing the race reports SQLSTATE **40001**
(`serialization_failure`), which a client is expected to retry; the table is
unchanged when it does. A statement redoes itself at most five times, so 40001
appears only under sustained contention on the SAME rows — measured, eight
concurrent writers on one row over twenty rounds: 120 statements reported
`UPDATE 1` and 40 reported 40001, and the table held the right rows every time.
PostgreSQL blocks the loser on a row lock instead of refusing it, so a client
that never retried would see errors here where it would see latency there.

What this does **not** give you is a unique constraint. Two concurrent
`INSERT`s of the same key, or two `MERGE`s that both take their `WHEN NOT
MATCHED` arm for the same key, both insert — PostgreSQL does the same without
a unique index, and wadjet has no unique indexes. Nothing here spans more than
one statement either: wadjet has no transactions, so `BEGIN`/`COMMIT` are
accepted and ignored and each statement commits on its own.

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
| DECIMAL(p,s) | Numeric literals: `123.45` (parsed exactly and rounded to the column's scale; a value with no 128-bit carrier is SQLSTATE 22003) |
| PORT | Integer 0-65535 |
| PROTOCOL | Integer 0-255 |
| DURATION | Integer nanoseconds |
| BYTES | Quoted: `'raw'` |
| IPV4, IPV6, MAC, CIDR, UUID | Quoted literal in the type's text form: `'10.0.0.1'`, `'aa:bb:cc:dd:ee:ff'` |

ARRAY, ROW, MAP and VECTOR columns cannot be written with `INSERT ... VALUES`:
the value parser accepts a single literal token per value, not a composite
expression.

### Errors

Every DML statement reports the same SQLSTATE a `SELECT` reports for the same
mistake, so a client can branch on the class rather than on the message text:

| Mistake | SQLSTATE | Message |
|---|---|---|
| The relation does not exist (INSERT, UPDATE, DELETE, and both MERGE relations) | `42P01` | `relation "x" does not exist` |
| A MERGE whose target and source have the same exposed name — `MERGE INTO t USING s AS t`, `MERGE INTO t USING t`, `MERGE INTO t AS x USING s AS x` | `42712` | `name "t" specified more than once` |
| An INSERT column list names a column the table does not have | `42703` | `column "c" of relation "x" does not exist` |
| A `WHERE` or `SET` names a column the table does not have | `42703` | names the column |
| A value cannot be parsed as the target column's type | `22P02` | names the value |
| A value does not fit the target column's declared type — a DECIMAL past its `(p, s)`, an integer past the column's width | `22003` | names the type or the bound, never the value — `INT64 out of range` for an integer column, `numeric field overflow: a field with precision p, scale s must round to an absolute value less than 10^(p−s)` for a DECIMAL one. The column comes from the caller: `row N, column "c"` on the INSERT path, `SET c` on the UPDATE path |

A statement that fails leaves the row set exactly as it found it: the INSERT
that names a bad column writes none of its rows, and a failed `UPDATE` — one
whose `SET` value the column's type refuses — deletes nothing.

Gates: `wadjet.TestEveryDMLDoorCarriesItsSQLState` (ten shapes, class and
message per shape, plus a row count afterwards proving a refused INSERT wrote
nothing), `wadjet.TestFailedUpdateLeavesTheRowSetUnchanged` (nine refused
`SET` values, row set asserted intact after each),
`wadjet.TestDMLSetExpressionFollowsPostgresAssignmentCast` for the `22003`
boundary, and the pgwire DML census
(`internal/server/pgwire/dml_census_test.go`), which carries the `42P01`,
`42703` and `22P02` classes as wire cells beside PostgreSQL 17's own answer —
it contains no `22003` cell, and no *DML* gate asserts a `22003` message
text — what the DML gates assert is the class and the unchanged row set.
(Elsewhere the text is gated: the decimal-overflow message is asserted in
`internal/engine/exec/decimal_coerce_test.go`, two coordinator two-path gates
and `internal/storage/parquet/wide_decimal_test.go`.)

## Limitations

- `NATURAL JOIN` — rejected (SQLSTATE `0A000`); write the join condition with `ON`
- `SELECT *` over a `JOIN ... USING` — rejected (`0A000`): `USING` merges the
  joined column into one output column, and a star's column set over a join is
  not resolved by the planner. Name the columns, or join with `ON`
- `JOIN ... USING` that follows another join on the same `FROM` item —
  rejected (`0A000`); the column could come from either relation on the left
- A SUBQUERY in an `UPDATE`'s `SET` list — `SET n = (SELECT max(n) FROM s)` — SQLSTATE 0A000. A subquery in a `DELETE` / `UPDATE` / `MERGE` PREDICATE is supported — see [A subquery in a DML predicate](#a-subquery-in-a-dml-predicate) — and an assignment is a different site.
- `RETURNING` on INSERT/UPDATE/DELETE/MERGE — SQLSTATE 0A000
- `MERGE ... WHEN NOT MATCHED BY SOURCE` / `BY TARGET` — SQLSTATE 0A000. `BY TARGET` is PostgreSQL 17's spelling of the ordinary `NOT MATCHED`; `BY SOURCE` walks the target rows no source row matched, which is how a MERGE expresses the delete half of a full-sync upsert. Eleven cells in the DML census carry PostgreSQL 17's answer for both forms beside the refusal.
- A `MERGE ... ON` condition that is not equality between a target column and a source column — SQLSTATE 0A000. That covers `ON t.id = s.id AND t.n > 5` and `ON t.id = s.id AND t.id IN (SELECT ...)` alike; PostgreSQL's `ON` is an arbitrary join condition and answers both. The merge executor keys the match on the ON equalities, so lifting this makes the match a join.
- `RANGE` window frames with a value offset, and the `GROUPS` frame mode
- `SELECT DISTINCT ON (...)`
- `ORDER BY <expression over an aggregate>` — `ORDER BY COUNT(*) * 2` — SQLSTATE
  `0A000`. A bare aggregate call is answered; a value computed from one has
  nowhere to be evaluated between the distributed aggregate and the gather.
  Select the expression and order by its alias
- An ORDERING quantifier over a subquery — `x < ALL (SELECT ...)`, `x > ANY (SELECT ...)` — SQLSTATE 0A000, on the DML doors and on the query path alike. The equality forms are supported: `= ANY` / `= SOME` over a subquery is `IN`, and `<> ALL` is `NOT IN`.
- A row comparison whose two sides have different arities — `(a, b) = (1)` — SQLSTATE 42601. PostgreSQL words the same refusal 42883.
- An AGGREGATE in a subquery's own `WHERE` — `x IN (SELECT y FROM t WHERE SUM(y) > 0)` — SQLSTATE `42803`, `aggregate functions are not allowed in WHERE`, which is what PostgreSQL raises. An aggregate belonging to the ENCLOSING query is legal there in PostgreSQL and is refused here with the same code: a lowering gap, recorded in ADR-0012's divergence list.
- A DERIVED TABLE inside a subquery's `FROM` that references the enclosing query — `WHERE EXISTS (SELECT 1 FROM (SELECT … WHERE t.k = a.k) d)` — SQLSTATE `0A000`, naming two workarounds: lift the correlated predicate ABOVE the derived table (`… (SELECT … ) d WHERE d.k = a.k`, which answers), or write the derived table as a `LATERAL` join. PostgreSQL answers the original — no `LATERAL` is needed for a reference to an OUTER query level — and this engine has no lowering for it. A reference to a SIBLING of the same `FROM` list is a different thing and stays `42P01`: `LATERAL` is what governs that one, and PostgreSQL refuses it too.
- A correlated subquery this engine cannot express as a join is re-run per outer row with the outer values substituted as literals, so an outer value with no literal spelling that reads back unchanged — an ARRAY / ROW / MAP / VECTOR, a BYTES value that is not valid UTF-8 or holds a NUL, NaN or ±Infinity — is SQLSTATE `0A000` rather than a wrong answer.
- No time-of-day type: a Parquet `TIME` column is read as its raw integer in the file's own unit
