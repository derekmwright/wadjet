# SQL Reference

Wadjet supports a broad subset of SQL for analytical queries, parsed by a custom recursive descent parser with precedence-climbing expression parsing.

## Supported Statement Types

| Statement | Description |
|-----------|-------------|
| `SELECT` | Query data from tables |
| `EXPLAIN [VERBOSE]` | Show the query execution plan without running it |
| `DESCRIBE table_name` | Show the schema of a table |
| `SHOW COLUMNS FROM table_name` | Alias for DESCRIBE |
| `SHOW TABLES` | List the tables the calling identity may read |
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

### The `__`-prefixed names are reserved

The planner materializes its own values into hidden columns — a window
function's output, a materialized `ORDER BY` or `GROUP BY` term, an aggregate's
derived argument, a decorrelated `LATERAL`'s correlation key — and every
consumer of one reads it BY NAME. A query that MINTS a name in that namespace
is refused with SQLSTATE `42939` rather than answered:

```sql
SELECT amount AS __key_0 FROM items   -- 42939, reserved column namespace "__key_"*
CREATE TABLE t (__win_0 BIGINT)       -- 42939, same rule at the DDL door
```

The reserved prefixes are `__agg_`, `__agg_expr_`, `__avg_count`, `__avg_sum`,
`__covar_state`, `__default__`, `__gb_expr_`, `__grouping_`, `__having_`,
`__key_`, `__precomp_agg_`, `__row_loc`, `__rowcount_only__`, `__scalar_`,
`__setop_`, `__sortkey_`, `__subsume_f`, `__tl_`, `__var_state`, `__win_` and
`__winkey_`. PostgreSQL has no such namespace and answers these queries; this
is a recorded divergence (ADR-0012), and the trade is deliberate — the
alternative is not answering them either, it is answering them WRONGLY, with
the user's column read in place of the planner's or the other way round.

READING is not minting: a table that already stores such a column stays
readable, `SELECT *` included. The planner renumbers its own slot to step
around a stored name.

### A relation or column name is also a storage location

A table's data lives at `tables/<name>/…` in the object store, and a partition
key's column name becomes a `<col>=<value>/` component below it. A name that
cannot be one component of an object key is therefore refused at `CREATE`, on
every door, with SQLSTATE `42602`:

| Refused | Why |
|---|---|
| `"a/b"`, `"../../tmp/x"` | contains `/` |
| `"a\b"` | contains `\` |
| `".."`, `"."` | is a path component with a meaning of its own |
| `".hidden"` | begins with `.` |
| a name containing a NUL byte | cannot be a path component |
| a name over 63 bytes (`42622`) | PostgreSQL truncates to 63; a location cannot be truncated |

Everything else PostgreSQL accepts inside double quotes is still accepted,
including spaces, embedded quotes and a `..` inside a name (`"x..y"`). 63 bytes
is PostgreSQL's own identifier length, so every name this engine accepts is a
name PostgreSQL stores unchanged. This is a deliberate divergence from
PostgreSQL, which has no such restriction because its relations are rows in
`pg_class` rather than locations; it is recorded in
`docs/adr/0012-sql-semantics-authority.md`.

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

### Table functions are a privileged capability

A table function is not a table. `read_csv('/etc/shadow')` reads a file off the
server's own disk, `read_json('http://10.0.0.5/x')` makes the **server** issue
an HTTP request from inside its network, and `postgres_query(dsn, sql)` opens
an outbound database connection to wherever the caller points it. None of that
is in the catalog, so the permissions that govern tables do not describe it.

- With **no auth provider** — the embedded engine, the CLI, `wadjet query` —
  nothing is enforced and table functions work exactly as this section
  describes. That is the single-user use they were built for.
- With **auth enabled** they are **denied by default**. An identity is granted
  the capability by an ABAC policy that names it, or — in a deployment with
  only legacy `roles:` and no hand-written ABAC — by holding the `admin`
  permission. A role restricted to a set of tables no longer reaches the
  filesystem through this door.

This is PostgreSQL's disposition for the equivalent primitives: `pg_read_file`
is superuser-only and `COPY … FROM PROGRAM` requires the
`pg_execute_server_program` role.

A denial is `42501` (HTTP 403) and happens **before** anything is opened: the
file is never read and the URL is never fetched. It applies wherever the
function appears — a CTE, a derived table, a join arm, a `UNION` arm, a scalar
/ `IN` / `EXISTS` subquery, the subquery in a DML predicate, and `EXPLAIN`.

`generate_series` and `unnest` compute over their own arguments and open
nothing, so they are not part of this and remain available to every identity.

See [Security](security.md#table-functions-as-a-capability) for how to write
the policy, including scoping it to a path or a host.

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

All set operations support ORDER BY and LIMIT on the combined result, including
positional `ORDER BY` terms, which address the result columns the leftmost arm
names — see "ORDER BY over a set operation". Operations are left-associative
when chained (e.g., `A UNION B EXCEPT C` is `(A UNION B) EXCEPT C`).

A set operation's arms always produce the operation's whole result row, whatever
the query above it reads. Before this release a filter or an aggregate above a set
operation of two `SELECT *` arms pruned those arms down to the columns it named,
which is not a narrowing a set operation can take: the arms are matched by
POSITION over the whole row, and for every spelling but `UNION ALL` that row is
also the deduplication key. `SELECT COUNT(*) FROM (SELECT * FROM t WHERE id <
2000 UNION ALL SELECT * FROM t WHERE id >= 2000) u WHERE id < 10` failed with
`column "g" does not exist in the input schema` on the distributed paths, and a
distinct `UNION`, an `INTERSECT` or an `EXCEPT` of two `SELECT *` arms answered
zero rows on every path. Arms that name their columns explicitly are unaffected
and are still pruned to what the query reads.

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

With auth enabled, `DESCRIBE` follows the same decision that governs *reading*
the table: an identity that may not read the table is refused `42501`
(HTTP 403) and learns nothing about its columns. `SHOW TABLES` likewise lists
only the tables the calling identity may read; it is never a refusal, and the
list may be empty. Explicit ABAC denies apply. This is deliberately unlike
PostgreSQL, which shows `\d` to any role — see
[Security](security.md#metadata-follows-the-table-decision).

A relation is described under the spelling the CATALOG holds: an unquoted
identifier folds at the lexer, and `DESCRIBE Ledger` finds the relation created
as `Ledger` exactly as `SELECT * FROM Ledger` does.

### psql's `\d` and the pg_catalog emulation

`psql`'s `\d <name>` does not look a relation up by equality: it sends the
anchored pattern `c.relname OPERATOR(pg_catalog.~) '^(name)$'`. The wire
protocol's catalog emulation models THAT form — the whole-name pattern `psql`
emits for a literal identifier, including the `E'^(a\\.b)$'` spelling for a
quoted name containing a metacharacter — so `\d`, `\dt` and a BI tool's
schema tree resolve the relation and print its columns.

The GENERAL regular-expression operator is not implemented. A pattern this
server cannot answer — an unanchored or wildcard one such as `\dt sec5*`, or
the `!~` / `~*` operators — is refused with SQLSTATE `0A000` naming the form
that works, rather than answered with the predicate silently ignored. List
relations with `SHOW TABLES`, or spell the lookup `relname = 'name'`.

With auth enabled the emulation renders only the relations the identity may
read, so `\d` on a denied relation answers nothing and `psql` reports "Did not
find any relation named …".

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

A scalar subquery's **result is its SELECT list**, and it declares that list's
type. `(SELECT MAX(bigint_col) FROM t)` is `bigint` on the wire and in
arithmetic above it, exactly as the plain `SELECT MAX(bigint_col) FROM t` is —
the two spellings answer the same value at the same type. An `ORDER BY` term
the SELECT list does not carry is engine scaffolding and is never the value.

A scalar subquery with **no `FROM` clause is its SELECT expression**, evaluated
in the enclosing query's scope — which is what lets it read the row around it:

```sql
SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM users) u   -- the row's x
SELECT SUM((SELECT u.id)) FROM users u                        -- SUM(u.id)
```

The name it reads may be a derived table's or a CTE's own output alias, a
column-alias list's name, or a base table's column, and the item declares the
type that expression declares (`int4` above, so its `SUM` is `bigint`). The
column is published under the name PostgreSQL gives it — the subquery's own
output column, ALIAS INCLUDED: `(SELECT 1 AS zzz)` publishes `zzz`, `(SELECT
u.x)` publishes `x`, `(SELECT 1)` publishes `?column?`.

A subquery WITH a `FROM` clause reads the outer row too — in its `SELECT` list,
its `GROUP BY`, its `ORDER BY`, a join's `ON` condition and its `HAVING` as
well as its `WHERE`:

```sql
-- 1, 2, 3: the outer row's x, per row
SELECT (SELECT u.x FROM other y WHERE y.id = 1) FROM (SELECT id AS x FROM t) u
```

A `GROUP BY` or `ORDER BY` term that substitutes to a plain number is written
as a CAST — `GROUP BY u.id` with the outer row's 1 in it becomes `GROUP BY
CAST(1 AS BIGINT)` — because a bare number in those two clauses is a
select-list POSITION and not a value.

Three shapes are refused (`0A000`) instead. A body that is a SET OPERATION has
no arm in the rebuild, and neither has a `LATERAL` body's outer reference,
which is decorrelated into a join with nothing left to respell. A SELECT item
holding an aggregate beside a nested subquery THAT NAMES THE ENCLOSING QUERY
is the third — the nested subquery's value is the outer row's, so the item has
no type until that row is known. A nested subquery naming nothing outside its
own block answers; its item's declared TYPE is still the engine's default
where PostgreSQL declares `numeric`, which is a separate gap.

A term written `ORDER BY (SELECT 1)` is unaffected by any of this: only an
integer literal WRITTEN IN THE CLAUSE is a select-list position, so a subquery
that evaluates to one is an ordinary constant sort and answers.

An AGGREGATE inside such a subquery whose argument names ONLY the enclosing
query is refused (`0A000`) as well: `SELECT id, (SELECT MAX(u.id) FROM x) FROM
t u`. PostgreSQL puts such an aggregate at the ENCLOSING query's level, where
it is an error because `id` is then ungrouped, and this engine has no lowering
for an aggregate level above the block it is written in. An aggregate that
also names a column of its own block (`SUM(x.visits + u.id)`) is that block's
and answers.

A clause that can make such a block produce no row keeps its own meaning:
`(SELECT u.id WHERE 1=0)`, `(SELECT u.id LIMIT 0)` and `(SELECT u.id OFFSET 1)`
are `NULL`.

An AGGREGATE or a WINDOW call in such a block belongs to whichever query its
ARGUMENT names, which is PostgreSQL's rule. One that names NOTHING is the
block's own, over the single row the block produces, and answers here:
`(SELECT MAX(1))`, `(SELECT COUNT(*))` and `(SELECT COUNT(*) OVER ())` are all
1 for every outer row. One whose argument names the ENCLOSING query belongs to
that query, and a subquery cannot hold it: `SELECT id, (SELECT MAX((SELECT
u.id)) FROM x) FROM t u` and `SELECT id, (SELECT MAX(u.id) FROM x) FROM t u`
are both refused (`0A000`), where PostgreSQL raises 42803 on the promoted
aggregate. Written in the enclosing query itself the same aggregate answers:
`SELECT MAX((SELECT u.id)) FROM t u` is one row, 3, on PostgreSQL and here.

A `ROW` field path may be an `IN` subquery's SELECT list: `x IN (SELECT c_row.b
FROM t)` answers what PostgreSQL's `x IN (SELECT (c_row).b FROM t)` answers,
including through `NOT IN`, whose result is empty when the field is NULL on any
row (SQL's three-valued rule). The predicate is not lowered to a semi join
there — a join key is a column, and a field path is a value that has to be
extracted — so it runs as a subquery predicate. The field path reads normally
everywhere else too, including as the OUTER key of the same predicate
(`c_row.b IN (SELECT b FROM u)`) and in a literal list.

A subquery used where ONE column is required must return one column. Two
columns is SQLSTATE `42601`:

```sql
SELECT (SELECT id, name FROM t LIMIT 1)      -- 42601 subquery must return only one column
SELECT * FROM u WHERE id IN (SELECT id, name FROM t)  -- 42601 subquery has too many columns
```

The count comes from the subquery's SELECT list, not from what it returns, so
an EMPTY multi-column subquery is refused too — as it is on PostgreSQL, which
decides this during parse analysis.

`EXISTS` reads no value, so `EXISTS (SELECT 1, 2 FROM t)` is legal, whatever
its column count. An `EXISTS` that reads no outer row is a constant for the
whole query: the coordinator evaluates it once and the predicate becomes that
boolean, on the stage DAG as on the single-process path — including under
`OR`, `NOT` and inside a `CASE`. A CORRELATED `EXISTS` that does not become a
semi join is answered by the coordinator's own single-process pipeline instead.

A SCALAR subquery in one arm of `OR`, `NOT` or `CASE` is evaluated LAZILY, as
PostgreSQL evaluates it: an arm the query never reaches is never run, so
`WHERE true OR x > (SELECT id FROM t)` answers even where that subquery would
return several rows, while the same subquery in an arm that IS reached raises
`21000`. On the distributed engine that shape is refused rather than answered.

With auth enabled, a subquery's relations are authorized like any others: an
identity that may not read `flow_logs` is refused `42501` whether it names the
table in the `FROM` clause or only inside a subquery, and a mask or row filter
that applies to the table applies inside the subquery too.

### IN Subqueries

```sql
SELECT * FROM flow_logs
WHERE src_ip IN (SELECT ip_address FROM device_inventory WHERE role = 'server')
```

### Correlated Subqueries

**A name is resolved innermost-first.** An unqualified column inside a subquery
is the subquery's own whenever the subquery's `FROM` supplies it — and that
holds for every kind of relation a `FROM` can name: a base table, a CTE
reference, a derived table or a set-operation arm. Only a name that NO relation
the subquery reads carries is a reference to the enclosing query.

```sql
WITH c AS (SELECT id, bytes_in AS v FROM flow_logs)
SELECT (SELECT MAX(v) FROM c WHERE id < 4000) AS mx FROM devices WHERE id < 2
```

Here `id` is `c`'s, not `devices`'s, so this subquery is not correlated: it is
evaluated once. Qualify the reference (`WHERE devices.id < 4000`) to mean the
enclosing query's column, as PostgreSQL requires when both scopes carry the
name. The rule holds inside a CORRELATED subquery too: in `(SELECT COUNT(*)
FROM c WHERE id < d.id)` the bare `id` is `c`'s and `d.id` is the enclosing
row's.

What a `FROM` item puts in scope is what it publishes. A column-alias list
renames its leading columns and hides the names it replaces, so `FROM (SELECT
id, v FROM t) x(idd, vv)` puts `idd` and `vv` in scope and not `id`. A
QUALIFIED star publishes only the source it names — `SELECT dim.* FROM dim JOIN
t ON …` publishes `dim`'s columns, so a name only `t` carries is still the
enclosing query's — while a bare `*` publishes every `FROM` item.

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

`SELECT *` over a lateral join publishes the OUTER relation's columns first
and the lateral's after them, which is PostgreSQL's order.

**A LATERAL body with NO FROM clause is a projection over the outer row.**
It yields exactly one row per outer row whose columns are functions of that
row, so it is computed as a projection and there is no join to run:

```sql
SELECT l.v FROM users u, LATERAL (SELECT u.id AS v) l          -- 1 | 2 | 3, bigint
SELECT SUM(l.v) FROM users u, LATERAL (SELECT u.id * 2 AS v) l
SELECT l.v FROM users u, LATERAL (SELECT u.id AS v) l WHERE l.v > 1
```

Each item declares what its expression declares — `u.id AS v` is the column's
own type, not text. The body's own `WHERE`, and a written `ON`, are predicates
over the outer row and are applied above the projection.

Such a body is computed as a projection or it is REFUSED (`0A000`) naming the
reason; it is never answered approximately. The classes that are refused are an
aggregate, a `GROUP BY`, a `HAVING`, a window function, `DISTINCT`, an
`ORDER BY`, a `LIMIT`/`OFFSET`, a set operation, a `WITH` clause, a star, a
subquery in the SELECT list — and, on an OUTER join, a body with a `WHERE` or
an `ON` condition that does not fold to true, because those pad rows a
projection cannot manufacture.

A query whose plan contains such a body is answered by the single-process
engine: a table-less SELECT has no distributed stage, so the coordinator runs
the whole query in process.

An UNGROUPED aggregate over an empty input still yields one row, so an outer
row the lateral matches nothing for **survives even an inner join**, with
`COUNT` reading 0 and every other aggregate NULL — the order with no line
items above comes back at `item_count = 0, total_amount = NULL`. A lateral
subquery that writes its own `GROUP BY` follows the ordinary rule instead: an
empty input yields no row, and the outer row is dropped by an inner join and
NULL-padded by a `LEFT JOIN LATERAL`.

**The join's own `ON` still decides.** The lateral produces its row first —
including the defaulted one for an outer row it matched nothing for — and the
`ON` is applied to that pair afterwards, so on an INNER `JOIN LATERAL`,
`ON s.item_count > 0` drops the empty order and `ON s.item_count = 0` keeps
only the empty ones. Writing `ON true` is what makes the empty-input row
unconditional; it is not the only supported spelling.

A `RIGHT` or `FULL` join written after the lateral turns the empty-input rule
off for that query: such a join can produce rows in which the lateral's
columns are NULL for a reason of its own, and those NULLs are left alone
rather than defaulted. The rest of the join behaves normally. A `LEFT` join
after the lateral is unaffected.

On an OUTER `LEFT JOIN LATERAL` a written `ON` is applied by the join, which
is BEFORE the empty-input value exists, and PostgreSQL applies it after. Where
the condition REJECTS the empty-input row the two orders agree — the outer row
is kept with the lateral's columns NULL either way — and the query answers, so
`LEFT JOIN LATERAL (…) s ON s.item_count > 0` is unaffected. Where it would
ACCEPT that row they do not agree, and rather than print NULL where PostgreSQL
prints a value the condition is REFUSED (`0A000`) in one sentence:

```sql
-- refused: PostgreSQL keeps the empty order at item_count = 0
SELECT o.customer, s.item_count FROM orders o
LEFT JOIN LATERAL (SELECT COUNT(*) AS item_count FROM line_items
                   WHERE order_id = o.id) s ON s.item_count = 0

-- answers: a WHERE is evaluated after the default
SELECT o.customer, s.item_count FROM orders o
LEFT JOIN LATERAL (SELECT COUNT(*) AS item_count FROM line_items
                   WHERE order_id = o.id) s ON true
WHERE s.item_count = 0
```

A condition over an OUTER column (`ON o.id > 1`) is refused for the same
reason: whether the empty-input row passes depends on the outer row, so
nothing can be proven about it. Move the condition to `WHERE`, or write the
INNER spelling — `JOIN LATERAL (…) s ON s.item_count = 0` answers, because
there the condition IS a filter and is applied after the default.

A condition that is a constant TRUE is not a condition at all: `ON 1 = 1` and
`ON 2 > 1` behave exactly as `ON true` does, and none of the three is
refused.

An ungrouped aggregate inside a lateral keeps PostgreSQL's empty-input value
for an outer row the lateral matches nothing for, and the value is the SELECT
ITEM's own over an empty input: `COUNT(*)` reads 0 there, `COUNT(*) + 1` reads
1, `COUNT(*) = 0` reads true, `COALESCE(SUM(x), 0)` reads 0, and
`CASE WHEN COUNT(*) > 5 THEN 1 END` and `SUM(x)` read NULL. It applies to that
row alone — a matched row whose value is legitimately NULL keeps its NULL, so
`NULLIF(COUNT(*), 2)` is NULL for a row that counted 2 — and it reaches every
spelling: `SELECT *`, `SELECT o.*, s.n`, a derived table's star, a CTE's star,
a `WHERE` over the column, a scalar subquery and an `EXISTS`. The value is the
item evaluated at the engine's own types, so it is right for every column type
— `CAST(COUNT(*) AS VARCHAR)` reads `'0'`, `CAST(COUNT(*) AS DECIMAL(9,2))`
reads `0.00`, `ARRAY[COUNT(*)]` reads `{0}` — and not only for numbers.

A lateral may PUBLISH its correlation key in its own `SELECT` list, once or
several times, under its own name or under aliases; every one of those is a
column of the answer under the name the query gave it. `JOIN LATERAL (SELECT
order_id, order_id AS oid, COUNT(*) AS n … GROUP BY order_id) s` publishes
`order_id`, `oid` and `n` on every execution path.

The same holds for an ordinary DERIVED TABLE or CTE under a `SELECT *`:
`SELECT * FROM orders o JOIN (SELECT order_id, order_id AS oid FROM items) s ON
s.order_id = o.id` publishes `order_id` and `oid`, a renamed column is
published under its alias, and an alias over an aggregate publishes the alias
alone. Every execution path answers the same relation.

Two shapes still run on the coordinator rather than across the workers: a bare
aggregate alias beside a computed sibling (`SUM(x) AS sa, SUM(x) * 1 AS sb`),
and a WINDOW function inside the subquery.

A subquery with its own `ORDER BY` runs there too when it sorts by a column it
does NOT select — `(SELECT order_id, product FROM items ORDER BY amount LIMIT
3)` — with or without a `LIMIT`. Sorting by a column the subquery does select
(`… ORDER BY product LIMIT 3`) stays distributed.

All of these answer the same rows either way; naming the columns instead of
writing `*` keeps them distributed.

A subquery item's TYPE never decides where the query runs: a container
(`ARRAY[amount]`, `ARRAY[COUNT(*)]`), a scalar subquery, an all-NULL `CASE` and
a bare `NULL` are published like any other column, and are declared the way the
same item is declared outside a subquery.

One shape is not covered and is wrong on the distributed path in a way this
release does not change: `SELECT *` over a subquery whose own body is a JOIN
publishes that join's columns rather than the subquery's `SELECT` list. Name
the columns.

The correlated equality is turned into a join, and a join needs the inner
value as a column, so the planner materializes one under a name from its
reserved namespace (`__key_0`). It is dropped again at the join, so it is not
in a `SELECT *` result, not in a derived table's or a CTE's star over the
join, and not in the wire's `RowDescription`: `SELECT *` over a lateral
returns the columns PostgreSQL returns.

A QUALIFIED star beside another select item publishes its own relation's
columns — `SELECT o.*, s.n` over a lateral join, `SELECT o.*, li.amount` over a
plain one, and `SELECT d.*, x.id` over a derived table, a `d(a, b)` alias list
or a CTE all answer what PostgreSQL answers, from that relation's own OUTPUT
list.

A QUALIFIED star ALONE (`SELECT o.*` with nothing beside it) publishes the same
columns: it names one relation whichever way it is written, over a lateral, a
plain join, a derived table or a CTE.

Where the list is not knowable the star is REFUSED (`0A000`) rather than
guessed: a derived table whose body is itself a BARE star over a join, and a
LATERAL's own star — `SELECT s.*, o.id` and `SELECT s.*` alike, whose output is
a projection this expansion does not enumerate. Name the columns in those two.

PostgreSQL answers BOTH lateral spellings (it publishes the lateral's own
columns), so both refusals are recorded divergences rather than bug reports.
They differ only in what they replaced: `SELECT s.*, o.id` has been refused
throughout, and `SELECT s.*` alone used to publish the whole join.

An inner `SELECT` list that aliases something to the correlation key's own name
answers what PostgreSQL answers. `JOIN LATERAL (SELECT MAX(t.id) AS g …
WHERE t.g = d.k) s` reads `s.g` as the MAX, `JOIN LATERAL (SELECT t.g AS gk,
MAX(t.id) AS g … GROUP BY t.g) s` reads `s.gk` as the key and `s.g` as the MAX,
and `JOIN LATERAL (SELECT amount AS order_id … WHERE order_id = o.id) li` reads
`li.order_id` as the amount — the key the planner adds cannot be shadowed by an
alias, because it does not use a name a query can write.

A WINDOW FUNCTION inside a CORRELATED lateral is refused (`0A000`). The
correlation is evaluated as a join, so the window would be computed over the
whole inner relation instead of over the correlated rows — a different
answer, not a near miss. Add the correlation column to the window's
`PARTITION BY` (`SUM(amount) OVER (PARTITION BY order_id)` beside
`WHERE order_id = o.id`), which answers, or compute the window outside the
lateral. An UNcorrelated lateral's window is unaffected.

A WINDOW FUNCTION inside a CORRELATED SUBQUERY is refused (`0A000`) for a
different reason than the lateral above: a correlated subquery that is not
decorrelated is re-run per outer row by substituting the outer values into its
text and rebuilding the statement, and a window call's `OVER` clause does not
survive that rebuild. It applies to a scalar subquery, an `IN` set and an
`EXISTS`, and to a window in the subquery's own SELECT list, `HAVING`,
`QUALIFY` or set-operation arms — whether or not it reads the outer row. A
window in the body's `ORDER BY`, one inside a derived table the body reads, and
one in a decorrelated `EXISTS` are re-emitted as written and answer normally:

```sql
-- refused: the window reads the outer row
SELECT id, (SELECT 1 + SUM(u.id) OVER () FROM users x WHERE x.id = 1) FROM users u
-- refused: the window is uncorrelated but the subquery is not
SELECT id, (SELECT SUM(x.id) OVER () FROM users x WHERE x.id = u.id) FROM users u
```

Compute the window outside the subquery, or make the subquery uncorrelated — an
UNcorrelated subquery's window is unaffected, and so is a window over the query
itself.

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
| `OHLCV(ts, price, volume)` | The whole bar as a ROW — see below | Skips a row where ANY argument is null |

`DISTINCT` is accepted by every aggregate in this table, not only `COUNT`:
`SUM(DISTINCT x)`, `AVG(DISTINCT x)` and `STRING_AGG(DISTINCT x, ',')` each
de-duplicate their input at the value's exact type before aggregating.
`MIN`/`MAX` are unaffected by de-duplication and answer the same either way.

### OHLCV — a whole bar as one aggregate

`OHLCV(ts, price, volume)` downsamples a stream into one bar per group and
returns it as a `ROW` with six fields, in this order:

```
(open, high, low, close, volume, vwap)
```

```sql
SELECT TIME_BUCKET(INTERVAL '5' MINUTE, ts) AS bucket,
       OHLCV(ts, price, size)              AS bar
FROM   trades
GROUP  BY 1
ORDER  BY 1;
```

To read one field, put the aggregate in a derived table or a CTE and take the
field off the resulting COLUMN:

```sql
SELECT bucket, (bar).open, (bar).close, (bar).vwap
FROM (
  SELECT TIME_BUCKET(INTERVAL '5' MINUTE, ts) AS bucket,
         OHLCV(ts, price, size)              AS bar
  FROM trades GROUP BY 1
) t
ORDER BY bucket;
```

It is general-purpose downsampling — latency, sensor readings, flow bytes —
not a finance-only function; "price" is any measure and "volume" any weight.

**What each field is.** `open` and `close` are the price at the earliest and
the latest instant in the group, `high` and `low` its extremes, `volume` the
sum of the weights, and `vwap` the weighted mean of the price — the exact
quotient of `SUM(price*volume)` by `SUM(volume)`, computed at `AVG(price)`'s
declared scale.

**`vwap` is a mean, not the quotient expression.** It is not digit-identical
to a hand-written `SUM(price*volume)/SUM(volume)` beside it, because that
expression takes ordinary division typing rather than `AVG`'s:

- over a **32-bit** integer price the written-out quotient is INTEGER division,
  which is a different function: `SUM(p*v)` and `SUM(v)` are both `BIGINT`, so
  over the same rows PostgreSQL and wadjet both answer `14` where the bar's
  `vwap` is `14.5833`. Over a **64-bit** integer price they are not —
  `SUM(int8)` is `NUMERIC` on both engines, so the quotient is exact and
  answers `14.583333` here and `14.5833333333333333` on the server (see below);
  a `BIGINT` price is the one integer case where the two spellings are both
  fractional and still differ, and they differ only in scale;
- over DECIMAL columns the quotient carries division's scale (§DECIMAL
  arithmetic) and `vwap` carries `AVG`'s, so the two agree to the LESSER of
  the two scales and the wider one keeps more digits;
- over FLOAT columns the two are the same float64 quotient.

Both are exact to the digits they keep. `AVG`'s scale is a fixed increment on
the input's rather than PostgreSQL's magnitude-dependent division scale, for
the reason recorded in [ADR-0024](adr/0024-decimal-is-finite-fixed-point-with-postgres-result-types.md)
and [ADR-0012](adr/0012-sql-semantics-authority.md): a scale that depends on
the values would let the same query over more rows change the declared type of
its own output column.

**Ties.** Two rows sharing an instant are ordered by PRICE: `open` is the
SMALLER price at the earliest instant and `close` the LARGER price at the
latest. The tiebreak is a value rather than a row order, so the answer does not
depend on how the query was executed.

**NULLs.** A row is skipped when ANY of the three arguments is null, which is
PostgreSQL's rule for a multi-argument aggregate. A group in which every row is
skipped answers a NULL bar — not a bar of nulls — and a field of a null ROW is
null.

**Zero total volume.** A bar whose volumes sum to zero has no weighted mean, so
`vwap` is null and the four prices stand. Writing the quotient out by hand
(`SUM(price*volume)/SUM(volume)`) raises `22012` instead, here as on the
server; the bar answers rather than failing the whole query for one bucket.

**Declared types.** Each field declares what its own spelled-out aggregate
declares:

| field | type |
|---|---|
| `open`, `high`, `low`, `close` | the price column's own type (`DECIMAL(p,s)` keeps its `(p,s)`) |
| `volume` | `SUM(volume)`'s type — `BIGINT` for `INT32`, `NUMERIC` for `INT64`, `DOUBLE PRECISION` for a float |
| `vwap` | `AVG(price)`'s type — `NUMERIC(38, scale+4)` when both inputs are exact, `DOUBLE PRECISION` when either is a float |

The whole ROW declares OID 25 (text) on the wire and renders as a PostgreSQL
composite — `(10.00,21.00,7.00,21.00,24,14.583333)`, with an empty slot for a
null field. See [data-types.md](data-types.md) §ROW.

**Refusals.** `ts` must be a `TIMESTAMP` or a `DATE` and `price`/`volume` must
be numeric; anything else is `42883`. `OHLCV(DISTINCT ...)` is `0A000`, and so
is `OHLCV(...) OVER (...)` — a bar over a moving frame is not computed here,
and a wrong bar is not offered in its place.

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

`ORDER BY` binds the output column even when the SELECT list SWAPS two names,
and on every execution path: `SELECT DISTINCT a AS b, b AS a FROM t ORDER BY a`
orders by the output `a`, whose value is the source `b`, and so does the same
query with a join under it (`… FROM t x JOIN u ON x.id = u.id`). The
distributed path ordered by the source `a` in both spellings before v0.18.58.

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

### ORDER BY over two output columns of the same name

A query may legally publish two output columns under one name, and each
`ORDER BY` term then binds to the item it NAMES rather than to the first
column that answers to the bare name:

```sql
-- Two outputs called `id`; the second key sorts by the SECOND one
WITH cte AS (SELECT id, a FROM t)
SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY a.id, b.id
```

A term that matches no single select item keeps resolving by name, and a term
whose bare name matches TWO items — `ORDER BY id` above — binds to the first
of them. PostgreSQL refuses that one as ambiguous; wadjet answers it.

A `SELECT *` has no select list to take a position from, and there a
**QUALIFIED** term names the column of the relation its qualifier names:

```sql
-- A total order across both references of one CTE: the third key is `b`'s
-- amount inside each of `a`'s peer groups
WITH q AS (SELECT order_id, amount FROM items)
SELECT * FROM q a JOIN q b ON b.order_id = a.order_id
ORDER BY a.order_id, a.amount, b.amount

-- Two relations sharing ONE column name: `o.id` is the orders id, not the
-- items id the join publishes first
SELECT * FROM orders o JOIN items i ON i.order_id = o.id ORDER BY o.id DESC, i.amount
```

Through v0.18.64 the single-process and spilled paths dropped the qualifier
from every sort key, so the two spellings became one key and both bound the
first column of that bare name: the trailing key was never applied and the
rows came back in the join's emission order. Both distributed paths were
already correct.

Through v0.18.68 the distributed paths had the mirror of that defect for a
QUALIFIED term beside a duplicated output name: only the single-process
engines resolved such a term to a select-list POSITION, so on the stage DAG
`SELECT DISTINCT a.order_id AS amount, b.amount FROM items a JOIN items b ON
b.order_id = a.order_id ORDER BY 1, b.amount DESC` — whose output list carries
`amount` twice — bound both keys to the first of them and returned the right
rows in a sequence the client did not ask for. Both spellings now bind by slot
on every path.

### ORDER BY over a set operation

A set operation's result columns are named by its **leftmost arm**, and a
positional `ORDER BY` term addresses those columns:

```sql
-- Two result columns called `amount`; key 2 is the SECOND one
SELECT order_id AS amount, amount FROM items
UNION SELECT id, total FROM orders
ORDER BY 1, 2 DESC

-- A position over star arms counts the expanded columns, in schema order
SELECT * FROM orders UNION SELECT * FROM orders WHERE id < 3 ORDER BY 3 DESC, 1
```

Through v0.18.68 a set operation lost those positions and the result column
names were the only address left. Where two of them were the same string the
consequences were a wrong ORDER on every path (the trailing key silently
unapplied) and, on the distributed paths, a wrong ROW COUNT and wrong VALUES:
the deduplication key bound one column twice, so the `UNION` above answered
three rows whose second column carried the first's values, and the
`INTERSECT`/`EXCEPT` spellings answered none. A position at or past a star in
the leftmost arm was refused outright — `ORDER BY position 3 is out of range
(1-1)`, counting the star as one column — for a query PostgreSQL answers. And a
chain of three or more operations whose result columns repeat a name failed the
query on the distributed paths with a column-type disagreement between the
operation's own output files. All of these answer now.

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
name the columns, or sort by the column itself. Every other relation kind
answers: a base table, an ordinary derived table
(`SELECT * FROM (SELECT * FROM t) x ORDER BY 1`), a CTE, a SET OPERATION
(`SELECT * FROM (SELECT … UNION ALL SELECT …) u ORDER BY 2`) and a `VALUES`
derived table, as do an explicit column list, aliased columns, and a nested
derived table inside one.

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

The prefix rule holds for a DERIVED TABLE and for a **CTE**: `WITH c(kk) AS
(SELECT id, s FROM t)` publishes `kk` AND `s`, and naming `s` — at top level or
inside a subquery over that CTE — resolves to the CTE's own column, as
PostgreSQL resolves it.

A list written over a subquery whose SELECT list contains `*` is applied where
the star's width is knowable, which is every relation kind the expansion
reaches: `WITH c(kk) AS (SELECT * FROM t)` publishes `kk` and the rest of `t`'s
columns under their own names, and an overlong list there is the same `42P10`.
Over a star the expansion DECLINES — a bare `*` over a join — the list cannot
be applied truthfully and the statement is refused with `0A000` naming the
relation; name the columns in the subquery.

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

A `WITH RECURSIVE` column list renames its body's columns BY POSITION, so a
body that publishes two columns under one name is renamed apart by it and both
values survive:

```sql
WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x UNION ALL
                           SELECT a + 1, b * b FROM t WHERE a < 3)
SELECT a, b FROM t ORDER BY a          -- 1,10 | 2,100 | 3,10000
```

A recursive CTE is answered by the single-process engine; the distributed
engine has no stage lowering for one and refuses such a query rather than
answering it differently. The refusal is LOUD but it is not yet a SQLSTATE:
a query that reads a recursive CTE fails on a distributed plan with the stage
builder's own message, `stage scan-0 has no dependencies and no ScanFiles`.
A misspelt constant in the CTE's body is refused before that, at plan time, on
every plan — the seed and the recursive term alike, and whether or not the
outer query reads the CTE at all.

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
column whatever it is called, on every execution path — including the
distributed one, where the ordinal used to fall back to the ambiguous name and
sort by the first column of it. Sorting by an ambiguous NAME (`ORDER BY u`
where two columns are called `u`) is answered here — it binds the first —
where PostgreSQL refuses it with SQLSTATE `42702`. A qualified reference beside
a duplicate output name (`ORDER BY 1, b.amount` where the SELECT list renames
`b.amount`) still binds the first column of that name on the distributed path;
write the ordinal for both keys.

Reading a result by column name cannot represent both columns; the embedded
API exposes the positional form (`QueryResult.Cells`) for exactly this case,
and the wire protocol sends every column regardless.

## Every result declares its columns

A statement that produces a result set produces COLUMNS, whether or not it
produces rows: `SELECT a, b FROM t WHERE false` comes back with `a` and `b`
and no rows, and so does `SELECT *` over a table, over ONE join, over ONE
`LATERAL` — grouped, ungrouped or `LEFT` — over a derived table, over a
non-recursive CTE, over a grouping and over a set operation.

The declaration is a walk over the PLAN, and it describes ONE join by asking
the join operator itself what it publishes. Three shapes are past that bound,
and a query of one of them that returns NO ROWS is REFUSED (`XX000`) rather
than answered with an empty column list:

* `SELECT *` over a join whose own SIDES contain a join — three or more
  relations, and equally TWO or more `LATERAL`s, because each one is a join the
  planner manufactures;
* `SELECT *` over a `LATERAL` whose subquery is an UNGROUPED aggregate
  (`SELECT MAX(x) …`), whose join carries a column the planner minted for
  itself and the declaration will not publish;
* `SELECT *` over a RECURSIVE CTE.

A single `LATERAL` that is not an ungrouped aggregate is NOT among them: a
zero-row `SELECT * FROM t JOIN LATERAL (SELECT c FROM u WHERE u.k = t.k) s ON
true` answers with its columns, and so do the `GROUP BY` and `LEFT JOIN
LATERAL` spellings.

PostgreSQL answers all three refused shapes with a header and zero rows. The
refusal is a wadjet-side bound and it replaces something worse: a result
carrying no columns at all, which psql prints as nothing, which pgJDBC's
`executeQuery` has no metadata for, and which a client cannot tell from a query
that legitimately found nothing. Naming the columns in the SELECT list answers
in every one of the three cases.

Every door answers the same way — the embedded API, the PostgreSQL wire
protocol, `POST /v1/queries`, `POST /v1/queries/async` with
`GET /v1/queries/{id}/results`, and gRPC. The asynchronous pair describes a
zero-row result from the plan, like the others, and reports the refusal on the
result's `error` field with SQLSTATE `XX000`.

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

The same input function applies when a boolean is COMPARED against a quoted
literal, and it applies to a boolean the query COMPUTED, not only to a boolean
column:

```sql
SELECT count(*) FROM t WHERE (NOT flag) = 'yes';        -- the FALSE-flag rows
SELECT count(*) FROM t WHERE (bytes_in > 1) = 'on';     -- a comparison is a boolean
SELECT count(*) FROM t WHERE COALESCE(flag, FALSE) = 'f';
SELECT count(*) FROM t WHERE (NOT flag) = 'bogus';      -- ERROR 22P02
```

`NOT`/`AND`/`OR`, every comparison, `IS NULL`, `IS TRUE`, `LIKE`, `IN`,
`BETWEEN`, `IS DISTINCT FROM`, a `CAST(... AS BOOLEAN)`, a `CASE` or `COALESCE`
over booleans, and a function declared boolean are all boolean operands here.
The EMPTY string is included: `= ''` is `22P02`, which is the one unparseable
spelling PostgreSQL names in its own error text.

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

A `SELECT DISTINCT *` over a SELF-JOIN — two references to one table, so one
column name belongs to two relations — is deduplicated by the coordinator on
the distributed paths, and the query's `ORDER BY` is applied after that dedup.
A result larger than one batch (2048 rows) is assembled there too, and before
this release that assembly skipped a NULL row instead of writing it: the next
non-null value of a text, bytes, IPv6, CIDR, UUID or nested column came back as
every value above it, run together. 116 of 5000 values in one column of one such
query were affected, with the rows and their order right.
Before this release that re-sort dropped every key whose written spelling the
merged result did not carry EXACTLY, so `SELECT DISTINCT * FROM t a JOIN t b ON
b.k = a.k ORDER BY a.k, a.amount, b.amount` came back ordered by `b.amount`
alone on the distributed paths: the right rows in the wrong order, and under an
`OFFSET` a different page. A qualified key binds the relation its qualifier
names on every path now, and an ordering the merge cannot apply is an error
rather than a silently different order.

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

`INT32`, `PORT`, `PROTOCOL` and `FLOAT32` convert and declare their own type.
`INT32` is a second spelling of `int4` and lands on the carrier every integer
spelling lands on (`bigint` on the wire); `PORT` and `PROTOCOL` declare
`integer` (OID 23), the same OID their columns declare; `FLOAT32` is `REAL`.
Those four, and `DATE` from an integer day count, are stored in a signed 32-bit
field, so a value with no room in one is `22003 integer out of range` — see the
table below and `docs/data-types.md`. `PORT` and `PROTOCOL` also read TEXT
(`'443'::PORT` is 443, `'abc'::PORT` is `22P02`).

The remaining type names are **accepted destinations this engine does not
convert to**: `BYTES`, `IPV4`, `IPV6`, `CIDR`, `MAC`, `DURATION`, `ARRAY`,
`MAP` and `VECTOR(n)` hand the operand's text back under a `text` declaration
(OID 25), and `ROW` is a syntax error. The four network ones are a recorded
divergence — ADR-0012's list, "a CAST to a NETWORK type does not read its
text", held fail-on-agree by `expr.TestCastToANetworkTypeStillPassesThrough`.
A name that answers to no type at all is `42704`, not a text column (#652).

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
| `CAST(x AS INT32 \| PORT \| PROTOCOL \| DATE)` past the int4 range those four are stored in | `22003` | integer out of range |
| `CAST(1e40 AS FLOAT32)` — the same type `REAL` names | `22003` | … is out of range for type real |
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

That table is the whole set. **Any other aggregate in the window position is
refused, SQLSTATE `0A000`**, and the refusal names the supported set:

```
wadjet=> SELECT STDDEV(x) OVER (ORDER BY id) FROM t;
ERROR:  STDDEV is not supported as a window function; the window functions are
        ROW_NUMBER, RANK, DENSE_RANK, SUM, COUNT, AVG, MIN, MAX, LAG, LEAD,
        FIRST_VALUE, LAST_VALUE, NTILE, PERCENT_RANK, CUME_DIST, NTH_VALUE
```

PostgreSQL answers every one of those — there, any aggregate may be used as a
window function — so this is a loud refusal of valid input rather than a
semantic difference, and it is in ADR-0012's divergence list. The alternative
was worse: the plan used to fall through to `ROW_NUMBER` with an output vector
typed for the function nobody recognized, and 23 of the 28 aggregates crashed
the query with an internal error instead of saying what was wrong.

The workaround is the grouped spelling with a join back, or a self-join on the
frame's bounds.

### The type a window aggregate answers

`SUM(x) OVER (…)` declares and answers exactly what `SUM(x) … GROUP BY`
declares and answers — one question written two ways:

| input | `SUM` | `AVG` |
|---|---|---|
| `INT32`, `PORT`, `PROTOCOL` | `BIGINT` | `NUMERIC(38,4)` |
| `INT64` | `NUMERIC(38,0)` | `NUMERIC(38,4)` |
| `DECIMAL(p,s)` | `DECIMAL(38,s)` | `DECIMAL(38,s+4)` |
| `FLOAT32` / `FLOAT64` | `DOUBLE PRECISION` \* | `DOUBLE PRECISION` |
| `DATE`, `TIMESTAMP`, `DURATION` | `DOUBLE PRECISION` | `DOUBLE PRECISION` |

\* The `FLOAT32` cell is the one row of this table that is not PostgreSQL's:
`sum(real)` is `real` there. It is a known divergence, recorded in
[ADR-0012](adr/0012-sql-semantics-authority.md) and deferred — settling it
means moving the grouped `FLOAT32` accumulator as well, which is a separate
change. Every other row matches PostgreSQL.

The integer and decimal rows accumulate exactly, in every frame form —
`OVER ()`, `PARTITION BY`, a running `ORDER BY` frame, a sliding `ROWS`/`RANGE`
frame — and a total the declared type cannot hold is SQLSTATE `22003`, never a
wrapped number. `MIN`, `MAX` and the value functions (`LAG`, `LEAD`,
`FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`) answer their input column's own type;
`COUNT`, `ROW_NUMBER`, `RANK`, `DENSE_RANK` and `NTILE` answer `BIGINT`.

A **computed** argument over `INT32`/`INT64` follows the same table, read from
the ARGUMENT's own width rather than from the column the expression is
materialized into — the same rule in the windowed and the `GROUP BY` spelling,
because they are one question written twice. `SUM(bigint_col * 2)` and
`SUM(ABS(bigint_col))` are `NUMERIC` in both; `SUM(CASE WHEN … THEN 1 ELSE 0
END)`, `SUM(int_col * 1)`, `SUM(-int_col)` and `SUM(MOD(int_col, 10))` are
`BIGINT` in both, because nothing in them is wider than `int4`. One `int8`
operand anywhere in the expression makes the whole of it `NUMERIC`, which is
PostgreSQL's answer as well: `SUM(CASE WHEN … THEN bigint_col ELSE 0 END)` is
`NUMERIC`.

A **FUNCTION answers in the width PostgreSQL declares for it**, which is not
the same as the width this engine carries its result in: every integer here
computes in a 64-bit box, so `REGEXP_COUNT`, whose PostgreSQL result is
`integer`, is carried exactly as `BIT_COUNT`, whose PostgreSQL result is
`bigint`, is. `SUM(REGEXP_COUNT(s,'a'))`, `SUM(LENGTH(s))`,
`SUM(PREFIX_LENGTH(c))` and `SUM(PAYLOAD_LENGTH(p))` are `BIGINT`;
`SUM(BIT_COUNT(x))`, `SUM(FROM_HEX(s))`, `SUM(PARSE_BYTES(s))` and
`SUM(HTTP_CONTENT_LENGTH(p))` are `NUMERIC`. A function PostgreSQL does not
have takes the width that holds its whole domain — `IP_TTL` is a byte and is
`BIGINT` summed, `IP_DIFF` reaches 2^32 and is `NUMERIC`.

The **bitwise family follows its operands**, exactly as arithmetic does:
`SUM(BITWISE_AND(int_col, 18))` is `BIGINT` and
`SUM(BITWISE_AND(bigint_col, 18))` is `NUMERIC`, which are PostgreSQL's
`sum(int_col & 18)` and `sum(bigint_col & 18)`. A shift follows the value it
shifts, not the count.

The width **survives a derived table, a CTE, a set operation and a window
slot**. It is part of the column's declaration, the way a `DECIMAL`'s
precision and scale are, so `SELECT SUM(v) FROM (SELECT BITWISE_AND(id,3) AS v
FROM flows) s` is `BIGINT` exactly as the direct `SUM(BITWISE_AND(id,3))` is,
and `SUM(v) OVER ()` over the same derived table agrees with both. A set
operation takes the WIDER arm, as PostgreSQL's common-type rule does: a
`UNION ALL` of two `int4` arms is `BIGINT` summed and one with a `bigint` arm
is `NUMERIC`. `MIN` and `MAX` hand back a value their ARGUMENT held and keep
its width — a column's or a computed expression's alike, so
`SUM(MIN(BITWISE_AND(id,3)))` is `BIGINT` and
`SUM(MIN(BITWISE_AND(bigint_col,18)))` is `NUMERIC`, grouped and `OVER ()` —
while `SUM` and `COUNT` answer `bigint`, so a `SUM` over a `SUM` is
`NUMERIC`.

A **scalar subquery's column keeps the subquery's own declaration** through a
derived table, a CTE or a window slot, so `SELECT SUM(v) FROM (SELECT (SELECT
a & 3 FROM u) AS v FROM t) s` is `BIGINT` and the `bigint` form is `NUMERIC`,
as PostgreSQL declares them. Written DIRECTLY as an aggregate's argument —
`SUM((SELECT …))` — it is still `DOUBLE PRECISION`.

A **`CAST` answers in its target's width**, whatever the operand's was, as
PostgreSQL does: `SUM(bigint_col::BIGINT)` and `SUM(int_col::BIGINT)` are
`NUMERIC`, `SUM(bigint_col::INTEGER)` is `BIGINT`, and a cast to a
non-integer type leaves the table entirely — `SUM(x::NUMERIC)` is `NUMERIC`
and `SUM(x::DOUBLE PRECISION)` is `DOUBLE PRECISION`.

`PORT` and `PROTOCOL` take this table only as a **bare** argument:
`SUM(port_col)` is `BIGINT` and `AVG(port_col)` is `NUMERIC(38,4)`. Under
arithmetic — `SUM(port_col * 1)`, `SUM(ABS(proto_col))` — both spellings
answer `DOUBLE PRECISION`, because network arithmetic is evaluated on the
float path. That is a recorded gap, not a rule
([ADR-0012](adr/0012-sql-semantics-authority.md), `#953`); the two spellings
agree with each other, and PostgreSQL has neither type.

**`DISTINCT` inside a window call is refused** with SQLSTATE `0A000`,
`DISTINCT is not implemented for window functions`, which is PostgreSQL's own
code and message: `SUM(DISTINCT x) OVER ()` has no answer there and previously
returned the non-distinct total here. `SUM(DISTINCT x)` without `OVER` is
unaffected, and so is `SELECT DISTINCT` beside a window function.

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

A QUALIFIED key names the relation it writes, including where two join arms
publish the same bare name: `SUM(y.w) OVER (PARTITION BY x.w)` over arms that
both publish `w` partitions on `x`'s column. A BARE key over such a pair is
ambiguous — PostgreSQL refuses it with `42702 column reference "w" is
ambiguous` and wadjet answers it by binding one of them (ADR-0012). Qualify the
key.

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

Wadjet includes 379 built-in scalar functions across several categories.

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER(protocol)` |
| `LOWER(s)` | Lowercase | `LOWER(hostname)` |
| `CONCAT(a, b, ...)` | Concatenate strings; **NULL arguments are ignored** (all-NULL gives `''`, never NULL) — `\|\|` propagates NULL instead | `CONCAT(src_ip, ':', src_port)` |
| `LENGTH(s)` / `LEN(s)` | String length in **characters**, the synonym of `CHAR_LENGTH` (use `OCTET_LENGTH` for bytes). Over a `BYTES` argument it is the **byte** count, as `length(bytea)` is on the server — for a bare column and a derived value alike | `LENGTH(message)` |
| `SUBSTR(s, start, len)` / `SUBSTRING` | Extract substring; `start` and `len` count **characters**, and a negative `len` is SQLSTATE 22011. Over a `BYTES` argument it returns `BYTES` and counts **bytes** | `SUBSTR(message, 1, 50)` |
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

### Version String Functions

A software version is a `STRING` in every table that holds one — a package
inventory, an agent or firmware roster, a container image tag, a CVE feed's
"affected versions" column. Ordering or filtering it **as text is wrong in a
way that looks right**: `'1.10.0' < '1.2.3'` and `'1.2.10' < '1.2.3'` are both
true as bytes and both false as versions. These functions give the string the
ordering [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)
gives it. There is no new type — a version is text, and `SEMVER_SORT_KEY` is
the text whose **byte order is precedence**, so `ORDER BY`, `MIN`/`MAX`, a
distributed merge and a spilled sort all order versions correctly with nothing
new to learn.

Nothing here is network-specific.

| Function | Description | Example |
|----------|-------------|---------|
| `SEMVER_PARSE(s)` | Parse once into ROW `(major bigint, minor bigint, patch bigint, prerelease text, build text)`; invalid strings return NULL | `(SEMVER_PARSE('1.2.3')).major` → `1` |
| `SEMVER_PARSE_STRICT(s)` | The same ROW, but invalid strings raise `22023` naming the input | `SEMVER_PARSE_STRICT('latest')` → error |
| `SEMVER_VALID(s)` | Whether the string is a version | `SEMVER_VALID('1.2.3')` → `true` |
| `SEMVER_MAJOR(s)` | The major number, `BIGINT` | `SEMVER_MAJOR('v1.2.3')` → `1` |
| `SEMVER_MINOR(s)` | The minor number, `BIGINT` | `SEMVER_MINOR('1.2.3')` → `2` |
| `SEMVER_PATCH(s)` | The patch number, `BIGINT` | `SEMVER_PATCH('1.2.3')` → `3` |
| `SEMVER_PRERELEASE(s)` | The pre-release, or `''` when there is none | `SEMVER_PRERELEASE('1.0.0-rc.1')` → `'rc.1'` |
| `SEMVER_BUILD(s)` | The build metadata, or `''` when there is none | `SEMVER_BUILD('1.0.0+exp.5114f85')` → `'exp.5114f85'` |
| `SEMVER_CMP(a, b)` | `-1`, `0` or `1` by the specification's precedence, `INTEGER` | `SEMVER_CMP('1.2.3','1.10.0')` → `-1` |
| `SEMVER_SORT_KEY(s)` | A `TEXT` key whose byte order equals precedence | `ORDER BY SEMVER_SORT_KEY(v)` |
| `SEMVER_NORMALIZE(s)` | The canonical spelling (the `v` prefix removed) | `SEMVER_NORMALIZE('v1.2.3')` → `'1.2.3'` |
| `SEMVER_NORMALIZE_STRICT(s)` | The same, but SQLSTATE `22023` instead of NULL | `SEMVER_NORMALIZE_STRICT('latest')` → error |
| `SEMVER_SATISFIES(v, range)` | Whether the version is in a node-semver range | `SEMVER_SATISFIES('1.5.0','^1.2.3')` → `true` |

**Data is lenient.** A string that is not a version is **NULL**, never an
error, from every function above except the strict forms — a `WHERE` over a
version column collected from the wild must filter rather than abort, and such
a column always holds junk. `SEMVER_NORMALIZE_STRICT` is the loud twin for a
job that asserts rather than filters: the same canonical string, and SQLSTATE
`22023` naming the value when it is not a version. `SEMVER_PARSE_STRICT` applies the same refusal to the ROW form. A NULL argument
is NULL everywhere, both strict forms included.

**`SEMVER_PRERELEASE` and `SEMVER_BUILD` answer `''`, not NULL, for a version
that has none.** A release *is* a version without a pre-release; returning NULL
for it would make `SEMVER_PRERELEASE(v) IS NULL` true for both `1.0.0` and
`not-a-version`. NULL means "not a version" throughout the family.

#### What is a version here

The specification's grammar, with **one** documented concession: a leading `v`
or `V` is accepted, because every real dataset has it (git tags, GitHub
releases, Go module versions) and `v1.2.3` and `1.2.3` are the same version.
Everything else is the grammar as written:

- all three components are required — `1.2` is not a version;
- **no leading zeroes** in the core or in a numeric pre-release identifier, so
  `01.2.3` and `1.0.0-01` are NULL (otherwise two spellings of one version
  would make `GROUP BY` and `DISTINCT` answer two different numbers);
- identifiers are `[0-9A-Za-z-]` and may not be empty, so `1.0.0-alpha..1`,
  `1.0.0-` and `1.0.0-alpha_1` are NULL;
- surrounding whitespace is not trimmed — `' 1.2.3'` is NULL;
- build identifiers **may** carry leading zeroes (`1.0.0+001`), because build
  metadata has no precedence at all;
- a numeric identifier past `BIGINT` (`99999999999999999999.0.0`) is NULL. The
  specification sets no upper bound; this engine's is `int64`, which is also
  the width `SEMVER_MAJOR`/`MINOR`/`PATCH` declare. Everything below that bound
  is exact, including the band from `2^53` to `2^63-1` that node-semver — being
  JavaScript — refuses outright, so `9007199254740993.0.0` is its own version
  here and compares exactly.

#### Ordering

`SEMVER_CMP` and `SEMVER_SORT_KEY` implement §11 exactly: the three numbers
compare numerically; a version **with** a pre-release ranks below the same core
**without** one; pre-release identifiers compare left to right, numerically
when both are all digits, by ASCII bytes otherwise, with a numeric identifier
always below an alphanumeric one and a longer list winning when every preceding
identifier is equal; **build metadata is ignored**.

```sql
-- The specification's own example, in order:
--   1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-alpha.beta < 1.0.0-beta
--             < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0
SELECT version FROM packages ORDER BY SEMVER_SORT_KEY(version);

-- The newest version per package, on one path or a hundred:
SELECT name, MAX(SEMVER_SORT_KEY(version)) AS newest_key
  FROM packages GROUP BY name;
```

`SEMVER_SORT_KEY` is not a canonical form and is not reversible:
`1.0.0+a` and `1.0.0+b` produce the **same** key, because they have the same
precedence. `SEMVER_NORMALIZE` is the canonical spelling. The key is printable
ASCII, 58 characters for a release and longer for a pre-release; its exact
bytes are an implementation detail and only its ORDER is a contract.

A predicate over `SEMVER_SORT_KEY(v)` is evaluated as an expression: it is not
pushed into the scan and **never prunes a row group**, because a row group's
recorded minimum and maximum are the column's own text bounds and say nothing
about the key's.

#### Ranges — `SEMVER_SATISFIES(version, range)`

The Semantic Versioning specification defines precedence and **no range
syntax**. The syntax people write is node-semver's — a `package.json`
dependency, a Dependabot alert, a Renovate rule, an advisory's affected-version
field — and that published grammar is what this function implements:

| Spelling | Means |
|---|---|
| `*`, `x`, `X` | any release |
| `1.2.3` | exactly `1.2.3` |
| `>=1.2.3`, `>1.2.3`, `<=1.2.3`, `<1.2.3`, `=1.2.3` | the comparison |
| `1.2.3 - 2.3.4` | `>=1.2.3 <=2.3.4` |
| `1.2 - 2.3.4` | `>=1.2.0 <=2.3.4` |
| `1.2.3 - 2.3` | `>=1.2.3 <2.4.0-0` |
| `1.2.3 - 2` | `>=1.2.3 <3.0.0-0` |
| `1`, `1.x`, `1.X`, `1.*` | `>=1.0.0 <2.0.0-0` |
| `1.2`, `1.2.x`, `1.2.*` | `>=1.2.0 <1.3.0-0` |
| `~1.2.3` | `>=1.2.3 <1.3.0-0` |
| `~1.2` | `>=1.2.0 <1.3.0-0` |
| `~1` | `>=1.0.0 <2.0.0-0` |
| `~1.2.3-beta.2` | `>=1.2.3-beta.2 <1.3.0-0` |
| `^1.2.3` | `>=1.2.3 <2.0.0-0` |
| `^0.2.3` | `>=0.2.3 <0.3.0-0` |
| `^0.0.3` | `>=0.0.3 <0.0.4-0` |
| `^1.2.x` | `>=1.2.0 <2.0.0-0` |
| `^0.0.x`, `^0.0` | `>=0.0.0 <0.1.0-0` |
| `^0.x` | `>=0.0.0 <1.0.0-0` |
| `A B` | both — whitespace is intersection |
| `A \|\| B` | either — union |

`~>` is accepted for `~`, a space may separate an operator from its version
(`>= 1.2.3`), and the leading `v` concession applies inside a range too.

**Pre-releases.** A version with a pre-release tag satisfies a range only if
some comparator of the same alternative names the same `major.minor.patch`
*and* itself carries a pre-release. So `1.2.3-beta` does **not** satisfy
`^1.2.3`, `3.4.5-alpha.9` does **not** satisfy `>1.2.3-alpha.3` even though it
is greater, `3.4.5` does, and `1.0.0-beta` does not satisfy `*`. That is
node-semver's published rule; there is no `includePrerelease` option here — a
query that wants pre-releases writes a comparator that has one.

**A range this grammar does not know is SQLSTATE `22023` naming it, never a
silent `false`.** A range is the query author's own text, so a spelling nobody
implements is a property of the query, and answering `false` for it drops every
row the author meant to select and looks exactly like an empty table. Three
things are refused that node-semver accepts:

- the **empty** range — node reads `''` as `*`; here `*` is the explicit
  spelling and an empty one is a query that meant something and did not say it;
- a pre-release or build on a **partial** version (`1.2.x-beta`) — node's
  regex captures it and then ignores it;
- everything else off the grammar: `>=` with nothing after it, `^^1.0.0`,
  `1.2.3 -`, a leading zero, a number past `BIGINT`, `latest`.

**A bound at the top of the domain is saturated, not wrapped.** Every spelling
above except an exact version closes its band by raising one component by one,
and a component is accepted all the way to `BIGINT`'s maximum — so
`^9223372036854775807.0.0` asks for a bound one past a number this engine can
spell. That bound is rewritten to the comparator it means over the versions
that exist rather than wrapping into a negative one: `<X.(Y+1).0-0` becomes
`<=X.Y.9223372036854775807` and `>=X.(Y+1).0` becomes
`>X.Y.9223372036854775807`. Nothing is lost by the rewrite, because no version
exists between them. So `^9223372036854775807.0.0` still names the versions
whose major is the maximum, `>9223372036854775807.x` matches nothing — there is
nothing above the top of the domain — and `1.9223372036854775807.x` still
excludes `2.0.0`. node-semver refuses any component past `2^53-1` instead; this
engine accepts the version, so it accepts the range over it.

**The trivial lower bound `>=0.0.0` is dropped from every comparator set**,
because node-semver drops it — its README publishes the identity `*` :=
`>=0.0.0`, and its parser deletes a comparator whose text is exactly that. It
is not housekeeping: `>=0.0.0` is false for precisely the **pre-releases of
`0.0.0`**, which sort below `0.0.0`, so keeping it made a range whose author
opted IN to those pre-releases name nothing at all. Go module pseudo-versions
are literally `v0.0.0-<timestamp>-<hash>`, so over a `go.sum` or an SBOM

```sql
-- every pseudo-version built before 2022
SELECT module, version FROM deps
 WHERE SEMVER_SATISFIES(version, '>=0.0.0 <0.0.0-20220101000000-000000000000');
```

answers rows, where the arithmetic reading answered none. `~0`, `^0.0`,
`^0.0.x`, `^0.x`, `0 - X` and `>=0` all reach the same deletion. Only the
**opt-in** shape changes: a range that names no pre-release still admits none,
so `SEMVER_SATISFIES('0.0.0-alpha','*')` and
`SEMVER_SATISFIES('0.0.0-alpha','>=0.0.0')` are both still `false`.
`>=0.0.0-0` is a different comparator and is kept.

The deletion is applied to the comparator **after** the leading-`v`
concession, so `>=v0.0.0` is the same comparator as `>=0.0.0` and is dropped
too. node-semver deletes by matching the comparator's text before it
normalizes the prefix away, so it keeps that one spelling:
`SEMVER_SATISFIES('0.0.0-alpha','>=v0.0.0 <=0.0.0-alpha')` is `true` here and
`false` there. Since `v1.2.3` and `1.2.3` are the same version everywhere else
in this family, they are the same comparator here as well.

The **version** argument keeps the family's lenient rule: NULL for a string
that is not a version. A NULL range is a NULL operand and answers NULL; a
malformed one is the refusal above, and the range is read **first**, so the
refusal does not depend on what the version argument holds.

A range written as a **constant is refused before any row**:
`WHERE id < 0 AND SEMVER_SATISFIES(v, '^^1.0')` is `22023`, not zero rows,
because the range is the query's own text and PostgreSQL raises for a
malformed literal under `WHERE false` too. Except for the coverage gaps the
[TCP flag section](#tcp-flag-functions) lists — they are the same two layers —
the refusal is the same in every expression position and on both plans. A
range that is a **column or an expression** is not a constant and keeps the
per-row refusal, where it first exists; a NULL literal is a NULL operand and is
left alone.

```sql
-- every deployed agent still on a vulnerable build
SELECT host, agent_version
  FROM agents
 WHERE SEMVER_SATISFIES(agent_version, '>=1.2.0 <1.2.7 || >=1.3.0 <1.3.2');
```

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
| `BITWISE_AND(a, b)` | Bitwise AND. Exact over the full 64-bit pattern; answers BIGINT | `BITWISE_AND(flags, 0xFF)` |
| `BITWISE_OR(a, b)` | Bitwise OR. Answers BIGINT | `BITWISE_OR(flags, 0x01)` |
| `BITWISE_XOR(a, b)` | Bitwise XOR. Answers BIGINT | `BITWISE_XOR(a, b)` |
| `BITWISE_NOT(a)` | Bitwise NOT. Answers BIGINT | `BITWISE_NOT(mask)` |
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
| `TCP_FLAGS_TO_STRING(flags)` | Convert TCP flags bitmask to comma-separated names | `TCP_FLAGS_TO_STRING(0x12)` → `'SYN,ACK'` |
| `HAS_TCP_FLAG(flags, name)` | Test if a TCP flag is set. An unrecognized name is SQLSTATE `22023` | `HAS_TCP_FLAG(flags, 'SYN')` → `true` |
| `TCP_FLAGS_FROM_STRING(names)` | Convert comma-separated flag names to a bitmask. An unrecognized name is SQLSTATE `22023` | `TCP_FLAGS_FROM_STRING('SYN,ACK')` → `18` |
| `IS_TCP_HANDSHAKE(flags)` | Test for SYN-only (connection init) | `IS_TCP_HANDSHAKE(flags)` |
| `IS_TCP_RESET(flags)` | Test for RST flag | `IS_TCP_RESET(flags)` |
| `TCP_SESSION_ID(src, dst, sport, dport, proto)` | Canonical 5-tuple session key | `TCP_SESSION_ID(src_ip, dst_ip, src_port, dst_port, protocol)` |
| `FLOW_DIRECTION(src_ip, dst_ip)` | Classify as inbound/outbound/internal/transit | `FLOW_DIRECTION(src_ip, dst_ip)` → `'outbound'` |

### TCP Flag Functions

A TCP flags column is an integer bitset. These name its bits, so a predicate
says what it means instead of spelling a mask. Names are case-insensitive.
Bits 0-7 are the control bits RFC 9293 §3.1 defines; bit 8 is the bit RFC 3540
named `NS` (Historic per RFC 8311) which the Accurate ECN work reuses as `AE`,
and both spellings are accepted with `AE` as the canonical rendering.

An unrecognized name is SQLSTATE `22023` naming it, and NULL flags give NULL.
Where both apply the NAME wins — `TCP_FLAGS_HAS_ALL(f,'ACKK')` is `22023` on a
row whose `f` is NULL — so whether a typo is an error never depends on the
data; a NULL *name* is a NULL mask and gives NULL.

A name written as a **constant is refused before any row**, and an empty name
list with it: `WHERE id < 0 AND TCP_FLAGS_HAS_ALL(f,'XX')` is `22023`, not zero
rows, because the names are the mask OPERAND and its spelling is a property of
the query. That is PostgreSQL's own rule for the arithmetic this family is
named for — `SELECT 'x'::int FROM t WHERE false` raises there too.

Except for the coverage gaps listed below, the refusal is the same in
**every expression position and on both plans** — a
`SELECT` item, `WHERE`, `JOIN ... ON`, `HAVING`, a `GROUP BY` key, an
`ORDER BY` key, a set-operation arm, a projection above a `GROUP BY`, a window
function's argument, `PARTITION BY` or `ORDER BY` terms, a
derived table's or a CTE's body, an `EXISTS` / `IN` / scalar subquery, and an
`UPDATE` or `DELETE` predicate — whether the query runs in one process or as a
distributed stage DAG. A subquery written in any of those positions is walked
the same way, including one nested inside another subquery.

Recursive CTE bodies include both the seed and recursive UNION arm, even
when the seed is empty or the CTE is unused. The self-reference remains an
open column scope while literal names are validated.

The expression-position guarantee above concerns supported SELECT syntax.
Expression-valued window frame bounds, named windows, LIMIT/OFFSET and
INSERT expressions are rejected by the parser before this check. DML runs
locally; UPDATE/DELETE predicates and UPDATE SET compile before rows, while
a MERGE WHEN clause no row reaches — a MATCHED UPDATE's SET or a MATCHED
`AND … DELETE` condition over an ON that matches nothing — can return `MERGE 0`
without checking its expressions (a NOT MATCHED VALUES tuple and a matching ON
both refuse). An injected policy filter on an empty distributed stage
may likewise never compile. Two further empty-input gaps remain: a shadowing CTE body can bypass the
binder's name map, and an ORDER BY expression on a whole set operation is
not validated. These remain coverage gaps (ADR-0012).

A flag NAME may be any text expression, including a column. A name that is not
a constant is refused where it first exists, per row; only literal names are
folded before execution and pushed into the scan.

| Bit | Value | Name |
|---|---|---|
| 0 | 1 | `FIN` |
| 1 | 2 | `SYN` |
| 2 | 4 | `RST` |
| 3 | 8 | `PSH` |
| 4 | 16 | `ACK` |
| 5 | 32 | `URG` |
| 6 | 64 | `ECE` |
| 7 | 128 | `CWR` |
| 8 | 256 | `AE` (RFC 3540 named this bit `NS`; both accepted) |

| Function | Description | PostgreSQL equivalent | Example |
|----------|-------------|-----------------------|---------|
| `TCP_FLAGS_HAS_ALL(flags, name, ...)` | Every named bit is set | `(flags & mask) = mask` | `TCP_FLAGS_HAS_ALL(tcp_flags, 'SYN', 'ACK')` |
| `TCP_FLAGS_HAS_ANY(flags, name, ...)` | At least one named bit is set | `(flags & mask) <> 0` | `TCP_FLAGS_HAS_ANY(tcp_flags, 'RST', 'FIN')` |
| `TCP_FLAGS_HAS_NONE(flags, name, ...)` | No named bit is set | `(flags & mask) = 0` | `TCP_FLAGS_HAS_NONE(tcp_flags, 'ACK')` |
| `TCP_FLAG_MASK(name, ...)` | The integer mask the names denote | — | `TCP_FLAG_MASK('SYN', 'ACK')` → `18` |
| `TCP_FLAGS(flags)` | The set bits as an ARRAY of names, in header bit order | — | `TCP_FLAGS(18)` → `[SYN ACK]` (see below) |
| `TCP_FLAGS_TEXT(flags)` | The same names joined with a pipe | — | `TCP_FLAGS_TEXT(18)` → `'SYN\|ACK'` |

`TCP_FLAGS` and `TCP_FLAGS_TEXT` name only the nine bits above; a bit outside
that table is not a TCP flag and is not named. The three predicates are bit
arithmetic over the mask and are unaffected by such a bit.

A `TCP_FLAGS_HAS_*` predicate over a bare `INT32`/`INT64` column with literal
names is evaluated inside the scan, as is the `BITWISE_AND(flags, 18) = 18`
spelling of the same test. On a dictionary-encoded column the mask is evaluated
once per dictionary ENTRY rather than once per row; Wadjet's own writer emits
no dictionary pages, so that applies to Parquet written elsewhere and a table
ingested through Wadjet is evaluated per value. Either way a flags column
referenced only by the filter is never materialized.
`WADJET_FLAG_DICT_PUSHDOWN=0` disables the pushdown. A flag predicate prunes
nothing BEFORE it is evaluated — a min/max range cannot prove a bit, and the
dictionary-probe prune answers only "is this exact value absent" — so no
statistics prune is ever attributed to one. A row group whose values the scan
has already read and found unmatching is still skipped, which is the ordinary
post-evaluation skip and a different counter.

A top-level projection of `TCP_FLAGS(flags)` is declared `TEXT` rather than
`ARRAY`, as every container-returning function's is, and renders Go's slice
form — `[SYN ACK]`, not PostgreSQL's `{SYN,ACK}` (issue #1017). `ELEMENT_AT`
and `ARRAY_LENGTH` read the same value as the array it is, and
`TCP_FLAGS_TEXT` is the function to use for a rendered list.

`TCP_FLAGS_FROM_STRING` reads a comma-separated list. An EMPTY string is a list
of no names and answers `0`; an empty ELEMENT (`'SYN,'`, `'SYN,,ACK'`) is
SQLSTATE `22023` naming the position, because a list that names something and
then names nothing is a slip rather than an empty list.

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
| `TIME_BUCKET(stride, ts)` / `TIME_BUCKET(stride, ts, origin)` | Floor `ts` to the largest multiple of `stride` measured from `origin` (default `1970-01-01`). PostgreSQL's `date_bin`, digit for digit. Returns TIMESTAMP | `TIME_BUCKET(INTERVAL '15' MINUTE, ts)` |
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

#### TIME_BUCKET

`TIME_BUCKET` is PostgreSQL's `date_bin` under the name every time-series
engine spells it, and PostgreSQL is its oracle for every answer:

```sql
SELECT TIME_BUCKET(INTERVAL '5' MINUTE, ts) AS bucket, COUNT(*)
FROM   flows
GROUP  BY 1 ORDER BY 1;
```

- The bucket is the largest `origin + k * stride` that does not exceed `ts`.
  The division **floors toward the past**, so a pre-1970 instant lands in the
  bucket below it, not the one above.
- A boundary belongs to the bucket it **opens**: with a 15-minute stride,
  `15:45:00` buckets to `15:45:00`.
- The origin defaults to `1970-01-01 00:00:00`. The three-argument form takes
  any instant, including one **after** the source — only its offset modulo the
  stride matters.
- A `DATE` source is the day's midnight, which is what PostgreSQL's implicit
  `date` → `timestamp` cast makes it. The result is always TIMESTAMP.
- NULL in any argument is NULL.
- Buckets are **UTC** and exactly `stride` wide. There is no DST here to make
  an hour bucket 3600 seconds one day and 7200 the next — which is also why a
  stride containing MONTHS or YEARS is refused (`0A000`, PostgreSQL's own
  refusal): those units have no fixed width. A stride of zero or less is
  `22008`.
- The stride must be spelled as an `INTERVAL` literal —
  `TIME_BUCKET('15 minutes', ts)` is `42804`. The accepted grammar is the SQL
  parser's single `N unit` pair, so `INTERVAL '1 day 6 hours'` is not
  spellable; write `INTERVAL '30' HOUR`.
- The units an `INTERVAL` literal accepts anywhere in this engine are `YEAR`,
  `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE` and `SECOND`. Any other unit —
  `MILLISECOND`, `MICROSECOND`, `QUARTER`, or a typo — is `0A000`. It used to
  be silently read as DAYS, so `INTERVAL '500' MILLISECOND` was a 500-day
  interval to `TIME_BUCKET` and to date arithmetic alike.
- A literal with **no unit at all** is SECONDS, which is what PostgreSQL means:
  `INTERVAL '2'` is two seconds and `INTERVAL '90'` is ninety. It used to
  default to DAYS.
- The plural keywords `SECONDS`, `MINUTES` and `HOURS` are `42601` in the
  trailing-keyword position (`INTERVAL '30' SECONDS`) — those three words are
  lexer keywords, so the parser never reaches its unit table. `DAYS`, `WEEKS`,
  `MONTHS` and `YEARS` are accepted there, and every plural works inside the
  combined spelling (`INTERVAL '30 seconds'`). PostgreSQL accepts all of them
  in both positions; this is a pre-existing gap, not a rule.
- The value must be a WHOLE NUMBER, so the clock and fractional spellings are
  `42601`: `INTERVAL '02:00:00'`, `INTERVAL '1:30'` and `INTERVAL '2.5'` are
  all refused where PostgreSQL reads them as two hours, ninety minutes and two
  and a half seconds. Write the unit — `INTERVAL '2' HOUR`, `INTERVAL '90'
  MINUTE` — or the combined form, `INTERVAL '90 minutes'`. Sub-second values
  have no spelling here at all (see the unit list above).
- It is a monotone function of its argument, so a range predicate on the same
  column still prunes row groups beside it — measured, not assumed: over a
  five-row-group fixture the same threshold removes the same two row groups
  with and without the bucket projection. Note the separate, pre-existing
  limit it does not change: a range predicate on a `TIMESTAMP` or `DATE`
  column reaches the row-group prune only when the threshold is an
  epoch-millisecond (or epoch-day) literal. `WHERE ts >= TIMESTAMP '…'` is
  evaluated per row today, which costs reads and never rows.

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
| `FROM_HEX(s)` | Convert a hex string to a BIGINT. The WHOLE string must be hexadecimal and fit a signed 64-bit integer; anything else is NULL, as `FROM_BASE(s,16)` answers | `FROM_HEX('ff')` → `255`, `FROM_HEX('12zz')` → NULL |
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

A UDF is **process-global**: it is registered once per server process and every
session resolves it, whatever identity opened that session. Replacing one
changes what other people's queries mean, which is why mutating the registry is
a privileged operation.

### Who may create, replace and drop a function

The registry is guarded by the same permissions as everything else this engine
mutates, and by the same rule on every door — the embedded API, the PostgreSQL
wire protocol and `POST /v1/queries`:

| Statement | Requires |
|---|---|
| `CREATE [OR REPLACE] FUNCTION`, `DROP FUNCTION [IF EXISTS]` | the `write` permission |
| replacing or dropping a function **another** owner created `WITH LOCK` | the `admin` permission |
| `SHOW FUNCTIONS` | any authenticated identity |

`WITH LOCK` records the creating identity as the function's owner:

```sql
CREATE FUNCTION classify_port(p) AS ... WITH LOCK
```

After that only that owner, or an identity holding `admin`, may replace or drop
it; anyone else is refused `42501` (HTTP 403) and the definition is left
exactly as it was — `DROP FUNCTION IF EXISTS` included, which forgives a
function that is *absent* and never one that is present and locked by somebody
else. Whether an identity is an administrator is read from the permissions its
role grants, never from the role's *name*: a role called `admin` whose `allow:`
list is `[read]` is not one, and a role called anything else whose list holds
`admin` is.

`SHOW FUNCTIONS` returns every function's name, parameters, body and owner to
any authenticated caller. That matches PostgreSQL, which shows `pg_proc.prosrc`
and `\sf` to a role with no privileges on the function at all.

With **no auth provider** — the default embedded and CLI use — none of this is
enforced, functions are created with no owner, and `WITH LOCK` records the flag
without an owner to check it against.

> `SHOW FUNCTIONS` is not reachable through the PostgreSQL wire protocol: that
> door answers an unrecognised `SHOW <name>` as a session variable. Use the
> embedded API or the HTTP endpoint.

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

**Background compaction plays by the same rule, in the other direction.** A
compaction reads the manifest, writes a merged replacement, and publishes the
removal of its inputs, the addition of the replacement and the delete-marker
change as a single conditional write. It is refused if the files it merged are
no longer the table's files, or if a `DELETE` committed against those files
after its output was written — so **a committed `DELETE` is never undone by
the compaction it raced**, and two compactors over the same files never leave
you two copies of every row. The refused compactor discards its output and
replans; nothing about the refusal is visible to a client. A compaction whose
publication FAILS leaves the table exactly as it was, fully queryable, rather
than partly rewritten.

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


Fixed-schema scalar ROW results retain their declared fields through projections,
DISTINCT, GROUP BY, set operations and window keys. A computed field can be read
as `(function(arg)).field`; derived-table field grouping uses the same parent
binding as aggregate inputs. See [scalar ROW declarations](internals/scalar-row-declarations.md).
The existing ARRAY/MAP scalar declaration limitation remains tracked by #1017.
