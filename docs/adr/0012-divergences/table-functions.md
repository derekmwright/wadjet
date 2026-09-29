# ADR-0012 divergences: Table functions and readers

File, database and series readers in FROM, and set-returning functions in SELECT: when their columns are known, what they refuse, and COPY's grammar. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**A reader over a regular file publishes its columns at plan time** (P040, P045)
`read_parquet` reads the footer and `read_json`/`read_csv` a sample of the first 100 rows, before the statement binds and after the table-function capability is authorized (ADR-0039 §3, ADR-0034). Column-alias and column-name errors are then plan-time, as over a base table, and aggregates declare from the column's type. A value past the sample that does not fit refuses with COPY's SQLSTATE (22P02, 22003, 22007); a key first seen past the sample is absent. `http(s)` sources, the database connectors (`postgres_scan`, `postgres_query`, `mysql_scan`, `mysql_query`), read-once inputs (FIFO, `/dev/stdin`, sockets) and globs matching one are not read at plan time and keep the first-batch behaviour.

**`read_csv` reads COPY (FORMAT csv)'s grammar with five departures** (P041)
A field is NULL only when empty and unquoted, a quote opens anywhere, whitespace is data, and an unterminated quote or a wrong-width record (short records included) is 22P04, as COPY. Departures: blank lines are skipped, LF/CR/CRLF may mix (a lone CR ends a record since after v0.24.0), a trailing empty field is dropped, `\.` is data, a leading UTF-8 BOM is skipped, and a NUL byte is stored. Bigint, double precision and boolean fields use PostgreSQL's input functions.

**Set-returning functions answer only as a whole SELECT item** (P107)
`unnest`, `generate_subscripts` and `information_schema._pg_expandarray` expand rows by PostgreSQL 10's rule when they are a whole SELECT item (the longest set decides, shorter sets pad with NULL). Elsewhere they are 0A000; `generate_subscripts`' dimension and every table function argument must be constants (ADR-0044).

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT * FROM read_json('/data/f.json') AS f(a, b, c)` | ERROR 42883: no read_json; over a table with too few columns an alias list is 42P10 | ERROR 42P10 at plan time, EXPLAIN too; an unknown column is 42703 at plan time (measured) | 42P10 | kept superset | — · P040 | #1184, #1210, #1230 | — |
| **r2** `SELECT * FROM read_json('https://host/f.json') AS f(a, b, c)` | ERROR 42883 | 42P10/42703 raised at the first batch, not at bind; a reader with no batch is never checked; EXPLAIN does not refuse | 42P10 | documented gap | — · P040 | #1184, #1210, #1230 | — |
| **r3** `SELECT * FROM read_csv('/data/ts.csv') -- a timestamp field past the 100-row sample reads Jan 2 2024` | COPY reads Jan 2 2024 as a timestamp | ERROR 22007 naming reader, file, row, column and types: only the sample's own spellings are read | 22007 | refusal | — · P040 | #1184, #1210, #1230 | — |
| **r4** `SELECT * FROM read_csv('/data/blank.csv') -- a,b / 1,x / (blank) / 2,y,` | COPY (FORMAT csv) is ERROR 22P04 (missing data; extra data after last expected column) | (1,x), (2,y): a blank line is skipped, a trailing empty field is dropped, LF/CR/CRLF may mix (measured) | — | kept superset | — · P041 | #1248, #1259 | — |
| **r5** `SELECT * FROM read_csv('/data/dot.csv') -- a line holding \. followed by more rows` | PostgreSQL 17 COPY ends the input at \. and reads no later row | \. is a data row and later rows are read; a leading UTF-8 byte-order mark is skipped where COPY keeps it | — | value divergence | — · P041 | #1248, #1259 | — |
| **r6** `SELECT * FROM read_csv('/data/nul.csv') -- a field holding a NUL byte` | COPY is ERROR 22021 | the NUL byte is stored | — | kept superset | — · P041 | #1248, #1259 | — |
| **r7** `EXPLAIN SELECT * FROM read_json('/missing.json')` | COPY FROM a missing file is 58P01; EXPLAIN over a missing relation is 42P01 | ERROR 58P01 for the query and its EXPLAIN (measured); 42501 unreadable file, 42809 a directory | 58P01 | refusal | — · P042 | #1245 | — |
| **r8** `EXPLAIN SELECT * FROM read_json('https://host/missing.json')` | ERROR 42883 | prints a plan; the query is refused at its first batch (a 404 is 58P01) | 58P01 | documented gap | — · P042 | #1245 | — |
| **r9** `SELECT * FROM read_json('/data/empty.json') -- a zero-byte file` | a zero-column relation is legal: CREATE TABLE t (); SELECT * FROM t answers zero rows, zero columns | ERROR 0A000 the table function "read_json" published no columns (measured); empty Parquet/CSV declare columns | 0A000 | refusal | — · P043 | #1230 | — |
| **r10** `SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g(a, b)` | (1,1), (2,2): columns generate_series and ordinality | ERROR 42P10: one column only; without the list it answers 1, 2 (measured); unnest adds ordinality | 42P10 | value divergence | — · P044 | #1210 | — |
| **r11** `SELECT SUM(a) FROM read_json('https://host/f.json')` | ERROR 42883 | declares float8; f.* over it is 0A000; over a local file SUM is numeric, MIN bigint (measured) | — | documented gap | — · P045 | #1211, #1230 | — |
| **r12** `SELECT * FROM generate_series(5, 1)` | zero rows of one column; generate_series(1,5,0) is ERROR 22023 | zero rows (measured); step 0 is ERROR 22023, PostgreSQL's sentence; v0.22.0 answered 5..1 | 22023 | documented gap | — · P046 | #1210 | — |
| **r13** `SELECT 1 + unnest(ARRAY[1, 2])` | 2, 3 | ERROR 0A000: an SRF answers only as a whole SELECT item (measured) | 0A000 | refusal | — · P107 | — | — |
| **r14** `SELECT * FROM tn, generate_series(1, tn.n)` | one row per series element per tn row (implicit LATERAL) | ERROR 0A000: argument 2 (tn.n) must be a constant expression (measured) | 0A000 | refusal | — · P107 | — | — |
| **r15** `SELECT unnest(ARRAY[1, 2.5])` | 1, 2.5 declared numeric | 1, 2.5 declared double precision (measured): ADR-0024's literal rule types the constructor | — | value divergence | — · P107 | — | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.
