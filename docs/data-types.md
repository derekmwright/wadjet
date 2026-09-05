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

**The catalog's `(precision, scale)` is the column's type, whatever a file says.**
A parquet DECIMAL column chunk stores only the unscaled integer — the scale
lives in the file's schema — so a file registered against this table that
declares a different scale holds the right *number* under a different half of
the declaration. Wadjet moves such a value to the column's declared scale as it
reads it, with PostgreSQL's assignment-cast rules: exact when the scale rises,
rounded half away from zero when it falls, and `22003 numeric field overflow`
when the result does not fit the declared precision. Row-group statistics are
moved the same way, so predicate pruning stays correct, and `COMPACT` rewrites
such a file at the declared scale. This is what makes it safe to register
files written by pyarrow, parquet-mr or Spark against an existing table whose
DECIMAL columns were declared with different parameters. It applies at every
depth — a DECIMAL inside a `ROW`, `ARRAY` or `MAP` is reconciled exactly as a
top-level one is. A file that declares a **wider precision** at the same scale
is likewise held to the column's band: a value the declared type cannot hold is
`22003`, as it is in PostgreSQL, rather than being answered. See
[ADR-0018 §9](adr/0018-parquet-file-numbers-are-input.md).

### String and Binary Types

| Type | Go Backing | Storage | Use Cases |
|------|-----------|---------|-----------|
| `String` | `string` | Variable-length (offset/data layout) | Hostnames, messages, labels |
| `Bytes` | `[]byte` | Variable-length (offset/data layout) | Raw payloads, binary data |

Variable-length types use an **offset/data** columnar layout: a contiguous data buffer with a parallel offset array indexing into it. This avoids per-row heap allocation.

#### `Bytes` is PostgreSQL's `bytea`

It declares OID 17 on the wire and renders as `\x` hex in the text format.

**A literal beside a `BYTES` column is read by `byteain`**, PostgreSQL's own
bytea input function, in both of its spellings — so all three of these name the
same two bytes:

```sql
SELECT b FROM t WHERE b = 'hi';        -- the escape form: no backslash, its own bytes
SELECT b FROM t WHERE b = '\x6869';    -- the hex form, which is what the wire prints
SELECT b FROM t WHERE b = '\150\151';  -- octal escapes
```

`\\` is one backslash and `\ooo` one octal byte; anything else after a
backslash is `22P02`, as it is on the server.

**Functions over `BYTES` follow PostgreSQL's catalog**, which means BYTES, not
characters:

| Expression | Result | Note |
|---|---|---|
| `length(b)` | `integer` | the BYTE count — bytea has no characters, so this is `octet_length` |
| `substring(b, from, for)` | `BYTES` | indexed by bytes; a negative length is `22011` |
| `b \|\| b`, `b \|\| 'x'` | `BYTES` | OID 17, rendered `\x` hex |
| `md5(b)` | `text` | as on the server |
| `CAST(b AS STRING)` | `text` | the `\x` hex form |

Two divergences remain and are recorded in ADR-0012's list: a TEXT-ONLY
function over a `BYTES` argument (`upper(b)`) still ANSWERS where PostgreSQL
raises `42883 function upper(bytea) does not exist`, and an unknown-typed
LITERAL beside a bytea operand of `||` contributes its own spelling
(`b || '\x41'` appends four characters where the server appends one byte).

#### `FLOAT(n)`

`FLOAT(n)` is the SQL-standard spelling of "a binary float with at least n bits
of mantissa", and it resolves by WIDTH exactly as PostgreSQL does:

| Spelling | Type |
|---|---|
| `FLOAT(1)` … `FLOAT(24)` | `Float32` (real, OID 700) |
| `FLOAT(25)` … `FLOAT(53)` | `Float64` (double precision, OID 701) |
| `FLOAT` (bare) | `Float64` — PostgreSQL's unqualified `float` is double precision |
| `REAL`, `FLOAT4` | `Float32` |
| `DOUBLE PRECISION`, `FLOAT8` | `Float64` |
| `FLOAT(0)`, `FLOAT(54)` | `ERROR 22023` — the same message PostgreSQL gives |

```sql
SELECT CAST(1.0/3 AS FLOAT(1));    -- 0.33333334          (real)
SELECT CAST(1.0/3 AS FLOAT(25));   -- 0.3333333333333333  (double precision)
CREATE TABLE t (f FLOAT(1));       -- a Float32 column
```

#### `VARCHAR(n)` and `CHAR(n)`

`VARCHAR`, `CHAR`, `CHARACTER`, `CHARACTER VARYING`, `NCHAR`, `NVARCHAR` and
`TEXT` all name the one `String` type. A **length parameter is honoured by an
explicit `CAST` and dropped by DDL** — but both doors read the modifier the
same way, so a length a `CAST` refuses is refused by `CREATE TABLE` too:

```sql
SELECT CAST('abcdef' AS VARCHAR(4));   -- abcd   (truncated to 4 CHARACTERS)
SELECT CAST('éàüxyz' AS VARCHAR(3));   -- éàü    (characters, not bytes)
SELECT CAST(12345 AS VARCHAR(3));      -- 123    (the rendering is truncated)

CREATE TABLE t (v VARCHAR(4));         -- accepted; the 4 is NOT stored
INSERT INTO t VALUES ('abcdef');       -- accepted (PostgreSQL raises 22001)
```

**Invalid length modifiers**, with PostgreSQL's own codes and messages. Each is
refused identically by a `CAST` and by `CREATE TABLE`:

| Modifier | SQLSTATE | Message |
|---|---|---|
| `VARCHAR(0)`, `CHAR(0)` | `22023` | `length for type varchar must be at least 1` (`char` for the `CHAR` family) |
| `VARCHAR(abc)` — not a number | `42601` | `syntax error at or near "abc"` |
| `VARCHAR(-1)` — negative | `42601` | `syntax error at or near "-"` |
| `VARCHAR(10485761)` — past the cap | `22023` | `length for type varchar cannot exceed 10485760` |
| `TEXT(5)` — `TEXT` takes no modifier | `42601` | `type modifier is not allowed for type "text"` |

```sql
SELECT CAST('abcdef' AS VARCHAR(0));   -- ERROR 22023: length for type varchar must be at least 1
CREATE TABLE t (v VARCHAR(0));         -- ERROR 22023: column "v": length for type varchar must be at least 1
SELECT CAST('abcdef' AS TEXT(5));      -- ERROR 42601: type modifier is not allowed for type "text"
CREATE TABLE t (v TEXT(5));            -- ERROR 42601: column "v": type modifier is not allowed for type "text"
```

The DDL door prefixes the offending column name and, because the DDL lexer
folds an unquoted identifier to upper case before the type name is read, echoes
a non-numeric modifier as `"ABC"` where the `CAST` door echoes `"abc"`. The
code and the rule are the same on both.

**The wire declares the type, and separately the length.** A `CAST` to the
VARCHAR family is described as `character varying` — OID 1043 — with or
without a length; the length rides in the atttypmod beside it, `n+4` when
there is one and `-1` when there is not. That is what PostgreSQL's own
`\gdesc` reports for both spellings: `CAST(x AS VARCHAR(4))` is `character
varying(4)` and `CAST(x AS VARCHAR)` is `character varying`.

`CAST(x AS TEXT)`, `CAST(x AS STRING)` and a bare column reference are
described as unconstrained `text` (OID 25) — a different type name from
`character varying`, holding the same bytes and compared the same way. A bare
column reference is unconstrained even when the column was created
`VARCHAR(n)`, because the catalog does not store the n.

One difference from PostgreSQL to know about, and it is one fact wearing three
hats: **wadjet has no blank-padded `bpchar`.**

- `CAST('ab' AS CHAR(4))` renders `ab` where PostgreSQL renders `ab  `.
- The same cast is described as `character varying(4)`, not `character(4)`.
- Bare `CHAR` is the unparameterized string, where PostgreSQL reads
  `character(1)` — so `CAST('abcdef' AS CHAR)` is `abcdef` here and `a` there,
  and `CREATE TABLE t (c CHAR)` is an unbounded string on both doors.

PostgreSQL pads a short `bpchar` to n and then strips the trailing blanks again
for `length()`, for `||` and for every comparison. Wadjet has one string type
and none of that, so padding alone would move `length`, `||` and `=` AWAY from
PostgreSQL — a wrong row set in exchange for a right rendering — and declaring
`character(n)` would name a type whose defining behaviours are not implemented.
As it stands those three consumers agree with the server exactly. Recorded in
ADR-0012's divergence list.

### Network Types

| Type | Go Backing | Size | Format | Use Cases |
|------|-----------|------|--------|-----------|
| `IPv4` | `uint32` | 4 bytes | Dotted-quad string on input ("10.0.1.1") | Source/destination addresses |
| `IPv6` | `[16]byte` | 16 bytes | Standard IPv6 notation on input | IPv6 addresses |
| `CIDR` | `string` | Variable | CIDR notation ("10.0.0.0/8") | Subnet definitions, ACLs |
| `MAC` | `uint64` | 8 bytes | Colon-separated hex on input ("aa:bb:cc:dd:ee:ff") | Interface identification |
| `Port` | `uint16` (in `Int32Data`) | 2 bytes | Integer 0–65535 | Transport-layer ports |
| `Protocol` | `uint8` (in `Int32Data`) | 1 byte | IANA protocol number | IP protocol (6=TCP, 17=UDP) |

IPv4, IPv6, MAC, Port and Protocol are stored in compact binary representations rather than as text, enabling efficient comparison and aggregation while keeping human-readable input/output formats. CIDR is the exception: it stores its text form directly.

**What a PostgreSQL client sees.** `Port` and `Protocol` declare `integer`
(OID 23) on the wire and `Duration` declares `bigint` (OID 20, counting
nanoseconds), because that is what the engine compares them as. The bytes on
the wire are unchanged — all three have always rendered as plain integers
(`443`, `6`, `1500000000`) — so a driver that used to hand your application a
`String` now hands it an `Integer` or a `Long`, and `WHERE port_col = $1` binds
an integer parameter. The remaining network types declare `text`:

| Type | Wire type | OID |
|------|-----------|-----|
| `Port`, `Protocol` | `integer` | 23 |
| `Duration` | `bigint` (nanoseconds) | 20 |
| `IPv4`, `IPv6`, `CIDR`, `MAC` | `text` | 25 |
| `UUID` | `uuid` | 2950 |

**Literal spellings in a comparison.** A `MAC` or `UUID` literal compared
against a column is read in every spelling PostgreSQL accepts, at every site
(`=`, `IN`, `CASE`, `IS DISTINCT FROM`, `GREATEST`, `LEAST`):

| Type | Accepted spellings |
|---|---|
| `MAC` | `08:00:2b:01:02:03`, `08-00-2b-01-02-03`, `0800.2b01.0203`, `08002b010203`, `08002b:010203`, `08002b-010203`, `0800-2b01-0203`, and the same in upper case. A grouped-hex spelling must split the twelve digits `6+6` or `4+4+4`; any other regrouping is `22P02`, as it is in PostgreSQL |
| `UUID` | dashed, undashed, braced (`{...}`), and any case |

An abbreviated `CIDR` **is** accepted, with PostgreSQL's own classful
inference (measured on 17.11, not remembered):

| Literal | Value | Literal | Value |
|---|---|---|---|
| `'10'` | `10.0.0.0/8` | `'10/8'` | `10.0.0.0/8` |
| `'10.1'` | `10.1.0.0/16` | `'192.168/16'` | `192.168.0.0/16` |
| `'128'` | `128.0.0.0/16` | `'192.168'` | `192.168.0.0/24` |
| `'224'` | `224.0.0.0/4` | `'240'` | `240.0.0.0/32` |

The mask comes from an explicit `/bits` when one is written; otherwise from the
CLASS of the first octet, widened to cover the octets that were written from
the second octet on. `'010.1'` is decimal, not octal. `'10.'`, `'10..1'`,
`'256.1'`, `'10.1.2.3.4'`, `'0x0a.1'`, `'10/33'` and any surrounding or
embedded whitespace are `22P02`, as they are on the server.

An `IPv4` or `IPv6` literal follows PostgreSQL's `inet` rather than its `cidr`:
an abbreviated form needs an explicit mask there (`'192.168'::inet` is an error
on the server too), and a HOST-width prefix is the address itself
(`'10.0.0.1/32'` equals `'10.0.0.1'`). A prefix NARROWER than the host width
names a network, which those two types have no room for — they hold a bare
address — so it is refused with `0A000` and a message saying so. Use a `CIDR`
column for a value that carries a prefix.

A literal that names no address at all is refused when the query is PLANNED for
`CIDR`, `MAC` and `UUID`, so the same query cannot answer over one file and
error over another. `IPv4` and `IPv6` still refuse at runtime, because their
accept-set is not yet a superset of the server's.

`INSERT` and the `mac_*` formatting functions read only the spellings Go's
parser takes (colon, hyphen, dotted, and the bare twelve digits), not the three
grouped-hex forms above.

**The printed form of an IPv6 value is PostgreSQL's `inet` output**, which
differs from Go's for two families: a v4-MAPPED address prints
`::ffff:10.0.0.1` and not the bare `10.0.0.1`, and a v4-COMPATIBLE one prints
`::1.2.3.4` and not `::102:304`. That text is also what `LIKE` matches against.

### Temporal Types

| Type | Go Backing | Precision | Use Cases |
|------|-----------|-----------|-----------|
| `Timestamp` | `int64` | Milliseconds since epoch | Event times, log timestamps |
| `Date` | `int32` | Days since 1970-01-01 | Calendar dates, partition keys |
| `Duration` | `int64` | Nanoseconds | Time intervals, latency measurements |

`Timestamp` is PostgreSQL's `timestamp without time zone`, which is the type it
declares on the wire. A literal that carries a UTC offset has that offset
**discarded** — `'2020-01-01T05:30:00+05:30'` is `2020-01-01 05:30:00`, not the
instant it names — and this holds for a value being stored and for the same
literal in a predicate, which read it through one accept-set. A literal whose
fields name no instant (`2020-02-30`, month 13, hour 25) is SQLSTATE 22008;
text that is not a timestamp at all is 22007.

**One rendering.** A `Timestamp` coerced to text — `CAST(ts AS TEXT)`,
`ts::text`, `ts || ''`, `CONCAT`, `UPPER` and every other string function,
`LIKE`, `FORMAT('%s', ts)` — produces the same text the wire carries for the
column: `1996-03-13 14:25:36`, UTC, space-separated, no zone suffix, and a
fractional second with its trailing zeros trimmed as PostgreSQL trims them
(`.5`, not `.500`). The instant, never the epoch-millisecond number the value
is stored as. The same holds for a timestamp produced by an expression rather
than read from a column, and for `Date` with its own `1996-03-13` form.

The rule covers timestamp-VALUED functions too — `DATE_TRUNC`,
`FROM_UNIXTIME`, `DATE_PARSE`, `TIMEZONE` and interval arithmetic over a text
operand all render this way, and it is PostgreSQL's text for each.
`DATE_TRUNC('day', ts)` is `2023-11-14 00:00:00`, the same text the column it
read produces.

Those functions also DECLARE `timestamp` (OID 1114), which is what a driver
reads to pick a column class — `DATE_TRUNC`, `FROM_UNIXTIME`, `DATE_PARSE`,
`TIMEZONE`, `NOW`, `CURRENT_TIMESTAMP` and `PG_POSTMASTER_START_TIME`. Three
temporal functions deliberately declare `text` instead, because text is what
they produce: `DATE_FORMAT` (the caller's format string), `TO_ISO8601` (its
name is its contract) and `AT_TIMEZONE` (a wall clock in another zone, whose
offset is load-bearing).

`CURRENT_DATE` is the UTC date of the same clock — one zone for every clock
function, which is what makes `CURRENT_DATE = CAST(NOW() AS DATE)` true here as
it is on the server. It read the machine's LOCAL date until #870, so west of
Greenwich the two disagreed by a day for the hours between local midnight and
UTC midnight.

`NOW`, `CURRENT_TIMESTAMP` and `PG_POSTMASTER_START_TIME` render the same way
and PostgreSQL does not: it types all three `timestamptz`, whose text carries a
**zone offset** and **six** fractional digits — `2026-09-04 21:21:01.708284+00`
where this engine says `2026-09-04 21:21:01.708`. This engine has no
`timestamptz`, and its instant is epoch milliseconds, so it has neither half to
give. See ADR-0012's divergence list.

**The resolution is the millisecond.** A `Timestamp` is epoch milliseconds, so
a rendered instant carries at most three fractional digits: `.5`, `.25`,
`.123` — never `.123456`. PostgreSQL's `timestamp` holds microseconds and
prints all six when they are there, so a value that reaches this engine with
finer precision (a microsecond literal, a Parquet column written at
microsecond or nanosecond resolution) is truncated to the millisecond on the
way in, and every rendering of it — the wire, a cast, a function result —
shows the truncated value consistently. It is one rendering of one stored
instant, not a rounding applied at print time.

`Duration` is the exception, and deliberately: it declares `int8` on the wire
counting nanoseconds, so `CAST(d AS TEXT)` renders that integer — the text
agrees with the declaration and with the projection.

Two functions keep an ISO 8601 rendering, and both are about the value rather
than the dialect. `TO_ISO8601` is named for its format. `AT_TIMEZONE` returns a
wall clock in the zone you asked for, and the rendering above carries no zone —
printed bare, `at_timezone(ts, 'America/New_York')` would read back as UTC and
be five hours wrong — so it keeps its offset.

What a timestamp-valued function gets wrong beyond that is its DECLARED type:
`DATE_TRUNC` returns `timestamp` (OID 1114) on PostgreSQL and `text` (OID 25)
here, because the scalar registry has one static return type per function and
no TIMESTAMP-valued function result exists. Recorded in ADR-0012's divergence
list, with the three clock functions above.

**Where this accept-set applies: everywhere.** The INGEST door (`COPY`, the Go
ingester, a Bento-written table registered through Wadjet), a DATE literal in
`INSERT … VALUES`, a literal in a predicate, and `CAST(<text> AS DATE)` all
read the same function, so a spelling that stores is a spelling a filter
matches and a cast converts — same value, same refusal, same SQLSTATE. The
`CAST` door used to be a second parser that answered NULL where the others
raised and accepted `'0000-01-01'`; it now takes both its value and its
refusal from the shared accept-set.

`Date` takes the unambiguous year-first spellings PostgreSQL's default
`DateStyle` reads exactly one way: a four-or-more-digit leading year with a
`-`, `/` or `.` separator (`2026-01-02`, `2026-1-2`, `2026/01/02`,
`2026.1.1`), the compact `20260102`, and any of those followed by a
time-of-day, which is truncated. Anything it cannot read identically to
PostgreSQL is an **error**, never a guess: a spelling whose field order
`DateStyle` decides (`01/02/2026`, two-digit years, month names), **year
zero** — PostgreSQL's calendar puts 1 BC immediately before 1 AD, so
`0000-01-01` is 22008 there — and a **month field of exactly three digits**
(`2026-003-12`), which PostgreSQL reads as a day-of-year and then rejects. A
four-digit month (`2026-0003-12`) and a three-digit day (`2026-01-003`) are
accepted, as they are by PostgreSQL.

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

A `container.field` reference is a FIELD PATH, not a table-qualified column,
and it is read that way wherever the container declares the field — even when
some other relation in the query publishes a column of the field's own name:

```sql
-- `c_row` is a ROW column of nested and `d` publishes a scalar column `b`.
-- `c_row.b` is the ROW's field, not d.b.
SELECT n.id, c_row.b FROM nested n JOIN dims d ON n.id = d.id
```

Two relations in scope carrying a container of the same name make the
reference ambiguous, and it is refused — `column reference "c_row" is
ambiguous`, SQLSTATE 42702, which is what PostgreSQL raises for `(c_row).b`
over the same two relations. Select the field through a derived table that
renames the other arm's container away:

```sql
-- Both arms publish `c_row`, so `c_row.b` is refused. Rename the OTHER arm's
-- container and `c_row` names one thing again.
SELECT x.id, c_row.b FROM nested x
  JOIN (SELECT id, c_row AS y_row FROM nested) y ON x.id = y.id
```

That query is driven as a gate — `docs-example/*` in
`internal/coordinator/derived_arm_join_chain_two_path_test.go` — on every
execution arm and in both spellings, so this page cannot drift from the engine.


A field the container does not declare is refused too, with PostgreSQL's
wording (`could not identify column "x" in record data type`).

**Which spellings are accepted.** PostgreSQL requires the parenthesised form
and reads the unparenthesised one as a table qualifier; wadjet reads both,
which is the deliberate superset ADR-0012 records:

| spelling | wadjet | PostgreSQL 17 |
|---|---|---|
| `c_row.b` | the field | `missing FROM-clause entry for table "c_row"` (42P01) |
| `(c_row).b` | the field | the field |
| `(x.c_row).b` | `0A000`, not supported | the field |
| `x.c_row.b` | syntax error | read as *schema.table.column*: `missing FROM-clause entry for table "c_row"` (42P01) |

Redundant parentheses are redundant: `((c_row)).b` is the same reference.
Field notation on something that is not composite is refused with PostgreSQL's
own 42809 — `(1+2).b`, and `(b).x` over a scalar column, which answered one
NULL per row until 2026-09-04.

A container the reference QUALIFIES needs a three-part identity that this
engine does not yet carry, so `(x.c_row).b` — and, for the same reason, the
nested `((c_row).rw).k` — is REFUSED rather than answered: a loud `0A000`
naming the derived-table workaround, never a silent value. That
is the one spelling PostgreSQL offers for an ambiguous container and wadjet
does not; the derived-table rename above is the way to write it today.
ADR-0022 carries the mechanism and what closing it takes.

**Array functions:** `cardinality`, `element_at`, `array_contains`, `array_join`, `array_min`, `array_max`, `array_length`

**Map functions:** `map_keys`, `map_values`, `map_entries`, `map_from_entries`

**JSON extraction:** `json_extract`, `json_extract_scalar`, `json_array_length`, `json_valid`

Nested types round-trip through Parquet in both directions — written as the standard LIST/MAP/STRUCT shapes and detected by the same patterns on read — and render in display output. Containers may nest inside containers.

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

-- Generate embeddings from text (requires a configured embedding provider)
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
export WADJET_EMBED_PROVIDER=openai              # openai (default) | voyage | ollama
export WADJET_OPENAI_API_KEY=sk-...              # openai
export WADJET_VOYAGE_API_KEY=pa-...              # voyage
export WADJET_OLLAMA_URL=http://localhost:11434  # ollama (keyless)
export WADJET_EMBED_MODEL=text-embedding-3-small # default for openai
export WADJET_EMBED_DIM=1536                     # when the model's width is not in the built-in table
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
import "github.com/derekmwright/wadjet/wadjet"

schema := wadjet.Schema{
    Columns: []wadjet.Column{
        {Name: "timestamp",    Type: wadjet.TypeTimestamp},
        {Name: "src_ip",      Type: wadjet.TypeIPv4},
        {Name: "dst_ip",      Type: wadjet.TypeIPv4},
        {Name: "src_port",    Type: wadjet.TypePort},
        {Name: "dst_port",    Type: wadjet.TypePort},
        {Name: "protocol",    Type: wadjet.TypeProtocol},
        {Name: "bytes_in",    Type: wadjet.TypeInt64},
        {Name: "bytes_out",   Type: wadjet.TypeInt64},
        {Name: "src_mac",     Type: wadjet.TypeMAC},
        {Name: "vlan_id",     Type: wadjet.TypeInt32},
        {Name: "is_encrypted", Type: wadjet.TypeBool},
        {Name: "flow_id",     Type: wadjet.TypeUUID},
        {Name: "duration",    Type: wadjet.TypeDuration},
    },
}
```

Column types are referenced as `wadjet.TypeXxx` constants (e.g., `wadjet.TypeIPv4`, `wadjet.TypeTimestamp`). The `Nullable` field on `Column` defaults to `false`; set it to `true` to allow nulls.

`wadjet.Schema` and `wadjet.Column` are usable from any module. The
`CREATE TABLE` DDL through `db.Query(...)` resolves the same declarations —
including parameterized `DECIMAL(p,s)`, `ARRAY(T)`, `ROW(...)`, `MAP(K,V)` and
`VECTOR(N)` — through the one checked converter, and is the shorter route for
anything a literal makes verbose.

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
| BYTE_ARRAY | none | String |
| INT32 | DATE | Date |
| INT64 | TIMESTAMP_MICROS / TIMESTAMP_NANOS | Timestamp (rescaled to milliseconds) |
| FIXED_LEN_BYTE_ARRAY / INT32 / BYTE_ARRAY | DECIMAL | Decimal |
| FIXED_LEN_BYTE_ARRAY | UUID | UUID |
| BYTE_ARRAY | JSON, ENUM | String |
| INT32 / INT64 | INTEGER | Int32 when bit width <= 32, otherwise Int64 |
| INT32 / INT64 | TIME_MILLIS / TIME_MICROS | Int32 / Int64 (raw, in the file's own unit — there is no time-of-day type) |

Network types (`IPv4`, `IPv6`, `CIDR`, `MAC`) require explicit schema registration since Parquet has no native representation for these. When creating a table via the API, specify the schema with the correct network types, and the ingester will handle conversion.

## Type Coercion in Expressions

The expression compiler handles implicit type promotion in arithmetic and comparisons:

| Left Type | Right Type | Result Type |
|-----------|-----------|-------------|
| Int32 | Int64 | Int64 |
| Int32 | Float64 | Float64 |
| Int64 | Float64 | Float64 |
| Float32 | Float64 | Float64 |
| Int32 | Int32 | Int64 (integer arithmetic stays exact; a result outside int64 is SQLSTATE 22003, never a wrapped number) |
| Int32 / Int64 | Decimal(p,s) | Decimal — the integer contributes its whole range at scale 0 (10 digits for Int32, 19 for Int64) |
| Decimal(p1,s1) | Decimal(p2,s2) | Decimal(min(38, max(p-s) + max(s)), max(s1,s2)); a value with no 128-bit carrier at that type is SQLSTATE 22003 |

The integer domain belongs to the expression's TYPE, not to the syntax that
produced its operands. `CAST(x AS BIGINT)`, a function whose result is an
integer (`ABS`, `MOD`), and a choice expression over integer branches (`CASE`,
`COALESCE`, `NULLIF`, `GREATEST`, `LEAST`) all type as integer, so arithmetic
over them is integer arithmetic: it answers `bigint`, and a result outside
int64's range is SQLSTATE 22003. One non-integer branch — a float column in a
`CASE` arm, a `FLOOR`/`ROUND`/`SIGN` result, a fractional literal — makes the
whole expression double precision, as it does in PostgreSQL.

Integer division truncates toward zero, following PostgreSQL. Aggregates over
integers are exact types, not float64: `SUM(int4)` is `bigint`, `SUM(int8)` is
`numeric(38,0)`, and `AVG` over any integer is `numeric(38,4)`. `SUM` over a
`DECIMAL(p,s)` is `DECIMAL(38,s)` and `AVG` is `DECIMAL(38,s+4)`. An overflow is
SQLSTATE 22003, never a wrapped or saturated number.

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
