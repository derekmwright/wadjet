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

#### What an integer expression declares

| Expression | Declared | OID | PostgreSQL |
|---|---|---|---|
| `1`, `2147483647` | `integer` | 23 | same |
| `2147483648`, `9007199254740993` | `bigint` | 20 | same |
| `MIN(i32)`, `MAX(i32)` | `integer` | 23 | same |
| `BITWISE_AND/OR/XOR/NOT` over `integer` operands | `integer` | 23 | same |
| `SUM(i32)` | `bigint` | 20 | same |
| `SUM(i64)`, `AVG(i32)`, `AVG(i64)` | `numeric` | 1700 | same |
| `i32 + 1`, `-i32`, `i32 * 3`, `i32 / 2` | `bigint` | 20 | `integer` |
| `CAST(x AS INT)`, `CAST(x AS SMALLINT)` | `bigint` | 20 | `integer` / `smallint` |
| `i32 << 2` | `bigint` | 20 | `integer`, and MODULAR |

The last three rows are recorded divergences (ADR-0012). Arithmetic and an
integer CAST keep `bigint` because this engine carries every computed integer
in an int64, which is also why `2147483647 + 1` answers 2147483648 here where
PostgreSQL raises `integer out of range`. The shifts keep it because
PostgreSQL's int4 shift is modular and this engine's is not.

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
backslash is `22P02`, as it is on the server — and so are an odd number of hex
digits, a non-hex digit, and the UPPERCASE `\X` form, which `byteain` does not
take. Whitespace inside the hex digits IS taken (`'\x68 69'`), because
`hex_decode` skips it. The refusal is decided when the query is planned, like
every other type's.

**Functions over `BYTES` follow PostgreSQL's catalog**, which means BYTES, not
characters:

| Expression | Result | Note |
|---|---|---|
| `length(b)` | `integer` | the BYTE count — bytea has no characters, so this is `octet_length`, over a bare column and a derived value alike |
| `substring(b, from, for)` | `BYTES` | indexed by bytes; a negative length is `22011` |
| `b \|\| b`, `b \|\| 'x'` | `BYTES` | OID 17, rendered `\x` hex |
| `md5(b)` | `text` | as on the server |
| `CAST(b AS STRING)` | `text` | the `\x` hex form |

`text || bytea` is TEXT, not bytea: the server resolves that pair through
`text || anynonarray`. Only `bytea || bytea` and `bytea || <unknown literal>`
are bytea.

Three divergences remain and are recorded in ADR-0012's list:

* a TEXT-ONLY function over a `BYTES` argument (`upper(b)`, `char_length(b)`)
  still ANSWERS where PostgreSQL raises
  `42883 function upper(bytea) does not exist`;
* an unknown-typed LITERAL beside a bytea operand of `||` contributes its own
  spelling (`b || '\x41'` appends four characters where the server appends one
  byte);
* where the pair is TEXT, the server RENDERS the bytea operand as its `\x` hex
  text and this engine splices the raw bytes — with `b = '\x6869'`,
  `'hi' || b` is `hi\x6869` there and `hihi` here.

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

**Arithmetic over two REALs is real, and a value SELECTed from it computes at
float4's width.** `r + CAST(1.0 AS REAL)` over a real holding 2^24 is
16777216, not 16777217 — in a SELECT list, a GROUP BY key, an ORDER BY key, an
aggregate argument and each arm of a set operation. Two positions still compute
it at `double precision` and are recorded in ADR-0012: the same expression
inside a WHERE, HAVING, ON or CASE WHEN condition, and an inner step of a
NESTED real expression such as `(r + CAST(1.0 AS REAL)) + CAST(1.0 AS REAL)`.
Spelling the step — `CAST(r + CAST(1.0 AS REAL) AS REAL) + CAST(1.0 AS REAL)` —
answers PostgreSQL's number today.
That pairing alone: `real + 1.0` (a numeric literal), `real + 1` (an integer)
and `real + double precision` are all double precision, which is what
PostgreSQL resolves them to. `-real` is real; `SUM(real)` is real grouped and
windowed alike; `AVG(real)` is double precision.

A real result that leaves float4's range is `22003 value out of range:
overflow`, and one that rounds to zero from a non-zero operand is
`22003 value out of range: underflow` — PostgreSQL's own two sentences,
applied wherever a wider value is stored into a `Float32` column.

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

#### A version string is a `String`

Wadjet adds no semantic-version type: a version is text, and the
`SEMVER_*` function family (see
[SQL Reference](sql-reference.md#version-string-functions)) gives that text the
ordering the Semantic Versioning 2.0.0 specification gives it. This is
general-purpose rather than network-specific — a package inventory, an agent or
firmware roster, a container image tag, a CVE feed's affected-versions column.

The reason it is a function family and not a type is that the ordering can be
carried by a `String`. `SEMVER_SORT_KEY(v)` is a `String` whose **byte order is
precedence**, so every consumer that already orders strings — `ORDER BY`,
`MIN`/`MAX`, a `GROUP BY` key, a distributed merge, a spilled external sort, a
window frame — orders versions correctly with no comparator of its own:

```sql
SELECT name, MAX(SEMVER_SORT_KEY(version)) AS newest
  FROM packages GROUP BY name;
```

A string that is not a version is NULL through the family's lenient forms, so
a column of mixed junk filters rather than failing — `SEMVER_VALID` answers
`false` for one, being the question, and the `_STRICT` twins
(`SEMVER_NORMALIZE_STRICT`, `SEMVER_PARSE_STRICT`) raise SQLSTATE `22023`
naming the string instead.

### Network Types

| Type | Go Backing | Size | Format | Use Cases |
|------|-----------|------|--------|-----------|
| `IPv4` | `uint32` | 4 bytes | PostgreSQL `inet` text ("10.0.1.1") | Source/destination addresses |
| `IPv6` | `[16]byte` | 16 bytes | PostgreSQL `inet` text ("2001:db8::1") | IPv6 addresses |
| `CIDR` | `string` | Variable | PostgreSQL `inet` text with a prefix ("10.0.0.0/8") | Subnet definitions, ACLs |
| `MAC` | `uint64` | 8 bytes | PostgreSQL `macaddr` text ("aa:bb:cc:dd:ee:ff") | Interface identification |
| `Port` | `uint16` (in `Int32Data`) | 2 bytes | Integer 0–65535 | Transport-layer ports |
| `Protocol` | `uint8` (in `Int32Data`) | 1 byte | IANA protocol number or name | IP protocol (6=TCP, 17=UDP) |

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

Because `Port` and `Protocol` declare `integer`, `SUM` and `AVG` over them
follow `int4`'s rules: `SUM(port)` is `bigint` and `AVG(port)` is
`numeric(38,4)`, in the grouped and the windowed spelling alike. `Duration`,
`Date` and `Timestamp` are excluded — PostgreSQL has no `sum(date)` or
`sum(timestamp)`, and an interval's sum is an interval rather than a number —
so those keep `double precision` in both spellings.

**Arithmetic over a `Port` or a `Protocol` is `int4` arithmetic.** A port is
constrained to 0–65535 at the TYPE boundary only; once it is an operand it is
an ordinary integer and the RESULT is an integer, the same way `smallint + 1`
is `integer` in PostgreSQL. So `port * 1`, `port + 0`, `-port`, `ABS(proto)`
and `MOD(proto, 2)` are integers, `SUM` over any of them is `bigint` and `AVG`
is `numeric(38,4)`, and **division truncates**: `proto / 2` over 255 is 127,
not 127.5. The address types (`IPv4`, `IPv6`, `MAC`, `CIDR`) and the temporal
ones keep their own arithmetic and are unaffected.

**Every network type has ONE text grammar, and every door reads it.** The
writer (the embedded ingester, `INSERT … VALUES`, `COPY`), a `CAST` at query
time, a literal beside a column in a `WHERE`, `IN`, `CASE`, `IS DISTINCT FROM`,
`GREATEST` or `LEAST`, and an unknown-typed literal in `INSERT … SELECT` all
read the same accept-set. It is PostgreSQL 17.11's own input function for the
type, measured cell by cell:

| Type | Accepted spellings |
|---|---|
| `IPv4`, `IPv6`, `CIDR` | `inet`'s grammar — see the abbreviated-address table below. Leading zeros are decimal (`010.1.2.3`), one trailing dot is ignored (`10.1.2.3.`), a HOST-width prefix is the address itself (`10.0.0.1/32`), and `inet6`'s mask has its OWN rule: digits only, no leading zeros, 0–128 (`::1/064` is `22P02`, at a `CIDR` column as well as an `IPv6` one). A v4 address is read the same way whether or not a `/32` follows it, at every one of the three types: `'010.1.2.3'` and `'010.1.2.3/32'` are one value, and an `IPv6` column stores it as `::ffff:10.1.2.3` |
| `MAC` | `08:00:2b:01:02:03`, `08-00-2b-01-02-03`, `08002b:010203`, `08002b-010203`, `0800.2b01.0203`, `0800-2b01-0203`, `08002b010203`, any case — `macaddr_in`'s seven `sscanf` patterns. The colon and hyphen forms read VARIABLE-width groups, so `a:b:c:d:e:f` is `0a:0b:0c:0d:0e:0f`; any other regrouping (`0800:2b01:0203`, `08.00.2b.01.02.03`) is `22P02`, and an octet above 255 is `22003 invalid octet value`. A field is converted the way C's `%x` converts it, which truncates TWICE: a value past 2^64−1 saturates and is `22003` (`'10000000000000000:0:0:0:0:0'`), and what survives that is taken mod 2^32 (`'100000001:0:0:0:0:0'` is `01:00:00:00:00:00`). Leading zeros do not count toward either — twenty-two of them are still the value they precede |
| `UUID` | 32 hex digits in any case, optionally wrapped in BOTH braces, with a hyphen permitted after any group of four and nowhere else: `a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11` is a value, `a-0eebc99…` is `22P02` |
| `Port` | a DECIMAL number in 0–65535, with an optional sign and surrounding whitespace. A FRACTION is not a decimal number: `'2.5'` is `22P02`, as `'2.5'::integer` is on the server, whether it is written as a literal, held in a STRING column or computed by a string-typed expression (`CAST(CONCAT('2','.5') AS PORT)`, `CAST(TRIM(s) AS PORT)`). A NUMBER still rounds, as PostgreSQL's numeric-to-integer cast does: `CAST(2.5 AS PORT)` is 3. int4's other spellings are NOT part of it: `'0x1bb'`, `'0o17'`, `'0b101'` and `'1_000'` are `22P02`, even though `'0x1bb'::integer` is 443. Service NAMES are not resolved either: `port_name()` is the function that names a port |
| `Protocol` | the same decimal form in 0–255, or the IANA NAME case-insensitively (`udp`, `TCP`, `icmp`, `ipv6-icmp`) — the text form `protocol_name()` prints, so `CAST(CAST(p AS TEXT) AS PROTOCOL)` round-trips |

**Whitespace is the type's own business, and the types disagree** — which is
what having ONE grammar per type means rather than one rule for all of them.
`macaddr` SKIPS it, before every group and after the last, so
`' 08:00:2b:01:02:03 '` and `'08: 00:2b:01:02:03'` are values; `inet` and
`uuid` REFUSE it, so `' 10.0.0.1'` and `'<uuid> '` are `22P02`; `Port` and
`Protocol` ignore it as their integer form does. Every door answers the same
way — the writer, `COPY`, `UPDATE`, a `CAST` and a comparison.

An empty string is the one place two kinds of door differ on purpose: at the
embedded ingester it is ABSENCE (the empty CSV or JSON field, stored as NULL),
and at every SQL door it is a value the type cannot read — `22P02`, which is
what `''::inet` is on the server.

**A `PORT` or `PROTOCOL` range is checked when a value ENTERS the type — by
cast or by write — and nowhere else.** A port is 0–65535 and a protocol
0–255, and a value outside the range is `22003` naming the value and the type,
at every door: `CAST(70000 AS PORT)`, `CAST('70000' AS PORT)`,
`CAST(-1 AS PROTOCOL)`, `INSERT INTO t (p) VALUES (70000)` and
`CREATE TABLE p AS SELECT CAST(70000 AS PORT)` all refuse. So does the Go API
handed a NUMBER rather than text — `db.NewIngester(…).Ingest` with
`int32(65536)` — which is the door `docs/ingestion.md` points at for native
columns. It is the same refusal and the same words at each one.

Arithmetic is not a type boundary and keeps `int4`'s rules: `port * 1`,
`port + 70000`, `-port` and `SUM(port)` are integers and may leave the range
without error, exactly as `smallint + 1` is `integer` in PostgreSQL. The
constraint is on the TYPE, not on the arithmetic that reads it.

Beside a COLUMN, both types read their OWN input function too, not the
`integer` they declare on the wire: `WHERE proto = 'udp'` answers the UDP rows
and `WHERE port = '0x1bb'` is `22P02`, the same two answers the writer and the
`CAST` give. A predicate, an `IN` list, a `BETWEEN`, a `CASE` or a `COALESCE`
arm, a join `ON` and a `HAVING` all read it, and the row-group prune reads the
same value the filter does. `CAST('udp' AS PROTOCOL)` and
`protocol_number('udp')` still answer; they are no longer the only spellings
that do.

**A value PostgreSQL accepts that this engine's type has no room for is
`0A000`, one class at every door.** An `IPv4` column cannot hold a NETWORK
(`'10/8'`) and cannot hold an IPv6 address (`'::1'`); an `IPv6` column cannot
hold a v4 network. All of them are `0A000` with a message naming the reason —
never `22P02`, which would claim the text is bad, and never a silent zero-row
answer.

An abbreviated address **is** accepted beside a `CIDR` column, in the grammar
PostgreSQL itself uses there — `inet`'s, not `cidr`'s. That distinction is the
whole rule: `cidr` has no comparison operators of its own, so the server
resolves `cidr_col = '<literal>'` through `=(inet, inet)` and reads the literal
with inet's parser (its error message names the type it used,
`invalid input syntax for type inet: "239"`). The classful inference people
associate with `'10'::cidr` belongs to the `cidr` TYPE and reaches no
comparison.

| Literal | Value | Literal | Value |
|---|---|---|---|
| `'10/8'` | `10.0.0.0/8` | `'10'` | `22P02` |
| `'192.168/16'` | `192.168.0.0/16` | `'192.168'` | `22P02` |
| `'10.1/8'` | `10.1.0.0/8` | `'239'` | `22P02` |
| `'1/0'` | `1.0.0.0/0` | `'10/16'` | `22P02` |
| `'172.31/12'` | `172.31.0.0/12` | `'0x0a'` | `22P02` |
| `'010.1.2.3'` | `10.1.2.3/32` | `'10.1.2.3'` | `10.1.2.3/32` |

Three rules, each measured on 17.11 over its whole domain rather than sampled:
a literal with **no mask** must name all four octets (all 256 one-octet values
are `22P02`); a **mask** may not name a byte the literal did not write
(`'10/15'` is a value and `'10/16'` is `22P02`, over every octet count from one
to four); and the bits to the RIGHT of the mask are **kept**, not zeroed —
`'10.1/8'` is `10.1.0.0/8` and `'255/1'` is `255.0.0.0/1`, both of which the
`cidr` type itself refuses. Leading zeros are digits and decimal, not octal
(`'010.1.2.3'`, `'10/008'`), and one trailing dot is ignored (`'10./8'`).
`'10.'`, `'10..1'`, `'256.1'`, `'10.1.2.3.4'`, `'0x0a'`, `'10/33'`, `'10/'` and
any surrounding or embedded whitespace are `22P02`, as they are on the server.

An `IPv4` or `IPv6` literal reads the same grammar, and a HOST-width prefix is
the address itself (`'10.0.0.1/32'` equals `'10.0.0.1'`). A prefix NARROWER than the host width
names a network, which those two types have no room for — they hold a bare
address — so it is refused with `0A000` and a message saying so. Use a `CIDR`
column for a value that carries a prefix.

**Every network literal is classified once, when the query is PLANNED**, for
all five types and at every site — `=`, `<>`, `<`, `>`, `IN`, a `CASE`, a
`GREATEST`, a `COALESCE`, a projection, and a scan no row survives. So the same
query cannot answer over one file and error over another, and cannot refuse in
a `WHERE` clause while answering inside a `CASE`. Two classes, and they are
different answers: text that names no address is `22P02`, and
PostgreSQL-valid text this engine's type cannot hold is `0A000`.

The fold sites — `GREATEST`, `LEAST`, `COALESCE` — take the same grammar, and
that is the server's answer too for the type a wadjet `CIDR` column actually
is. Those functions unify their arguments' types rather than resolving an
operator, so beside a PostgreSQL `cidr` column they would read the CIDR
parser; but a wadjet `CIDR` column holds host bits under a mask
(`192.168.5.7/24`), which `cidr` refuses and `inet` accepts, so `inet` is what
this engine's differential oracle maps it to — and over an `inet` column
PostgreSQL refuses `'239'`, `'192.168'` and `'zzz'` at every fold, exactly as
this engine does.

The `mac_*` formatting functions read only the spellings Go's parser takes
(colon, hyphen, dotted, and the bare twelve digits), not the grouped-hex forms
above. Every other door — `INSERT`, the ingester, a `CAST`, a comparison —
reads the full `macaddr` grammar.

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
reads to pick a column class — `DATE_TRUNC`, `TIME_BUCKET`, `FROM_UNIXTIME`,
`DATE_PARSE`, `TIMEZONE`, `NOW`, `CURRENT_TIMESTAMP` and
`PG_POSTMASTER_START_TIME`. Three
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
microsecond or nanosecond resolution, a `read_csv` / `read_json` field, a
`postgres_scan` value, a binary wire parameter) is floored to the millisecond
on the way in, and every rendering of it — the wire, a cast, a function result —
shows the truncated value consistently. It is one rendering of one stored
instant, not a rounding applied at print time.

**There is no infinity.** PostgreSQL's `timestamp` accepts `'infinity'` and
`'-infinity'`; this engine's millisecond carrier has no such value, so the text
is refused (`22007`) and so is a binary wire parameter carrying PostgreSQL's
infinity encoding (`22023` at Bind), rather than stored as some far-off year.

`Duration` is the exception, and deliberately: it declares `int8` on the wire
counting nanoseconds, so `CAST(d AS TEXT)` renders that integer — the text
agrees with the declaration and with the projection.

Two functions keep an ISO 8601 rendering, and both are about the value rather
than the dialect. `TO_ISO8601` is named for its format. `AT_TIMEZONE` returns a
wall clock in the zone you asked for, and the rendering above carries no zone —
printed bare, `at_timezone(ts, 'America/New_York')` would read back as UTC and
be five hours wrong — so it keeps its offset.

That paragraph used to end with a divergence — `DATE_TRUNC` declaring `text`
(OID 25) where PostgreSQL declares `timestamp` — and it is CLOSED: #868 gave
`Vector.SetValue` a TIMESTAMP arm that reads an instant's own text back into
epoch milliseconds, so a timestamp-valued function declares what it returns.
`TIME_BUCKET` (#965) is registered the same way and declares OID 1114 too. The
three functions above that still declare `text` do so because text is what they
produce, not because the declaration is missing.

**Where this accept-set applies: everywhere.** The INGEST door (`COPY`, the Go
ingester, a Bento-written table registered through Wadjet), a DATE literal in
`INSERT … VALUES`, a literal in a predicate, and `CAST(<text> AS DATE)` all
read the same function, so a spelling that stores is a spelling a filter
matches and a cast converts — same value, same refusal, same SQLSTATE. The
`CAST` door used to be a second parser that answered NULL where the others
raised and accepted `'0000-01-01'`; it now takes both its value and its
refusal from the shared accept-set.

An INT32, a DATE, a PORT and a PROTOCOL are all stored in a signed 32-bit
field, and a number with no room in one is `22003 integer out of range` —
never a wrapped value. That bound is applied by the CAST itself, so it holds
whether or not the result is ever written to a column:

| cast | in range | out of range |
|---|---|---|
| `::INT32` | the number, declared `bigint` | `22003` |
| `::PORT`, `::PROTOCOL` | the number, declared `integer` (OID 23, the same OID the column declares) | `22003` |
| `::DATE` | the day count, declared `date` | `22003` |
| `::FLOAT32` | the value rounded to float4, declared `real` | `22003`, `value … is out of range for type real` |

`3000000000::DATE` used to answer `-3543531-12-19`, a date rendered like any
other, and `9223372036854775807::DATE` answered `1969-12-31`: the day count was
read through calendar arithmetic that wrapped, so what reached the column was a
number an `int32` holds and the column's own guard had nothing to reject.
`3000000000::INT32`, `::PORT` and `::PROTOCOL` answered `3000000000` as TEXT,
and `CAST(1e40 AS FLOAT32)` answered `1e+40` the same way, because those four
spellings named a type the cast did not convert to.

(PostgreSQL has no integer-to-date cast at all — `3000000000::date` is `42846`
there — so that one is a wadjet superset, and inside a superset a value it
cannot represent is loud. `3000000000::int4` is `22003 integer out of range` on
PostgreSQL, which is where `::INT32`'s bound comes from.)

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

A container the reference QUALIFIES answers too: `(x.c_row).b` reads the field
of `x`'s container — PostgreSQL's spelling for a container two relations both
publish — and the nested `((c_rownest).s).x` reads a field of a field. The
qualified reference is resolved as any column reference is and the field is
read from its value (ADR-0022's 2026-09-23 amendment).

**A ROW an aggregate CONSTRUCTS.** Until `OHLCV` (see
[sql-reference.md](sql-reference.md) §OHLCV) every ROW value in a result had
been read out of some table's column, so its field names and their ORDER could
be recovered from the catalog. An aggregate that builds one is described by no
catalog, and a composite renders in DECLARED field order — `(open, high, low,
close, volume, vwap)`, which is not alphabetical — so the declaration travels
with the RESULT instead: `wadjet.ColumnMeta.Fields` carries a ROW column's
field list, and every door reads it before falling back to the catalog. A
renderer with no declaration can only sort the keys, which produces a
well-formed row carrying the right values in the wrong places.

On the wire a ROW column declares OID **25 (text)** and its value is
PostgreSQL's own composite text: parentheses, comma-separated fields, an EMPTY
slot for a NULL field, and a field quoted (with `"` doubled) when it contains a
comma, a quote, or is empty.

```
(10.00,21.00,7.00,21.00,24,14.583333)     a bar
(1,,3)                                     the middle field is NULL
("a,b","has ""quote""","")                 quoting
```

PostgreSQL declares `record` = OID 2249 for an anonymous composite. Declaring
25 instead is recorded in ADR-0012's divergence list; the VALUE — which is what
a client parses — is PostgreSQL's, byte for byte.

**An ARRAY column declares the array OF its element** (#992). PostgreSQL has no
generic `array` type, so the OID is the element's own array type and a typed
client (`getArray` in JDBC, pgx's array scanning, DataGrip's column typing)
reads a real array rather than a string:

| Element | Wire type | OID |
|---|---|---|
| `Int32`, `Port`, `Protocol` | `integer[]` | 1007 |
| `Int64`, `Duration` | `bigint[]` | 1016 |
| `Float32` | `real[]` | 1021 |
| `Float64` | `double precision[]` | 1022 |
| `String`, and every type rendered as text (`IPv4`, `IPv6`, `CIDR`, `MAC`, `Vector`) | `text[]` | 1009 |
| `Bool` | `boolean[]` | 1000 |
| `Bytes` | `bytea[]` | 1001 |
| `Timestamp` | `timestamp[]` | 1115 |
| `Date` | `date[]` | 1182 |
| `Decimal` | `numeric[]` | 1231 |
| `UUID` | `uuid[]` | 2951 |

The BINARY format carries PostgreSQL's array wire form under those OIDs, and
the text format is PostgreSQL's `array_out`: `{…}`, an element quoted (with `"`
and `\` escaped) when it is empty, contains a delimiter, brace, quote,
backslash or space, or is the word `NULL`; a NULL element as bare `NULL`; a
`TIMESTAMP` or `DATE` element in its text form (`{"2024-06-15 12:30:45.5",NULL}`).
Two element kinds keep OID 25, and both are a fact about PostgreSQL rather than
a gap: a NESTED array, because PostgreSQL's `int4[][]` is rectangular and this
engine's nested arrays are ragged (`{{1,2},{3}}` is a value here and a syntax
error there; it renders bare, as `array_out` does), and a `ROW` or `MAP`
element, which would need a registered composite OID (an array of ROW renders
`{"(1,a)","(2,\"b c\")"}`).

**Every producer declares the same way** (ADR-0045). The declaration is not a
property of a stored column: the `ARRAY[…]` constructor, an array cast
(`'{1,2}'::int[]`), a container-returning function (`tcp_flags` is `text[]`;
`map_keys`/`map_values` are arrays of the MAP's key/value type, in its stored
order; `map_entries` an array of `(key,value)` composites), a column read
through a derived table, a CTE, `VALUES` or a `UNION`, and a ZERO-ROW result
all declare the element and render as above. So an array read back through a
derived table is still an array — `v[1]` is its element (a `TIMESTAMP` element
is a timestamp, not an integer), `2 = ANY(v)` compares elements — and `ORDER
BY`, `MIN`, `MAX`, `DISTINCT`, `GROUP BY`, a window's `ORDER BY` and
`PARTITION BY`, a join key, the comparison operators (`=`, `<>`, `<`, `<=`,
`>`, `>=`), `BETWEEN`, `IN`, a simple `CASE`, `IS [NOT] DISTINCT FROM`,
`GREATEST`, `LEAST`, `NULLIF`, and `IN` / `= ANY` / `<> ALL` over a subquery
compare arrays ELEMENT-WISE as PostgreSQL does — one kernel for all of them,
under each operand's declared element, whatever produced the operand (a
column, an expression, a subquery, an aggregate, a window, a LATERAL): a
`numeric` element orders as a number, and two `numeric` elements of different
scales by value (`ARRAY[10.00] = ARRAY[10.0000]` is true, as a join key, an
`IN` member and a `UNION` member too); an empty array first, a shorter prefix before a longer array,
a NULL element after every value and equal to another NULL element
(`ARRAY[1,NULL] = ARRAY[1,NULL]` is true). A multi-dimensional array orders by
its flattened elements, then their count, then its dimensions
(`{{1,2},{3,4}}` > `{{1,2,3}}`), as PostgreSQL's does. Two arrays whose ELEMENT
types differ meet at ONE common element type wherever they meet — a comparison,
`IN` / `= ANY`, a hash or sort-merge join key, `UNION` / `INTERSECT` / `EXCEPT`,
`CASE`, `COALESCE`, `GREATEST`, `LEAST`, `ARRAY[a, b]` — PostgreSQL's numeric
promotion over the element: `int4[]` and `bigint[]` meet at `bigint[]`, an
integer and a `numeric` at `numeric` (exactly), an integer, a `numeric` or
nothing wider than `real` and `real` at `real` (`int4[]` and `real[]` meet at
`real[]`, not `double precision[]`), anything and `double precision` at
`double precision`, two `numeric(p,s)` at their common `numeric(p,s)` (the
larger scale, so no digit is rounded: `CASE … THEN numeric(5,2)[] ELSE
numeric(9,4)[] END` keeps `{1.2345}`). PostgreSQL has no `int[] = float8[]`
operator without the coercion; this engine answers the comparison it would
make after it. Arms with no common element type are `42804`. `CAST(container AS TEXT)` (and `VARCHAR(n)`) is the same
rendering under the operand's DECLARED element, whatever expression built it —
`CAST(ARRAY[COALESCE(ts, …)] AS TEXT)` is `{"2024-01-01 01:00:00"}` and
`CAST(ARRAY[d] AS TEXT)` `{2024-01-02}`, and a subquery that returns the
array renders the same (`CAST((SELECT ARRAY[ts] …) AS TEXT)`); `CAST(container AS TEXT[])` converts
each element as a column of its type converts (a timestamp's text, not its
number) — a multi-dimensional array passes through unchanged (its
multi-dimensional semantics are not PostgreSQL's; see postgres-differences) — a
`numeric(p,s)[]` destination declares `numeric(p,s)` elements, and `CAST(container AS JSON)` is `to_json`'s text
(`["2024-01-01T01:00:00"]`). An `INTERVAL` element has no text form here and
the cast refuses (`0A000`); PostgreSQL prints `{01:00:00}`.

A MAP has no PostgreSQL type; it declares OID 25 and renders `{a: 1, b: 2}`
(`{}` when empty).

**The other doors render the same value.** The CLI's table and CSV output print
the PostgreSQL text above for every container. Its JSON output keeps a JSON
array / object (as PostgreSQL's `to_json` of an array is a JSON array), with
each temporal leaf in its text form. The HTTP query API and the async query
API return raw values: an ARRAY is a JSON array and a ROW or MAP a JSON object,
with each leaf boxed as a top-level value of its type is (a `TIMESTAMP` element
is epoch milliseconds there, as a `TIMESTAMP` column is).

A container value that reaches a TEXT column through some path that did not
carry its declaration is refused with a type-mismatch error rather than
published as text (ADR-0045 §2).

**Array functions:** `cardinality`, `element_at`, `array_contains`, `array_join`, `array_min`, `array_max`, `array_length`

**Map functions:** `map_keys`, `map_values`, `map_entries`, `map_from_entries`

**JSON extraction:** `json_extract`, `json_extract_scalar`, `json_array_length`, `json_valid`

Nested types round-trip through Parquet in both directions — written as the standard LIST/MAP/STRUCT shapes and detected by the same patterns on read — and render in display output. Containers may nest inside containers.

### Vector Type

| Type | Storage | Parquet | Use Cases |
|------|---------|---------|-----------|
| `VECTOR(N)` | N x float32 per row | FIXED_LEN_BYTE_ARRAY | Embeddings, similarity search |

`VECTOR(N)` stores fixed-dimension float32 vectors for embedding-based workflows. Each row occupies exactly N x 4 bytes with zero overhead.

A `VECTOR(N)` value has exactly N components. Write one in PostgreSQL's
`vector` spelling — `'[1,2,3]'` — and a value of any other width is refused at
every write door, never padded with zeros and never truncated. Text that is not
a vector — no brackets, a component that is not a number, `NaN` or an infinity
— is refused too.

| door | off-width value | malformed text |
|---|---|---|
| `INSERT`, `UPDATE`, `MERGE`, `COPY` | `22000`, `expected N dimensions, not M` | `22P02`, `invalid input syntax for type vector` |
| embedded ingest API | `22000`, `column "v": expected N dimensions, not M` | — (it takes Go values, not text) |
| `CAST(… AS VECTOR(N))` | `22000`, `expected N dimensions, not M` | `22P02`, `invalid input syntax for type vector` |

A query vector is written `CAST(ARRAY[0.1, 0.2, …] AS VECTOR(N))` (or
`CAST('[0.1,0.2,…]' AS VECTOR(N))`): the cast converts, as pgvector's does, and
a NULL element is `22004`.

`22000` with pgvector's wording is what PostgreSQL's `vector` extension answers
for `'[1]'::vector(2)`, and it is the class this engine means at every door.
The ingest API prefixes the column name because it takes a whole ROW, and which
column was wrong is the localization pgvector's own message does not carry; it
used to answer `22023` with a sentence of its own, so the same value had two
classes decided only by which door it arrived at.

A raw `[]byte` handed to the ingest API is held to the same width. A byte count
that is a whole number of `float32`s is a vector of the wrong width and takes
the refusal above; one that is not is not a vector of any width, and is `22023`
naming the byte counts.

```sql
INSERT INTO doc_embeddings (doc_id, embedding) VALUES (1, '[0.1,0.2,0.3]')
```

A set operation between two columns of DIFFERENT declared widths is refused
where it would have to carry both in one column: `VECTOR(2) UNION VECTOR(3)`
raises `22000` rather than truncating the wider arm. `INTERSECT` and `EXCEPT`
emit values from the left arm only and are unaffected. PostgreSQL answers the
`UNION` (its `vector` is one type with a width typmod, which the union drops);
wadjet's storage is fixed-width per column and has no carrier for a mixed-width
result, so it says so instead of answering something else.

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

**A `FLOAT64` result outside the type is SQLSTATE 22003, not an infinity.**
`+`, `-`, `*` and `/` follow PostgreSQL's own range rule: a non-finite result
computed from FINITE operands is `22003 value out of range: overflow`, and for
`*` and `/` a result that flushes to zero from non-zero operands is
`22003 value out of range: underflow`. `1e308 * 10`, `1e308 + 1e308` and
`1e308 / 0.5` are all refused, and so is a `SUM` or `AVG` whose running total
leaves the type. An infinity that ARRIVES as a value is not affected: a
`FLOAT64` column may hold `Infinity` or `NaN`, every operator over one answers,
and `SUM` over such a column answers `Infinity`. Unary minus has no rule at
all, because negation cannot leave the range.

**`SUM` over a `FLOAT32` accumulates at `real`'s width**, which is what
PostgreSQL's `sum(real)` does, in the grouped and the windowed spelling alike.
`AVG` over the same column does not: PostgreSQL's `avg(real)` is
`double precision` and totals each value at that width. One consequence is
worth knowing: a `real` total carries 24 bits, so the order the rows reach the
accumulator can move its last digits — over a five-row group crossing 2^24 the
distributed arms answer `1.6777226e+07` where the single-process one answers
`1.6777224e+07`. Both are the `real` sum of the same values under different
groupings; PostgreSQL's own parallel aggregate has the same property. `SUM`
over a `FLOAT64` moves in its tenth significant digit for the same reason.

**A numeric LITERAL is read from its own text.** `CAST(9007199254740993.25 AS
DECIMAL(30,2))` keeps every digit, including the ones past a double's reach.

The **windowed** spelling answers the same type and the same digits:
`SUM(x) OVER (…)` and `SUM(x) … GROUP BY` are one question written twice, and
the exact accumulator is the same one — in every frame form, including a
`ROWS`/`RANGE` frame that slides (its exit subtraction is exact too) and the
spilled evaluators. Before this, a windowed integer `SUM` accumulated in
float64: past 2^53 it lost digits, and because a float sum depends on the order
its rows arrive in, a distributed run could answer a *different* wrong number
each time.

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
