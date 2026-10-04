# How wadjet differs from PostgreSQL

PostgreSQL 17.11 is the SQL semantics authority. Its wire protocol is the contract. This list contains every deliberate difference; report any other difference as a bug. Each entry names its row in the [ADR-0012 divergence catalog](adr/0012-divergences/README.md), which records the cell, both answers, the SQLSTATE and the gate that pins it.

## Values

**AVG renders fewer digits.**

`AVG` over an integer or a `DECIMAL` column declares `numeric` with no fixed modifier, as PostgreSQL does, and the value is the same number; the rendered text carries scale 4 for an integer input and `min(s+4, 38)` for a decimal one, where PostgreSQL prints up to sixteen significant digits. `AVG(c_i32)`: wadjet `7497.6450`; PostgreSQL `7497.6449875724937862` for the same rows. (catalog: [aggregates-windows#r1](adr/0012-divergences/aggregates-windows.md#catalog); ADR-0012 §9/AVG)

**Decimal statistics use double precision.**

Decimal statistics (`STDDEV`, `VARIANCE`, `CORR`, `COVAR`, `MEDIAN`, `PERCENTILE`) use float64; PostgreSQL uses numeric. Fixed-point roots and running means are unavailable. (catalog: [aggregates-windows#r2](adr/0012-divergences/aggregates-windows.md#catalog); ADR-0012 §9/statistics)

**TIMESTAMP truncates microseconds.**

Millisecond storage truncates `.123456` return as `.123`; PostgreSQL retains `.123456`. (catalog: [temporal#r1](adr/0012-divergences/temporal.md#catalog); #692-residual)

**TIMESTAMP has no infinity.**

`'infinity'` and `'-infinity'` are refused as timestamp input (22007), and a binary timestamp parameter carrying PostgreSQL's infinity encoding (the int64 extremes) is refused at Bind (22023); PostgreSQL stores and returns both. (catalog: [temporal#r2, r3](adr/0012-divergences/temporal.md#catalog); #1266)

**Clock functions return zoneless UTC.**

`NOW`, `CURRENT_TIMESTAMP`, `PG_POSTMASTER_START_TIME`: millisecond timestamp OID 1114 versus PostgreSQL’s microsecond timestamptz OID 1184. No session zone exists; `CURRENT_DATE` uses UTC. (catalog: [temporal#r9, r10, r11](adr/0012-divergences/temporal.md#catalog); #544, #870)

**CHAR does not blank-pad.**

No blank-padded type exists. `CAST('ab' AS CHAR(4))`: wadjet `ab`; PostgreSQL `ab  `. The OID is 1043, not 1042; bare CHAR is unconstrained, not CHAR(1). (catalog: [text-collation#r5, r6, r7](adr/0012-divergences/text-collation.md#catalog); #708-residual)

**Decimal choices print one scale.**

Storage has one scale per column. `COALESCE(numeric(15,2), 12.3456789012345)`: the ADR’s column value prints `12.7500000000000` versus `12.75`, and the elements of a numeric[] array share one scale too: `ARRAY[n, 1]` over a numeric(10,2) prints `{2.25,1.00}` versus `{2.25,1}`. (catalog: [numeric-decimal#r18](adr/0012-divergences/numeric-decimal.md#catalog); #764)

**A decimal quotient keeps one scale.**

`m / i` over a numeric(10,2) 1.25 and an integer 3 prints `0.4166666666667` (max(6, s1 + p2 + 1) fraction digits, one scale per column) where PostgreSQL prints `0.41666666666666666667` (at least sixteen significant digits per value). A quotient over an operand a cast made exact — `CAST(i AS INTEGER) / n`, the cast under `NULLIF`, `COALESCE`, `CASE`, `GREATEST`, `LEAST`, `abs`, unary minus, integer or numeric arithmetic, as in `n / NULLIF(CAST(b AS BIGINT), 0)` and `(CAST(i AS INTEGER) * 1.0) / n`, a bare `CAST(… AS NUMERIC)` as in `CAST(i AS NUMERIC) / n`, or either under `abs`, `mod`, `round`, `ceil`, `floor`, `trunc` or `sign` as in `ceil(CAST(i AS NUMERIC)) / n` — keeps the double precision quotient (OID 701, `1.3333333333333333`, `1.111111111111111e-09`) where PostgreSQL answers numeric `1.3333333333333333`, `0.0000000011111111111111111111`, because the one-scale quotient would drop digits the double carries. A quotient of two constants is the double too, an integer literal past the bigint range among them (`9223372036854775808 / 2` prints `4.611686018427388e+18`, PostgreSQL `4611686018427387904`); the same literal over a column is the one-scale numeric (`n / 9223372036854775808` prints `0.0000000000000000002439`, PostgreSQL `0.000000000000000000243945488809238498`). An alias does not change a quotient's type: `7 / t.n AS x` prints `3.11111111111` as `7 / t.n` does. (catalog: [numeric-decimal#r19](adr/0012-divergences/numeric-decimal.md#catalog); #1422, #1450)

**Multi-statement strings are not transactions.**

No transactions exist; BEGIN/COMMIT are ignored. `INSERT …; SELECT 1/0`: wadjet retains the insert; PostgreSQL rolls it back. (catalog: [parameters-pgwire#r1, r2](adr/0012-divergences/parameters-pgwire.md#catalog); #711)

**Integer shifts operate on 64 bits.**

`BITWISE_LEFT_SHIFT(2147483647, 2)` gives `8589934588` declared bigint; PostgreSQL's `2147483647 << 2` gives `-4`, because its int4 shift is modular. The operator spellings `<<`, `>>` and `&` are not accepted here (42601); use `BITWISE_LEFT_SHIFT`, `BITWISE_ARITHMETIC_SHIFT_RIGHT` and `BITWISE_AND`. (catalog: [extensions#r15](adr/0012-divergences/extensions.md#catalog); #1018-shifts)

**TO_HEX prints a 64-bit word.**

Integer arguments arrive widened. `TO_HEX(int32_col)` for −1 returns `ffffffffffffffff`; PostgreSQL returns `ffffffff`. (catalog: [extensions#r18](adr/0012-divergences/extensions.md#catalog); #966-TO_HEX)

**Declared DDL returns SELECT 1.**

Declared CREATE/DROP TABLE uses row results; PostgreSQL sends DDL tags without rows. (catalog: [dml-assignment#r10](adr/0012-divergences/dml-assignment.md#catalog); ADR-0012 §13/#1024-tags)

**`LIKE` does not honour the default backslash escape.**

`'a%b' LIKE 'a\%b'` answers `f` where PostgreSQL 17.11 answers `t`: this engine's LIKE reads `\` as an ordinary character, so a pattern that escapes a wildcard with the DEFAULT escape matches nothing. Write the escape explicitly — `LIKE 'a!%b' ESCAPE '!'` — which is read exactly as PostgreSQL reads it (#1169). Four matchers implement the pattern language (the scan's pushdown filter, the exec filter, the comparison kernel and the expression evaluator) and all four agree with each other; the default escape belongs to all four. (catalog: [text-collation#r16](adr/0012-divergences/text-collation.md#catalog); #1169-like-default-escape)

**`LOCALTIMESTAMP(p)`'s precision is accepted and ignored.**

`LOCALTIMESTAMP(0) = LOCALTIMESTAMP(6)` is `f` on PostgreSQL 17.11, which truncates the value to the requested precision, and `t` here: this engine renders an instant to milliseconds and has no per-call precision. The wire declares the base type either way. (catalog: [temporal#r12](adr/0012-divergences/temporal.md#catalog); #1169-localtimestamp-precision)

**`LOCALTIMESTAMP`, `CURRENT_TIMESTAMP` and `NOW()` are read PER ROW.**

PostgreSQL answers the statement's start time for every row, so `WHERE LOCALTIMESTAMP >= LOCALTIMESTAMP` selects every row. Here the clock is read where the expression is evaluated, so a row that straddles a millisecond can answer FALSE. (catalog: [temporal#r13](adr/0012-divergences/temporal.md#catalog); #1169-per-row-clock)

**An explicit integer CAST of a materialized float-carried numeric rounds half to even.**

`CAST(s.x AS INTEGER)` over `(SELECT DISTINCT 5 / 2.0 + t.id * 0 AS x FROM t) s` answers 2 where PostgreSQL answers 3, and so does the same column read from a derived table, an aggregate, a CTE, a set operation, VALUES, a window or a join on the single-process arms (on the stage DAG a derived table's or a CTE's such column reads NULL, a separate defect): the column is a float64 in the batch and carries no PostgreSQL category, so the cast rounds it by float8's rule. A cast whose operand computes the value — `CAST(5 / 2.0 AS INTEGER)`, `CAST(SQRT(6.25) AS INTEGER)`, `CAST(POWER(2.5, 1) AS INTEGER)`, negated, SMALLINT and BIGINT alike — rounds half away from zero, 3, as PostgreSQL does, and an assignment of the materialized column rounds 3 too (see "Division and the transcendental functions over numeric declare double precision"). (catalog: [dml-assignment#r2](adr/0012-divergences/dml-assignment.md#catalog); #1353-cast, ADR-0024 §2c)

**An integer CAST of a JSON field read reads the JSON number.**

`CAST(j->>'k' AS INTEGER)` over `{"k": 2.5}` answers 2 where PostgreSQL raises 22P02 (`->>` is text there, and `2.5` is not an integer's text); assigning `j->>'k'` or `j->'k'` itself to an integer column is 42804 on every write door, as in PostgreSQL. (catalog: [dml-assignment#r3](adr/0012-divergences/dml-assignment.md#catalog); #1353-json, #1406)

**An abbreviated CIDR value is rendered as it was written.**

`SELECT CAST('10/8' AS CIDR)` answers `10/8`, and a CIDR column written with `'10/8'` reads back `10/8`; PostgreSQL renders both `10.0.0.0/8`. The value compares equal to `10.0.0.0/8`, so only the text differs. Write the full network address if the rendered text matters. (catalog: [network#r4](adr/0012-divergences/network.md#catalog))

**Chained REAL arithmetic and REAL comparisons round once, not per operator.**

Over a real column holding 16777216, `(r + 1.0::real) + 1.0::real` answers 16777218 where PostgreSQL answers 16777216, and `WHERE r + 1.0::real = 16777216::real` selects no row where PostgreSQL selects one (`>` and a `CASE` condition diverge the same way). A single `real op real` in a select list rounds to real as PostgreSQL does; a nested step or a comparison keeps double-precision digits. Write `CAST(r + 1.0::real AS REAL)` at each step to get PostgreSQL's answer. (catalog: [numeric-decimal#r6, r7, r8, r9](adr/0012-divergences/numeric-decimal.md#catalog))

**Text concatenated with bytes splices the raw bytes.**

With `t = 'hi'` and `b = '\x6869'`, `t || b` answers `hihi` (length 4) and `b || t` answers `hihi`; PostgreSQL renders the bytea operand through its hex output and answers `hi\x6869` (length 8) and `\x6869hi`. Both declare text. Write `t || encode(b, 'hex')` to control the rendering explicitly. (catalog: [text-collation#r12, r13](adr/0012-divergences/text-collation.md#catalog))

**A quoted literal beside a bytea `||` contributes its spelling.**

`b || '\x41'` appends the four characters `\`, `x`, `4`, `1` here (`\x68695c783431`), where PostgreSQL reads the unknown literal as bytea and appends one byte (`\x686941`). Write `b || DECODE('41', 'hex')` to append the byte. (catalog: [text-collation#r14](adr/0012-divergences/text-collation.md#catalog))

**`TABLESAMPLE SYSTEM` samples blocks of rows, not pages.**

`SYSTEM (50)` keeps or drops each block of 2048 rows, as the scan reads them, as a whole — on a cluster each worker's scan task samples its own files the same way; PostgreSQL keeps or drops heap pages. Both answer a random subset; `SYSTEM (0)` and `SYSTEM (100)` answer no row and every row on both. (catalog: [other#r20](adr/0012-divergences/other.md#catalog); #1411)

**A text-typed parameter is a `TABLESAMPLE` percentage.**

`TABLESAMPLE BERNOULLI ($1)` with `$1` declared text (OID 25) and bound `'100'` answers every row here; PostgreSQL raises 42804 (the argument must be real). Bind renders a text parameter as an untyped literal, which the argument reads through real's input. (catalog: [parameters-pgwire#r16](adr/0012-divergences/parameters-pgwire.md#catalog); #1411)

## Declared types

**Some address and UUID functions declare text; assigned to a typed column, their text is read as a literal.**

`INT_TO_IP(n)`, `IP_ADD(ip, n)`, `IP_SUBTRACT(ip, n)`, `MASK_IP(ip, bits)`, `IP_SUBNET(ip)`, `IP_NETMASK(cidr)`, `NETWORK_ADDRESS(cidr)`, `BROADCAST_ADDRESS(cidr)` and `UUID()` declare TEXT (OID 25) though each value is an address or a UUID — PostgreSQL's analogues are typed `inet` / `uuid`. Assigned to a typed column by any write (`INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE`, `MERGE`), such a call is read by the column's input function, as a quoted literal is, where a genuine text expression is 42804: a superset, and the one exception the assignment table keeps. Every date/time function declares and produces its own type (sql-reference.md, Declared types). (catalog: [network#r15, r16](adr/0012-divergences/network.md#catalog); #1254-siblings)

**`DATE_ADD` over text is a timestamp.**

`DATE_ADD(x, n)` and `DATE_SUB(x, n)` (this engine's own functions; PostgreSQL has neither) answer a DATE only for a DATE `x` shifted by whole days; over text — `DATE_ADD('2026-03-03', 1)` — the result is a TIMESTAMP (`2026-03-04 00:00:00`), PostgreSQL's preferred datetime type for an unknown-typed argument. They used to answer text that rendered a date or an instant by the spelling of the input. (catalog: [temporal#r14](adr/0012-divergences/temporal.md#catalog); #1254-siblings)

**`tcp_flags` and the MAP functions have no PostgreSQL spelling; their arrays are PostgreSQL's.**

`tcp_flags(18)` is `text[]` (OID 1009) rendering `{SYN,ACK}`; `map_keys`/`map_values` are arrays of the MAP's key/value type in the MAP's stored order, and `map_entries` is an array of `(key,value)` composites (OID 25, as every ROW-element array is). (catalog: [containers#r16](adr/0012-divergences/containers.md#catalog); ADR-0045)

**A MAP renders as `{k: v, …}` under OID 25.**

PostgreSQL has no MAP type, so the rendering is this engine's rule: `{a: 1, b: 2}`, `{}` for an empty map. (catalog: [containers#r17](adr/0012-divergences/containers.md#catalog); ADR-0045)

**An array of a network type declares `text[]`.**

`ARRAY[CAST('1.2.3.4' AS IPV4)]` is `text[]` (1009) rendering `{1.2.3.4}`; PostgreSQL's `ARRAY['1.2.3.4'::inet]` is `inet[]` (1041) with the same text. The scalar network types declare text as well. (catalog: [containers#r13](adr/0012-divergences/containers.md#catalog); ADR-0045)

**`x::int[]` is `bigint[]`, and a fractional literal array is `double precision[]`.**

An array cast's element follows the scalar cast of the same spelling, and `CAST(x AS INT)` is bigint here (ADR-0012 item 12), so `ARRAY[]::int[]` declares 1016 where PostgreSQL declares 1007. `ARRAY[1.5, 2.25]` is `float8[]` (1022) where PostgreSQL's is `numeric[]` (1231) — ADR-0024's literal deferral; the text `{1.5,2.25}` agrees. (catalog: [containers#r14, r15](adr/0012-divergences/containers.md#catalog); ADR-0045)

**Division and the transcendental functions over numeric declare double precision.**

`5 / 2.0`, `SQRT(6.25)`, `POWER(2.5, 1)`, `EXP`, `LN`, `LOG` and `EXTRACT` over numeric operands are computed in float64 and declared `double precision` (OID 701) where PostgreSQL computes and declares `numeric` (OID 1700); `2.5 * 1` and `ABS(2.5)` are numeric on both. The value agrees to float64's precision. Assigned to an integer column by any write, such a value rounds as the numeric it is in PostgreSQL — half away from zero, `5 / 2.0` stores 3 — because the declaration carries PostgreSQL's category beside the carrier. (catalog: [dml-assignment#r1](adr/0012-divergences/dml-assignment.md#catalog); ADR-0024 §2c, #1353)

**Integer expressions declare bigint.**

This keeps execution paths consistent. `CAST(SUM(a) OVER () AS INTEGER)` declares OID 20 rather than PostgreSQL’s 23; SMALLINT/INT2 also widen: int16 storage is unavailable. (catalog: [numeric-decimal#r1, r2](adr/0012-divergences/numeric-decimal.md#catalog); #1070)

**ROW declares text.**

Composite text agrees; wire mapping uses OID 25 versus record OID 2249. (catalog: [containers#r2](adr/0012-divergences/containers.md#catalog); ROW)

**Some ARRAY results still declare text.**

Nested arrays and ROW/MAP elements use OID 25; ordinary arrays — stored, constructed, returned by a function, read through a derived table, VALUES or UNION, and a zero-row result — use PostgreSQL's array OIDs. A nested array renders as PostgreSQL's array_out does (`{{1,2},{3,4}}`) but may be ragged (`{{1,2},{3}}`), which PostgreSQL cannot represent. Nested arrays are stored and rendered, and they order as `array_cmp` does (the flattened elements, then their count, then the dimensions); their multi-dimensional SEMANTICS are not PostgreSQL's. `unnest` of a stored nested column yields its inner arrays (`unnest(v)` over `{{1,2},{3,4}}` is two rows `{1,2}`, `{3,4}`; PostgreSQL yields the four leaves), and of a constructed or cast one refuses 0A000; `cardinality(ARRAY[ARRAY[1,2],ARRAY[3,4]])` is 2 (PostgreSQL 4); `array_length(…, 2)` is NULL (PostgreSQL 2); `array_to_string(…, ',')` joins the inner arrays (`[1 2],[3 4]`; PostgreSQL `1,2,3,4`); `3 = ANY (ARRAY[ARRAY[1,2],ARRAY[3,4]])` refuses 0A000 (PostgreSQL true); `array_ndims` / `array_dims` do not exist here (42883). A cast of a nested array into `T[]` passes the value through unchanged (`CAST(ARRAY[ARRAY[1.5,2.5]] AS INT[])` is `{{1.5,2.5}}`; PostgreSQL `{{2,3}}`), and a text literal spelling one (`CAST('{{1,2},{3,4}}' AS INT[])`) is that text (OID 25). A subscript reads the OUTER array (`(ARRAY[ARRAY[1,2]])[1]` is `{1,2}`; PostgreSQL's one-subscript read of a 2-D array is NULL). Bringing these to PostgreSQL's flat N-dimensional semantics is #1337. (catalog: [containers#r3, r4, r5, r6, r7, r8, r9, r10, r11, r12](adr/0012-divergences/containers.md#catalog); #992-residuals, ADR-0045 §2)

**TIME, JSON and XML casts declare text.**

These casts pass text through. `CAST('12:34:56' AS time)` returns `12:34:56` on both engines, but wadjet declares OID 25. (#652)

**json_build_object declares text.**

`json_build_object` writes PostgreSQL's object text but declares text (OID 25) where PostgreSQL declares json (114). A json value that reaches it declared text — a derived table's, a CTE's or a stored column, a scalar subquery over a table — is therefore written as a JSON string (`{"o" : "{\"a\" : 1}"}`), where PostgreSQL nests the object; a `json_build_object` or a `CAST(… AS JSON)` in the argument itself nests. Being text, the object can be compared and grouped, where PostgreSQL raises 42883 for json. (catalog: [other#r16](adr/0012-divergences/other.md#catalog); #1470)

Comparing arrays whose element types differ within the numeric family (`ARRAY[1.5] > ARRAY[1]`) answers by the numbers, where PostgreSQL has no `numeric[] > integer[]` operator and raises 42883. (ADR-0045)

Arrays whose element types have NO common type — an integer array beside a text array — are not yet refused as PostgreSQL refuses them (42804 for `CASE`, `COALESCE` and `UNION`; 42883 for `=`): `CASE WHEN true THEN ARRAY[1] ELSE ARRAY['a'] END` and `COALESCE(ARRAY[1], ARRAY['a'])` answer `{1}` as `integer[]`, `ARRAY[1] = ARRAY['a']` answers false, and `SELECT ARRAY[1] UNION SELECT ARRAY['a']` is 42000 (`cannot store string into INT32 vector`) — 22P02 for a `numeric` array beside the text one. At v0.25.0 the `CASE` and the `UNION` answered Go-rendered text (`[1]`, `[a]`) and the `COALESCE` was 42000. Recorded for repair, not a kept superset.

`CAST(ARRAY[1,2] AS JSON)` is `[1,2]` — `to_json`'s text, which `json_array_length` and the other JSON functions read — where PostgreSQL has no cast from `integer[]` to `json` and raises 42846; a ROW is its `to_json` object. Every other non-text destination of a container is 42846 as on PostgreSQL. `CAST(ARRAY[1,2] AS VECTOR(2))` converts as pgvector's cast does. (ADR-0045)

An array of `INTERVAL` has no text here: `CAST(ARRAY[INTERVAL '1 hour'] AS TEXT)` (and `AS JSON`, `AS TEXT[]`) raises 0A000 where PostgreSQL prints `{01:00:00}` — this engine has no interval text form for a container element (a scalar `INTERVAL` prints its own text, `1 day`). Everywhere else — a projection nothing reads, `=`, `<`, `GROUP BY`, `DISTINCT`, `UNION`, a hash or sort-merge join key, `MIN`/`MAX` — an INTERVAL element declares this engine's DURATION (nanoseconds; #351 above) and compares, groups and orders BY VALUE (PostgreSQL's own `interval_cmp`: a month is 30 days), never by its rendered text; a bare `SELECT ARRAY[INTERVAL '1 hour']` with no CAST answers that nanosecond count (`[3600000000000]`) rather than raising 42000 as it used to. An interval that already became text before the array was built (through a derived table or a scalar subquery) is that text. (catalog: [other#r3, r5, r6, r7, r8, r9, r10, r11](adr/0012-divergences/other.md#catalog); ADR-0045 §1)

**Decimal set operations keep one declared scale.**

Storage requires one scale. PostgreSQL declares unconstrained numeric; wadjet retains `(p,s)` and prints `12.7500` where PostgreSQL prints `12.75`. (catalog: [set-operations#r4](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/decimal-carrier)

**Mixed int4-family sets declare bigint.**

PORT with INT32/PROTOCOL uses the common integer representation, OID 20; PostgreSQL’s int4 pair stays OID 23. (catalog: [set-operations#r5](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/integer-width)

**CTAS stores constrained decimals.**

Stored columns require `(p,s)`. A CTAS over `COALESCE(numeric(15,2), numeric(38,10))` stores DECIMAL(38,10); PostgreSQL stores unconstrained numeric (`12.7500000000` versus `12.75`). (catalog: [dml-assignment#r11](adr/0012-divergences/dml-assignment.md#catalog); ADR-0012 §13/#1024-CTAS)

**OHLCV decimal fields retain their typmod.**

Fields retain storage types. `(b).open` over DECIMAL(9,2) has typmod 589830; PostgreSQL’s corresponding composite aggregate field has −1. (catalog: [extensions#r8](adr/0012-divergences/extensions.md#catalog); ADR-0012 §9/#965-field-typmod)

**A star over a LATERAL whose body is itself `SELECT *` qualifies a name the two arms share.**

`SELECT * FROM lt_o o JOIN LATERAL (SELECT * FROM lt_i i WHERE i.k = o.k) s ON true` names the body's `id` and `k` `s.id` and `s.k` on every execution path, where PostgreSQL names them `id` and `k`. A star over a LATERAL is expanded into the FROM items' own lists, but a body whose own list is a star is not enumerated there, so that arm is read off the join's stream, which qualifies a duplicate name by its owning alias. Values, types and positions agree. A body that names its columns publishes PostgreSQL's names. (catalog: [lateral-subqueries#r15](adr/0012-divergences/lateral-subqueries.md#catalog); #1126)

**`regexp_count` declares bigint.**

`SELECT regexp_count('abab', 'a')` declares `bigint` (OID 20); PostgreSQL declares `integer` (OID 23). The value is the same. `SUM` over it declares `bigint`, as in PostgreSQL; write `CAST(regexp_count(…) AS integer)` where the column type matters to a client. (catalog: [extensions#r21](adr/0012-divergences/extensions.md#catalog))

**MIN and MAX over an integer scalar subquery declare bigint.**

`SELECT MIN((SELECT CAST(1 AS INT))) FROM users` answers `1` declared `bigint` (OID 20); PostgreSQL declares `integer` (OID 23). The values agree. Cast the result to `integer` where the declared type matters. (catalog: [extensions#r22](adr/0012-divergences/extensions.md#catalog))

**PORT and PROTOCOL declare integer; DURATION declares bigint nanoseconds.**

`SELECT p, pr, d FROM np` over PORT, PROTOCOL and DURATION columns declares `integer`, `integer` and `bigint` and prints `443`, `6`, `1500000000`; PostgreSQL has none of these types. A DURATION counts nanoseconds and is not declared `interval`, whose text and unit differ. A quoted value that is no number, `p = 'abc'`, is 22P02 `invalid input syntax for type integer`. (catalog: [parameters-pgwire#r3](adr/0012-divergences/parameters-pgwire.md#catalog); #834)

**A parameter whose type this engine lacks declares the nearest one.**

A parameter declared `smallint` declares `integer` (no smallint type); one declared `timestamptz` is the instant at UTC, the session TimeZone the server reports, and declares `timestamp` (`'2024-03-04 17:00:00+05'` is `2024-03-04 12:00:00`, compared as PostgreSQL compares it; a numeric offset is applied, a zone name such as `UTC` or `America/New_York` is refused 22007); one declared `bytea`, `integer[]` or `text[]` declares `text`, and an array parameter in binary format is refused 22023. (catalog: [parameters-pgwire#r8, r9, r10](adr/0012-divergences/parameters-pgwire.md#catalog); #1410, #1426)

**The expression around a parameter keeps this engine's declaration.**

A parameter takes PostgreSQL's type for its position — `SELECT $1 + 1` and `WHERE $1 = n * 2` type `$1` integer, PostgreSQL's operator type over the operands — but integer arithmetic, an integer `CAST` and `NTILE` declare `bigint` here, so that result is `bigint` where PostgreSQL's is `integer`, and `WHERE m = $1` over a derived `n * 2 AS m` types the parameter `bigint`. `SELECT $1` declared integer (or smallint) and bound a negative value executes as `bigint` while the statement's Describe declared `integer`: eight bytes in a binary row where PostgreSQL sends four (two), and a client that decodes binary rows by that Describe refuses the value. A parameter beside an aggregate (`HAVING SUM(f) > $1`) or among a table function's arguments (`generate_series(1, $1)`) is left undetermined (OID 0), and the Describe of that table function over a parameter answers no columns. (catalog: [parameters-pgwire#r12, r13](adr/0012-divergences/parameters-pgwire.md#catalog); #1410)

## Errors and refusals

**A text-typed parameter assigned to an integer column is 22P02.**

Bind renders a parameter declared text (OID 25) as a quoted literal, which the integer column's input function reads, so a MERGE `SET n = $1` bound as the text `2.5` raises 22P02 where PostgreSQL raises 42804 (text is not assignable to integer without a cast); neither writes. A float8 or numeric parameter keeps its type and rounds by it. (catalog: [dml-assignment#r4](adr/0012-divergences/dml-assignment.md#catalog); #1353-param, #1408)

**A binary bytea parameter holding a backslash is 22P02.**

`WHERE bt = $1` with `$1` declared `bytea` and bound in binary as the bytes `00 ff 5c` raises 22P02: the bytes are spliced into a literal that the BYTES comparison reads through bytea input again. PostgreSQL matches the row. (catalog: [parameters-pgwire#r11](adr/0012-divergences/parameters-pgwire.md#catalog); #1410)

**Arithmetic over a text expression is evaluated.**

Arithmetic between a bare text column and a number is 42883, as in PostgreSQL, but a text EXPRESSION is read as its number: `UPPER(x) * 2` over `'12'` answers 24, `-x` answers -12, and `CAST(n AS TEXT) * 2`, `CASE … x END * 2` and `(x || '') * 1` answer likewise, and every write stores the same value (MERGE `SET n = UPPER(s.x) * 1` stores 12), where PostgreSQL raises 42883 for each. (catalog: [dml-assignment#r5, r6](adr/0012-divergences/dml-assignment.md#catalog); #1353-text-expr, #1409)

**A WHEN NOT MATCHED clause that no row reaches is not resolved.**

`MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, (SELECT s2.i FROM s s2 WHERE s2.id = t.id))` answers `MERGE 0` and writes nothing when every source row matches, where PostgreSQL raises 42P01 (the target is not visible under WHEN NOT MATCHED) at parse. When a row reaches the clause it is 42P01 here too. (catalog: [dml-assignment#r7](adr/0012-divergences/dml-assignment.md#catalog); #1043)

**`SUBSTRING(text SIMILAR pattern ESCAPE escape)` is refused.**

The standard's capture-marker spelling raises 0A000 naming the construct, where PostgreSQL 17.11 answers the part of the string between the pattern's `#"` markers: this engine translates a SIMILAR TO pattern into a regular expression and that translation has no notion of a returned portion. `SUBSTRING(text FROM regexp)` and `REGEXP_EXTRACT(text, regexp, group)` both answer. (catalog: [text-collation#r17](adr/0012-divergences/text-collation.md#catalog); #1169-substring-similar)

**A SIMILAR TO pattern the translation cannot compile is 2201B with this engine's own message.**

`'abc' SIMILAR TO '*'` and `'abc' SIMILAR TO '['` raise 2201B on both engines; the message names the pattern here and names the regex engine's own complaint there. (catalog: [text-collation#r18](adr/0012-divergences/text-collation.md#catalog); #1168)

**A repeated name in a column-alias list is refused at the list.**

`FROM t AS a(k, k)` raises 42701 naming the spelling, where PostgreSQL accepts the list and raises 42702 at every reference. This engine renames positionally and cannot publish one name for two columns. On a TABLE FUNCTION whose relation is narrower than the list, PostgreSQL raises 42P10 for the same statement. (catalog: [names-scopes#r13](adr/0012-divergences/names-scopes.md#catalog); #959, #1184)

**A file or database reader whose input is not readable at plan time is measured when it produces its first batch.**

A reader over a REGULAR file publishes its columns at PLAN time: `read_parquet` from the file's footer, `read_json` and `read_csv` from a SAMPLE of its first 100 rows, read before the statement binds and only after the table-function capability has been authorized for the calling identity (ADR-0039 §3, ADR-0034). Over such a reader `FROM read_json(…) AS f(a, b, c)` is 42P10 at plan time and an unknown column is 42703 at plan time, exactly as over a base table. A non-NULL value past the sample that does not fit the inferred type refuses, naming the reader, file, row, column and types, with the SQLSTATE PostgreSQL's COPY raises for the same field (`22P02`, `22003` out of range, `22007` for a timestamp); a `read_csv` field is read with PostgreSQL's input function for bigint, double precision and boolean. A timestamp column takes only the sample's own spellings (`Jan 2 2024` is `22007` where PostgreSQL reads it) and an inet column refuses `10.0.0.1/32`. A `read_json` key first seen past the sample is `22P04` and a nested field first seen past it `22P02`; `sample_size = -1` types the columns from every row instead (about 2× a read for CSV, 5× for JSON); see [the SQL reference](sql-reference.md).

Four inputs are not read at plan time and keep the first-batch behaviour: an `http(s)` source — a plan-time fetch would be a second request for every statement and would make `EXPLAIN` reach the network; the database connectors (`postgres_scan`, `postgres_query`, `mysql_scan`, `mysql_query`), whose schema is a remote query's; an input that can only be read ONCE (a FIFO, `/dev/stdin`, a socket, a process substitution), because the plan-time read opens the input and the execution opens it again; and a glob any of whose matches is one of those. For those, `42P10` and `42703` are raised at execution rather than while the statement is bound, a reader that produces NO batch is never measured against its alias list, and `EXPLAIN` over such a statement does not refuse. (catalog: [table-functions#r1, r2, r3](adr/0012-divergences/table-functions.md#catalog); #1184, #1210, #1230)

**`read_csv` reads `COPY … (FORMAT csv)`'s grammar, except that blank lines are skipped, line endings may be mixed, a trailing empty field is dropped, `\.` is data and a byte-order mark is skipped.**

A field is NULL only when it is empty and unquoted (`""` is the empty string), a quote opens anywhere in a field, whitespace is data, and an unterminated quote and a record of the wrong width are `22P04` — as `COPY` reads the same bytes. A blank line in a multi-column file (a trailing one included) is skipped, where `COPY` raises `22P04 missing data`, LF, CR and CRLF may be mixed in one file, where `COPY` raises `22P04 unquoted carriage return found in data`, and a trailing delimiter whose extra fields are empty (`1,x,`) reads as the record without them, where `COPY` raises `22P04 extra data after last expected column` — blank lines, mixed LF/CRLF endings and trailing empty fields retain their earlier readings; a lone CR now ends a record, where v0.24.0 read it into the field. A SHORT record stays `22P04`, as in `COPY`, although this reader used to pad it with NULLs. A line holding `\.` is DATA here, where PostgreSQL 17 ends the input at it and reads no later row (PostgreSQL 18 no longer does in a file); a UTF-8 byte-order mark at a file's start is skipped (with `header=false`, from the first data value), where `COPY` keeps it in the first field; and a NUL byte is stored where `COPY` raises `22021`. (catalog: [table-functions#r4, r5, r6](adr/0012-divergences/table-functions.md#catalog); #1248, #1259)

**A reader whose input cannot be opened is `58P01` / `42501` / `42809` at plan time, and `EXPLAIN` over it is refused.**

`SELECT * FROM read_json('/missing.json')` and `EXPLAIN` over it raise `58P01 could not open file "/missing.json" for reading: no such file or directory` — `COPY FROM`'s and `pg_read_file`'s class, as are `42501` for a file that may not be read and `42809` for a directory. PostgreSQL's `EXPLAIN` over a missing RELATION is `42P01`; the class here is the input's. An `http(s)` source is refused at its first batch (a 404 is `58P01`), so `EXPLAIN` over one prints a plan. (catalog: [table-functions#r7, r8](adr/0012-divergences/table-functions.md#catalog); #1245)

**A reader whose input declares no columns at all is a named refusal, where PostgreSQL has a zero-column relation.**

`SELECT * FROM read_json('<zero-byte file>')` raises `0A000 the table function "read_json" published no columns: its input "…" is empty`. PostgreSQL permits a relation with zero columns (`CREATE TABLE t (); SELECT * FROM t` answers zero rows of zero columns) and this engine does not, at any door — a result that declares no columns is not an answer it has. A Parquet file carries its schema in the footer and a CSV in its header row, so an empty file of either kind is an ordinary empty relation: zero rows, columns declared. (catalog: [table-functions#r9](adr/0012-divergences/table-functions.md#catalog); #1230)

**`generate_series(…) WITH ORDINALITY` publishes one column.**

PostgreSQL adds a second `ordinality` column to any function in FROM; this engine adds it for `unnest` only, so `generate_series(1,2) WITH ORDINALITY` publishes `generate_series` alone — and a two-name column-alias list over it is 42P10. (catalog: [table-functions#r10](adr/0012-divergences/table-functions.md#catalog); #1210-ordinality)

**An aggregate over an HTTP or database reader's column declares double precision.**

`SELECT SUM(a) FROM read_json('http://…')` declares and boxes float8, and `SELECT f.* FROM read_json('http://…') AS f` is 0A000, because those two input kinds are the ones with no plan-time column list for the result-type rules to read (above). Over a LOCAL file the reader's column carries its type: `SUM` over a whole-number column is `numeric` and `MIN`/`MAX` keep its width, which is what PostgreSQL declares for the same `bigint` column. The declared functions carry their width too: `generate_series(1,3)` publishes `integer` and its SUM is `bigint`. (catalog: [table-functions#r11](adr/0012-divergences/table-functions.md#catalog); #1211, #1230)

**`generate_series` never flips the caller's step.**

`generate_series(5,1)` is an EMPTY relation — zero rows of one column — on both engines; the descending series is `generate_series(5,1,-1)`. Through v0.22.0 this engine negated a positive default step whenever start > stop and answered the descending series instead. A zero step is 22023 with PostgreSQL's own sentence. (catalog: [table-functions#r12](adr/0012-divergences/table-functions.md#catalog); #1210-series)

**Some known casts leave values unchanged.**

`CAST('abc' AS DURATION)` and `CAST('abc' AS BYTES)` return `abc`: this engine has those types but does not convert text into them, so the operand passes through where PostgreSQL has no such type. VECTOR, container and network destinations convert or refuse: `CAST('abc' AS VECTOR(3))`, `CAST('abc' AS INTEGER[])` and `CAST('abc' AS IPV4)` raise 22P02 as PostgreSQL's input functions do, and `CAST(ARRAY[1,2] AS VECTOR(2))` is `[1,2]`. Recognizing a type name does not by itself perform a conversion. (catalog: [other#r4](adr/0012-divergences/other.md#catalog); #652)

**Undescribable results are refused.**

A `SELECT *` over a LATERAL whose own list names one column twice declares both columns with no rows, the second named `s.m` where PostgreSQL says `m` — the name the non-empty result gives it too (the list cannot be enumerated by name, so the star reads the join's output, which qualifies a duplicate name by its owning alias). Before 2026-09-25 the empty result declared the duplicate once: four columns for PostgreSQL's five. Over an UNGROUPED AGGREGATE body (`SELECT MAX(x) AS m, MIN(x) AS m …`) the zero-row star is `XX000`: the join carries the empty-input pad marker, which the declaration will not publish. A star over any other LATERAL — two or more of them, one beside another join, or an ungrouped aggregate — is expanded into the FROM items' own lists and declares its columns with no rows (#1013). Recursive CTEs now retain the seed's declared columns for an empty result (ADR-0021 §1o-b). (catalog: [names-scopes#r25, r26](adr/0012-divergences/names-scopes.md#catalog); #1008, #1010)

**Table metadata follows table access.**

Tables unavailable for reading are omitted from listings; DESCRIBE raises 42501. PostgreSQL exposes more catalog metadata; wadjet includes names in table access. (catalog: [other#r1](adr/0012-divergences/other.md#catalog); 2026-09-06/metadata)

**UNION refuses mixed VECTOR widths.**

Fixed-width UNION/UNION ALL raises 22000. PostgreSQL/pgvector drops the width modifier; its INTERSECT/EXCEPT behavior is unverified. (catalog: [set-operations#r1, r2](adr/0012-divergences/set-operations.md#catalog); #900)

**Derived names use expression spelling.**

Internal names must distinguish expressions. Wadjet accepts `"g + 1"` and refuses `"?column?"` with 42703; PostgreSQL accepts the published `"?column?"`. (catalog: [names-scopes#r9](adr/0012-divergences/names-scopes.md#catalog); #732-derived-names)

**Some quoted names are refused.**

Names become storage components: `/`, `\`, NUL, initial dots, `.` and `..` cause 42602 where PostgreSQL accepts the identifier. (catalog: [names-scopes#r11](adr/0012-divergences/names-scopes.md#catalog); 2026-09-05/names)

**Names over 63 bytes are refused.**

Wadjet raises 42622 instead of PostgreSQL’s truncation and notice, to keep storage locations distinct. (catalog: [names-scopes#r12](adr/0012-divergences/names-scopes.md#catalog); 2026-09-05/name-length)

**Ambiguous ORDER BY names select the first output.**

First-match binding means `ORDER BY u` over two outputs called `u` answers here, versus PostgreSQL 42702. (catalog: [names-scopes#r5](adr/0012-divergences/names-scopes.md#catalog); #557)

**Name lookup tolerates case differences.**

For imported names, `SELECT WatchID FROM hits` can read `WatchID` where PostgreSQL raises 42703. Tables/lowercase quoted names also qualify; case-colliding join columns cause 42702 where PostgreSQL answers. (catalog: [names-scopes#r1, r2, r3, r4](adr/0012-divergences/names-scopes.md#catalog); #731)

**Bare ROW field paths answer.**

`c_row.b` resolves a field; PostgreSQL raises 42P01 and requires parentheses. The parenthesised spellings, qualified `(x.c_row).b` and nested `((c_row).rw).k` included, answer as PostgreSQL does. (catalog: [containers#r1](adr/0012-divergences/containers.md#catalog); #769, ADR-0022)

**A boolean cast to a temporal type returns NULL.**

`CAST(true AS DATE)` answers NULL where PostgreSQL raises the type-pair error 42846 `cannot cast type boolean to date`; 22007/22008 describe text, not a boolean. A container source, `CAST(ARRAY[1] AS DATE)`, is refused 42846 as in PostgreSQL. (catalog: [temporal#r4](adr/0012-divergences/temporal.md#catalog))

**Integer-to-DATE casts answer.**

Signed 32-bit day counts work; out-of-range values raise 22003. PostgreSQL raises 42846, including for `0::date`. (catalog: [temporal#r5, r6](adr/0012-divergences/temporal.md#catalog); #911)

**DMY date errors differ.**

`'31/12/1996'` raises 22007 here versus PostgreSQL’s 22008 under ISO, MDY. Wadjet rejects DateStyle-dependent field order. (catalog: [temporal#r7](adr/0012-divergences/temporal.md#catalog); #840-temporal-grammar)

**SELECT compares numeric spelling with text.**

Generic comparison accepts `s = 1.50` as `s = '1.50'`; PostgreSQL raises 42883. DML predicates refuse this pair with 42883. (catalog: [comparison-membership#r1](adr/0012-divergences/comparison-membership.md#catalog); #504, #721)

**Unary minus accepts numeric text.**

Generic resolution means `SELECT -'5'` returns varchar `-5`; PostgreSQL raises 42725. Non-numeric text raises 22P02 here. (catalog: [numeric-decimal#r13](adr/0012-divergences/numeric-decimal.md#catalog); #505)

**A quoted number beside an integer is read as the number it spells.**

`MOD(8, '2.5')` returns the double 0.5 and `8 + '2.5'` returns 10.5; PostgreSQL resolves the unknown literal to `integer` and raises 22P02. A quoted integer keeps the integer type: `MOD(i, '3')` is `integer`, as on PostgreSQL. (catalog: [numeric-decimal#r21](adr/0012-divergences/numeric-decimal.md#catalog))

**HAVING resolves output aliases.**

Output aliases share the grouping scope. `SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 1` answers here; PostgreSQL raises 42703. (catalog: [names-scopes#r6](adr/0012-divergences/names-scopes.md#catalog); #591)

**BIGINT casts to BOOLEAN.**

The int4 rule extends to int8: zero=false, otherwise=true. PostgreSQL refuses the int8 cast with 42846. (catalog: [numeric-decimal#r14](adr/0012-divergences/numeric-decimal.md#catalog); #592)

**DECIMAL cannot store NaN or infinities.**

Finite storage raises 22003 versus unconstrained PostgreSQL numeric values. Comparisons remain available. (catalog: [numeric-decimal#r15, r16](adr/0012-divergences/numeric-decimal.md#catalog); #534; 12/carrier)

**JOIN ON sees comma-join siblings.**

An ON may name a relation an EARLIER comma-separated FROM item declares. `FROM a, b JOIN c ON a.k = c.k` answers here; PostgreSQL refuses the reference. (catalog: [names-scopes#r7](adr/0012-divergences/names-scopes.md#catalog); #617)

**A membership's numeric literal past 38 digits is refused.**

A numeric literal compared with a NUMERIC subquery — `x IN`, `= ANY`, `NOT IN`, `<> ALL (SELECT numeric …)`, the literal quoted, under a bare `CAST(… AS NUMERIC)` or unquoted — is the exact number its text spells, and so is one under a bare CAST or unquoted against an integer subquery (numeric = integer is numeric). One this engine's DECIMAL cannot hold exactly — more than 38 significant digits (`'14.0000000000000000000000000000000000000001'`), a digit past scale 38 (`'1e-40'`), an integer past 38 digits (`'1e40'`), NaN or ±Infinity — raises 22003 `numeric field overflow` naming the literal on every arm, rather than being compared at any other precision; PostgreSQL's numeric is unconstrained and answers (0 rows for `IN`, every row for `NOT IN`, against members it cannot equal). Zeros that carry no value do not count: `'12.5'` followed by forty zeros is 12.5 and answers. (catalog: [comparison-membership#r9](adr/0012-divergences/comparison-membership.md#catalog); #1372, #1388)

**A membership's outer computed by a numeric function is refused.**

An outer operand computed from numeric constants — `CASE … THEN 14.0000000000000000001 END`, `COALESCE(…)`, `-(-…)`, `CAST(CAST('…' AS TEXT) AS NUMERIC)`, arithmetic over them — is folded to the exact number it computes before it is compared with a NUMERIC or integer subquery. One that applies a function this engine computes in double precision to a numeric constant (`sqrt(12.5 * 12.5) IN (SELECT numeric …)`; exp, ln, power) is not folded, so it raises 0A000 naming the operand on every arm; PostgreSQL computes it as numeric and answers (every row here). An explicit float CAST, and a function PostgreSQL defines over double precision alone (sin, degrees, cbrt) called alone, are doubles in PostgreSQL too and answer; combined with a numeric constant (`sin(0.0) + 12.5`) the operand is refused 0A000 as well, where PostgreSQL answers (#1420). (catalog: [comparison-membership#r10, r11](adr/0012-divergences/comparison-membership.md#catalog); #1372, #1387)

**Decimal arithmetic stops at 38 digits.**

Precision stops at 38 while exact operators retain scale. DECIMAL(38,10) multiplication produces DECIMAL(38,20), raising 22003 beyond its range where PostgreSQL numeric answers. (catalog: [numeric-decimal#r17](adr/0012-divergences/numeric-decimal.md#catalog); #749)

**SUM overflow survives cancellation.**

Overflow remains recorded even after cancellation: `+9e37, +9e37, -9e37` fails here while PostgreSQL numeric answers. This prevents wrapping. (catalog: [aggregates-windows#r3](adr/0012-divergences/aggregates-windows.md#catalog); ADR-0012 §9/SUM-overflow)

**Computed SUM follows the argument's width.**

`SUM(b * 1)` and `SUM(ABS(b))` over a bigint column declare `numeric` and answer past bigint, as PostgreSQL does: the argument's width is read from the expression, and one int8 operand makes it numeric. Where the width cannot be inferred the result stays `bigint`, and a total beyond bigint raises 22003 where PostgreSQL answers a numeric; cast the argument to `numeric` to avoid it. (catalog: [aggregates-windows#r4](adr/0012-divergences/aggregates-windows.md#catalog))

**Qualified GROUP BY can refuse.**

`SELECT g + 1 ... GROUP BY typemx.g + 1` raises 42803 here; PostgreSQL answers. Evaluation requires the input’s unqualified name. (catalog: [names-scopes#r10](adr/0012-divergences/names-scopes.md#catalog); #738)

**VARCHAR declarations discard length.**

`CREATE TABLE t (v VARCHAR(4))` discards the length; an overlong INSERT answers here versus PostgreSQL 22001. Casts enforce length. (catalog: [text-collation#r8, r9](adr/0012-divergences/text-collation.md#catalog); #708-DDL)

**Integer calculations can exceed int4.**

`2147483647 + 1` returns `2147483648` versus PostgreSQL 22003. Widening also permits ABS predicates at int4’s minimum. (catalog: [numeric-decimal#r3, r4, r5](adr/0012-divergences/numeric-decimal.md#catalog); #1070; int4-store-residual)

**Text functions over BYTES refuse, except `strpos`.**

`upper(b)`, `lower(b)`, `trim(b)`, `reverse(b)`, `replace(b,…)`, `starts_with(b,…)`, `split_part(b,…)`, `lpad(b,…)`, `repeat(b,…)` and `char_length(b)` raise 42883 here as they do on PostgreSQL. `strpos(bytea,bytea)` still answers, because `POSITION(sub IN b)` — which PostgreSQL DOES have over bytea — is rewritten into it; one spelling answering where the other refuses is the residue. `ENCODE`/`DECODE` are the supported bridge. (catalog: [text-collation#r11](adr/0012-divergences/text-collation.md#catalog); #583)

**Text functions render a non-text COLUMN.**

`upper(mac_col)`, `substr(date_col, 1, 4)`, `length(ipv4_col)` and `upper(int_col)` answer here versus PostgreSQL 42883: a column of another type is rendered as its text before a string function reads it, which is what makes the network-analytics shapes work. A numeric LITERAL in the same position is 42883 on both, and so are a `BYTES` operand in a text-only position and a TEXT operand in `ENCODE`'s. (catalog: [text-collation#r15](adr/0012-divergences/text-collation.md#catalog); #500, #1056)

**Planner column-name prefixes are reserved.**

`SELECT amount AS __key_0` raises 42939 here; PostgreSQL answers. Intermediate columns need these names; stored columns remain readable. (catalog: [other#r12](adr/0012-divergences/other.md#catalog); #956)

**A BYTES, container or DURATION value is not assigned to a text column.**

`INSERT INTO t (text_col) SELECT bytes_col` (and the same through VALUES, UPDATE and MERGE) raises 42804 here; PostgreSQL converts a bytea, an array or an interval to its text. Every other scalar is assigned to TEXT as PostgreSQL renders it. (catalog: [dml-assignment#r13](adr/0012-divergences/dml-assignment.md#catalog); ADR-0012 §13/#1024-assignment)

**A quoted INSERT SELECT into a container column is refused.**

`INSERT INTO qa (a) SELECT '{1,2}'` into an ARRAY column raises 42804 rather than reading the text through PostgreSQL's array input conversion; write `ARRAY[1,2]` or an explicit cast instead. BOOL and BYTES targets read the quoted text as PostgreSQL does (`'true'` stores `t`, `'abc'` stores `\x616263`). (catalog: [dml-assignment#r14](adr/0012-divergences/dml-assignment.md#catalog))

**FROM_HEX can return NULL.**

`FROM_HEX('12zz')` returns NULL; PostgreSQL’s `decode('12zz','hex')` raises 22023. Wadjet follows its parse-or-NULL convention. (catalog: [extensions#r20](adr/0012-divergences/extensions.md#catalog); #966-FROM_HEX)

**Skipped CTAS sends no notice.**

`CREATE TABLE IF NOT EXISTS … AS SELECT` over an existing table sends PostgreSQL’s command tag but omits its notice; interfaces lack a shared notice channel. (catalog: [dml-assignment#r12](adr/0012-divergences/dml-assignment.md#catalog); ADR-0012 §13/#1024-notices)

**An invalid string modifier in DDL echoes the upper-cased token.**

`CREATE TABLE vt2 (v VARCHAR(abc))` raises 42601 as PostgreSQL does, but the message is `column "v": syntax error at or near "ABC"`: the DDL lexer folds an unquoted identifier to upper case before the type is read, while `CAST(x AS VARCHAR(abc))` echoes `"abc"`. The code and the rule are the same on both doors. (catalog: [text-collation#r10](adr/0012-divergences/text-collation.md#catalog))

**A `TABLESAMPLE` alias follows the clause.**

`FROM t TABLESAMPLE BERNOULLI (10) s` names the sampled relation `s` here; PostgreSQL takes the alias before the clause (`FROM t s TABLESAMPLE BERNOULLI (10)`), and each refuses the other's order with 42601. (catalog: [other#r19](adr/0012-divergences/other.md#catalog); #1411)

**A subquery as a `TABLESAMPLE` argument is refused.**

`TABLESAMPLE BERNOULLI ((SELECT 50))` raises 0A000: the argument is evaluated once when the statement is planned, before any row exists. PostgreSQL runs the subquery first and samples at its answer. Write the constant. (catalog: [other#r18](adr/0012-divergences/other.md#catalog); #1411)

## Ordering and collation

**Strings use binary collation.**

Binary comparisons avoid locale-dependent work; PostgreSQL C collation gives matching order. (catalog: [text-collation#r1](adr/0012-divergences/text-collation.md#catalog); Collation)

**MAP and VECTOR have wadjet-defined order.**

Neither type exists in PostgreSQL core; wadjet defines their total orders. (catalog: [containers#r18](adr/0012-divergences/containers.md#catalog); MAP-VECTOR-ordering)

## Extensions

**`DOUBLE` and the quoted keywords `"float"` / `"real"` name float types.**

`CREATE TABLE t (c DOUBLE)` is a double precision column and `CAST(1 AS "float")` / `CAST(1 AS "real")` are double precision / real; PostgreSQL has no type `double` and resolves a quoted name against its catalog (`"float4"` and `"float8"` are the types on both), so it raises 42704. `FLOAT32` and `FLOAT64` are this engine's own names for real and double precision. Every PostgreSQL spelling — `FLOAT`, `FLOAT(n)`, `FLOAT4`, `FLOAT8`, `REAL`, `DOUBLE PRECISION` — means what it means there. (catalog: [numeric-decimal#r20](adr/0012-divergences/numeric-decimal.md#catalog); #1464)

**QUALIFY follows DuckDB 1.1.3.**

PostgreSQL has no QUALIFY. It filters after windows, can read unprojected inputs, resolves inputs before output aliases, and requires a window function. (catalog: [aggregates-windows#r17](adr/0012-divergences/aggregates-windows.md#catalog); ADR-0012 §13/#1076)

**A text or NULL literal is a window value function's argument.**

`FIRST_VALUE('b')`, `LAST_VALUE('b')`, `NTH_VALUE('b', 2)`, `LAG('b')` and `LEAD('b')` answer the text, and the same with `NULL` answer NULL; PostgreSQL cannot resolve the polymorphic type of an unknown literal there and raises `42804`. (catalog: [aggregates-windows#r18](adr/0012-divergences/aggregates-windows.md#catalog); #1394)

**A bound text parameter is a window integer argument.**

`LAG(x, $1)` with `$1` declared `text` and bound `'1'` answers `LAG(x, 1)`; PostgreSQL raises `42883` (`lag(bigint, text)` does not exist). A text parameter is spliced as SQL's unknown literal and read by its value. One declared `int8` or `numeric` raises `42883` as on PostgreSQL. (catalog: [aggregates-windows#r20](adr/0012-divergences/aggregates-windows.md#catalog); #1399, #1439)

**A LAG / LEAD default is evaluated on every row.**

`LAG(b, 1, 10 / (id - 2))` raises `22012` where PostgreSQL evaluates the default only on the rows it fills and answers. The default is computed as a column before the window runs, so a default that raises on any row raises the query. (catalog: [aggregates-windows#r21](adr/0012-divergences/aggregates-windows.md#catalog); #1435)

**A LAG / LEAD default is coerced when a row reads it.**

`LAG(b, 1, 'a')` over a bigint raises `22P02` when the query reads a row, as on PostgreSQL, but over no rows it answers no rows where PostgreSQL raises when the query is planned. A quoted literal default of an ARRAY value raises `cannot store string into ARRAY vector` where PostgreSQL raises `22P02 malformed array literal`. (catalog: [aggregates-windows#r22](adr/0012-divergences/aggregates-windows.md#catalog); #1435)

**A LAG / LEAD default that reads a table is not evaluated.**

`LAG(b, 1, (SELECT max(d) FROM t))` fails the query, with no SQLSTATE, where PostgreSQL evaluates the subquery once and answers; a constant subquery default such as `(SELECT 9)` answers, as does a column default. (catalog: [aggregates-windows#r23](adr/0012-divergences/aggregates-windows.md#catalog); #1435)

**A table created from a numeric LAG / LEAD result takes the result's scale.**

`CREATE TABLE c AS SELECT id, LAG(b, 1, CAST(NULL AS NUMERIC)) OVER (ORDER BY id) AS v FROM t` over a bigint `b` creates a DECIMAL(38,0) column, so a later `INSERT INTO c VALUES (9, 0.75)` stores 1 where PostgreSQL's column is unconstrained and stores 0.75. (catalog: [aggregates-windows#r24](adr/0012-divergences/aggregates-windows.md#catalog); #1436)

**Network-native types have separate storage domains.**

`IPV4`, `IPV6`, `CIDR` and `MAC` are native column types with PostgreSQL's `inet` and `macaddr` input grammar at every boundary — the writer, `CAST`, and a literal — but they declare `text` (OID 25) on the wire; `UUID` declares `uuid` (2950); `PORT` and `PROTOCOL` declare `integer` (23). A `CIDR` reads `inet`'s grammar, not `cidr`'s: it keeps host bits an `inet` would keep, where PostgreSQL's `cidr` refuses them, and `CAST('10' AS CIDR)` is 22P02 where PostgreSQL's classful reading answers `10.0.0.0/8`. See the [input grammar table](data-types.md#network-types). (catalog: [network#r1, r2, r3](adr/0012-divergences/network.md#catalog))

**PORT/PROTOCOL constrain casts and writes.**

Casts/writes enforce 0–65535 and 0–255 (22003); arithmetic may leave those ranges. Both declare int4. Their own text grammar is read at every door, the comparison included: `CAST('udp' AS PROTOCOL)` and `WHERE proto = 'udp'` are both 17, and int4's radix spellings are not theirs — `WHERE port = '0x1bb'` is 22P02. (catalog: [network#r10, r11, r12](adr/0012-divergences/network.md#catalog); #1092-residual-2, #1137)

**The `^` operator answers two spellings PostgreSQL rejects.**

`2 ^ -1` is 0.5 here; PostgreSQL lexes `^-` as one operator name and answers `operator does not exist: integer ^- integer` — write `2 ^ (-1)`, which both answer. `2 ^ 3 % 5` is 3 here, because `^` binds tighter and this engine's `%` takes the float result; PostgreSQL has no `double precision % integer`. The values, the `2201F`/`22003` error classes and the declared type are `POWER()`'s. (catalog: [numeric-decimal#r10, r11](adr/0012-divergences/numeric-decimal.md#catalog); #1155)

**DURATION counts nanoseconds.**

Storage and wire use bigint nanoseconds, OID 20, versus PostgreSQL’s microsecond interval. (catalog: [temporal#r15](adr/0012-divergences/temporal.md#catalog); #834)

**A FULL JOIN on a non-equi ON condition answers.**

`FULL JOIN b ON a.n < b.n` raises `FULL JOIN is only supported with merge-joinable or hash-joinable join conditions` on PostgreSQL, which has no executor for it. The join is still DEFINED there — the LEFT JOIN plus the build rows no probe row satisfies — and that is what this engine answers. Superset, kept; the 14 shapes are cells of `coordinator.TestJRAOuterJoinOnResidualsAgreeOnFiveArms`. (catalog: [names-scopes#r8](adr/0012-divergences/names-scopes.md#catalog); ADR-0012 §13/#1153)

**MIN/MAX accepts additional types.**

BOOL, UUID, MAC, BYTES, MAP and VECTOR have defined orders here; PostgreSQL lacks these aggregates. BYTES uses bytewise order and retains bytea OID 17. MIN/MAX over a ROW is 42883, as on PostgreSQL. (catalog: [aggregates-windows#r10, r11](adr/0012-divergences/aggregates-windows.md#catalog); #569, #570, #1061)

**Aggregate arguments read the wire types.**

SUM, AVG, STDDEV, VARIANCE, CORR and COVAR accept PORT and PROTOCOL as the int4 the wire declares them, so `SUM(c_port)` is `bigint` and `AVG(c_port)` is `numeric`; over DURATION, whose column declares `bigint`, they answer `double precision` (PostgreSQL's `interval` has no STDDEV). MEDIAN, QUANTILE_* and the plain calls `mode(x)`, `percentile_cont(p, x)` and `percentile_disc(p, x)` answer over numbers where PostgreSQL raises 42809; refused, MODE and PERCENTILE_DISC raise 42809 `WITHIN GROUP is required`, PERCENTILE_CONT and MEDIAN/QUANTILE_* 42883. `STRING_AGG` renders BOOL, numbers, network values, UUID and DATE as their text where PostgreSQL raises 42883; over TIMESTAMP or a container it raises 42883, over BYTEA 0A000 (PostgreSQL answers). Every other argument PostgreSQL has no overload for raises its 42883 (`function sum(text) does not exist`), and `SUM('5')` / `SUM(NULL)` its 42725. (catalog: [aggregates-windows#r5, r6, r7, r8, r9](adr/0012-divergences/aggregates-windows.md#catalog))

**Text compares with typed values, pair by pair.**

Text compared with an integer, double, numeric, PORT, PROTOCOL, DURATION, UUID, IPv6 or CIDR value — directly or in an IN list — answers through the value's text; an `IN` / `= ANY` / `NOT IN` / `<> ALL` subquery is accepted only where its body selects `CAST(x AS TEXT)` and the text provably converts (x renders every value as the compared type does: the same type, two integer kinds, or a PORT or PROTOCOL against double precision — never numeric against float, a fractional rendering against an integer kind, an integer's text against numeric, or a bigint, integer or DURATION's text against double precision); with a DATE, TIMESTAMP or boolean it answers directly and in an IN list. PostgreSQL raises 42883 for all of them. Any other text body raises 42883 here too, as PostgreSQL does: a stored TEXT column, a derived table's or a CTE's column (including one nested inside a CTE or a further derived table, and one addressed through a correlated `EXISTS` two levels down), a TEXT literal with a `FROM`, a set-operation body of such literals (UNION, UNION ALL, INTERSECT, EXCEPT alike), an aggregate over text, and the mirror (a text value against a subquery selecting a typed column). The rule reads each side whatever its shape: an expression over text in the body (`upper(s)`, `s || ''`, `coalesce(s, '0')`, a `CASE` over it) and a typed expression outside (`v + 0`, `coalesce(v, v)`) are 42883 like the columns. A `FROM`-less literal body (`id IN (SELECT '12')`) is not this rule: the parser folds it into the plain `IN` list before any subquery is planned, so it answers exactly as `id IN ('12')` does, where PostgreSQL refuses both alike 42883 — a kept superset, not a refusal. So does an `EXISTS` / `NOT EXISTS` (or a correlated `IN`) whose body equates an outer column with a text column of the other class, which is a join key exactly as `JOIN … ON a.x = b.s` is: `EXISTS (SELECT 1 FROM b WHERE b.s = a.v)` is 42883; a key with an expression side takes the membership rule — `a.v = CAST(b.v AS TEXT)` of the compared value's own type keeps the direct comparison's reading, any other text side (`coalesce(a.v, 0) = b.s`, `a.v_dec = CAST(b.v_i64 AS TEXT)`) is 42883 — and the same equality under `OR` keeps the direct reading; the rule reaches a correlated key sitting inside a nested `AND` conjunct, and `IS [NOT] DISTINCT FROM` taken as `=`. A `LATERAL` body's correlated equality is the join's key, and no text/typed pair is kept there: `FROM a, LATERAL (SELECT … FROM b WHERE b.s = a.v) l` is 42883, and so is its `CAST(b.v AS TEXT)` key. Through v0.25.1 a stored, derived-table or CTE column body, and an EXISTS over one, answered no rows for matching values (NOT IN and NOT EXISTS every row); a TEXT literal with a FROM, a set-operation body of such literals and an ungrouped `max(s)` body answered the membership for the ten kept types (0 rows on the single-process arms for DATE, TIMESTAMP and BOOLEAN); the mirror raised #615's key error for the integer kinds on most arms and answered 0 rows on every arm for numeric, float8, UUID, IPv6 and CIDR. Through v0.25.1 too, an expression outer (`v + 0 IN (SELECT s …)`) answered 0 rows on the single-process arms and 2 on the DAG, a body selecting `upper(s)` or `s || ''` 0 rows on the single-process arms and 22P02 on the DAG, a `LATERAL` key 0 rows on every arm, and `v_dec IN (SELECT CAST(v_i64 AS TEXT) …)` 1 row where the same `EXISTS` answered 0. A set-operation subquery body (UNION, UNION ALL, INTERSECT, EXCEPT) is kept only when each arm selects `CAST(x AS TEXT)` of a value that renders as the compared type does — with one gap: where the arms' types share a class, the body is judged by its FIRST arm's type alone, so a later arm's numeric or double precision CAST against a bigint is not refused (`v IN (SELECT CAST(v AS TEXT) … UNION SELECT CAST(d AS TEXT) …)` over a numeric `d` answers; the same arms in the other order are 42883, as PostgreSQL refuses both). Text against REAL, BYTEA, IPv4 or MAC, text membership against a DATE/TIMESTAMP/boolean subquery, and two text/typed COLUMNS as a JOIN key raise 42883 here too. For `<> ALL` the message names `=` where PostgreSQL names `<>` (the engine reads `x <> ALL (subquery)` as `x NOT IN (subquery)`); the SQLSTATE is the same. (catalog: [comparison-membership#r3, r4, r5, r6, r7, r8](adr/0012-divergences/comparison-membership.md#catalog); #826, #1073, #1308, #1368, #1369, #1370, #1374)

**A timestamp minus a timestamp answers milliseconds; a date minus a date is bigint.**

`ts1 - ts2` (and `now() - now()`) answers the difference as a number of milliseconds where PostgreSQL answers an `interval` (`01:00:00`): this engine has no INTERVAL column type, only the INTERVAL literal. Such a value assigned to a DATE, TIMESTAMP or address column is 42804, as PostgreSQL's interval is. `date1 - date2` is the day count PostgreSQL answers, declared `bigint` (OID 20) where PostgreSQL declares `integer`, and `(d - DATE '2024-01-01') + 1` and `/ 7` are integer arithmetic declared `bigint` (`/ 7` is 9); beside a `numeric` it is an integer operand of `numeric` arithmetic (`(d - DATE '2024-01-01') * n` is `numeric`, as on PostgreSQL). A day count divided by zero answers NULL where PostgreSQL raises 22012. Inside a scalar subquery a day count is `integer`.

A date minus a timestamp (PostgreSQL: an `interval`) is refused 42883 here, as the pairs PostgreSQL has no operator for are (`timestamp + integer`, `date + numeric`, `integer - date`, `date + timestamp`) — it used to answer the day count minus the millisecond count. The refusal is part of typing the expression, so it holds in every statement that evaluates one: a SELECT's clauses, `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` SET and WHERE, `DELETE` WHERE, `MERGE` and CTAS. `ts - ts` is declared `double precision` (the millisecond count).

A quoted operand beside a DATE or TIMESTAMP is typed by PostgreSQL's operator resolution, in every statement: `date + '…'` and `'…' + date` are 42725 `operator is not unique` (NULL too); the literal of `date - '…'` is a DATE (a day count, `d - '2026-03-01'`), of `ts + '…'` an INTERVAL (a TIMESTAMP, `ts + '1 day'`), and of `ts - '…'` a TIMESTAMP — whose difference is the millisecond count above, where PostgreSQL answers an `interval`. A literal that is not the type it resolves to is 22007 at plan time. It used to be read by its leading number (`ts + '1 day'` was ts plus one millisecond).

**INTERVAL.** There is no INTERVAL column type; an INTERVAL value is declared `text` (OID 25) on the wire where PostgreSQL declares `interval` (OID 1186), printed in PostgreSQL's `postgres` style (`1 day`, `-1 days`, `1 year 2 mons`, `25:00:00`). The single-unit spellings the `INTERVAL '…'` literal takes (`'1 day'`, `'90 minutes'`, `'5'` seconds) are that interval in a text CAST too; any other interval text PostgreSQL reads (`'1 day 02:00:00'`, `'00:30:00'`, `'1 year 2 mons'`, `'P1D'`, `'1.5 days'`) is kept as written — PostgreSQL's own value when the text is PostgreSQL's output — and applying it to a date or timestamp is 0A000. Text PostgreSQL refuses is 22007. Into a TEXT column an INTERVAL stores its text; into any other column it is 42804. An INTERVAL field past PostgreSQL's own range (`INTERVAL '100000000000 hours'`) is 22015 there at the literal and 22008 here when it is applied. (catalog: [temporal#r16, r17, r18, r19, r20, r21, r22](adr/0012-divergences/temporal.md#catalog); arc VL)

**A number literal against a timestamp reads epoch milliseconds.**

`c_ts >= 1700000000000` compares against the instant that many milliseconds after the epoch; PostgreSQL raises 42883. (A number against TEXT is the entry "SELECT compares numeric spelling with text" above, kept by arc BR.) A number against a boolean, and a boolean literal against a number, raise 42883 on both. (catalog: [comparison-membership#r2](adr/0012-divergences/comparison-membership.md#catalog); arc BR, #1216)

**Two ROW shapes do not fold.**

A CASE, COALESCE, GREATEST, LEAST or set operation over two ROW columns of different shapes raises 42846 `could not convert type`, as PostgreSQL does for named composite types; its anonymous `ROW(…)` records answer. A quoted literal in a ROW's or ARRAY's own grammar inside such a fold raises 0A000 where PostgreSQL reads it. (catalog: [comparison-membership#r16, r17](adr/0012-divergences/comparison-membership.md#catalog); arc BR, #1060, #1065)

**A set operation's ORDER BY takes the first arm's qualified column.**

`SELECT a.id … UNION ALL … ORDER BY a.id` answers; PostgreSQL raises 42P01. Any other qualifier raises 42P01, a name that is no result column 42703 (exactly, `ORDER BY "ID"` over `id` included), and an expression PostgreSQL's transform error or 0A000. (catalog: [set-operations#r3](adr/0012-divergences/set-operations.md#catalog); arc BR, #1236)

**DISTINCT works on additional aggregates.**

`MEDIAN(DISTINCT a)`, `APPROX_DISTINCT(DISTINCT a)` and `MIN_BY(DISTINCT a,b)` deduplicate here; PostgreSQL refuses the corresponding ordered-set form or has no function. (catalog: [aggregates-windows#r13](adr/0012-divergences/aggregates-windows.md#catalog); #703)

**TIME_BUCKET supplies a default origin.**

It follows date_bin with default origin `1970-01-01`; PostgreSQL requires an origin. The stride must be an INTERVAL value — an `INTERVAL '…'` literal or a `CAST(… AS INTERVAL)` — and a text stride is 42804; the interval grammar is the SQL parser's, so `INTERVAL '1 day 6 hours'` is 22007. Months/years raise 0A000 and non-positive strides 22008, as in date_bin. (catalog: [extensions#r1, r2, r3](adr/0012-divergences/extensions.md#catalog); #965-TIME_BUCKET)

**OHLCV returns a bar.**

PostgreSQL requires separate aggregates. VWAP uses AVG’s scale; zero volume returns NULL versus the quotient’s 22012, preserving other bars. DISTINCT/window forms raise 0A000. (catalog: [extensions#r4, r5, r6, r7](adr/0012-divergences/extensions.md#catalog); #965; §9/VWAP)

**TCP flag functions name bits.**

`tcp_flags_has_all(f,'SYN','ACK')` equals PostgreSQL `(f & 18) = 18`. Empty or unknown name lists raise 22023 instead of dropping requested bits; PostgreSQL has no named-bit functions. (catalog: [extensions#r9, r10, r11](adr/0012-divergences/extensions.md#catalog); #966-TCP-flags)

**Bitwise helpers follow Trino conventions.**

`BITWISE_RIGHT_SHIFT` is logical; PostgreSQL `>>` preserves sign. Out-of-range shift counts return NULL rather than modular counts; `TO_BASE` uses signed text for negatives. (catalog: [extensions#r16, r17, r19](adr/0012-divergences/extensions.md#catalog); #966-bitwise)

**Semver has its own authority.**

PostgreSQL has none. SemVer 2.0.0 defines precedence; node-semver defines ranges. Leading v/V is accepted; components stop at int64. Malformed versions return NULL (strict normalization: 22023). Empty ranges, partial-version suffixes, leading zeros and oversized components raise 22023. (catalog: [extensions#r25, r26, r27, r28, r29, r30](adr/0012-divergences/extensions.md#catalog); #967)

**`NORMALIZE` accepts a quoted form.**

`NORMALIZE(s, 'NFC')` answers here and is a syntax error on PostgreSQL 17.11, which admits only the bare keyword. Both spellings mean the same thing. (catalog: [text-collation#r19](adr/0012-divergences/text-collation.md#catalog); #1169)

**`#` accepts operands PostgreSQL refuses.**

`5.0 # 3` and `'a' # 'b'` answer here (the bitwise family reads its operands as integers) where PostgreSQL raises 42883 and 42725. The declared WIDTH agrees: `int4 # int4` is `integer` and a bigint operand makes it `bigint`, on both engines. (catalog: [numeric-decimal#r12](adr/0012-divergences/numeric-decimal.md#catalog); #1179)

**Plain-call MEDIAN, QUANTILE, MODE and PERCENTILE answer over numbers.**

`SELECT percentile_cont(0.5, id), median(id) FROM t` answers over a numeric column; PostgreSQL has no MEDIAN or QUANTILE (42883) and requires `WITHIN GROUP` for MODE, PERCENTILE_CONT and PERCENTILE_DISC (42809). Over a non-numeric argument this engine refuses with PostgreSQL's per-function state: MODE and PERCENTILE_DISC 42809, PERCENTILE_CONT, MEDIAN and QUANTILE_* 42883. Write the `WITHIN GROUP (ORDER BY …)` form for portability. (catalog: [comparison-membership#r12](adr/0012-divergences/comparison-membership.md#catalog); arc BR)

**STRING_AGG renders a non-text argument as its text.**

`SELECT string_agg(id, ',') FROM t` over a bigint answers `12,14`; PostgreSQL has no `string_agg(bigint, text)` and raises 42883. BOOL, the integers, REAL, DOUBLE, DECIMAL, the network types, PORT, PROTOCOL, DURATION, UUID and DATE render their own text; TIMESTAMP and typed containers are 42883 as in PostgreSQL, and BYTEA is 0A000. Write `string_agg(CAST(id AS TEXT), ',')` for portability. (catalog: [comparison-membership#r13](adr/0012-divergences/comparison-membership.md#catalog); arc BR)

**Numeric aggregates accept PORT, PROTOCOL and DURATION.**

SUM, AVG, STDDEV, VARIANCE, CORR and COVAR read PORT and PROTOCOL as int4 and DURATION as int8 nanoseconds, as the wire declares them, so `SELECT stddev(d) FROM np` over a DURATION column answers. PostgreSQL has none of these types, and its `interval` has no stddev. (catalog: [comparison-membership#r15](adr/0012-divergences/comparison-membership.md#catalog); arc BR, #834)

**Arrays of different numeric element types compare by value.**

`ARRAY[1.5] > ARRAY[1]` answers `t` here; PostgreSQL has no operator for `numeric[] > integer[]` and raises 42883. Cast one side to the other's element type to write it portably. (catalog: [containers#r20](adr/0012-divergences/containers.md#catalog))

**An array casts to JSON.**

`CAST(ARRAY[1,2] AS JSON)` answers `[1,2]`, `to_json`'s text; PostgreSQL has no array-to-json cast and raises 42846. `to_json(ARRAY[1,2])` is the portable spelling. (catalog: [containers#r21](adr/0012-divergences/containers.md#catalog))

**`tcp_flags` names the set bits as a text array.**

`SELECT tcp_flags(18)` answers `{SYN,ACK}`, declared `text[]`; PostgreSQL has no named-bit function and needs a join from bit to name. Only the nine TCP flag bits are named (`AE` is the canonical name, `NS` an input alias); any other set bit is omitted rather than refused, so one malformed row does not fail the query. (catalog: [extensions#r12](adr/0012-divergences/extensions.md#catalog))

**`tcp_flags_from_string` reads an empty string as no flags.**

`tcp_flags_from_string('')` is `0`, the mask of no names; PostgreSQL has no such function. An empty element inside a list (`'SYN,,ACK'`, `'SYN,'`) is 22023 naming its position, and an unknown name is 22023 naming it. (catalog: [extensions#r13](adr/0012-divergences/extensions.md#catalog))

**The older flag helpers use the same nine names.**

`has_tcp_flag(256, 'AE')` is `t` and `tcp_flags_to_string(256)` is `AE`; PostgreSQL has neither function. An unknown flag name, or a flags argument that is not an integer, is 22023 rather than NULL, a smaller mask, or "no flags set". (catalog: [extensions#r14](adr/0012-divergences/extensions.md#catalog))

**A semver range drops a `>=0.0.0` lower bound, as node-semver does.**

`semver_satisfies('0.0.0-alpha', '>=0.0.0 <=0.0.0-alpha')` is `t`, matching node-semver 7.7.3, where reading the bound numerically would admit nothing; PostgreSQL has no semver functions. This makes ranges over Go pseudo-versions (`v0.0.0-…`) answer. `>=0.0.0-0` is a different comparator and is kept. (catalog: [extensions#r31](adr/0012-divergences/extensions.md#catalog))

**A semver range at the top of int64 saturates.**

`semver_satisfies('9223372036854775807.5.0', '^9223372036854775807.0.0')` is `t`: an upper bound that would need a component past int64 is rewritten to the largest version below it rather than wrapped. node-semver refuses components past 2^53-1; this engine accepts and compares components up to 2^63-1 exactly, and PostgreSQL has no semver functions. (catalog: [extensions#r32](adr/0012-divergences/extensions.md#catalog))

**`semver_parse` returns a fixed row.**

`semver_parse('1.2.3-rc.1+b')` answers `(1,2,3,rc.1,b)` as `(major bigint, minor bigint, patch bigint, prerelease text, build text)`; PostgreSQL has no equivalent. An invalid string is NULL, and `semver_parse_strict` raises 22023 for it; a missing pre-release or build is the empty string. (catalog: [extensions#r33](adr/0012-divergences/extensions.md#catalog))

**`TO_ISO8601` and `AT_TIMEZONE` render ISO 8601.**

`SELECT TO_ISO8601(CAST('2023-11-14 05:06:07' AS TIMESTAMP))` answers `2023-11-14T05:06:07Z`; PostgreSQL has no such function and spells it with `to_char`. Every other timestamp-valued function renders PostgreSQL's `2023-11-14 05:06:07` and declares `timestamp`. `AT_TIMEZONE` keeps the ISO rendering because its result is a wall clock in another zone, which a zoneless rendering would publish as UTC. (catalog: [temporal#r8](adr/0012-divergences/temporal.md#catalog))

**A text parameter is SQL's unknown literal.**

A parameter declared `text` or `varchar` is read by the position it lands in, as an untyped literal is: `WHERE n = $1` with `$1` text `'7'` over an integer `n` answers the row where PostgreSQL raises 42883 (`integer = text`). `SELECT $1` declared `varchar` declares `text`, and an undeclared parameter beside a VARCHAR column is typed `text`, as such a column declares here. (catalog: [parameters-pgwire#r5](adr/0012-divergences/parameters-pgwire.md#catalog); #1410)

**An undetermined parameter answers.**

An undeclared parameter in a FROM-less subquery — `n IN (SELECT $1)`, `n = (SELECT $1)` — takes the comparison's type and answers, where PostgreSQL types it text and raises 42883; `SELECT $1 IS NULL, $1` declares the parameter `text` and `WHERE $1 IS NULL` leaves it undetermined (OID 0), both answering where PostgreSQL raises 42P08 / 42P18. (catalog: [parameters-pgwire#r6, r7](adr/0012-divergences/parameters-pgwire.md#catalog); #1410)

## Not supported

**A SMALLINT / INT2 column.**

`CREATE TABLE t (n SMALLINT)` and `(n INT2)` raise 42704 (`type "SMALLINT" does not exist`) where PostgreSQL creates an int2 column: there is no int16 storage. `CAST(x AS SMALLINT)` answers, declared bigint (see "Integer expressions declare bigint"). (catalog: [dml-assignment#r8, r9](adr/0012-divergences/dml-assignment.md#catalog); #1353-int2, #1407)

**The system catalog describes one database, one role and this server's objects.**

`pg_database` lists one database and `pg_roles` one role, the connection's identity, which is not a superuser; PostgreSQL also lists its templates and bootstrap superuser. `pg_class.relam` is 0 and `pg_am` is empty (a stored table has no PostgreSQL access method), `pg_type` lists the types the wire declares and their arrays but no DOMAIN types, `pg_proc` lists no functions, and the relations for objects this server does not have (indexes, triggers, rules, policies, publications, sequences) are empty. A column is typed by the engine type that carries it — an OID column declares `int8`, and `current_schemas()` is `text[]` where PostgreSQL's is `name[]`. A masked column's definition is listed and its values arrive masked; a denied column is absent. A string literal cast to `regclass` is read to its OID and prints as the OID where PostgreSQL prints the name. The server reports PostgreSQL 17 (`server_version` 17.0, `server_version_num` 170000), the major whose catalog it models, so psql and pgJDBC send the catalog spellings this catalog has. (catalog: [other#r13, r14, r15](adr/0012-divergences/other.md#catalog); ADR-0044, #1251)

**A syntax error names no position field, only its sentence's own `at or near`.**

A statement the parser cannot read is 42601 with PostgreSQL's sentence — `syntax error at or near "…"`, `syntax error at end of input`, and for `E'…'` strings `invalid Unicode surrogate pair at or near "…"` (42601), `invalid Unicode escape` (22025), `invalid Unicode escape value at or near "…"` (42601) and `invalid byte sequence for encoding "UTF8": 0x…` (22021) — the escape errors' own `at or near "…"` suffix names the offending escape's own source text (`\uDE00`, `\U00110000`) or the character that broke a surrogate pair, matching PostgreSQL's wording (#1307). PostgreSQL also sends a structured error POSITION field, which psql renders separately as `LINE 1: …` with a caret; this server sends none — the sentence carries the location, the wire protocol's own position field does not. Where the parser stops early inside a subquery, the token named is the `)` that closes it, as in PostgreSQL. (catalog: [parameters-pgwire#r4](adr/0012-divergences/parameters-pgwire.md#catalog); ADR-0044)

**Set-returning functions answer only as a whole SELECT item.**

`unnest(array)`, `generate_subscripts(array, dim)` and `information_schema._pg_expandarray(array)` expand each row when they ARE a SELECT item, by PostgreSQL 10's rule (the longest set decides the row count, shorter sets are padded with NULL). Inside an expression, in WHERE, beside an aggregate, a window function or DISTINCT they are refused 0A000 where PostgreSQL answers (or, in WHERE, refuses with the same code). `generate_subscripts`' dimension must be a constant, and a table function's arguments must be constants: `generate_series(1, t.n)` is 0A000. An `ARRAY[…]` constructor's elements take one common type, as in PostgreSQL; a constructor of constants only is typed by ADR-0024's literal rule, so `unnest(ARRAY[1,2.5])` answers 1, 2.5 declared `double precision` where PostgreSQL declares `numeric`. (catalog: [table-functions#r13, r14, r15](adr/0012-divergences/table-functions.md#catalog); ADR-0044)

**A subquery in `INSERT ... VALUES` is refused.**

PostgreSQL accepts a scalar subquery in a plain `INSERT INTO t VALUES (...)` cell — it is a constant to the statement, the same as it is in a `SELECT` list. Each VALUES cell here is a full scalar expression (#1252) evaluated with no row and no query environment, so a subquery is refused 0A000 rather than run. `MERGE ... WHEN NOT MATCHED THEN INSERT ... VALUES` is not this restriction: its VALUES clause has the merged row's environment already, and a subquery there compiles and runs as it does in a `WHEN` condition. (catalog: [dml-assignment#r15](adr/0012-divergences/dml-assignment.md#catalog))

**`CURRENT_TIME` is not a supported function.**

This engine has no TIME type among its 22 (`Type System`), so `CURRENT_TIME`'s SQL-standard niladic spelling parses — the parser recognizes it as a reserved keyword the way it does `CURRENT_DATE` — but the call itself is refused, `unknown function: current_time`, rather than declaring a value with no representation. `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` and `NOW()` are unaffected (#1254). (catalog: [temporal#r23](adr/0012-divergences/temporal.md#catalog))

**The pattern-match operators match with RE2.**

`~ ~* !~ !~*` translate PostgreSQL's ARE form by form; a back reference, lookahead/lookbehind, `\m`/`\M`, `[[:<:]]`, a collating element and the `b e n p w x` embedded options are refused 0A000. Case-insensitive matching folds ASCII letters only, as PostgreSQL does under the C collation. `regexp_replace` reads its pattern through the same translation; its `n`, `m`, `p`, `w`, `x`, `b` and `e` flags and an integer start position are refused 0A000, and an RE holding a non-greedy quantifier is matched leftmost-first rather than shortest-first (`regexp_replace('Hello', 'x*?H*', '#')` is `#ello`; PostgreSQL `#Hello`). (catalog: [text-collation#r3, r4, r23, r24](adr/0012-divergences/text-collation.md#catalog); ADR-0044)

**COLLATE accepts the byte-order collations only.**

`C`, `POSIX`, `ucs_basic` and `default` are this server's order and are accepted; any other collation is refused 0A000 rather than compared by bytes. (ADR-0044)

[SQL reference](sql-reference.md). (catalog: [text-collation#r2](adr/0012-divergences/text-collation.md#catalog))

**`LOCALTIME`, `IS [form] NORMALIZED` and the `U&'…'` literal have no grammar.**

`LOCALTIME` needs a TIME type this engine does not have; `'abc' IS NORMALIZED` and `U&'\0065\0301'` are 42601 where PostgreSQL answers. `LOCALTIMESTAMP` and `NORMALIZE(s, form)` are supported. (catalog: [text-collation#r20, r21, r22](adr/0012-divergences/text-collation.md#catalog); #1169)

**A column-alias list over a star whose width is not known is refused.**

A star without a known width cannot be renamed positionally: 0A000 where PostgreSQL answers. (catalog: [names-scopes#r15](adr/0012-divergences/names-scopes.md#catalog); #958)

**Unresolved decimal set-operation arms can refuse.**

Unknown types/scales cause distributed refusal to avoid decimal reinterpretation; local execution and PostgreSQL can answer. (catalog: [set-operations#r6](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/#551)

**JOIN USING merges, but not for every shape.**

`SELECT *` over a `JOIN … USING` publishes the joined column once and first, as PostgreSQL does, and publishes a name the two arms share OUTSIDE the USING list twice, as PostgreSQL does. A chain of USING joins (`FROM ua JOIN ub USING (id) JOIN uc USING (id)`) and an arm that publishes one name twice both raise 0A000 where PostgreSQL answers. Name the columns, or write the join condition with ON. (catalog: [names-scopes#r19, r20](adr/0012-divergences/names-scopes.md#catalog))

**A `SELECT *` over a `JOIN … USING` with a LATERAL arm publishes the joined column twice.**

`SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id FROM lat_item i WHERE i.order_id = o.id) l USING (id)` publishes `id, customer, total, id` where PostgreSQL publishes `id, customer, total`: the star is expanded into both arms' own lists, but the USING merge is not applied over a LATERAL's lowered join, so the joined column is published a second time. (catalog: [names-scopes#r21](adr/0012-divergences/names-scopes.md#catalog); #1177-lateral-using)

**A USING merge of two DECIMAL columns at different scales declares the left arm's.**

`SELECT * FROM zzp JOIN zzj USING (id, d92)` declares the merged `d92` at the left arm's DECIMAL(9,2) where PostgreSQL declares unconstrained `numeric`, the common type of the two. The merged column's VALUE is the left arm's by the same rule on both engines; only the declaration differs. (catalog: [names-scopes#r22](adr/0012-divergences/names-scopes.md#catalog); #1177)

**A bare reference to a USING join's merged column is ambiguous here, outside a sort or window key.**

`SELECT id FROM a JOIN b USING (id)` raises 42702 where PostgreSQL answers, because USING merges the column and the reference is not ambiguous there; the same for a WHERE, a GROUP BY, a HAVING and a DISTINCT. Qualify it (`a.id`). In an ORDER BY or a window key the same reference BINDS THE MERGE and answers. (catalog: [names-scopes#r16](adr/0012-divergences/names-scopes.md#catalog); #655)

**A bare SELECT * over a FULL JOIN … USING cannot be ordered by the merged column.**

It raises 0A000 where PostgreSQL answers: the merged value is COALESCE of the two sides, computed by the projection the star expands into, and this planner materializes a computed sort key beside a NAMED select list. Name the columns, which answers; so does the same statement without the ORDER BY. The positional spelling is the same refusal. (catalog: [names-scopes#r17](adr/0012-divergences/names-scopes.md#catalog); #655)

**A window key naming a FULL JOIN … USING merged column is refused.**

It raises 0A000 where PostgreSQL answers: the merged value is a COALESCE and a window PARTITION BY / ORDER BY key here is a column name. Write the expression. A window ARGUMENT is an expression and binds the merge on RIGHT and FULL alike. The refusal is drawn on the SHAPE, so on data where the merged and left-arm partitionings coincide it withdraws an answer that would have been right. (catalog: [names-scopes#r18](adr/0012-divergences/names-scopes.md#catalog); #655)

**A per-row window integer argument is refused.**

`LAG(x, o)`, `LAG(x, o + 1)`, `NTILE(o)` and `LAG(x, (SELECT 1))` raise `0A000`; PostgreSQL evaluates the argument (LAG / LEAD per row, NTILE / NTH_VALUE once per partition). A constant expression (`LAG(x, 2 - 1)`, `NTILE(abs(-2))`) is folded when the query is planned and answers as on PostgreSQL, except `'1' + 1`, which the fold types numeric (`42883`; PostgreSQL reads 2). An integer literal (the int4 minimum included), a quoted or cast integer, NULL and a bound `integer` or untyped parameter are read as PostgreSQL reads them. (catalog: [aggregates-windows#r19](adr/0012-divergences/aggregates-windows.md#catalog); #1399, #1440)

**NATURAL JOIN is refused.**

It raises 0A000 where PostgreSQL answers: the keys are whatever columns the two sides share, which is a catalog question the parser cannot answer. Write the condition with ON or USING. (catalog: [names-scopes#r23](adr/0012-divergences/names-scopes.md#catalog); #655)

**A column-alias list on a WITH-query REFERENCE is refused.**

`FROM c z(x, y)` raises 0A000 where PostgreSQL answers; the rename would land above the query's own block and every renamed reference would read NULL. Put the list on the definition, `WITH c(x, y) AS (…)`. (catalog: [names-scopes#r14](adr/0012-divergences/names-scopes.md#catalog); #959)

**A column-alias list that repeats a name is refused.**

`FROM t a(k, k)` raises 42701 where PostgreSQL accepts the list and refuses only a reference to `k` (42702). This planner renames positionally and cannot publish one name for two columns; refusing the list is narrower than PostgreSQL, never a wrong value. (catalog: [names-scopes#r13](adr/0012-divergences/names-scopes.md#catalog); #959)

**A subquery inside an OUTER join's ON clause is refused.**

`LEFT JOIN b ON a.x = (SELECT max(y) FROM c)` raises where PostgreSQL answers. An outer join's ON is evaluated AT the join, per probe row against each candidate build row, because a conjunct lifted above it would delete the rows the join preserves — and a subquery's value is not available there. The refusal names the construct. An INNER join lifts the same ON into a filter above the join and answers it. (catalog: [lateral-subqueries#r1](adr/0012-divergences/lateral-subqueries.md#catalog); #1153)

**BYTEA, MONEY and INET type names are refused.**

These types lack representations: casts/declarations raise 42704 versus PostgreSQL values. (catalog: [other#r2](adr/0012-divergences/other.md#catalog); #652)

**Address storage restricts accepted values.**

A value PostgreSQL inet holds can exceed IPV4/IPV6’s representation: `CAST('10/8' AS IPV4)` raises 0A000. Host-width prefixes are accepted. (catalog: [network#r6](adr/0012-divergences/network.md#catalog); #627, #1092)

**Some set-operation type pairs are refused.**

Missing representations cause 0A000 versus PostgreSQL values: DATE/TIMESTAMP, different address types, or PORT/PROTOCOL/DURATION with DECIMAL, in either order. (catalog: [set-operations#r7](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/carrier-pairs; DATE/TIMESTAMP #1430)

**CASE, COALESCE, GREATEST and LEAST over a DATE and a TIMESTAMP are refused.**

PostgreSQL resolves the arms to timestamp, a DATE arm at its midnight; here the choice has no carrier for a DATE arm in a TIMESTAMP result and raises 0A000 — `CAST` the DATE arm to TIMESTAMP. The refusal holds whatever the arm is: a column (of a table, a derived table, a CTE or recursive CTE — inside the recursive CTE's own recursive term too — a join, a set operation, VALUES or a LATERAL output, whatever produced it there), a literal, an expression, a scalar subquery (correlated or not), a window call such as `max(ts) OVER ()` or `lag(ts) OVER (…)`, or an aggregate. `NULLIF(d, ts)` is declared by its first argument and answers. A comparison (a scalar-subquery operand included), an IN / EXISTS membership and a join key between the two answer PostgreSQL's rows. A bind parameter typed TIMESTAMP (OID 1114) against a DATE column answers PostgreSQL's rows as well: `d = $1` with `'1969-12-31 23:59:59.999'` matches nothing (#1426). (catalog: [temporal#r24](adr/0012-divergences/temporal.md#catalog); #1378, #1316)

**Some set-operation literals are refused.**

Text cannot initialize BOOL/integer/float/TIMESTAMP/PORT/PROTOCOL/DURATION vectors: 0A000 versus PostgreSQL values. NULL works. (catalog: [set-operations#r8](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/quoted-literals)

**Decimal set operations can exceed 38 digits.**

A common scale can leave insufficient integer digits; the query raises 22003 where PostgreSQL numeric answers rather than discarding stored digits. (catalog: [set-operations#r9](adr/0012-divergences/set-operations.md#catalog); ADR-0012 §12/precision-cap)

**Correlated window subqueries are refused.**

Reconstructing their window clauses is unsupported: 0A000 where PostgreSQL answers. (catalog: [lateral-subqueries#r7](adr/0012-divergences/lateral-subqueries.md#catalog); #1045)

**Some correlated bodies are refused.**

Set-operation/LATERAL bodies and outer-level aggregates lack the required evaluation form: 0A000 where PostgreSQL answers. (catalog: [lateral-subqueries#r3, r4, r5](adr/0012-divergences/lateral-subqueries.md#catalog); #1044)

**An UNQUALIFIED outer reference over a table function is refused in the subquery's WHERE.**

An unqualified name inside a correlated subquery binds to the enclosing row when the subquery's own relations do not have it, as on PostgreSQL. When the subquery's FROM reads a table function, its columns are not known at that point: `o.id IN (SELECT b.k FROM dc_in b JOIN generate_series(1, 9) g(x) ON g.x = b.k WHERE total > 100)` fails with `filter column "total" does not exist in the input schema` where PostgreSQL answers. Write the qualifier — `o.total > 100`. (catalog: [lateral-subqueries#r8](adr/0012-divergences/lateral-subqueries.md#catalog); #1104, ADR-0021 §1r)

**A correlated subquery's own WITH item that shadows an outer WITH item is refused.**

`WITH d AS (…) SELECT … WHERE EXISTS (WITH d AS (…) SELECT 1 FROM d …)` reads the subquery's own `d` on PostgreSQL; here it is 0A000 `a WITH item inside a correlated subquery that shadows an outer WITH item is not supported`, because the per-row execution would read the outer `d`. Rename one of the two items. (catalog: [lateral-subqueries#r10](adr/0012-divergences/lateral-subqueries.md#catalog); ADR-0021 §1r)

**Outer aggregates in subquery WHERE are refused.**

The standalone subquery cannot retain the aggregate’s outer scope: 42803 where PostgreSQL answers. (catalog: [lateral-subqueries#r2](adr/0012-divergences/lateral-subqueries.md#catalog); #809)

**Some LATERAL ON conditions are refused.**

An outer LATERAL’s ON retaining an empty-input default raises 0A000: `ON s.n = 0` requires PostgreSQL’s `Carol, 0`, which this evaluation cannot produce. (catalog: [lateral-subqueries#r13](adr/0012-divergences/lateral-subqueries.md#catalog); #977)

**A bare star over a LATERAL whose list names one column twice is refused where the key is an outer EXPRESSION.**

`SELECT * FROM o JOIN LATERAL (SELECT i.id AS m, i.v AS m FROM i WHERE i.k = o.k - 0) s ON true` raises 0A000 where PostgreSQL answers: a list naming one column twice cannot be enumerated by name, so the star reads the join's output, which carries the body's key column the equality is evaluated against. A bare star over any other LATERAL — an expression key included — is expanded into the FROM items' own lists and answers. (catalog: [lateral-subqueries#r14](adr/0012-divergences/lateral-subqueries.md#catalog); #1302)

**A LATERAL nested in another that names the OUTERMOST relation is refused.**

`SELECT … FROM o JOIN LATERAL (SELECT … FROM i JOIN LATERAL (SELECT j.k FROM i j WHERE j.k = o.k) t ON true …) s ON true` raises 0A000 where PostgreSQL answers: a LATERAL is decorrelated against the relation it joins, and `o` is two levels out. Any condition of a LATERAL body naming a relation that is neither the body's nor to its left is refused the same way. Before 2026-09-24 the reference was compared as the text `o.k` — an error for an integer key and zero rows for a text key; until 2026-09-25 it was refused 42000. (catalog: [lateral-subqueries#r19](adr/0012-divergences/lateral-subqueries.md#catalog); arc JP)

**A LATERAL body's condition holding a correlated subquery with a LATERAL join is refused.**

`SELECT o.id, s.* FROM o JOIN LATERAL (SELECT q.qid FROM q WHERE q.qk = o.k AND EXISTS (SELECT 1 FROM j JOIN LATERAL (SELECT x.v AS xv FROM x WHERE x.oid = j.oid) t ON true WHERE j.id = q.qid AND t.xv > o.id)) s ON true` raises 0A000 where PostgreSQL answers, and so does the same EXISTS reading only the body's `q.qid`: a subquery whose FROM holds a LATERAL join does not keep its correlation with the query around it (the same EXISTS at top level admits every row — a wrong answer this engine has at every level, recorded for repair), so the condition would not be evaluated per row. An uncorrelated one answers. (catalog: [lateral-subqueries#r17](adr/0012-divergences/lateral-subqueries.md#catalog); arc JP)

**A window in a correlated LATERAL body beside a non-equality correlation is refused.**

`JOIN LATERAL (SELECT i.v, row_number() OVER (ORDER BY i.v) FROM i WHERE i.k = o.k AND i.v < o.total) s` raises 0A000 where PostgreSQL answers: the correlation is evaluated as a join, the inequality as a filter over it, and the window would number rows that filter then removes. So does a window in an ungrouped aggregate body (`SELECT count(*), rank() OVER (…)`), whose row for an outer row with no matches is supplied by the join. A window beside equality correlations answers PostgreSQL's rows in every body position, `QUALIFY` included. (catalog: [lateral-subqueries#r20, r21](adr/0012-divergences/lateral-subqueries.md#catalog); arc JP)

**Qualified stars refuse duplicate names.**

Name-based expansion cannot distinguish positions: `SELECT x.*` raises 0A000 where PostgreSQL returns both columns. A bare star reads positions and answers. (catalog: [names-scopes#r24](adr/0012-divergences/names-scopes.md#catalog); 2026-09-13/duplicate-star)

**Some bounded LATERAL bodies are refused.**

Equality-keyed correlated bodies apply `LIMIT`/`OFFSET` per outer row, including `SELECT s.*`. A bound over an inequality correlation, a mixed inner/outer key expression, an outer-side expression, a DISTINCT body other than exactly the key, a set operation or a body with its own QUALIFY raises `0A000` where PostgreSQL evaluates it per outer row. This engine has no general relation-valued per-row runner. See [LATERAL joins](sql-reference.md#lateral-joins). (catalog: [lateral-subqueries#r22](adr/0012-divergences/lateral-subqueries.md#catalog); ADR-0021 §1s)

**Window aggregate support is limited.**

Only SUM/COUNT/AVG/MIN/MAX aggregates support OVER; others raise 0A000 where PostgreSQL supports windows. (catalog: [aggregates-windows#r14](adr/0012-divergences/aggregates-windows.md#catalog); #965-window-forms)

**NATURAL JOIN is refused.**

It raises 0A000; use ON. PostgreSQL supports the implicit matching-column join. (catalog: [names-scopes#r23](adr/0012-divergences/names-scopes.md#catalog))

**CREATE VIEW is refused.**

No executor exists: 0A000 versus PostgreSQL creating the view. (catalog: [dml-assignment#r17](adr/0012-divergences/dml-assignment.md#catalog))

**ALTER TABLE is refused.**

Schema evolution is unavailable: 0A000 versus PostgreSQL performing the alteration. (catalog: [dml-assignment#r19](adr/0012-divergences/dml-assignment.md#catalog))

**DROP VIEW is refused.**

No executor exists: 0A000 versus PostgreSQL dropping the view. (catalog: [dml-assignment#r18](adr/0012-divergences/dml-assignment.md#catalog))

**UPDATE SET cannot contain a subquery.**

`SET n = (SELECT max(n) FROM s)` raises 0A000 where PostgreSQL answers; assignment subqueries are unsupported, and so is one in `INSERT … VALUES`. A MERGE action's `SET` and `INSERT … VALUES` answer one, declared from the subquery's own plan. (catalog: [dml-assignment#r16](adr/0012-divergences/dml-assignment.md#catalog))

**DML RETURNING is refused.**

INSERT/UPDATE/DELETE/MERGE RETURNING raises 0A000 versus PostgreSQL rows; that result form is unsupported. (catalog: [dml-assignment#r20](adr/0012-divergences/dml-assignment.md#catalog))

**MERGE BY SOURCE or BY TARGET is refused.**

Both WHEN NOT MATCHED variants raise 0A000 versus PostgreSQL support; these forms are unimplemented. (catalog: [dml-assignment#r21](adr/0012-divergences/dml-assignment.md#catalog))

**MERGE ON requires column equalities.**

Other conditions raise 0A000 versus PostgreSQL joins; matching requires equality keys. (catalog: [dml-assignment#r22](adr/0012-divergences/dml-assignment.md#catalog))

**Computed aggregate ORDER BY is refused.**

`ORDER BY COUNT(*) * 2` raises 0A000 versus PostgreSQL values; select it and sort by its alias because intermediate evaluation is unavailable. (catalog: [aggregates-windows#r15](adr/0012-divergences/aggregates-windows.md#catalog))

**Ordering subquery quantifiers are refused.**

`x < ALL (SELECT ...)` raises 0A000 where PostgreSQL answers; only equality forms are implemented. (catalog: [lateral-subqueries#r11](adr/0012-divergences/lateral-subqueries.md#catalog))

**Derived-table outer references can refuse.**

A subquery’s derived table cannot evaluate enclosing-query references: 0A000 versus PostgreSQL values. (catalog: [lateral-subqueries#r9](adr/0012-divergences/lateral-subqueries.md#catalog))

**Some correlated values cannot be substituted.**

A correlated subquery this engine re-runs per outer row substitutes the outer values as literals, and a value with no literal that reads back as itself stops the query: `(SELECT c.f + x.v FROM x WHERE x.id = 1)` raises 0A000 when a row of `c.f` is a non-finite `DOUBLE`, and so does a `BYTES` value that is not text, where PostgreSQL answers per row. An `ARRAY` outer value is substituted and its expressions answer (`c.arr[1]`, `c.arr = ARRAY[…]`), and so does a subquery that returns the array itself, `(SELECT c.arr …)`, declared as the column is. Where the correlation can be written as a join, write it as one. (catalog: [lateral-subqueries#r12](adr/0012-divergences/lateral-subqueries.md#catalog))

**Recursive UNION without ALL is refused.**

It raises 0A000 where PostgreSQL deduplicates each step; this recursive evaluation form is unavailable. (catalog: [recursion#r5](adr/0012-divergences/recursion.md#catalog))

**A recursive CTE stops at 1,000,000 iterations with 54000.**

PostgreSQL has no iteration limit and runs a recursion that never reaches a fixed point until `statement_timeout` or `temp_file_limit` ends it. This engine iterates to the fixed point the same way and raises 54000 at the millionth iteration of a recursive term that still produces rows; one iteration larger than the memory budget is 53200. Nothing is truncated: the error replaces the answer. (catalog: [recursion#r1](adr/0012-divergences/recursion.md#catalog); ADR-0021 §1o-b, #1246)

**A LIMIT does not stop a recursion early.**

PostgreSQL evaluates a recursive CTE lazily, so `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5` answers five rows there. This engine materializes the closure before it is read, so the same statement is 54000 at the iteration limit. Write the stop into the recursive term's WHERE. (catalog: [recursion#r2](adr/0012-divergences/recursion.md#catalog); ADR-0021 §1o-b)

**A forward reference in a WITH RECURSIVE list is refused.**

PostgreSQL lets any item of a `WITH RECURSIVE` list name a LATER item; this engine resolves an item's body against the items before it, so the later name is 42P01. Mutual recursion between items is 42P01 here and 0A000 there. Both are refusals; reorder the items. (catalog: [recursion#r3, r4](adr/0012-divergences/recursion.md#catalog); arc RC)

**Some recursive terms PostgreSQL refuses are answered.**

An `ORDER BY` or `LIMIT` on the whole recursive body (0A000 there), a term whose integer width differs from the seed's (`SELECT 1 UNION ALL SELECT (n + 1)::bigint …`, 42804 there — the value is range-checked into the seed's width here), a `text` term under a `varchar(n)` seed (one carrier here), and a term of the wrong type that never produces a row (42804 there at parse time; this engine checks the values the term produces) are answered here. A quoted seed (`SELECT '5' UNION ALL SELECT 2 …`) is text here and resolved from the term there, so a non-text term under it is 42804 here. The measured table is `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType`. Two seeds PostgreSQL types `numeric(p,s)` seed an unconstrained `numeric` here, so a term such as `v + 1` answers where PostgreSQL raises 42804: a scalar subquery over a `numeric(p,s)` column (`SELECT 1, (SELECT x.n FROM t x WHERE x.id = 1) UNION ALL …`; this engine declares the subquery's answer without the column's modifier), and a `numeric(38,s)` column or CAST (38 digits is the widest this engine holds). Superset, kept: none of them answers a value PostgreSQL would answer differently. (catalog: [recursion#r6, r7, r8, r9, r10, r11, r12](adr/0012-divergences/recursion.md#catalog); arc RC, arc SS)

**A `TABLESAMPLE` beside an empty join input, or under an `EXISTS` no row reaches, follows this engine's evaluation order.**

`SELECT count(*) FROM e JOIN t TABLESAMPLE BERNOULLI (101) ON e.id = t.id` with `e` empty raises 2202H here in either join order; PostgreSQL answers 0 when its plan reads `e` first and raises when it reads the sample first. `WHERE id < 0 AND EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (101))`, and `WHERE EXISTS (…)` over an empty table, answer 0 on the embedded engine, which evaluates the conjunction per row; PostgreSQL and a cluster evaluate a conjunct that reads no row once, before any row, and raise 2202H. `WHERE id IN (SELECT id FROM big TABLESAMPLE BERNOULLI (101))`, and its `= ANY` form, over an empty table raise 2202H on the embedded engine, which builds the set before it reads a row; PostgreSQL builds it on the first probe and answers 0. (catalog: [other#r21](adr/0012-divergences/other.md#catalog); #1411)

**A subquery that cannot be planned, in an arm no row reaches, answers.**

`SELECT count(*) FROM t WHERE CASE WHEN id > 5 THEN EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (1e400)) ELSE true END` over ids 1–3 answers 3: a subquery's failure is raised when a row evaluates it, and no row reaches the arm. PostgreSQL plans the subquery with the statement, and the argument's coercion to real raises 22003 there. Over ids 1–7, where rows reach the arm, both raise 22003. The same holds for a subquery PostgreSQL fails while planning by folding a constant (`EXISTS (SELECT 1 FROM t WHERE id = CAST('x' AS INT))` in a CASE arm or an OR arm no row needs: PostgreSQL 22P02, 3 here), and for a WHERE CASE over a join input, which PostgreSQL pushes to that input's scan (rows of the input that the join later drops reach the arm there and raise; here the condition is evaluated over the joined rows and the statement answers). The answer is the value the statement has without the raise. (catalog: [other#r22](adr/0012-divergences/other.md#catalog); #1411)

**Two failures in one statement raise in this engine's order.**

`SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (101)) AND 1/0 = 1` raises 2202H; PostgreSQL folds the constant `1/0` when it plans the statement and raises 22012 first. (catalog: [other#r23](adr/0012-divergences/other.md#catalog); #1411)

**`TABLESAMPLE … REPEATABLE (seed)`.**

The repeatable-seed clause is not parsed (42601); PostgreSQL samples with the seed's sequence, so the same seed answers the same rows. (catalog: [other#r17](adr/0012-divergences/other.md#catalog); #1411)

## What is NOT on this list

A value, a row set, a declared type or an error that differs from PostgreSQL 17.11 and is not on this list is a defect: [report them](https://github.com/derekmwright/wadjet/issues/new/choose). EXPLAIN output and timing are not SQL semantics, and row order without ORDER BY is unspecified under [ADR-0013’s legal classes](adr/0013-correctness-gates-and-their-boundaries.md). The SQLancer harness (`task sqlancer:triage`) reads this page as its known-difference list: a generated query whose error matches an entry here is reported under that entry, and everything else it reports is a candidate defect.
