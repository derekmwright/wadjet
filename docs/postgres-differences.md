# How wadjet differs from PostgreSQL

PostgreSQL 17.11 is the SQL semantics authority. Its wire protocol is the contract. This list contains every deliberate difference; report any other difference as a bug.

## Values

**AVG renders fewer digits.**

`AVG` over an integer or a `DECIMAL` column declares `numeric` with no fixed modifier, as PostgreSQL does, and the value is the same number; the rendered text carries scale 4 for an integer input and `min(s+4, 38)` for a decimal one, where PostgreSQL prints up to sixteen significant digits. `AVG(c_i32)`: wadjet `7497.6450`; PostgreSQL `7497.6449875724937862` for the same rows. (ADR-0012 §9/AVG)

**Decimal statistics use double precision.**

Decimal statistics (`STDDEV`, `VARIANCE`, `CORR`, `COVAR`, `MEDIAN`, `PERCENTILE`) use float64; PostgreSQL uses numeric. Fixed-point roots and running means are unavailable. (ADR-0012 §9/statistics)

**TIMESTAMP truncates microseconds.**

Millisecond storage truncates `.123456` return as `.123`; PostgreSQL retains `.123456`. (ADR-0012 §5/#692-residual)

**TIMESTAMP has no infinity.**

`'infinity'` and `'-infinity'` are refused as timestamp input (22007), and a binary timestamp parameter carrying PostgreSQL's infinity encoding (the int64 extremes) is refused at Bind (22023); PostgreSQL stores and returns both. (ADR-0012 §5/#1266)

**Clock functions return zoneless UTC.**

`NOW`, `CURRENT_TIMESTAMP`, `PG_POSTMASTER_START_TIME`: millisecond timestamp OID 1114 versus PostgreSQL’s microsecond timestamptz OID 1184. No session zone exists; `CURRENT_DATE` uses UTC. (ADR-0012 §5/#544, #870)

**CHAR does not blank-pad.**

No blank-padded type exists. `CAST('ab' AS CHAR(4))`: wadjet `ab`; PostgreSQL `ab  `. The OID is 1043, not 1042; bare CHAR is unconstrained, not CHAR(1). (ADR-0012 §5/#708-residual)

**Decimal choices print one scale.**

Storage has one scale per column. `COALESCE(numeric(15,2), 12.3456789012345)`: the ADR’s column value prints `12.7500000000000` versus `12.75`. (ADR-0012 §5/#764)

**Multi-statement strings are not transactions.**

No transactions exist; BEGIN/COMMIT are ignored. `INSERT …; SELECT 1/0`: wadjet retains the insert; PostgreSQL rolls it back. (ADR-0012 §5/#711)

**Integer shifts operate on 64 bits.**

Widening changes int4 results: `2147483647 << 2` gives wadjet `8589934588`, PostgreSQL `-4`; shift results declare bigint. (ADR-0012 §5/#1018-shifts)

**TO_HEX prints a 64-bit word.**

Integer arguments arrive widened. `TO_HEX(int32_col)` for −1 returns `ffffffffffffffff`; PostgreSQL returns `ffffffff`. (ADR-0012 §5/#966-TO_HEX)

**Declared DDL returns SELECT 1.**

Declared CREATE/DROP TABLE uses row results; PostgreSQL sends DDL tags without rows. (ADR-0012 §13/#1024-tags)

**`LIKE` does not honour the default backslash escape.**

`'a%b' LIKE 'a\%b'` answers `f` where PostgreSQL 17.11 answers `t`: this engine's LIKE reads `\` as an ordinary character, so a pattern that escapes a wildcard with the DEFAULT escape matches nothing. Write the escape explicitly — `LIKE 'a!%b' ESCAPE '!'` — which is read exactly as PostgreSQL reads it (#1169). Four matchers implement the pattern language (the scan's pushdown filter, the exec filter, the comparison kernel and the expression evaluator) and all four agree with each other; the default escape belongs to all four. (ADR-0012 §5/#1169-like-default-escape)

**`LOCALTIMESTAMP(p)`'s precision is accepted and ignored.**

`LOCALTIMESTAMP(0) = LOCALTIMESTAMP(6)` is `f` on PostgreSQL 17.11, which truncates the value to the requested precision, and `t` here: this engine renders an instant to milliseconds and has no per-call precision. The wire declares the base type either way. (ADR-0012 §5/#1169-localtimestamp-precision)

**`LOCALTIMESTAMP`, `CURRENT_TIMESTAMP` and `NOW()` are read PER ROW.**

PostgreSQL answers the statement's start time for every row, so `WHERE LOCALTIMESTAMP >= LOCALTIMESTAMP` selects every row. Here the clock is read where the expression is evaluated, so a row that straddles a millisecond can answer FALSE. (ADR-0012 §5/#1169-per-row-clock)

## Declared types

**Some address and UUID functions declare text; assigned to a typed column, their text is read as a literal.**

`INT_TO_IP(n)`, `IP_ADD(ip, n)`, `IP_SUBTRACT(ip, n)`, `MASK_IP(ip, bits)`, `IP_SUBNET(ip)`, `IP_NETMASK(cidr)`, `NETWORK_ADDRESS(cidr)`, `BROADCAST_ADDRESS(cidr)` and `UUID()` declare TEXT (OID 25) though each value is an address or a UUID — PostgreSQL's analogues are typed `inet` / `uuid`. Assigned to a typed column by any write (`INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE`, `MERGE`), such a call is read by the column's input function, as a quoted literal is, where a genuine text expression is 42804: a superset, and the one exception the assignment table keeps. Every date/time function declares and produces its own type (sql-reference.md, Declared types). (ADR-0012 §5/#1254-siblings)

**`DATE_ADD` over text is a timestamp.**

`DATE_ADD(x, n)` and `DATE_SUB(x, n)` (this engine's own functions; PostgreSQL has neither) answer a DATE only for a DATE `x` shifted by whole days; over text — `DATE_ADD('2026-03-03', 1)` — the result is a TIMESTAMP (`2026-03-04 00:00:00`), PostgreSQL's preferred datetime type for an unknown-typed argument. They used to answer text that rendered a date or an instant by the spelling of the input. (ADR-0012 §5/#1254-siblings)

**`tcp_flags` and the MAP functions have no PostgreSQL spelling; their arrays are PostgreSQL's.**

`tcp_flags(18)` is `text[]` (OID 1009) rendering `{SYN,ACK}`; `map_keys`/`map_values` are arrays of the MAP's key/value type in the MAP's stored order, and `map_entries` is an array of `(key,value)` composites (OID 25, as every ROW-element array is). (ADR-0045)

**A MAP renders as `{k: v, …}` under OID 25.**

PostgreSQL has no MAP type, so the rendering is this engine's rule: `{a: 1, b: 2}`, `{}` for an empty map. (ADR-0045)

**An array of a network type declares `text[]`.**

`ARRAY[CAST('1.2.3.4' AS IPV4)]` is `text[]` (1009) rendering `{1.2.3.4}`; PostgreSQL's `ARRAY['1.2.3.4'::inet]` is `inet[]` (1041) with the same text. The scalar network types declare text as well. (ADR-0045)

**`x::int[]` is `bigint[]`, and a fractional literal array is `double precision[]`.**

An array cast's element follows the scalar cast of the same spelling, and `CAST(x AS INT)` is bigint here (ADR-0012 item 12), so `ARRAY[]::int[]` declares 1016 where PostgreSQL declares 1007. `ARRAY[1.5, 2.25]` is `float8[]` (1022) where PostgreSQL's is `numeric[]` (1231) — ADR-0024's literal deferral; the text `{1.5,2.25}` agrees. (ADR-0045)

**Grouped MIN/MAX over REAL declares float8.**

The grouped aggregate's accumulator is the wider type, so `MIN(c_real) … GROUP BY g` declares `double precision` where PostgreSQL declares `real`; the window spelling `MIN(c_real) OVER (…)` keeps `real`. Over `integer` both spellings declare `integer`, as PostgreSQL does. (ADR-0012 §5/#569)

**Integer expressions declare bigint.**

This keeps execution paths consistent. `CAST(SUM(a) OVER () AS INTEGER)` declares OID 20 rather than PostgreSQL’s 23; SMALLINT/INT2 also widen: int16 storage is unavailable. (ADR-0012 §5/#1070)

**ROW declares text.**

Composite text agrees; wire mapping uses OID 25 versus record OID 2249. (ADR-0012 §5/ROW)

**Some ARRAY results still declare text.**

Nested arrays and ROW/MAP elements use OID 25; ordinary arrays — stored, constructed, returned by a function, read through a derived table, VALUES or UNION, and a zero-row result — use PostgreSQL's array OIDs. A nested array renders as PostgreSQL's array_out does (`{{1,2},{3,4}}`) but may be ragged (`{{1,2},{3}}`), which PostgreSQL cannot represent. (ADR-0012 §5/#992-residuals, ADR-0045)

**TIME, JSON and XML casts declare text.**

These casts pass text through. `CAST('12:34:56' AS time)` returns `12:34:56` on both engines, but wadjet declares OID 25. (ADR-0012 §5/#652)

Comparing arrays whose element types differ within the numeric family (`ARRAY[1.5] > ARRAY[1]`) answers by the numbers, where PostgreSQL has no `numeric[] > integer[]` operator and raises 42883. (ADR-0045)

`CAST(ARRAY[1,2] AS JSON)` is `[1,2]` — `to_json`'s text, which `json_array_length` and the other JSON functions read — where PostgreSQL has no cast from `integer[]` to `json` and raises 42846; a ROW is its `to_json` object. Every other non-text destination of a container is 42846 as on PostgreSQL. `CAST(ARRAY[1,2] AS VECTOR(2))` converts as pgvector's cast does. (ADR-0045)

An array of `INTERVAL` has no text here: `CAST(ARRAY[INTERVAL '1 hour'] AS TEXT)` (and `AS JSON`, `AS TEXT[]`) raises 0A000 where PostgreSQL prints `{01:00:00}`, and `SELECT ARRAY[INTERVAL '1 hour']` raises 42000 — this engine has no interval text form (a scalar `INTERVAL` prints its internal fields). An interval that already became text before the array was built (through a derived table or a scalar subquery) is that text. (ADR-0045)

**Decimal set operations keep one declared scale.**

Storage requires one scale. PostgreSQL declares unconstrained numeric; wadjet retains `(p,s)` and prints `12.7500` where PostgreSQL prints `12.75`. (ADR-0012 §12/decimal-carrier)

**Mixed int4-family sets declare bigint.**

PORT with INT32/PROTOCOL uses the common integer representation, OID 20; PostgreSQL’s int4 pair stays OID 23. (ADR-0012 §12/integer-width)

**CTAS stores constrained decimals.**

Stored columns require `(p,s)`. A CTAS over `COALESCE(numeric(15,2), numeric(38,10))` stores DECIMAL(38,10); PostgreSQL stores unconstrained numeric (`12.7500000000` versus `12.75`). (ADR-0012 §13/#1024-CTAS)

**OHLCV decimal fields retain their typmod.**

Fields retain storage types. `(b).open` over DECIMAL(9,2) has typmod 589830; PostgreSQL’s corresponding composite aggregate field has −1. (ADR-0012 §9/#965-field-typmod)

**A star over a LATERAL whose body is itself `SELECT *` qualifies a name the two arms share.**

`SELECT * FROM lt_o o JOIN LATERAL (SELECT * FROM lt_i i WHERE i.k = o.k) s ON true` names the body's `id` and `k` `s.id` and `s.k` on every execution path, where PostgreSQL names them `id` and `k`. A star over a LATERAL is expanded into the FROM items' own lists, but a body whose own list is a star is not enumerated there, so that arm is read off the join's stream, which qualifies a duplicate name by its owning alias. Values, types and positions agree. A body that names its columns publishes PostgreSQL's names. (ADR-0012 §5/#1126)

## Errors and refusals

**`SUBSTRING(text SIMILAR pattern ESCAPE escape)` is refused.**

The standard's capture-marker spelling raises 0A000 naming the construct, where PostgreSQL 17.11 answers the part of the string between the pattern's `#"` markers: this engine translates a SIMILAR TO pattern into a regular expression and that translation has no notion of a returned portion. `SUBSTRING(text FROM regexp)` and `REGEXP_EXTRACT(text, regexp, group)` both answer. (ADR-0012 §5/#1169-substring-similar)

**A SIMILAR TO pattern the translation cannot compile is 2201B with this engine's own message.**

`'abc' SIMILAR TO '*'` and `'abc' SIMILAR TO '['` raise 2201B on both engines; the message names the pattern here and names the regex engine's own complaint there. (ADR-0012 §5/#1168)

**A repeated name in a column-alias list is refused at the list.**

`FROM t AS a(k, k)` raises 42701 naming the spelling, where PostgreSQL accepts the list and raises 42702 at every reference. This engine renames positionally and cannot publish one name for two columns. On a TABLE FUNCTION whose relation is narrower than the list, PostgreSQL raises 42P10 for the same statement. (ADR-0012 §5/#959, #1184)

**A file or database reader whose input is not readable at plan time is measured when it produces its first batch.**

A reader over a REGULAR file publishes its columns at PLAN time: `read_parquet` from the file's footer, `read_json` and `read_csv` from a SAMPLE of its first 100 rows, read before the statement binds and only after the table-function capability has been authorized for the calling identity (ADR-0039 §3, ADR-0034). Over such a reader `FROM read_json(…) AS f(a, b, c)` is 42P10 at plan time and an unknown column is 42703 at plan time, exactly as over a base table. A non-NULL value past the sample that does not fit the inferred type refuses, naming the reader, file, row, column and types, with the SQLSTATE PostgreSQL's COPY raises for the same field (`22P02`, `22003` out of range, `22007` for a timestamp); a `read_csv` field is read with PostgreSQL's input function for bigint, double precision and boolean. A timestamp column takes only the sample's own spellings (`Jan 2 2024` is `22007` where PostgreSQL reads it) and an inet column refuses `10.0.0.1/32`. A key first seen past the sample is still absent; see [the SQL reference](sql-reference.md).

Four inputs are not read at plan time and keep the first-batch behaviour: an `http(s)` source — a plan-time fetch would be a second request for every statement and would make `EXPLAIN` reach the network; the database connectors (`postgres_scan`, `postgres_query`, `mysql_scan`, `mysql_query`), whose schema is a remote query's; an input that can only be read ONCE (a FIFO, `/dev/stdin`, a socket, a process substitution), because the plan-time read opens the input and the execution opens it again; and a glob any of whose matches is one of those. For those, `42P10` and `42703` are raised at execution rather than while the statement is bound, a reader that produces NO batch is never measured against its alias list, and `EXPLAIN` over such a statement does not refuse. (ADR-0012 §5/#1184, #1210, #1230)

**`read_csv` reads `COPY … (FORMAT csv)`'s grammar, except that blank lines are skipped, line endings may be mixed, a trailing empty field is dropped, `\.` is data and a byte-order mark is skipped.**

A field is NULL only when it is empty and unquoted (`""` is the empty string), a quote opens anywhere in a field, whitespace is data, and an unterminated quote and a record of the wrong width are `22P04` — as `COPY` reads the same bytes. A blank line in a multi-column file (a trailing one included) is skipped, where `COPY` raises `22P04 missing data`, LF, CR and CRLF may be mixed in one file, where `COPY` raises `22P04 unquoted carriage return found in data`, and a trailing delimiter whose extra fields are empty (`1,x,`) reads as the record without them, where `COPY` raises `22P04 extra data after last expected column` — blank lines, mixed LF/CRLF endings and trailing empty fields retain their earlier readings; a lone CR now ends a record, where v0.24.0 read it into the field. A SHORT record stays `22P04`, as in `COPY`, although this reader used to pad it with NULLs. A line holding `\.` is DATA here, where PostgreSQL 17 ends the input at it and reads no later row (PostgreSQL 18 no longer does in a file); a UTF-8 byte-order mark at a file's start is skipped (with `header=false`, from the first data value), where `COPY` keeps it in the first field; and a NUL byte is stored where `COPY` raises `22021`. (ADR-0012 §5/#1248, #1259)

**A reader whose input cannot be opened is `58P01` / `42501` / `42809` at plan time, and `EXPLAIN` over it is refused.**

`SELECT * FROM read_json('/missing.json')` and `EXPLAIN` over it raise `58P01 could not open file "/missing.json" for reading: no such file or directory` — `COPY FROM`'s and `pg_read_file`'s class, as are `42501` for a file that may not be read and `42809` for a directory. PostgreSQL's `EXPLAIN` over a missing RELATION is `42P01`; the class here is the input's. An `http(s)` source is refused at its first batch (a 404 is `58P01`), so `EXPLAIN` over one prints a plan. (ADR-0012 §5/#1245)

**A reader whose input declares no columns at all is a named refusal, where PostgreSQL has a zero-column relation.**

`SELECT * FROM read_json('<zero-byte file>')` raises `0A000 the table function "read_json" published no columns: its input "…" is empty`. PostgreSQL permits a relation with zero columns (`CREATE TABLE t (); SELECT * FROM t` answers zero rows of zero columns) and this engine does not, at any door — a result that declares no columns is not an answer it has. A Parquet file carries its schema in the footer and a CSV in its header row, so an empty file of either kind is an ordinary empty relation: zero rows, columns declared. (ADR-0012 §5/#1230)

**`generate_series(…) WITH ORDINALITY` publishes one column.**

PostgreSQL adds a second `ordinality` column to any function in FROM; this engine adds it for `unnest` only, so `generate_series(1,2) WITH ORDINALITY` publishes `generate_series` alone — and a two-name column-alias list over it is 42P10. (ADR-0012 §5/#1210-ordinality)

**An aggregate over an HTTP or database reader's column declares double precision.**

`SELECT SUM(a) FROM read_json('http://…')` declares and boxes float8, and `SELECT f.* FROM read_json('http://…') AS f` is 0A000, because those two input kinds are the ones with no plan-time column list for the result-type rules to read (above). Over a LOCAL file the reader's column carries its type: `SUM` over a whole-number column is `numeric` and `MIN`/`MAX` keep its width, which is what PostgreSQL declares for the same `bigint` column. The declared functions carry their width too: `generate_series(1,3)` publishes `integer` and its SUM is `bigint`. (ADR-0012 §5/#1211, #1230)

**`generate_series` never flips the caller's step.**

`generate_series(5,1)` is an EMPTY relation — zero rows of one column — on both engines; the descending series is `generate_series(5,1,-1)`. Through v0.22.0 this engine negated a positive default step whenever start > stop and answered the descending series instead. A zero step is 22023 with PostgreSQL's own sentence. (ADR-0012 §5/#1210-series)

**Some known casts leave values unchanged.**

DURATION, BYTES, VECTOR and container destinations can retain the operand because conversion is unimplemented; recognizing the type does not perform a conversion. (ADR-0012 §5/#652)

**Undescribable results are refused.**

A `SELECT *` over a LATERAL whose own list names one column twice declares both columns with no rows, the second named `s.m` where PostgreSQL says `m` — the name the non-empty result gives it too (the list cannot be enumerated by name, so the star reads the join's output, which qualifies a duplicate name by its owning alias). Before 2026-09-25 the empty result declared the duplicate once: four columns for PostgreSQL's five. Over an UNGROUPED AGGREGATE body (`SELECT MAX(x) AS m, MIN(x) AS m …`) the zero-row star is `XX000`: the join carries the empty-input pad marker, which the declaration will not publish. A star over any other LATERAL — two or more of them, one beside another join, or an ungrouped aggregate — is expanded into the FROM items' own lists and declares its columns with no rows (arc JP round 4, #1013). Recursive CTEs now retain the seed's declared columns for an empty result (ADR-0021 §1o-b). (ADR-0012 §5/#1008, #1010)

**Table metadata follows table access.**

Tables unavailable for reading are omitted from listings; DESCRIBE raises 42501. PostgreSQL exposes more catalog metadata; wadjet includes names in table access. (ADR-0012 §5/2026-09-06/metadata)

**UNION refuses mixed VECTOR widths.**

Fixed-width UNION/UNION ALL raises 22000. PostgreSQL/pgvector drops the width modifier; its INTERSECT/EXCEPT behavior is unverified. (ADR-0012 §5/#900)

**Derived names use expression spelling.**

Internal names must distinguish expressions. Wadjet accepts `"g + 1"` and refuses `"?column?"` with 42703; PostgreSQL accepts the published `"?column?"`. (ADR-0012 §5/#732-derived-names)

**Some quoted names are refused.**

Names become storage components: `/`, `\`, NUL, initial dots, `.` and `..` cause 42602 where PostgreSQL accepts the identifier. (ADR-0012 §5/2026-09-05/names)

**Names over 63 bytes are refused.**

Wadjet raises 42622 instead of PostgreSQL’s truncation and notice, to keep storage locations distinct. (ADR-0012 §5/2026-09-05/name-length)

**Ambiguous ORDER BY names select the first output.**

First-match binding means `ORDER BY u` over two outputs called `u` answers here, versus PostgreSQL 42702. (ADR-0012 §5/#557)

**Name lookup tolerates case differences.**

For imported names, `SELECT WatchID FROM hits` can read `WatchID` where PostgreSQL raises 42703. Tables/lowercase quoted names also qualify; case-colliding join columns cause 42702 where PostgreSQL answers. (ADR-0012 §5/#731)

**Bare ROW field paths answer.**

`c_row.b` resolves a field; PostgreSQL raises 42P01 and requires parentheses. The parenthesised spellings, qualified `(x.c_row).b` and nested `((c_row).rw).k` included, answer as PostgreSQL does. (ADR-0012 §5/#769, ADR-0022)

**Some temporal casts return NULL.**

Boolean/container temporal casts retain NULL where PostgreSQL raises the type-pair error 42846; 22007/22008 describe text, not these inputs. (ADR-0012 §5/#836, #840)

**Integer-to-DATE casts answer.**

Signed 32-bit day counts work; out-of-range values raise 22003. PostgreSQL raises 42846, including for `0::date`. (ADR-0012 §5/#911)

**DMY date errors differ.**

`'31/12/1996'` raises 22007 here versus PostgreSQL’s 22008 under ISO, MDY. Wadjet rejects DateStyle-dependent field order. (ADR-0012 §5/#840-temporal-grammar)

**SELECT compares numeric spelling with text.**

Generic comparison accepts `s = 1.50` as `s = '1.50'`; PostgreSQL raises 42883. DML predicates refuse this pair with 42883. (ADR-0012 §5/#504, #721)

**Unary minus accepts numeric text.**

Generic resolution means `SELECT -'5'` returns varchar `-5`; PostgreSQL raises 42725. Non-numeric text raises 22P02 here. (ADR-0012 §5/#505)

**HAVING resolves output aliases.**

Output aliases share the grouping scope. `SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 1` answers here; PostgreSQL raises 42703. (ADR-0012 §5/#591)

**BIGINT casts to BOOLEAN.**

The int4 rule extends to int8: zero=false, otherwise=true. PostgreSQL refuses the int8 cast with 42846. (ADR-0012 §5/#592)

**DECIMAL cannot store NaN or infinities.**

Finite storage raises 22003 versus unconstrained PostgreSQL numeric values. Comparisons remain available. (ADR-0012 §5/#534; 12/carrier)

**JOIN ON sees comma-join siblings.**

An ON may name a relation an EARLIER comma-separated FROM item declares. `FROM a, b JOIN c ON a.k = c.k` answers here; PostgreSQL refuses the reference. (ADR-0012 §5/#617)

**Decimal arithmetic stops at 38 digits.**

Precision stops at 38 while exact operators retain scale. DECIMAL(38,10) multiplication produces DECIMAL(38,20), raising 22003 beyond its range where PostgreSQL numeric answers. (ADR-0012 §5/#749)

**SUM overflow survives cancellation.**

Overflow remains recorded even after cancellation: `+9e37, +9e37, -9e37` fails here while PostgreSQL numeric answers. This prevents wrapping. (ADR-0012 §9/SUM-overflow)

**Computed SUM can remain bigint.**

Unknown argument width leaves bigint results: totals beyond bigint raise 22003 versus PostgreSQL numeric values. (ADR-0012 §9/computed-SUM-residual)

**Qualified GROUP BY can refuse.**

`SELECT g + 1 ... GROUP BY typemx.g + 1` raises 42803 here; PostgreSQL answers. Evaluation requires the input’s unqualified name. (ADR-0012 §5/#738)

**VARCHAR declarations discard length.**

`CREATE TABLE t (v VARCHAR(4))` discards the length; an overlong INSERT answers here versus PostgreSQL 22001. Casts enforce length. (ADR-0012 §5/#708-DDL)

**Integer calculations can exceed int4.**

`2147483647 + 1` returns `2147483648` versus PostgreSQL 22003. Widening also permits ABS predicates at int4’s minimum. (ADR-0012 §5/#1070; int4-store-residual)

**Text functions over BYTES refuse, except `strpos`.**

`upper(b)`, `lower(b)`, `trim(b)`, `reverse(b)`, `replace(b,…)`, `starts_with(b,…)`, `split_part(b,…)`, `lpad(b,…)`, `repeat(b,…)` and `char_length(b)` raise 42883 here as they do on PostgreSQL. `strpos(bytea,bytea)` still answers, because `POSITION(sub IN b)` — which PostgreSQL DOES have over bytea — is rewritten into it; one spelling answering where the other refuses is the residue. `ENCODE`/`DECODE` are the supported bridge. (ADR-0012 §5/#583)

**Text functions render a non-text COLUMN.**

`upper(mac_col)`, `substr(date_col, 1, 4)`, `length(ipv4_col)` and `upper(int_col)` answer here versus PostgreSQL 42883: a column of another type is rendered as its text before a string function reads it, which is what makes the network-analytics shapes work. A numeric LITERAL in the same position is 42883 on both, and so are a `BYTES` operand in a text-only position and a TEXT operand in `ENCODE`'s. (ADR-0012 §5/#500, #1056)

**Planner column-name prefixes are reserved.**

`SELECT amount AS __key_0` raises 42939 here; PostgreSQL answers. Intermediate columns need these names; stored columns remain readable. (ADR-0012 §5/#956)

**A BYTES, container or DURATION value is not assigned to a text column.**

`INSERT INTO t (text_col) SELECT bytes_col` (and the same through VALUES, UPDATE and MERGE) raises 42804 here; PostgreSQL converts a bytea, an array or an interval to its text. Every other scalar is assigned to TEXT as PostgreSQL renders it. (ADR-0012 §13/#1024-assignment)

**Quoted INSERT SELECT targets are limited.**

BOOL, BYTES and container targets raise 42804 rather than using PostgreSQL’s input conversion, because writers cannot parse text. (ADR-0012 §5/#1088)

**FROM_HEX can return NULL.**

`FROM_HEX('12zz')` returns NULL; PostgreSQL’s `decode('12zz','hex')` raises 22023. Wadjet follows its parse-or-NULL convention. (ADR-0012 §5/#966-FROM_HEX)

**Skipped CTAS sends no notice.**

`CREATE TABLE IF NOT EXISTS … AS SELECT` over an existing table sends PostgreSQL’s command tag but omits its notice; interfaces lack a shared notice channel. (ADR-0012 §13/#1024-notices)

## Ordering and collation

**Strings use binary collation.**

Binary comparisons avoid locale-dependent work; PostgreSQL C collation gives matching order. (ADR-0012 §5/Collation)

**MAP and VECTOR have wadjet-defined order.**

Neither type exists in PostgreSQL core; wadjet defines their total orders. (ADR-0012 §5/MAP-VECTOR-ordering)

## Extensions

**QUALIFY follows DuckDB 1.1.3.**

PostgreSQL has no QUALIFY. It filters after windows, can read unprojected inputs, resolves inputs before output aliases, and requires a window function. (ADR-0012 §13/#1076)

**Network-native types have separate storage domains.**

`IPV4`, `IPV6`, `CIDR` and `MAC` are native column types with PostgreSQL's `inet`, `cidr` and `macaddr` input grammar at every boundary — the writer, `CAST`, and a literal — but they declare `text` (OID 25) on the wire; `UUID` declares `uuid` (2950); `PORT` and `PROTOCOL` declare `integer` (23). A `CIDR` keeps host bits an `inet` would keep; PostgreSQL's `cidr` refuses them. See the [input grammar table](data-types.md#network-types). (ADR-0012 §5/#627; §5/ROW)

**PORT/PROTOCOL constrain casts and writes.**

Casts/writes enforce 0–65535 and 0–255 (22003); arithmetic may leave those ranges. Both declare int4. Their own text grammar is read at every door, the comparison included: `CAST('udp' AS PROTOCOL)` and `WHERE proto = 'udp'` are both 17, and int4's radix spellings are not theirs — `WHERE port = '0x1bb'` is 22P02. (ADR-0012 §5/#1092-residual-2, §5/#1137)

**The `^` operator answers two spellings PostgreSQL rejects.**

`2 ^ -1` is 0.5 here; PostgreSQL lexes `^-` as one operator name and answers `operator does not exist: integer ^- integer` — write `2 ^ (-1)`, which both answer. `2 ^ 3 % 5` is 3 here, because `^` binds tighter and this engine's `%` takes the float result; PostgreSQL has no `double precision % integer`. The values, the `2201F`/`22003` error classes and the declared type are `POWER()`'s. (ADR-0012 §5/#1155)

**DURATION counts nanoseconds.**

Storage and wire use bigint nanoseconds, OID 20, versus PostgreSQL’s microsecond interval. (ADR-0012 §5/#834)

**A FULL JOIN on a non-equi ON condition answers.**

`FULL JOIN b ON a.n < b.n` raises `FULL JOIN is only supported with merge-joinable or hash-joinable join conditions` on PostgreSQL, which has no executor for it. The join is still DEFINED there — the LEFT JOIN plus the build rows no probe row satisfies — and that is what this engine answers. Superset, kept; the 14 shapes are cells of `coordinator.TestJRAOuterJoinOnResidualsAgreeOnFiveArms`. (ADR-0012 §13/#1153)

**MIN/MAX accepts additional types.**

BOOL, UUID, MAC, BYTES, MAP and VECTOR have defined orders here; PostgreSQL lacks these aggregates. BYTES uses bytewise order and retains bytea OID 17. MIN/MAX over a ROW is 42883, as on PostgreSQL. (ADR-0012 §5/#569, #570, #1061)

**Aggregate arguments read the wire types.**

SUM, AVG, STDDEV, VARIANCE, CORR and COVAR accept PORT, PROTOCOL and DURATION as the int4/int8 the wire declares them; PostgreSQL's `interval` has no STDDEV. MEDIAN, QUANTILE_* and the plain calls `mode(x)`, `percentile_cont(p, x)` and `percentile_disc(p, x)` answer over numbers (PostgreSQL raises 42809 for the plain ordered-set calls); refused, MODE and PERCENTILE_DISC raise 42809 `WITHIN GROUP is required`, PERCENTILE_CONT and MEDIAN/QUANTILE_* 42883. `STRING_AGG` renders BOOL, numbers, network values, UUID and DATE as their text where PostgreSQL raises 42883; over TIMESTAMP or a container it raises 42883, over BYTEA 0A000 (PostgreSQL answers). Every other argument PostgreSQL has no overload for raises its 42883 (`function sum(text) does not exist`), and `SUM('5')` / `SUM(NULL)` its 42725. (ADR-0012 §5/arc BR, #1249)

**Text compares with typed values, pair by pair.**

Text compared with an integer, double, numeric, PORT, PROTOCOL, DURATION, UUID, IPv6 or CIDR value — directly or in an IN list — answers through the value's text; an `IN`/`= ANY` subquery is accepted where its body selects `CAST(x AS TEXT)` of the compared type; with a DATE, TIMESTAMP or boolean it answers directly and in an IN list. A stored-text body can answer no rows for matching values (and NOT IN every row), tracked by #1308; this is not a working extension. PostgreSQL raises 42883 for all of them. A set-operation subquery body (UNION, INTERSECT, EXCEPT) is kept only when each arm selects `CAST(x AS TEXT)` of a value of the compared type; any other text there raises 42883. Text against REAL, BYTEA, IPv4 or MAC, text membership against a DATE/TIMESTAMP/boolean subquery, and two text/typed COLUMNS as a JOIN key raise 42883 here too. (ADR-0012 §5/arc BR, #826, #1073)

**A timestamp minus a timestamp answers milliseconds; a date minus a date is bigint.**

`ts1 - ts2` (and `now() - now()`) answers the difference as a number of milliseconds where PostgreSQL answers an `interval` (`01:00:00`): this engine has no INTERVAL column type, only the INTERVAL literal. Such a value assigned to a DATE, TIMESTAMP or address column is 42804, as PostgreSQL's interval is. `date1 - date2` is the day count PostgreSQL answers, declared `bigint` (OID 20) where PostgreSQL declares `integer`. (ADR-0012 §5/arc VL)

A date minus a timestamp (PostgreSQL: an `interval`) is refused 42883 here, as the pairs PostgreSQL has no operator for are (`timestamp + integer`, `date + numeric`, `integer - date`, `date + timestamp`) — it used to answer the day count minus the millisecond count. The refusal is part of typing the expression, so it holds in every statement that evaluates one: a SELECT's clauses, `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` SET and WHERE, `DELETE` WHERE, `MERGE` and CTAS. `ts - ts` is declared `double precision` (the millisecond count). (ADR-0012 §5/arc VL)

A quoted operand beside a DATE or TIMESTAMP is typed by PostgreSQL's operator resolution, in every statement: `date + '…'` and `'…' + date` are 42725 `operator is not unique` (NULL too); the literal of `date - '…'` is a DATE (a day count, `d - '2026-03-01'`), of `ts + '…'` an INTERVAL (a TIMESTAMP, `ts + '1 day'`), and of `ts - '…'` a TIMESTAMP — whose difference is the millisecond count above, where PostgreSQL answers an `interval`. A literal that is not the type it resolves to is 22007 at plan time. It used to be read by its leading number (`ts + '1 day'` was ts plus one millisecond). (ADR-0012 §5/arc VL)

**INTERVAL.** There is no INTERVAL column type; an INTERVAL value is declared `text` (OID 25) on the wire where PostgreSQL declares `interval` (OID 1186), printed in PostgreSQL's `postgres` style (`1 day`, `-1 days`, `1 year 2 mons`, `25:00:00`). The single-unit spellings the `INTERVAL '…'` literal takes (`'1 day'`, `'90 minutes'`, `'5'` seconds) are that interval in a text CAST too; any other interval text PostgreSQL reads (`'1 day 02:00:00'`, `'00:30:00'`, `'1 year 2 mons'`, `'P1D'`, `'1.5 days'`) is kept as written — PostgreSQL's own value when the text is PostgreSQL's output — and applying it to a date or timestamp is 0A000. Text PostgreSQL refuses is 22007. Into a TEXT column an INTERVAL stores its text; into any other column it is 42804. An INTERVAL field past PostgreSQL's own range (`INTERVAL '100000000000 hours'`) is 22015 there at the literal and 22008 here when it is applied. (ADR-0012 §5/arc VL)

**A number literal against a timestamp reads epoch milliseconds.**

`c_ts >= 1700000000000` compares against the instant that many milliseconds after the epoch; PostgreSQL raises 42883. (A number against TEXT is the entry "SELECT compares numeric spelling with text" above, kept by arc BR.) A number against a boolean, and a boolean literal against a number, raise 42883 on both. (ADR-0012 §5/arc BR, #1216)

**Two ROW shapes do not fold.**

A CASE, COALESCE, GREATEST, LEAST or set operation over two ROW columns of different shapes raises 42846 `could not convert type`, as PostgreSQL does for named composite types; its anonymous `ROW(…)` records answer. A quoted literal in a ROW's or ARRAY's own grammar inside such a fold raises 0A000 where PostgreSQL reads it. (ADR-0012 §5/arc BR, #1060, #1065)

**A set operation's ORDER BY takes the first arm's qualified column.**

`SELECT a.id … UNION ALL … ORDER BY a.id` answers; PostgreSQL raises 42P01. Any other qualifier raises 42P01, a name that is no result column 42703 (exactly, `ORDER BY "ID"` over `id` included), and an expression PostgreSQL's transform error or 0A000. (ADR-0012 §5/arc BR, #1236)

**DISTINCT works on additional aggregates.**

`MEDIAN(DISTINCT a)`, `APPROX_DISTINCT(DISTINCT a)` and `MIN_BY(DISTINCT a,b)` deduplicate here; PostgreSQL refuses the corresponding ordered-set form or has no function. (ADR-0012 §5/#703)

**TIME_BUCKET supplies a default origin.**

It follows date_bin with default `1970-01-01`; PostgreSQL requires an origin. Only INTERVAL literals work (otherwise 42804); months/years raise 0A000, non-positive strides 22008. (ADR-0012 §5/#965-TIME_BUCKET)

**OHLCV returns a bar.**

PostgreSQL requires separate aggregates. VWAP uses AVG’s scale; zero volume returns NULL versus the quotient’s 22012, preserving other bars. DISTINCT/window forms raise 0A000. (ADR-0012 §5/#965; §9/VWAP)

**TCP flag functions name bits.**

`tcp_flags_has_all(f,'SYN','ACK')` equals PostgreSQL `(f & 18) = 18`. Empty or unknown name lists raise 22023 instead of dropping requested bits; PostgreSQL has no named-bit functions. (ADR-0012 §5/#966-TCP-flags)

**Bitwise helpers follow Trino conventions.**

`BITWISE_RIGHT_SHIFT` is logical; PostgreSQL `>>` preserves sign. Out-of-range shift counts return NULL rather than modular counts; `TO_BASE` uses signed text for negatives. (ADR-0012 §5/#966-bitwise)

**Semver has its own authority.**

PostgreSQL has none. SemVer 2.0.0 defines precedence; node-semver defines ranges. Leading v/V is accepted; components stop at int64. Malformed versions return NULL (strict normalization: 22023). Empty ranges, partial-version suffixes, leading zeros and oversized components raise 22023. (ADR-0012 §5/#967)

**`NORMALIZE` accepts a quoted form.**

`NORMALIZE(s, 'NFC')` answers here and is a syntax error on PostgreSQL 17.11, which admits only the bare keyword. Both spellings mean the same thing. (ADR-0012 §5/#1169)

**`#` accepts operands PostgreSQL refuses.**

`5.0 # 3` and `'a' # 'b'` answer here (the bitwise family reads its operands as integers) where PostgreSQL raises 42883 and 42725. The declared WIDTH agrees: `int4 # int4` is `integer` and a bigint operand makes it `bigint`, on both engines. (ADR-0012 §5/#1179)

## Not supported

**The system catalog describes one database, one role and this server's objects.**

`pg_database` lists one database and `pg_roles` one role, the connection's identity, which is not a superuser; PostgreSQL also lists its templates and bootstrap superuser. `pg_class.relam` is 0 and `pg_am` is empty (a stored table has no PostgreSQL access method), `pg_type` lists the types the wire declares and their arrays but no DOMAIN types, `pg_proc` lists no functions, and the relations for objects this server does not have (indexes, triggers, rules, policies, publications, sequences) are empty. A column is typed by the engine type that carries it — an OID column declares `int8`, and `current_schemas()` is `text[]` where PostgreSQL's is `name[]`. A masked column's definition is listed and its values arrive masked; a denied column is absent. A string literal cast to `regclass` is read to its OID and prints as the OID where PostgreSQL prints the name. The server reports PostgreSQL 17 (`server_version` 17.0, `server_version_num` 170000), the major whose catalog it models, so psql and pgJDBC send the catalog spellings this catalog has. (ADR-0044, #1251)

**A syntax error names no position field, only its sentence's own `at or near`.**

A statement the parser cannot read is 42601 with PostgreSQL's sentence — `syntax error at or near "…"`, `syntax error at end of input`, and for `E'…'` strings `invalid Unicode surrogate pair at or near "…"` (42601), `invalid Unicode escape` (22025), `invalid Unicode escape value at or near "…"` (42601) and `invalid byte sequence for encoding "UTF8": 0x…` (22021) — the escape errors' own `at or near "…"` suffix names the offending escape's own source text (`\uDE00`, `\U00110000`) or the character that broke a surrogate pair, matching PostgreSQL's wording (#1307). PostgreSQL also sends a structured error POSITION field, which psql renders separately as `LINE 1: …` with a caret; this server sends none — the sentence carries the location, the wire protocol's own position field does not. Where the parser stops early inside a subquery, the token named is the `)` that closes it, as in PostgreSQL. (ADR-0044)

**Set-returning functions answer only as a whole SELECT item.**

`unnest(array)`, `generate_subscripts(array, dim)` and `information_schema._pg_expandarray(array)` expand each row when they ARE a SELECT item, by PostgreSQL 10's rule (the longest set decides the row count, shorter sets are padded with NULL). Inside an expression, in WHERE, beside an aggregate, a window function or DISTINCT they are refused 0A000 where PostgreSQL answers (or, in WHERE, refuses with the same code). `generate_subscripts`' dimension must be a constant, and a table function's arguments must be constants: `generate_series(1, t.n)` is 0A000. An `ARRAY[…]` constructor's elements take one common type, as in PostgreSQL; a constructor of constants only is typed by ADR-0024's literal rule, so `unnest(ARRAY[1,2.5])` answers 1, 2.5 declared `double precision` where PostgreSQL declares `numeric`. (ADR-0044)

**A subquery in `INSERT ... VALUES` is refused.**

PostgreSQL accepts a scalar subquery in a plain `INSERT INTO t VALUES (...)` cell — it is a constant to the statement, the same as it is in a `SELECT` list. Each VALUES cell here is a full scalar expression (#1252) evaluated with no row and no query environment, so a subquery is refused 0A000 rather than run. `MERGE ... WHEN NOT MATCHED THEN INSERT ... VALUES` is not this restriction: its VALUES clause has the merged row's environment already, and a subquery there compiles and runs as it does in a `WHEN` condition.

**`CURRENT_TIME` is not a supported function.**

This engine has no TIME type among its 22 (`Type System`), so `CURRENT_TIME`'s SQL-standard niladic spelling parses — the parser recognizes it as a reserved keyword the way it does `CURRENT_DATE` — but the call itself is refused, `unknown function: current_time`, rather than declaring a value with no representation. `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` and `NOW()` are unaffected (#1254).

**The pattern-match operators match with RE2.**

`~ ~* !~ !~*` translate PostgreSQL's ARE form by form; a back reference, lookahead/lookbehind, `\m`/`\M`, `[[:<:]]`, a collating element and the `b e n p w x` embedded options are refused 0A000. Case-insensitive matching folds ASCII letters only, as PostgreSQL does under the C collation. (ADR-0044)

**COLLATE accepts the byte-order collations only.**

`C`, `POSIX`, `ucs_basic` and `default` are this server's order and are accepted; any other collation is refused 0A000 rather than compared by bytes. (ADR-0044)

[SQL reference](sql-reference.md).

**`LOCALTIME`, `IS [form] NORMALIZED` and the `U&'…'` literal have no grammar.**

`LOCALTIME` needs a TIME type this engine does not have; `'abc' IS NORMALIZED` and `U&'\0065\0301'` are 42601 where PostgreSQL answers. `LOCALTIMESTAMP` and `NORMALIZE(s, form)` are supported. (ADR-0012 §5/#1169)

**A column-alias list over a star whose width is not known is refused.**

A star without a known width cannot be renamed positionally: 0A000 where PostgreSQL answers. (ADR-0012 §5/#958)

**Unresolved decimal set-operation arms can refuse.**

Unknown types/scales cause distributed refusal to avoid decimal reinterpretation; local execution and PostgreSQL can answer. (ADR-0012 §12/#551)

**JOIN USING merges, but not for every shape.**

`SELECT *` over a `JOIN … USING` publishes the joined column once and first, as PostgreSQL does, and publishes a name the two arms share OUTSIDE the USING list twice, as PostgreSQL does. A chain of joins raises 0A000 where PostgreSQL answers. An arm publishing one name twice raises 42702 naming the column, which is PostgreSQL's own class for it. (ADR-0012 §5/#810, #655, #1177)

**A `SELECT *` over a `JOIN … USING` with a LATERAL arm publishes the joined column twice.**

`SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id FROM lat_item i WHERE i.order_id = o.id) l USING (id)` publishes `id, customer, total, id` where PostgreSQL publishes `id, customer, total`: the star is expanded into both arms' own lists, but the USING merge is not applied over a LATERAL's lowered join, so the joined column is published a second time. (ADR-0012 §5/#1177-lateral-using)

**A USING merge of two DECIMAL columns at different scales declares the left arm's.**

`SELECT * FROM zzp JOIN zzj USING (id, d92)` declares the merged `d92` at the left arm's DECIMAL(9,2) where PostgreSQL declares unconstrained `numeric`, the common type of the two. The merged column's VALUE is the left arm's by the same rule on both engines; only the declaration differs. (ADR-0012 §5/#1177)

**A bare reference to a USING join's merged column is ambiguous here, outside a sort or window key.**

`SELECT id FROM a JOIN b USING (id)` raises 42702 where PostgreSQL answers, because USING merges the column and the reference is not ambiguous there; the same for a WHERE, a GROUP BY, a HAVING and a DISTINCT. Qualify it (`a.id`). In an ORDER BY or a window key the same reference BINDS THE MERGE and answers. (ADR-0012 §5/#655)

**A bare SELECT * over a FULL JOIN … USING cannot be ordered by the merged column.**

It raises 0A000 where PostgreSQL answers: the merged value is COALESCE of the two sides, computed by the projection the star expands into, and this planner materializes a computed sort key beside a NAMED select list. Name the columns, which answers; so does the same statement without the ORDER BY. The positional spelling is the same refusal. (ADR-0012 §5/#655)

**A window key naming a FULL JOIN … USING merged column is refused.**

It raises 0A000 where PostgreSQL answers: the merged value is a COALESCE and a window PARTITION BY / ORDER BY key here is a column name. Write the expression. A window ARGUMENT is an expression and binds the merge on RIGHT and FULL alike. The refusal is drawn on the SHAPE, so on data where the merged and left-arm partitionings coincide it withdraws an answer that would have been right. (ADR-0012 §5/#655)

**NATURAL JOIN is refused.**

It raises 0A000 where PostgreSQL answers: the keys are whatever columns the two sides share, which is a catalog question the parser cannot answer. Write the condition with ON or USING. (ADR-0012 §5/#655)

**A column-alias list on a WITH-query REFERENCE is refused.**

`FROM c z(x, y)` raises 0A000 where PostgreSQL answers; the rename would land above the query's own block and every renamed reference would read NULL. Put the list on the definition, `WITH c(x, y) AS (…)`. (ADR-0012 §5/#959)

**A column-alias list that repeats a name is refused.**

`FROM t a(k, k)` raises 42701 where PostgreSQL accepts the list and refuses only a reference to `k` (42702). This planner renames positionally and cannot publish one name for two columns; refusing the list is narrower than PostgreSQL, never a wrong value. (ADR-0012 §5/#959)

**A subquery inside an OUTER join's ON clause is refused.**

`LEFT JOIN b ON a.x = (SELECT max(y) FROM c)` raises where PostgreSQL answers. An outer join's ON is evaluated AT the join, per probe row against each candidate build row, because a conjunct lifted above it would delete the rows the join preserves — and a subquery's value is not available there. The refusal names the construct. An INNER join lifts the same ON into a filter above the join and answers it. (ADR-0012 §5/#1153)

**BYTEA, MONEY and INET type names are refused.**

These types lack representations: casts/declarations raise 42704 versus PostgreSQL values. (ADR-0012 §5/#652)

**Address storage restricts accepted values.**

A value PostgreSQL inet holds can exceed IPV4/IPV6’s representation: `CAST('10/8' AS IPV4)` raises 0A000. Host-width prefixes are accepted. (ADR-0012 §5/#627, #1092)

**Some set-operation type pairs are refused.**

Missing representations cause 0A000 versus PostgreSQL values: DATE/TIMESTAMP, different address types, or PORT/PROTOCOL/DURATION with DECIMAL, in either order. (ADR-0012 §12/carrier-pairs)

**Some set-operation literals are refused.**

Text cannot initialize BOOL/integer/float/TIMESTAMP/PORT/PROTOCOL/DURATION vectors: 0A000 versus PostgreSQL values. NULL works. (ADR-0012 §12/quoted-literals)

**Decimal set operations can exceed 38 digits.**

A common scale can leave insufficient integer digits; the query raises 22003 where PostgreSQL numeric answers rather than discarding stored digits. (ADR-0012 §12/precision-cap)

**Correlated window subqueries are refused.**

Reconstructing their window clauses is unsupported: 0A000 where PostgreSQL answers. (ADR-0012 §5/#1045)

**Some correlated bodies are refused.**

Set-operation/LATERAL bodies and outer-level aggregates lack the required evaluation form: 0A000 where PostgreSQL answers. (ADR-0012 §5/#1044)

**An UNQUALIFIED outer reference over a table function is refused in the subquery's WHERE.**

An unqualified name inside a correlated subquery binds to the enclosing row when the subquery's own relations do not have it, as on PostgreSQL. When the subquery's FROM reads a table function, its columns are not known at that point: `o.id IN (SELECT b.k FROM dc_in b JOIN generate_series(1, 9) g(x) ON g.x = b.k WHERE total > 100)` fails with `filter column "total" does not exist in the input schema` where PostgreSQL answers. Write the qualifier — `o.total > 100`. (ADR-0012 §5/#1104, ADR-0021 §1r)

**A correlated subquery's own WITH item that shadows an outer WITH item is refused.**

`WITH d AS (…) SELECT … WHERE EXISTS (WITH d AS (…) SELECT 1 FROM d …)` reads the subquery's own `d` on PostgreSQL; here it is 0A000 `a WITH item inside a correlated subquery that shadows an outer WITH item is not supported`, because the per-row execution would read the outer `d`. Rename one of the two items. (ADR-0021 §1r)

**Outer aggregates in subquery WHERE are refused.**

The standalone subquery cannot retain the aggregate’s outer scope: 42803 where PostgreSQL answers. (ADR-0012 §5/#809)

**Some LATERAL ON conditions are refused.**

An outer LATERAL’s ON retaining an empty-input default raises 0A000: `ON s.n = 0` requires PostgreSQL’s `Carol, 0`, which this evaluation cannot produce. (ADR-0012 §5/#977)

**A bare star over a LATERAL whose list names one column twice is refused where the key is an outer EXPRESSION.**

`SELECT * FROM o JOIN LATERAL (SELECT i.id AS m, i.v AS m FROM i WHERE i.k = o.k - 0) s ON true` raises 0A000 where PostgreSQL answers: a list naming one column twice cannot be enumerated by name, so the star reads the join's output, which carries the body's key column the equality is evaluated against. A bare star over any other LATERAL — an expression key included — is expanded into the FROM items' own lists and answers. (ADR-0012 §5/#1302)

**A LATERAL nested in another that names the OUTERMOST relation is refused.**

`SELECT … FROM o JOIN LATERAL (SELECT … FROM i JOIN LATERAL (SELECT j.k FROM i j WHERE j.k = o.k) t ON true …) s ON true` raises 0A000 where PostgreSQL answers: a LATERAL is decorrelated against the relation it joins, and `o` is two levels out. Any condition of a LATERAL body naming a relation that is neither the body's nor to its left is refused the same way. Before 2026-09-24 the reference was compared as the text `o.k` — an error for an integer key and zero rows for a text key; until 2026-09-25 it was refused 42000. (arc JP rounds 3 and 5)

**A LATERAL body's condition holding a correlated subquery with a LATERAL join is refused.**

`SELECT o.id, s.* FROM o JOIN LATERAL (SELECT q.qid FROM q WHERE q.qk = o.k AND EXISTS (SELECT 1 FROM j JOIN LATERAL (SELECT x.v AS xv FROM x WHERE x.oid = j.oid) t ON true WHERE j.id = q.qid AND t.xv > o.id)) s ON true` raises 0A000 where PostgreSQL answers, and so does the same EXISTS reading only the body's `q.qid`: a subquery whose FROM holds a LATERAL join does not keep its correlation with the query around it (the same EXISTS at top level admits every row — a wrong answer this engine has at every level, recorded for repair), so the condition would not be evaluated per row. An uncorrelated one answers. (arc JP rounds 4 and 5)

**A window in a correlated LATERAL body beside a non-equality correlation is refused.**

`JOIN LATERAL (SELECT i.v, row_number() OVER (ORDER BY i.v) FROM i WHERE i.k = o.k AND i.v < o.total) s` raises 0A000 where PostgreSQL answers: the correlation is evaluated as a join, the inequality as a filter over it, and the window would number rows that filter then removes. So does a window in an ungrouped aggregate body (`SELECT count(*), rank() OVER (…)`), whose row for an outer row with no matches is supplied by the join. A window beside equality correlations answers PostgreSQL's rows in every body position, `QUALIFY` included. (arc JP round 5)

**Qualified stars refuse duplicate names.**

Name-based expansion cannot distinguish positions: `SELECT x.*` raises 0A000 where PostgreSQL returns both columns. A bare star reads positions and answers. (ADR-0012 §5/2026-09-13/duplicate-star)

**Some bounded LATERAL bodies are refused.**

Equality-keyed correlated bodies apply `LIMIT`/`OFFSET` per outer row, including `SELECT s.*`. A bound over an inequality correlation, a mixed inner/outer key expression, an outer-side expression, a DISTINCT body other than exactly the key, a set operation or a body with its own QUALIFY raises `0A000` where PostgreSQL evaluates it per outer row. This engine has no general relation-valued per-row runner. See [LATERAL joins](sql-reference.md#lateral-joins). (ADR-0021 §1s)

**Window aggregate support is limited.**

Only SUM/COUNT/AVG/MIN/MAX aggregates support OVER; others raise 0A000 where PostgreSQL supports windows. (ADR-0012 §5/#965-window-forms)

**NATURAL JOIN is refused.**

It raises 0A000; use ON. PostgreSQL supports the implicit matching-column join. (ADR-0012 §5—unlocated)

**CREATE VIEW is refused.**

No executor exists: 0A000 versus PostgreSQL creating the view. (ADR-0012 §5—unlocated)

**ALTER TABLE is refused.**

Schema evolution is unavailable: 0A000 versus PostgreSQL performing the alteration. (ADR-0012 §5—unlocated)

**DROP VIEW is refused.**

No executor exists: 0A000 versus PostgreSQL dropping the view. (ADR-0012 §5—unlocated)

**UPDATE SET cannot contain a subquery.**

`SET n = (SELECT max(n) FROM s)` raises 0A000 where PostgreSQL answers; assignment subqueries are unsupported. (ADR-0012 §5—unlocated)

**DML RETURNING is refused.**

INSERT/UPDATE/DELETE/MERGE RETURNING raises 0A000 versus PostgreSQL rows; that result form is unsupported. (ADR-0012 §5—unlocated)

**MERGE BY SOURCE or BY TARGET is refused.**

Both WHEN NOT MATCHED variants raise 0A000 versus PostgreSQL support; these forms are unimplemented. (ADR-0012 §5—unlocated)

**MERGE ON requires column equalities.**

Other conditions raise 0A000 versus PostgreSQL joins; matching requires equality keys. (ADR-0012 §5—unlocated)

**Computed aggregate ORDER BY is refused.**

`ORDER BY COUNT(*) * 2` raises 0A000 versus PostgreSQL values; select it and sort by its alias because intermediate evaluation is unavailable. (ADR-0012 §5—unlocated)

**Ordering subquery quantifiers are refused.**

`x < ALL (SELECT ...)` raises 0A000 where PostgreSQL answers; only equality forms are implemented. (ADR-0012 §5—unlocated)

**Derived-table outer references can refuse.**

A subquery’s derived table cannot evaluate enclosing-query references: 0A000 versus PostgreSQL values. (ADR-0012 §5—unlocated)

**Some correlated values cannot be substituted.**

Containers, non-text BYTES and non-finite numbers lack reconstructable literals: 0A000 versus PostgreSQL values. (ADR-0012 §5—unlocated)

**Recursive UNION without ALL is refused.**

It raises 0A000 where PostgreSQL deduplicates each step; this recursive evaluation form is unavailable. (ADR-0012 §5—unlocated)

**A recursive CTE stops at 1,000,000 iterations with 54000.**

PostgreSQL has no iteration limit and runs a recursion that never reaches a fixed point until `statement_timeout` or `temp_file_limit` ends it. This engine iterates to the fixed point the same way and raises 54000 at the millionth iteration of a recursive term that still produces rows; one iteration larger than the memory budget is 53200. Nothing is truncated: the error replaces the answer. (ADR-0021 §1o-b, ADR-0012 §5/#1246)

**A LIMIT does not stop a recursion early.**

PostgreSQL evaluates a recursive CTE lazily, so `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5` answers five rows there. This engine materializes the closure before it is read, so the same statement is 54000 at the iteration limit. Write the stop into the recursive term's WHERE. (ADR-0021 §1o-b)

**A forward reference in a WITH RECURSIVE list is refused.**

PostgreSQL lets any item of a `WITH RECURSIVE` list name a LATER item; this engine resolves an item's body against the items before it, so the later name is 42P01. Mutual recursion between items is 42P01 here and 0A000 there. Both are refusals; reorder the items. (ADR-0012 §5/arc RC)

**Some recursive terms PostgreSQL refuses are answered.**

An `ORDER BY` or `LIMIT` on the whole recursive body (0A000 there), a term whose integer width differs from the seed's (`SELECT 1 UNION ALL SELECT (n + 1)::bigint …`, 42804 there — the value is range-checked into the seed's width here), a `text` term under a `varchar(n)` seed (one carrier here), and a term of the wrong type that never produces a row (42804 there at parse time; this engine checks the values the term produces) are answered here. A quoted seed (`SELECT '5' UNION ALL SELECT 2 …`) is text here and resolved from the term there, so a non-text term under it is 42804 here. The measured table is `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType`. Superset, kept: none of them answers a value PostgreSQL would answer differently. (ADR-0012 §5/arc RC)

## What is NOT on this list

A value, a row set, a declared type or an error that differs from PostgreSQL 17.11 and is not on this list is a defect: [report them](https://github.com/derekmwright/wadjet/issues/new/choose). EXPLAIN output and timing are not SQL semantics, and row order without ORDER BY is unspecified under [ADR-0013’s legal classes](adr/0013-correctness-gates-and-their-boundaries.md). The SQLancer harness (`task sqlancer:triage`) reads this page as its known-difference list: a generated query whose error matches an entry here is reported under that entry, and everything else it reports is a candidate defect.
