# How wadjet differs from PostgreSQL

PostgreSQL 17.11 is the SQL semantics authority. Its wire protocol is the contract. This list contains every deliberate difference; report any other difference as a bug.

## Values

**AVG renders fewer digits.**

`AVG` over an integer or a `DECIMAL` column declares `numeric` with no fixed modifier, as PostgreSQL does, and the value is the same number; the rendered text carries scale 4 for an integer input and `min(s+4, 38)` for a decimal one, where PostgreSQL prints up to sixteen significant digits. `AVG(c_i32)`: wadjet `7497.6450`; PostgreSQL `7497.6449875724937862` for the same rows. (ADR-0012 §9/AVG)

**Decimal statistics use double precision.**

Decimal statistics (`STDDEV`, `VARIANCE`, `CORR`, `COVAR`, `MEDIAN`, `PERCENTILE`) use float64; PostgreSQL uses numeric. Fixed-point roots and running means are unavailable. (ADR-0012 §9/statistics)

**TIMESTAMP truncates microseconds.**

Millisecond storage truncates `.123456` return as `.123`; PostgreSQL retains `.123456`. (ADR-0012 §5/#692-residual)

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

## Declared types

**Array-returning functions can publish text.**

`SELECT tcp_flags(f)` returns `[SYN ACK]` under OID 25 because the projection lacks an element declaration; PostgreSQL has no corresponding function. (ADR-0012 §5/#1017)

**Grouped MIN/MAX over REAL declares float8.**

The grouped aggregate's accumulator is the wider type, so `MIN(c_real) … GROUP BY g` declares `double precision` where PostgreSQL declares `real`; the window spelling `MIN(c_real) OVER (…)` keeps `real`. Over `integer` both spellings declare `integer`, as PostgreSQL does. (ADR-0012 §5/#569)

**Integer expressions declare bigint.**

This keeps execution paths consistent. `CAST(SUM(a) OVER () AS INTEGER)` declares OID 20 rather than PostgreSQL’s 23; SMALLINT/INT2 also widen: int16 storage is unavailable. (ADR-0012 §5/#1070)

**ROW declares text.**

Composite text agrees; wire mapping uses OID 25 versus record OID 2249. (ADR-0012 §5/ROW)

**Some ARRAY results still declare text.**

Nested arrays, ROW/MAP elements and unknown element types use OID 25; ordinary arrays use PostgreSQL OIDs. PostgreSQL cannot represent ragged arrays. (ADR-0012 §5/#992-residuals)

**TIME, JSON and XML casts declare text.**

These casts pass text through. `CAST('12:34:56' AS time)` returns `12:34:56` on both engines, but wadjet declares OID 25. (ADR-0012 §5/#652)

**Decimal set operations keep one declared scale.**

Storage requires one scale. PostgreSQL declares unconstrained numeric; wadjet retains `(p,s)` and prints `12.7500` where PostgreSQL prints `12.75`. (ADR-0012 §12/decimal-carrier)

**Mixed int4-family sets declare bigint.**

PORT with INT32/PROTOCOL uses the common integer representation, OID 20; PostgreSQL’s int4 pair stays OID 23. (ADR-0012 §12/integer-width)

**CTAS stores constrained decimals.**

Stored columns require `(p,s)`. A CTAS over `COALESCE(numeric(15,2), numeric(38,10))` stores DECIMAL(38,10); PostgreSQL stores unconstrained numeric (`12.7500000000` versus `12.75`). (ADR-0012 §13/#1024-CTAS)

**OHLCV decimal fields retain their typmod.**

Fields retain storage types. `(b).open` over DECIMAL(9,2) has typmod 589830; PostgreSQL’s corresponding composite aggregate field has −1. (ADR-0012 §9/#965-field-typmod)

**Set-operation column labels use expression text.**

Set operations lack naming projections. `SELECT g + 1 FROM t UNION ALL SELECT g + 2 FROM t` labels its column `g + 1`; PostgreSQL uses `?column?`. (ADR-0012 §5/#732-set-operations)

**An unaliased expression inside a block a LATERAL reads is named by its text.**

`SELECT * FROM lat_ord o JOIN LATERAL (SELECT order_id, i.amount + 1 FROM lat_item i WHERE i.order_id = o.id) s ON true` names the second column `i.amount + 1` where PostgreSQL names it `?column?`. Values, types and positions agree; only the name differs. The same item inside a plain derived table or a join is named `?column?` as PostgreSQL names it. (ADR-0012 §5/LATERAL-labels)

## Errors and refusals

**Some known casts leave values unchanged.**

DURATION, BYTES, VECTOR and container destinations can retain the operand because conversion is unimplemented; recognizing the type does not perform a conversion. (ADR-0012 §5/#652)

**Undescribable results are refused.**

Empty results over an ungrouped-aggregate LATERAL or recursive CTE can raise XX000 where PostgreSQL supplies column metadata. Refusal prevents shapeless results. (ADR-0012 §5/#1008, #1010)

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

**Qualified duplicate names select the first.**

First-match lookup means `d.id` over two published `id` columns answers here; PostgreSQL raises 42702. (ADR-0012 §5/2026-09-13/duplicate-names)

**Ambiguous PARTITION BY names can answer.**

Binding chooses one input. `SUM(y.w) OVER (PARTITION BY w)` over duplicate `w` inputs answers here; PostgreSQL raises 42702. (ADR-0012 §5/#975)

**Name lookup tolerates case differences.**

For imported names, `SELECT WatchID FROM hits` can read `WatchID` where PostgreSQL raises 42703. Tables/lowercase quoted names also qualify; case-colliding join columns cause 42702 where PostgreSQL answers. (ADR-0012 §5/#731)

**Bare ROW field paths answer.**

`c_row.b` resolves a field; PostgreSQL raises 42P01 and requires parentheses. Qualified `(x.c_row).b` instead raises 0A000 here because three-part identity is unavailable. (ADR-0012 §5/#769)

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

The planner has no corresponding scope restriction. `FROM a, b JOIN c ON a.k = c.k` answers here; PostgreSQL refuses the reference. (ADR-0012 §5/#617)

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

**Text functions accept BYTES.**

`upper(b)`, `lower(b)`, `trim(b)`, `reverse(b)`, `char_length(b)` and `strpos(bytea,bytea)` answer here versus PostgreSQL 42883. Functions read the bytes; character length counts bytes. (ADR-0012 §5/#583)

**Planner column-name prefixes are reserved.**

`SELECT amount AS __key_0` raises 42939 here; PostgreSQL answers. Intermediate columns need these names; stored columns remain readable. (ADR-0012 §5/#956)

**INSERT SELECT refuses some text assignments.**

`INSERT INTO t (text_col) SELECT bigint_col` raises 42804 here; PostgreSQL converts the integer to text. Assignment supports numeric-family conversions. (ADR-0012 §13/#1024-assignment)

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

Casts/writes enforce 0–65535 and 0–255 (22003); arithmetic may leave those ranges. Both declare int4. `CAST('udp' AS PROTOCOL)` → 17; comparison literals instead use integer grammar. (ADR-0012 §5/#1092-residuals-2–3)

**DURATION counts nanoseconds.**

Storage and wire use bigint nanoseconds, OID 20, versus PostgreSQL’s microsecond interval. (ADR-0012 §5/#834)

**MIN/MAX accepts additional types.**

BOOL, UUID, MAC, BYTES and ROW have defined orders here; PostgreSQL lacks these aggregates. BYTES uses bytewise order and retains bytea OID 17. (ADR-0012 §5/#569, #570)

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

## Not supported

[SQL reference](sql-reference.md).

**A column-alias list over a star whose width is not known is refused.**

A star without a known width cannot be renamed positionally: 0A000 where PostgreSQL answers. (ADR-0012 §5/#958)

**Unresolved decimal set-operation arms can refuse.**

Unknown types/scales cause distributed refusal to avoid decimal reinterpretation; local execution and PostgreSQL can answer. (ADR-0012 §12/#551)

**JOIN USING has output limitations.**

Stars over USING, or USING after another join, raise 0A000: wadjet cannot merge columns as PostgreSQL does. (ADR-0012 §5/#810, #655)

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

**Outer aggregates in subquery WHERE are refused.**

The standalone subquery cannot retain the aggregate’s outer scope: 42803 where PostgreSQL answers. (ADR-0012 §5/#809)

**Some LATERAL ON conditions are refused.**

An outer LATERAL’s ON retaining an empty-input default raises 0A000: `ON s.n = 0` requires PostgreSQL’s `Carol, 0`, which this evaluation cannot produce. (ADR-0012 §5/#977)

**Qualified stars refuse duplicate names.**

Name-based expansion cannot distinguish positions: `SELECT x.*` raises 0A000 where PostgreSQL returns both columns. A bare star reads positions and answers. (ADR-0012 §5/2026-09-13/duplicate-star)

**Qualified stars refuse bounded LATERALs.**

No per-outer-row bound is available: the qualified star raises 0A000 where PostgreSQL answers. (ADR-0012 §5/2026-09-13/bounded-LATERAL)

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

**Catalog regex support is limited.**

Only psql’s anchored literal-name patterns work; other patterns/operators raise 0A000 versus PostgreSQL results. (ADR-0012 §5—unlocated)

## What is NOT on this list

A value, a row set, a declared type or an error that differs from PostgreSQL 17.11 and is not on this list is a defect: [report them](https://github.com/derekmwright/wadjet/issues/new/choose). EXPLAIN output and timing are not SQL semantics, and row order without ORDER BY is unspecified under [ADR-0013’s legal classes](adr/0013-correctness-gates-and-their-boundaries.md).
