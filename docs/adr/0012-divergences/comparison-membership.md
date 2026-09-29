# ADR-0012 divergences: Comparisons and membership

Comparisons and IN/ANY/EXISTS memberships across text and typed values, numeric literals in memberships, and bind-time type refusals. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**The binder asks PostgreSQL's type questions before any row** (E01)
Aggregate argument classes (42883, 42725), comparison operand classes (42883), fold arms (42804), a window in HAVING (42P20), a star under grouping (42803) and a set operation's ORDER BY term (42P01/42703/0A000) are decided at bind time, where each once answered a misleading value. Kept wider than PostgreSQL: the wire's view of PORT/PROTOCOL (int4) and DURATION (int8) as numbers, DuckDB's plain-call MEDIAN/QUANTILE/MODE/PERCENTILE over numbers, STRING_AGG over a non-text value's own text, the first arm's qualified ORDER BY column, and the text/typed pairs below. Gated by `coordinator.TestArcBRWherePostgresRefusesEveryArmRefuses` and `physical.TestArcBR*`.

**Text against a typed value is decided per pair** (E01, P091)
`physical.textConversionAnswers` records which pairs are kept directly and in IN lists; a subquery membership is kept only where the body selects `CAST(x AS TEXT)` and the text provably converts (`physical.textOriginConverts`: same type, two integer kinds, or PORT/PROTOCOL against float8). `physical.comparisonTyper.memberPair` reads both operands whatever their shape; a correlated equality is a semi/anti join key and takes the JOIN rule (`refuseCorrelatedKeys`), an expression-sided key takes the membership rule (`comparisonTyper.keyPair`), and two plain columns as a JOIN key keep no text reading. A set-operation body keeps the left arm's origin when the arms share a comparisonClass, a recorded gap.

**A literal meeting a membership is typed from the body** (E01, P066, P067)
A quoted outer literal takes the body's TypeID (not its typmod) through `expr.MemberLiteralCast`, and is written typed into the logical plan (`physical.typeMemberLiterals`, `expr.MemberProbe`) so every arm compares a typed value. A numeric literal against a NUMERIC body, or under a CAST or unquoted against an integer body, is NUMERIC(38, its own scale) via `batch.DecimalValueType`; constant choices and arithmetic fold exactly at PostgreSQL's scales. A form the fold cannot compute that evaluates in float8 is 0A000, and a number no DECIMAL(38,s) holds is 22003.

**An unquoted number meets TEXT by its source text on reads only** (E42, P060)
PostgreSQL refuses `text = numeric` by overload resolution; the engine has one generic comparison and compares the column's bytes with the literal's source text (`expr.boxedPair`, `kernel.toString`, `exec.decimalLitValue`), which is PostgreSQL's answer to the quoted spelling. The opposite pair, a quoted literal against a number column, takes the number column's type (`boxQuoted`). A DML statement's WHERE refuses the pair 42883 before any row is read (`wadjet.refuseDMLLiteralPairs`), together with a number against any non-numeric column, a boolean literal against a non-BOOL column, and a quoted literal naming no value of a numeric column (22P02); PORT, PROTOCOL and DURATION keep answering. The CIDR key question E42 opened was closed 2026-08-25.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT id FROM t WHERE s = 1.50` | ERROR 42883 operator does not exist: text = numeric | compares s with the literal's source text '1.50': 1 row; s = 1.5 matches none (measured) | — | kept superset | 2026-08-24 · [E42](#e42), P060 | #504 | `internal/server/pgwire/dml_census_test.go` |
| **r2** `SELECT id FROM t WHERE ts >= 1700000000000` | ERROR 42883 | compares with the instant 1700000000000 ms after the epoch (measured) | — | kept superset | 2026-09-22 · [E42](#e42), [E01](#e01), P093 | #1216 | — |
| **r3** `SELECT id FROM t WHERE id = s` | ERROR 42883 operator does not exist: bigint = text | answers through the value's text (also in an IN list; int4, float8, numeric, PORT, PROTOCOL, DURATION, UUID, IPv6, CIDR) | — | kept superset | 2026-09-22 · [E01](#e01), P091 | #826, #1073 | — |
| **r4** `SELECT id FROM t WHERE ts = s` | ERROR 42883 | answers directly and in an IN list (DATE, TIMESTAMP, boolean likewise); their subquery membership is 42883 | — | kept superset | 2026-09-22 · [E01](#e01), P091 | #1073 | — |
| **r5** `SELECT id FROM t WHERE id IN (SELECT CAST(id AS TEXT) FROM t)` | ERROR 42883 operator does not exist: bigint = text | 12, 14 (the body's text provably converts; any other text body is 42883; measured) | — | kept superset | 2026-09-27 · [E01](#e01), P091 | #1308, #1374 | — |
| **r6** `SELECT id FROM t WHERE id IN (SELECT '12')` | ERROR 42883 | 12 (a FROM-less literal body folds into the IN list, as id IN ('12'); measured) | — | kept superset | — · P091 | #1308 | — |
| **r7** `SELECT id FROM t WHERE id IN (SELECT CAST(id AS TEXT) FROM t UNION SELECT CAST(d AS TEXT) FROM t)` | ERROR 42883 | 12, 14; the same arms in the other order are 42883 (the body is judged by its first arm; measured) | — | documented gap | 2026-09-28 · [E01](#e01), P091 | — | — |
| **r8** `SELECT id FROM t WHERE id <> ALL (SELECT s FROM t)` | ERROR 42883 operator does not exist: bigint <> text | ERROR 42883 naming = (read as NOT IN) | 42883 | documented gap | — · [E01](#e01), P091 | — | — |
| **r9** `SELECT id FROM t WHERE '14.0000000000000000000000000000000000000001' IN (SELECT d FROM t)` | 0 rows (NOT IN every row; numeric is unconstrained) | ERROR 22003 numeric field overflow naming the literal, every arm (measured) | 22003 | refusal | 2026-09-27 · [E01](#e01), P066 | #1372, #1388 | — |
| **r10** `SELECT id FROM t WHERE sqrt(12.5 * 12.5) IN (SELECT d FROM t)` | answers (computed as numeric; every row) | ERROR 0A000 naming the operand, every arm (measured); exp, ln, power the same | 0A000 | refusal | 2026-09-27 · [E01](#e01), P067 | #1372, #1387 | — |
| **r11** `SELECT id FROM t WHERE sin(0.0) + 12.5 IN (SELECT d FROM t)` | answers | ERROR 0A000 (a float8-only function combined with a numeric constant; measured) | 0A000 | refusal | 2026-09-27 · [E01](#e01), P067 | #1420 | — |
| **r12** `SELECT percentile_cont(0.5, id), median(id) FROM t` | ERROR 42809 (plain call; WITHIN GROUP required); median 42883 | 13, 13 over numbers (measured); over non-numbers MODE and PERCENTILE_DISC 42809, MEDIAN 42883 | — | kept superset | 2026-09-22 · [E01](#e01) | #1249 | — |
| **r13** `SELECT string_agg(id, ',') FROM t` | ERROR 42883 (no string_agg over bigint) | 12,14 (the value's own text; TIMESTAMP and typed containers 42883; measured) | — | kept superset | 2026-09-22 · [E01](#e01) | — | — |
| **r14** `SELECT string_agg(bt, ',') FROM t` | answers | ERROR 0A000 string_agg over bytea is not supported (measured) | 0A000 | refusal | 2026-09-22 · [E01](#e01) | — | — |
| **r15** `SELECT stddev(d) FROM np` | no DURATION type; interval has no stddev | answers over DURATION as int8 nanoseconds (PORT, PROTOCOL as int4 likewise) | — | kept superset | 2026-09-22 · [E01](#e01) | #834 | — |
| **r16** `SELECT COALESCE(r2, r3) FROM rt` | anonymous ROW(…) records answer; named composites ERROR 42846 | ERROR 42846 could not convert type (CASE, GREATEST, LEAST, set operations the same) | 42846 | refusal | 2026-09-22 · [E01](#e01), P094 | #1060, #1065 | — |
| **r17** `SELECT COALESCE(arr, '{1,2}') FROM rt` | answers (the literal is read as the array) | ERROR 0A000 (a quoted literal in a ROW's or ARRAY's own grammar inside a fold) | 0A000 | refusal | 2026-09-22 · [E01](#e01), P094 | #1060, #1065 | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E01

ADR lines 62-305. Catalog rows: r2, r3, r4, r5, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17. Stated in [Mechanisms](#mechanisms).

- **Where PostgreSQL refuses by TYPE, wadjet refuses — and the supersets it
  keeps are the ones whose value is meaningful on every arm.** (Added
  2026-09-22, arc BR: #1249 #1073 #1205 #1061 #1060 #1065 #1233 #1236
  #1216.) The binder now asks, before any row, the type questions
  PostgreSQL's parse analysis asks: an aggregate's argument class
  (`function sum(text) does not exist`, 42883; `sum(unknown) is not
  unique`, 42725), a comparison's operand classes (`operator does not
  exist: bigint = text`, 42883), a fold's arms (`COALESCE types … cannot be
  matched`, 42804), a window's placement in HAVING (42P20), a star's columns
  under a grouping (42803) and a set operation's ORDER BY term (42P01 /
  42703 / 0A000). Each of those answered a misleading value before — NULL
  for a SUM over text, zero rows for a type mismatch, every row for a HAVING
  that was never applied — measured over the 22-type matrix on five arms
  (the tables are in the arc's landing notes and the gates are
  `physical.TestArcBR*` and `coordinator.TestArcBRWherePostgresRefuses
  EveryArmRefuses`). What stays WIDER than PostgreSQL, deliberately:

  - The accept-sets read this engine's types by what the WIRE declares
    them: PORT and PROTOCOL are int4 and DURATION is int8 (nanoseconds,
    #834), so SUM/AVG/STDDEV/VARIANCE/CORR/COVAR and the comparisons treat
    them as numbers where PostgreSQL's `interval` has no stddev.
  - MEDIAN, QUANTILE_*, and the plain-call MODE, PERCENTILE_CONT and
    PERCENTILE_DISC (DuckDB's spellings; PostgreSQL has none, or only the
    ordered-set WITHIN GROUP form, and raises 42809 for the plain call)
    answer over numbers — `percentile_cont(0.5, c_i32)` 28.5 and
    `percentile_disc(0.5, c_i32)` 27 on every arm. Refused over anything
    else they carry PostgreSQL's per-function state: MODE and
    PERCENTILE_DISC 42809 `WITHIN GROUP is required for ordered-set
    aggregate mode`; PERCENTILE_CONT 42883 `function
    percentile_cont(numeric, text) does not exist` (its overloads resolve
    first); MEDIAN and QUANTILE_* 42883.
  - STRING_AGG renders a non-text argument as its own text — BOOL, the
    integers, REAL, DOUBLE, DECIMAL, IPV4, IPV6, CIDR, MACADDR, PORT,
    PROTOCOL, DURATION, UUID and DATE (ISO) — which base answered
    identically on every arm (arc BR round 2). TIMESTAMP (epoch
    milliseconds) and typed container columns are 42883; BYTEA is
    below. An `ARRAY(subquery)` aggregate argument can instead return
    an incorrect value (#1309).
  - TEXT compared with a typed operand is decided PER PAIR, from what the
    base engine answered for a value against its own text rendering over
    20 rows on five arms (`physical.textConversionAnswers`):

    | typed side | direct `=`/`<`/…, IN list | IN / = ANY / NOT IN / <> ALL (subquery) |
    |---|---|---|
    | int4, int8, float8, numeric, port, protocol, duration | kept | kept where the body selects `CAST(x AS TEXT)` and the text provably converts |
    | uuid, ipv6, cidr | kept | kept where the body selects `CAST(x AS TEXT)` and the text provably converts |
    | date, timestamp, boolean | kept | 42883 (0 rows single, 20 DAG) |
    | real | 42883 (3 of 20 matched) | 42883 |
    | bytea, ipv4, macaddr | 42883 (0 of 20 matched) | 42883 (0 single, 20 DAG) |

    The subquery column holds for EVERY body, and "kept where the body
    selects `CAST(x AS TEXT)`" means only there: any other text body is
    42883 in the explicit JOIN's words (`operator does not exist: bigint
    = text`). (Amended 2026-09-26, arc ST, #1308: a body selecting a
    stored, derived-table or CTE TEXT column became a semi/anti join
    whose key pair (typed, text) was never converted — 0 rows over
    matching values, NOT IN every row, on all five arms at v0.25.1, the
    mirror failing with #615's key error on the single arms; a TEXT
    literal, an aggregate, `upper(s)` or a LIMITed column body is a
    filter whose DAG casts the text while the single arms compare it, so
    it answered data-dependently — a literal body 0 rows single and 1 on
    the DAG for DATE/TIMESTAMP/BOOLEAN, a `'zz'` body 0 rows single and
    22P02 on the DAG. PostgreSQL refuses every one 42883; the rule is
    `physical.comparisonTyper.memberPair`, a quoted literal in the body's
    target list reading as text, as PostgreSQL resolves it.) "Provably
    converts" (`physical.textOriginConverts`) is that x RENDERS every
    value as the compared type renders it: the same type, two integer
    kinds (int4, int8, port, protocol, duration), or a port or protocol
    against float8, which prints each of their values as they do.
    (Amended 2026-09-27, arc SM, #1374: it was the same comparisonClass
    and never a fractional rendering into an integer kind, so `v_dec IN
    (SELECT CAST(v_i64 AS TEXT) …)` was kept and converted the text — 1
    row — while the same comparison as an EXISTS key compared it
    directly, `'14'` against `14.0000` — 0 rows, on every arm; both are
    42883 now. A float8 against a port's or a protocol's text answers
    one value in both spellings, PostgreSQL's `CAST(CAST(x AS TEXT) AS
    float8)` rewrite, and stays kept.) The rule as first
    amended kept by CLASS alone, so a body selecting `CAST(v_dec AS
    TEXT)` against a bigint outer value was "kept" while the DAG's cast
    of the rendered text (`'14.0000'`) back to bigint is 22P02 and the
    single arms compared the text as it stood — an arm-dependent cell
    inside the rule's own kept set. (Amended 2026-09-26, arc ST round 2,
    #1308.) The body's CORRELATED equalities are the
    semi/anti join's keys and take the JOIN-key rule below: `EXISTS (…
    WHERE b.s = a.v)`, a correlated `IN`'s key, is 42883
    (`refuseCorrelatedKeys`); a correlated comparison under `OR` is a
    filter and keeps the direct reading.

    The rule reads BOTH operands whatever their shape (amended
    2026-09-27, arc SM, #1369 #1370 #1368 #1372 #1374), one table of
    positions × dispositions: an operand the structural walk does not
    type is typed by its declaration for the text/typed question, so
    an expression outer (`v + 0 IN (SELECT s …)`: 0 rows single, 2 DAG)
    and an expression body (`IN (SELECT upper(s) …)`: 0 rows single,
    22P02 DAG) are 42883 like the columns; a correlated key with an
    expression side takes the membership rule (`physical.
    comparisonTyper.keyPair`) — a `CAST(x AS TEXT)` that provably
    converts keeps the direct reading, any other text side is 42883; a
    LATERAL body's correlated equality is its decorrelated JOIN key and
    keeps no text/typed pair (`FROM a, LATERAL (… WHERE r.s = a.v)`
    answered 0 rows on every arm, its CAST key 0 as well); and a QUOTED
    literal outer value takes the body's TYPE — never its typmod — as
    PostgreSQL resolves it (`expr.MemberLiteralCast` over the TypeID
    alone), 22P02 / 22007 at plan time when its text is no value of it
    (`'zz' IN (SELECT bigint …)` answered 0 rows on every arm). The
    typed literal is written into the logical plan every arm consumes
    (`physical.typeMemberLiterals`, `expr.MemberProbe`), so the DAG's
    inlined IN list compares a typed value too: `'2024-1-2'`,
    `'20240102'`, `'2001:DB8::1'`, a braced uuid, `'1_2'` and `'0x0C'`
    against a DATE, inet, uuid or bigint body matched only on the
    single-process arms while the literal was typed at compile time —
    the DAG compared the text, 0 rows and NOT IN every row, matching only
    a literal spelled as a member renders — and a NUMERIC body's scale
    rounded `'12.50001'` to a
    member while the probe took the column's typmod. Against a NUMERIC
    body a numeric literal — quoted, under a bare `CAST(… AS NUMERIC)`
    or `::numeric`, or an unquoted constant — is typed NUMERIC(38, its
    value's own scale) by one rule over every spelling numeric input
    accepts (`batch.DecimalValueType`): a bare NUMERIC of a literal boxes
    as float8, which matched `'1.25000000000000001e13'` to the member
    12500000000000.0000; the unquoted and CAST spellings take the same
    type against an integer body, numeric = integer being numeric
    (`14.0000000000000000001 IN (SELECT bigint …)` matched 14 at
    float8), and the per-row evaluator meets an integer member and a
    decimal probe by value. An outer operand computed from numeric
    constants — a choice (CASE / COALESCE / NULLIF / GREATEST / LEAST)
    over constant results, a unary minus of an expression, a bare
    NUMERIC CAST of anything but a quoted literal — is typed by the same
    rule after `expr.MemberProbe` folds it exactly at plan time: ADR-0024
    evaluates those forms as float8, so `CASE WHEN a.id > 0 THEN
    14.0000000000000000001 END IN (SELECT numeric … WHERE r.id = a.id)`
    matched the member 14 on every arm (v0.25.1 answered 0 rows only
    because its box comparison missed every member). A choice with a
    column result has each constant result typed instead (beside a
    float8 column the choice is float8, as in PostgreSQL). Arithmetic
    folds at PostgreSQL's result scales, a numeric quotient at
    select_div_scale rounded half away from zero as div_var rounds
    (`(14.0000000000000000001 / 7) * 7` is 14 there), and a bare
    `CAST(16777216 AS NUMERIC)` — a double that the integer set's rung
    never matched (0 rows single-process, every row on the DAG at
    v0.25.1) — folds to the integer. A
    form the fold does not compute that evaluates as float8 while a
    numeric constant feeds it — `sqrt(12.5 * 12.5)`, exp, ln, power —
    is **0A000, a recorded divergence** (PostgreSQL answers); a function
    PostgreSQL defines over float8 alone (sin, degrees, cbrt) called
    alone, and an explicit float CAST, are float8 there too and answer;
    combined with a numeric constant (`sin(0.0) + 12.5`) the operand is
    refused 0A000 as well, where PostgreSQL answers (#1420). A number no DECIMAL(38,s) holds — more than 38
    significant digits, a digit past scale 38, NaN, ±Infinity — is
    **22003 on every arm, a recorded divergence**: PostgreSQL's numeric
    is unconstrained and answers (0 rows, NOT IN every row). The single-process
    membership filter compares the pair in that one type: the set keyed
    by its declaration, a NUMERIC member read on the numeric rung by
    the per-row evaluator too — `'12' IN (SELECT bigint …)` answered 0
    rows there and every row on the DAG (#1372), a DATE against any body
    that stays a filter (a set operation, a literal, an expression) 0
    rows there and the matches on the DAG (#1373). Cells base answered
    identically on every arm that now take PostgreSQL's 42883 with the
    rule, recorded: 100 membership bodies and 96 set-operation bodies
    selecting `CAST(x AS TEXT)` of a type that renders differently from
    the compared one — twelve pairs: numeric against bigint, integer,
    port, protocol, duration and float8, float8 against numeric,
    bigint, integer and duration (each IN spelling answered the
    converted reading where the same EXISTS key compared the text as
    written, `14` against `14.0000`, `1e+16` against
    `10000000000000000`), and inet against cidr both ways (arm-split at
    base) — and 60 EXISTS keys equating a value with `CAST(x AS TEXT)`
    of another type, which answered the direct text reading (0 rows, or
    the NOT EXISTS complement); seven
    text-expression bodies whose data happened to convert on every arm
    (`coalesce(s, '0')`, `substr(s, 1, 2)`, a CASE over s, `lower(s)`
    against uuid, `CAST(v AS TEXT) || ''`); a set operation of a quoted
    literal and a stored text column; and a LATERAL key, comma and JOIN
    spellings (0 rows). Thirteen cells base answered identically now
    refuse with the rule, recorded (base's own value, then 42883): a
    derived table's `CAST(x AS TEXT)` column IN body (3; the JOIN
    refuses the same derived-column key); the same body under EXISTS
    (3); `IN (SELECT max(s) …)` without GROUP BY (1); a TEXT-literal
    body against a numeric column (1); a CTE's `CAST(x AS TEXT)` column
    IN body (3); an ungrouped `max(CAST(x AS TEXT))` body (1); `uuid IN
    (SELECT CAST(ipv6 AS TEXT) …)` (0); the mirror, `ipv6 IN (SELECT
    CAST(uuid AS TEXT) …)` (0); an EXISTS correlated key inside a nested
    AND conjunct (2); a two-level nested EXISTS, the correlated key one
    level down (2); an EXISTS correlated key under `IS NOT DISTINCT
    FROM` (2); the same under `IS DISTINCT FROM` (4); and its `NOT
    EXISTS` mirror (2). Two further families the same rule moves from
    a right answer to 42883 (PostgreSQL refuses each): a set-operation
    body of quoted literals against a kept type, with or without a
    FROM (`v_i64 IN (SELECT '12' … UNION ALL SELECT '13' …)` answered
    2 on every arm; 336 cells), and a fractional `CAST(x AS TEXT)` body
    against an integer kind where nothing matched (`port IN (SELECT
    CAST(f64 AS TEXT) …)` answered 0; 84 cells).

    A SET-OPERATION subquery body (UNION ALL / UNION / INTERSECT /
    EXCEPT) is kept only where its text PROVABLY converts: every arm of
    the body is `CAST(x AS TEXT)` of a value of the typed side's own (kept) class whose text provably converts (the rendering rule above) applies the same way. There
    the DAG casts the body's text to the typed side while the single
    arms compare the text, so any other text converts data-dependently
    — #1073's `id IN (SELECT product … UNION ALL …)` answered 0 rows on
    the single arms and failed the cast on the DAG — and is 42883 (arc
    BR round 3b). A quoted-literal body of the set-operation kind
    (`v_date IN (SELECT '2024-01-02' … UNION ALL SELECT '2024-03-04'
    …)`) lost its text origin through the merge and kept a silent,
    data-dependent reading: for DATE, TIMESTAMP and BOOLEAN, and for a
    literal no arm converts (`'zz'`), 0 rows on the single arms and the
    matching values or 22P02 only on the DAG; for the ten kept types
    every arm answered the membership (2). The merge now carries the literal's origin
    through every set operator, so the body refuses 42883 the same way
    a single-SELECT literal body already did. (Amended 2026-09-26, arc
    ST round 2, #1308.)
    The merge still keeps the LEFT arm's origin when the two arms'
    origins share a comparisonClass, so a later arm whose CAST does not
    provably convert is never judged: `v IN (SELECT CAST(v AS TEXT) …
    UNION SELECT CAST(d AS TEXT) …)`, a bigint against a numeric `d`'s
    text, answers (and did at v0.25.1) where the same arms in the other
    order are 42883 and PostgreSQL refuses both. A recorded gap, not a
    kept superset. (Recorded 2026-09-28.)
    One shape keeps no text reading for any type: two plain COLUMNS as a
    JOIN key (the hash-join key path: #615's error on three arms, 0 rows
    on the shuffled one, and 0 rows for every text/typed derived-column
    pair).
  - An UNQUOTED numeric literal keeps its recorded readings: against TEXT
    its source text (#504), against a TIMESTAMP the epoch-millisecond
    instant the carrier holds — directly and in an IN list; as the outer
    value of a subquery membership it takes the membership rule above
    (`12 IN (SELECT s …)` answered 0 rows single and 4 on the DAG, #1308). A literal is refused only against a
    BOOLEAN, and a boolean literal only against a number (`1 = true`,
    `id = true`, `(id > 1) = 1`).
  - A set operation's ORDER BY takes one qualified spelling PostgreSQL
    does not: the FIRST arm's selected `q.col` under its own name
    (`SELECT a.id … UNION ALL … ORDER BY a.id DESC`), which base answered
    correctly on all five arms. Every other qualifier is 42P01; result
    names match EXACTLY, as PostgreSQL's do (`ORDER BY "ID"` over `id` is
    42703).

  Refused where PostgreSQL ANSWERS, loudly and by name: `string_agg` over
  BYTEA (0A000 — the accumulator renders each value with Go's fmt), a
  quoted literal written in a ROW's or ARRAY's own grammar inside a fold
  (0A000 — the fold answered the literal's TEXT), and two ROWs of
  different SHAPES in a fold or a set operation (42846 `could not convert
  type`, PostgreSQL's state for two named composites; its anonymous
  `ROW(…)` records answer, and this engine's ROW columns are typed like
  the named ones).

### E42

ADR lines 1695-1836. Catalog rows: r1, r2. Stated in [Mechanisms](#mechanisms).

- **A TEXT value compared against a NUMBER.** (Added 2026-08-24, #504.)
  PostgreSQL refuses the pair outright — verified live, `WHERE s = 1.5`
  over a `text` column is 42883 "operator does not exist: text = numeric",
  and so are `>`, `text = bigint`, `CASE s WHEN 1.5`, `s IS DISTINCT FROM
  1.5` and `GREATEST(s, 1.5)`. That is an OVERLOAD RESOLUTION failure, the
  same class as the unary-minus bullet below: wadjet has ONE generic
  comparison operator and no overload set to fail resolution against, so
  reproducing 42883 would mean building the overload machinery first.

  Wadjet instead gives the pair the column's own rule: a STRING column
  compares its BYTES against the literal's SOURCE TEXT, on the vectorized
  path and the row-at-a-time path alike. The literal's text is the carrier
  for the same reason item 6 makes it one — `s = 1.50` and `s = 1.5` are
  different predicates, exactly as `s = '1.50'` and `s = '1.5'` already
  were.

  This is a narrower divergence than it looks. Every answer it produces is
  PostgreSQL's answer to the QUOTED spelling of the same predicate, checked
  entry by entry against live postgres:17-alpine over a `text COLLATE "C"`
  column: `s = '1.5'` 1, `s > '1.5'` 4, `s > '10'` 2, `s < '10'` 2, and the
  three boxed sites likewise. The only thing wadjet does that PostgreSQL
  does not is RESOLVE the unquoted spelling at all.

  What was there before was neither rule. `compare()` read any string
  operand that PARSED as a number numerically — a guess about where the box
  came from, since a DECIMAL column and a STRING column both box as Go
  strings — while the vectorized kernel rendered the numeric constant as
  the EMPTY STRING and compared against that. So `WHERE s = 1.5` found the
  row holding "1.50" through a projected CASE and no rows at all through a
  scan-pushed filter, and `WHERE s > 1.5` admitted every row including
  "1.5" itself. One predicate, two answers, decided by which lowering the
  query happened to take. The guess is gone: `expr.boxedPair` selects the
  rule from the operands' DECLARED types (item 8), and
  `kernel.toString`/`exec.decimalLitValue` give the kernel the same
  literal text the row path uses.

  **This rule is about a NUMERIC literal meeting a TEXT column, and it does
  not run backwards.** (Added 2026-08-25, from the #504 review; the opposite
  pair is item 13's whole subject as of #646.) A QUOTED
  literal meeting a NUMBER column is the opposite pair and takes the
  opposite rule: PostgreSQL types an unknown-typed literal FROM the operand
  it meets, so `WHERE k > '2'` over a BIGINT column is the integer
  comparison `k > 2` — not a text comparison, and not the comparison
  against ZERO the constant used to become. Deleting the box-sniffing
  branch removed all three of the readings it was doing at once, and only
  the DECIMAL one was re-stated; `boxedPair` carries a distinct `boxQuoted`
  kind for exactly this reason, so the two directions cannot be collapsed
  again. Verified live on `k BIGINT` 0..11: `k > '2'` 9, `k >= '2'` 10,
  `k < '2'` 2, `k = '2'` 1, and the same for a FLOAT column under the
  float rule.

  Two more of the box-sniff's jobs came back with it. `compare()`'s
  temporal branch guarded itself with "the string parsed OR the number is
  zero", which is true of ANY unparseable string against a zero — so
  `0 = '0.0001'` was TRUE, and once the sniff above it was gone every
  `int_col = 'anything'` matched the row holding zero. It asks the parser
  whether it parsed now (`parseTemporalInt64OK`). And `IN`/`BETWEEN` are
  `=` and `>=`/`<=` chained, so they take the same binding: `s = 2.00` and
  `s IN (2.00)` disagreed until they did.

  `kernel.toString`'s empty string was not only for numbers. A BOOL
  constant reached it too, so `WHERE s = TRUE` compared every row against
  `""` on the scan path and matched the row spelled `""` rather than the
  one spelled `"true"` — a wrong ROW, not a wrong count. It renders
  `true`/`false`, which is PostgreSQL's own `boolean::text` (the
  single-letter `t` is psql's display) and what the row path's `fmt.Sprint`
  already produced.

  **The divergence is a READ-side concession, and a DML statement's
  qualifying predicate does not get it.** (Added 2026-09-03, #721.) The
  entry above was reasoned entirely about the query path, where its
  consequence is a wrong COUNT for a spelling PostgreSQL refuses to
  resolve. On a DELETE the same rule DESTROYS ROWS:

      DELETE FROM pr WHERE name > 5     PostgreSQL 42883
                                        wadjet DELETE 3, table EMPTIED

  `"a" > "5"` is true for every row (0x61 > 0x35), so wadjet answered
  PostgreSQL's answer to a DIFFERENT predicate and emptied a three-row
  table. Nobody wrote that consequence down because no fixture attempted
  it — the issue that reported the class even claimed it was "the SAFE
  direction … no row is destroyed", which the measurement refutes.

  So the rule now splits by what the predicate DECIDES. A SELECT keeps the
  byte rule and its reasoning: the overload machinery this entry says would
  be needed is still not built, and every answer the rule gives is
  PostgreSQL's answer to the quoted spelling. A DML statement's WHERE
  refuses the pair with **42883**, before any row is read, at
  `wadjet.refuseDMLLiteralPairs`. Two more pairs go with it, for the same
  reason: any non-BOOL column against a BOOLEAN literal (`id = true`,
  PostgreSQL's `bigint = boolean`, which the DML door answered `DELETE 0`
  and the SELECT door 22P02 — two doors disagreeing about one predicate),
  and a numeric column against a quoted literal naming no value of it,
  which the runtime already refuses but only once a ROW reaches it, so
  `DELETE FROM empty WHERE id = 'abc'` answered `DELETE 0`.

  The asymmetry is the point, not an oversight: a concession whose cost is
  an answer is not the same decision as a concession whose cost is the
  table. Temporal and network columns against a number are deliberately
  NOT refused — those parsers are stricter than PostgreSQL's input grammar
  and refusing on them would reject input PostgreSQL accepts, which item 1
  forbids.

  The refused pairs are: an unquoted NUMBER against any column type that is
  not a numeric family — STRING, BYTES, BOOL, TIMESTAMP, DATE and the
  network types, since PostgreSQL refuses the OPERATOR for all of them —
  a BOOLEAN literal against a non-BOOL column, and a quoted literal naming
  no value of a numeric column. PORT, PROTOCOL and DURATION keep answering:
  they are wadjet-native, PostgreSQL has no such type and therefore no
  opinion, and the superset rule applies. The first pass listed only STRING
  and BYTES and justified the rest with an argument about quoted input
  reaching a type parser; that argument cannot apply to an unquoted number,
  and the measurement refuted it — `DELETE … WHERE ts > 5` EMPTIED a
  TIMESTAMP table, as did the BOOL and IPv4 spellings.

  Every half is pinned in the DML census
  (`internal/server/pgwire/dml_census_test.go`): the DML entries assert the
  class PostgreSQL gives — 42883 for a refused OPERATOR, 22P02 for a quoted
  literal naming no value, which are two different conditions and word
  themselves differently — the two SELECT entries stay pinned as this
  divergence, and the boundary entries assert both the pairs that must keep
  working and the pairs that must keep refusing. All four DML verbs are
  covered: MERGE's `WHEN … AND` condition runs the same check as a DELETE's
  and an UPDATE's WHERE.

  **The equivalent question for CIDR is open, and must not be answered one
  site at a time.** (Added 2026-08-25, #546.) A CIDR value is stored as
  TEXT, and every KEY and every column-to-column comparison uses that text
  while the column-to-LITERAL comparison re-keys through
  `kernel.CidrSortKey` (#492) — so `WHERE c = '10.0.0.1'` finds both
  spellings of one address and `WHERE c = d`, `GROUP BY c`, `DISTINCT` and
  every set operation find neither. PostgreSQL says they are one value
  (`inet '10.0.0.1' = inet '10.0.0.1/32'` is TRUE). Both wadjet paths agree
  with each other today, so keying the local set operation by inet ALONE
  would create the divergence it looks like it closes; the fix moves the
  whole key layer and the shuffle router together, the way #459 did for
  floats — predicate kernels, hash keys, and the set-operation key
  (`physical.keyValueText`, `internal/planner/physical/set_op_key.go`) all
  at once, not one at a time. (Amended 2026-08-25. Closed 2026-08-25 — see
  item 10's #546 and #565 residuals for the landed fix and the
  column-to-column comparison it turned out to share a cause with.)
