# ADR-0012 divergences: Temporal types

Timestamp resolution, infinity, clock zone, integer and text casts to DATE/TIMESTAMP, date/time arithmetic and the INTERVAL declaration. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**TIMESTAMP means timestamp without time zone** (E36)
TIMESTAMP is declared as PostgreSQL's `timestamp without time zone`, so a literal's offset is discarded: `'2020-01-01T05:30:00+05:30'` is `2020-01-01 05:30:00`, not the UTC instant the offset names. The rule is one function, `parquet.ParseTimestampMillis`; the comparison kernels reach it through `kernel.TimestampFilterConst` rather than keeping copies of the layout list, so a literal that stores is a literal a predicate reads the same way. A quoted text compared with a TIMESTAMP or DATE operand raises the input function's refusal — `ts = 'garbage'` 22007, `ts = '2024-03-04 12:00:00+16'` 22009, `ts = '0000-01-01'` 22008, as PostgreSQL — and a comparison with a column the planner can type raises it before any row, so an empty table, a conjunct no row passes and a pruned scan raise too (#1512: at 93e4804e that path read a refused spelling as 0, `1970-01-01 00:00:00`, and matched the epoch row; `coordinator.TestArcTCTimestampTextComparisonEveryArm`). CAST, a TIMESTAMP literal, INSERT, COPY and a bound parameter raise. Fields naming no instant (2020-02-30, month 13, hour 25) are 22008; text that is not a timestamp is 22007, as DATE classifies. Hour 24 and second 60 are read as the next day and the next minute, as PostgreSQL reads them. `CAST(<bad literal> AS TIMESTAMP)` raises the same pair of codes through `expr.fatalEval` (#836/#840).

**The millisecond carrier and its residuals** (E36)
TIMESTAMP stores epoch milliseconds. Sub-millisecond digits are floored toward the past (UnixMilli, TimestampToEngineMillis); floored and truncated differ only before 1970. The carrier has no value for infinity, so `'infinity'` text is 22007 and PostgreSQL's binary infinity encoding (the int64 extremes) is 22023 at Bind rather than being decoded as the year 294247.

**A temporal CAST refuses only text** (E38)
`CAST(<text> AS DATE|TIMESTAMP)` raises 22007/22008 for text naming no instant. PostgreSQL answers other source types at parse time with 42846, a type-pair refusal, and a data-exception code is not minted for a type error. A boolean source keeps NULL; a container source is refused 42846 at planning, as in PostgreSQL. `expr.TestCastTemporalRefusalStopsAtText` attempts the boundary from outside.

**An integer-to-DATE cast is bounded by the carrier** (E39)
PostgreSQL has no integer-to-date cast in any spelling (42846), so the bound is the engine's DATE carrier, a signed 32-bit day count, and a number with no room in it is 22003, the class `batch.IntegerRangeError` raises at the store. 22008 stays the answer for TEXT naming a calendar date that does not exist; splitting by source keeps `'…'::DATE` agreeing with PostgreSQL.

**One temporal accept-set** (E41)
`CAST(<text> AS DATE|TIMESTAMP)` takes its value and its refusal from `parquet.ParseDateDays` / `ParseTimestampMillis`, the functions the ingest boundary, the parquet writers, the row-to-batch builder and the filter kernel read. A spelling whose field order DateStyle would decide is refused 22007. `'0000-01-01'` is 22008 and `'2024-001-01'` is 22007, PostgreSQL's codes, added once in `parquet.ParseDateDays` (#641) and inherited by the CAST door. `0001-01-01`, `10000-01-01` and the compact `'20240101'` / `'19960110'` answer, as on 17.11. Pinned by `expr.TestCastToDateRaisesForTextThatNamesNoDay` and `expr.TestCastToDateStillReadsTheYearsAroundTheRefusal`. Since 2026-10-02 the two functions read ONE grammar (`parquet.parseTemporalText`, [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410)): a year-first date (`-`, `/` or `.`, one or more digits a field, or YYYYMMDD), then — after whitespace or `T` / `t` — an optional clock `H[H]:M[M][:S[S][.digits]]` and an optional zone `Z` or a numeric offset read by PostgreSQL's digit rule — `±h[h]`, `±h[h]:m[m][:s[s]]`, or an un-coloned run of three or more digits whose last two are the minute and the rest the hour (`+530`, `+0530`, `+00130` and `+000130` are 05:30, 05:30, 01:30 and 01:30; there is no run-together seconds form, so `+053000` is hour 530, 22009, as on 17.11) — whitespace before it or not, and after its sign (`+ 05` is +05, `- 05` −05: PostgreSQL's lexer skips whitespace there; the grammar's whitespace is PostgreSQL's isspace — space, tab, `\n`, `\r`, `\v`, `\f` — between every field); DATE drops the validated clock, TIMESTAMP keeps it (`24:00:00` and second 60 roll forward), the zone is discarded (E36). Before, TIMESTAMP was a list of two-digit, dash-only, seconds-required layouts, so `'2024/03/04'`, `'2024-3-4'` and `'2024-03-04 12:00'` were 22007 as a TIMESTAMP while the DATE reader took them, and a timestamp parameter bound with one of them was refused (#1426's splice). Field ranges are 22008, a displacement past ±15:59:59 is 22009, as on 17.11; the forms PostgreSQL also reads and this grammar does not are r25. Pinned per spelling by `pgwire.TestArcPWRound2MatchesPostgres` (b3/\*), the zone set on five doors (literal, CAST, INSERT, a date / timestamp / timestamptz parameter) by `coordinator.TestArcPWZoneSpellingsEveryArm`, and the offset digit rule by `parquet.TestTheZoneOffsetReadsPostgresDigitRule`.

**One timestamp renderer** (E59, E60)
Every timestamp-valued function (`date_trunc`, `from_unixtime`, `date_parse`, `timezone`, interval arithmetic over a text operand, and the clock functions) renders through `expr.formatInstant`, which is `batch.FormatTimestamp`, and declares `timestamp` (OID 1114) since #868; for data-derived results the value is PostgreSQL's byte for byte. `TO_ISO8601` keeps its ISO 8601 rendering because its name is its format, and `AT_TIMEZONE` because its result is a wall clock in another zone that a zoneless rendering would publish as UTC. Gated by `wadjet.TestTimestampHasOneRenderingAtEverySite`.

**Clock functions: zoneless UTC at millisecond resolution** (E60)
The engine has one TIMESTAMP type, an instant with no zone, so `NOW`, `CURRENT_TIMESTAMP` and `PG_POSTMASTER_START_TIME` print no offset and at most three fractional digits where PostgreSQL's `timestamptz` prints an offset and six. Closing it needs a zone-aware type and a microsecond carrier. `expr.clockNow` is the one clock and reads in UTC, so `CURRENT_DATE` and `NOW()` name the same day; `expr.SetClockForTest` lets `wadjet.TestEveryClockFunctionReadsOneZone` pin an instant straddling midnight UTC. If a session TimeZone is implemented, both move together.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT CAST('2020-01-01 00:00:00.123456' AS TIMESTAMP)` | 2020-01-01 00:00:00.123456 | 2020-01-01 00:00:00.123 (measured): the millisecond carrier floors sub-millisecond digits | — | value divergence | 2026-09-03 · [E36](#e36), P003 | #692 | — |
| **r2** `SELECT CAST('infinity' AS TIMESTAMP)` (likewise `'infinity'` / `'-infinity'` compared with a DATE or TIMESTAMP operand: `ts = 'infinity'`, `d < 'infinity'`) | infinity (stored and returned; '-infinity' likewise; the comparison answers by it — `ts = 'infinity'` no row) | ERROR 22007 invalid input syntax for type timestamp / date (measured; the comparison on five doors since #1512 — at 93e4804e `ts = 'infinity'` matched the epoch row on the embedded doors) | 22007 | refusal | 2026-09-22 · [E36](#e36), P004; 2026-10-04 (#1512) | #1266, #1512 | `coordinator.TestArcTCTimestampTextComparisonEveryArm` (kept \*/"infinity", \*/"-infinity") |
| **r3** `binary TIMESTAMP parameter carrying the int64 extreme (PostgreSQL's infinity encoding) at Bind` | infinity | ERROR 22023 at Bind; the millisecond carrier has no value to hold it | 22023 | refusal | 2026-09-22 · [E36](#e36), P004 | #1266 | — |
| **r4** `SELECT CAST(true AS DATE)` | ERROR 42846 cannot cast type boolean to date | NULL (measured); a container, CAST(ARRAY[1] AS DATE), is 42846 as in PostgreSQL (measured) | — | kept superset | 2026-09-03 · [E38](#e38), P057 | #836, #840 | `expr.TestCastTemporalRefusalStopsAtText` |
| **r5** `SELECT CAST(0 AS DATE)` | ERROR 42846 cannot cast type integer to date | 1970-01-01 (measured); 2932896 is 9999-12-31 | — | kept superset | 2026-09-06 · [E39](#e39), P058 | #911 | `coordinator.TestAnInt32DomainRefusalHoldsOnEveryArm` |
| **r6** `SELECT CAST(3000000000 AS DATE)` | ERROR 42846 cannot cast type bigint to date | ERROR 22003 integer out of range (measured): the signed 32-bit day count is the bound | 22003 | kept superset | 2026-09-06 · [E39](#e39), P058 | #911 | `coordinator.TestAnInt32DomainRefusalHoldsOnEveryArm`, `wadjet.TestAnOutOfRangeCastRefusesAtTheDoor` |
| **r7** `SELECT CAST('31/12/1996' AS DATE)` | ERROR 22008 date/time field value out of range (DateStyle ISO, MDY) | ERROR 22007 invalid input syntax for type date (measured): no spelling whose field order DateStyle decides is read | 22007 | value divergence | 2026-09-03 · [E41](#e41), P059 | #840 | — |
| **r8** `SELECT TO_ISO8601(CAST('2023-11-14 05:06:07' AS TIMESTAMP))` | ERROR 42883 (no to_iso8601 function) | 2023-11-14T05:06:07Z (measured); AT_TIMEZONE likewise keeps an ISO 8601 rendering | — | kept superset | 2026-09-04 · [E59](#e59) | #544 | — |
| **r9** `SELECT NOW()` | 2026-09-04 21:21:01.708284+00, timestamp with time zone (OID 1184) | 2026-09-29 09:49:57.575 (measured), timestamp without time zone (OID 1114), zoneless UTC at millisecond resolution | — | value divergence | 2026-09-04 · [E60](#e60), P005 | #544, #868 | `expr.TestPgPostmasterStartTime` |
| **r10** `SELECT PG_POSTMASTER_START_TIME()` | microsecond timestamptz with an offset | zoneless millisecond timestamp (OID 1114, measured); CURRENT_TIMESTAMP likewise | — | value divergence | 2026-09-04 · [E60](#e60), P005 | #544, #868 | `expr.TestPgPostmasterStartTime` |
| **r11** `SELECT CURRENT_DATE` | the day in the session's TimeZone; current_date = now()::date is TRUE | the UTC day; no session zone exists; CURRENT_DATE = CAST(NOW() AS DATE) is t (measured) | — | value divergence | 2026-09-05 · [E60](#e60), P005 | #870 | `wadjet.TestEveryClockFunctionReadsOneZone` |
| **r12** `SELECT LOCALTIMESTAMP(0) = LOCALTIMESTAMP(6)` | f (the value is truncated to the requested precision) | t (measured): precision accepted and ignored; declared timestamp either way | — | value divergence | — · P013 | #1169 | — |
| **r13** `SELECT count(*) FROM t WHERE LOCALTIMESTAMP >= LOCALTIMESTAMP` | every row (the statement's start time for every row) | the clock is read per evaluation; a row straddling a millisecond can answer FALSE | — | value divergence | — · P014 | #1169 | — |
| **r14** `SELECT DATE_ADD('2026-03-03', 1)` | ERROR 42883 (no date_add function) | 2026-03-04 00:00:00, declared timestamp (measured); over a DATE x it is date | — | kept superset | — · P018 | #1254 | — |
| **r15** `CREATE TABLE dur_t (d DURATION); SELECT d FROM dur_t` | no DURATION type; interval counts microseconds | bigint nanoseconds, OID 20 (declared bigint, measured) | — | kept superset | — · P087 | #834 | — |
| **r16** `SELECT CAST('2020-01-01 01:00:00' AS TIMESTAMP) - CAST('2020-01-01 00:00:00' AS TIMESTAMP)` | 01:00:00, interval | 3600000, declared double precision (measured): milliseconds | — | value divergence | — · P092 | — | — |
| **r17** `SELECT CAST('2020-01-03' AS DATE) - CAST('2020-01-01' AS DATE)` | 2, integer | 2, declared bigint (measured); `(d - DATE '2024-01-01') + 1` and `/ 7` are integer arithmetic declared bigint too (`/ 7` divides as integers; double precision at v0.25.3, same values), and beside a NUMERIC a day count is an integer operand of numeric arithmetic (`(d - DATE '2024-01-01') * n` numeric, as on PostgreSQL; double precision at v0.25.3); a day count divided by zero answers NULL where PostgreSQL raises 22012 (`(d - DATE '2024-01-01') / 0`, as at v0.25.3: a recorded filing candidate); inside a scalar subquery a day count is integer, as on PostgreSQL (`(SELECT (x.d - DATE '2024-01-01') + 1 …)`) | — | value divergence | — · P092 | — | `coordinator.TestArcSSOperandClassAndWidthEveryArm` |
| **r18** `SELECT CAST('2020-01-03' AS DATE) - CAST('2020-01-01 00:00:00' AS TIMESTAMP)` | 2 days, interval | ERROR 42883 operator does not exist: date - timestamp without time zone (measured) | 42883 | refusal | — · P092 | — | — |
| **r19** `SELECT ts - '2026-03-01 00:00:00' FROM t` | an interval | the millisecond count: the literal resolves to a TIMESTAMP as in PostgreSQL, the difference is r16's | — | value divergence | — · P092 | — | — |
| **r20** `SELECT INTERVAL '25 hours'` | 25:00:00, declared interval (OID 1186) | 25:00:00, declared text (OID 25) (measured): no INTERVAL column type | — | value divergence | — · P092 | — | — |
| **r21** `SELECT CAST('2020-01-01' AS DATE) + CAST('1 day 02:00:00' AS INTERVAL)` | 2020-01-02 02:00:00 | ERROR 0A000 only a single-unit interval is supported in arithmetic (measured) | 0A000 | refusal | — · P092 | — | — |
| **r22** `SELECT ts + INTERVAL '100000000000 hours' FROM t` | ERROR 22015 at the literal | ERROR 22008 when the interval is applied | 22008 | value divergence | — · P092 | — | — |
| **r23** `SELECT CURRENT_TIME` | the current time, time with time zone | ERROR 42883 unknown function: current_time (measured); there is no TIME type | 42883 | documented gap | — · P109 | #1254 | — |
| **r24** `SELECT COALESCE(d, ts) FROM t` | the first non-null arm as timestamp without time zone, a DATE arm at its midnight | ERROR 0A000 COALESCE types date and timestamp without time zone are not supported together (measured, every arm); CASE, GREATEST and LEAST the same, whatever the arm's shape — a column (of a table, a derived table, a CTE or recursive CTE — read by the statement or inside the recursive CTE's own recursive term — a join, a set operation, VALUES or a LATERAL output, whatever expression produced it there), a literal, an expression, a scalar subquery (correlated or not, a union, CTE or derived-table body included), a window aggregate or value function (`max(ts) OVER ()`, `lag(ts) OVER (…)`), an aggregate — in the SELECT list, WHERE, ORDER BY, GROUP BY and HAVING alike. NULLIF(d, ts), declared by its first argument, answers PostgreSQL's rows. The DATE / TIMESTAMP declaration such a column carries (a scalar subquery, a window call or a recursive CTE's non-recursive term) is read by every other operator too, as a stored column's is: `ts ± 1`, `ts + ts`, `d + d`, `sum(d)`, `avg(ts)` and an integer = DATE / TIMESTAMP key (EXISTS, IN, JOIN, scalar comparison) over it raise 42883 as PostgreSQL does, and `ts − d` raises r18's 42883 (at v0.25.2 they answered a number, a day count or 0 rows). A comparison (a scalar-subquery operand on either side, correlated or not, included), an IN / = ANY / NOT IN / <> ALL membership, an EXISTS or LATERAL key and a JOIN key between the two answer PostgreSQL's rows (the DATE at its midnight); a TIMESTAMP-typed bind parameter against a DATE answers PostgreSQL's rows too — it is spliced as a TIMESTAMP literal since 2026-10-02 ([PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410), #1426; at v0.25.3 it was read at DATE) | 0A000 | refusal | 2026-09-29 · [DT](../0012-amendments.md#2026-09-29-a-date-against-a-timestamp-arc-dt-1378) | #1378, #1316 | `coordinator.TestArcDTDateTimestampEveryArm`, `coordinator.TestArcDTR2ScalarAndFoldArmsEveryArm` |
| **r25** `SELECT CAST('2024-03-04 12:00:00 UTC' AS TIMESTAMP)` (likewise `… America/New_York`, `… EST`, `…12:00:00Z+05`, `…12:00:00z+05`, `12:00 PM`, `epoch`, `2024-03-04 BC`, `March 4 2024`, `Mar 4, 2024 12:00`, `J2460374`, `20240304T120000`, `2024-03-04 012:00`; as DATE, TIMESTAMP, a TIMESTAMP literal or a timestamp / timestamptz parameter; and, since 2026-10-04, a quoted text compared with a DATE or TIMESTAMP operand — `ts = 'epoch'`, `d = 'Jan 15 2024'`, `ts = 'J2460325'`, `ts = '2024-01-15 BC'`, `ts = '…Z+05'`, `ts < 'now'`, `d = 'today'`, in every comparison and membership position) | 2024-03-04 12:00:00 (each read: a zone name or abbreviation — `Z+05` is the POSIX zone five hours WEST of UTC, `TIMESTAMPTZ '2024-03-04 12:00:00Z+05'` is 17:00 UTC, not `Z` and then an offset — a meridiem, the special values, BC, month names, a Julian day, an ISO-basic clock, a three-digit hour) | ERROR 22007 invalid input syntax (measured): outside the one date/time grammar (E41) — refused, never guessed; `DATE '2024-03-04 12:00:00Z+05'` and `…z+05` read 2024-03-04 at 978cd0e5, which discarded what followed the clock | 22007 | refusal | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1426 | `pgwire.TestArcPWRound2MatchesPostgres` (b3/\*), `coordinator.TestArcPWZoneSpellingsEveryArm` (`…Z+05`, `…z+05`, kept), `coordinator.TestArcTCTimestampTextComparisonEveryArm` and `coordinator.TestArcTCBoundParameterEveryDoor` (kept, row temporal r25) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E36

ADR lines 1369-1418. Catalog rows: r1, r2, r3. Stated in [Mechanisms](#mechanisms).

- **TIMESTAMP is `timestamp without time zone`, and a literal's offset is
  DISCARDED.** (Added 2026-09-03, #692; this entry records a divergence
  CLOSED, not one kept.) Wadjet declares TIMESTAMP as PostgreSQL's
  `timestamp without time zone` on the wire and therefore has to mean what
  that type means: `'2020-01-01T05:30:00+05:30'` is `2020-01-01 05:30:00`.
  It used to be read as the INSTANT the offset names and normalized to UTC
  — `time.Parse` yields a fixed +05:30 zone and `UnixMilli` converts it —
  so wadjet stored `2020-01-01 00:00:00` and answered a different question
  than the literal asked. Verified live: `'…+05:30'::timestamp` is
  `2020-01-01 05:30:00` and `'…+05:30'::timestamptz AT TIME ZONE 'UTC'` is
  `2020-01-01 00:00:00`, which is what wadjet was storing.

  The rule is one function, `parquet.ParseTimestampMillis`, and the two
  comparison kernels reach it through `kernel.TimestampFilterConst`
  (until #1512, `parquet.ParseTimestampMillisOrZero`, which read a refused
  text as the epoch) instead of keeping copies of the layout list. They had two copies and both had DRIFTED from the writer's,
  so the space-separated millisecond form stored fine and no predicate
  could read it back — a literal that STORES has to be a literal a
  predicate over the same column reads the same way, and one function is
  the only thing that keeps that true. The classification goes with it:
  a timestamp whose FIELDS name no instant (2020-02-30, month 13, hour 25)
  is **22008**, text that is not a timestamp at all is **22007**, exactly
  as DATE has classified since #560. Hour 24 and second 60 are NOT
  field-range failures: PostgreSQL reads them as the next day and the next
  minute, and so does this — refusing input PostgreSQL accepts is what item
  1 forbids, and the first pass refused both.

  `CAST(<bad literal> AS TIMESTAMP)` **raises** the same pair of codes since
  #836/#840. The residual this paragraph used to record — "the CAST path has
  no per-row error channel for a temporal conversion, so it produces a value
  or nothing, and nothing is NULL" — was FALSE when it was written: the
  channel is `expr.fatalEval`, the numeric casts had used it since #367, and
  #836 is the issue that read the tree instead of the record. Method 9 in the
  correctness-fix protocol is about exactly this direction of error. The
  census pin is deleted; both engines answer 22008 for an impossible day and
  22007 for text that is not a timestamp.

  **Residual, kept.** Sub-millisecond precision is TRUNCATED to the
  millisecond the column stores — `.123456` reads back `.123` — which is a
  declared-type property of TIMESTAMP here and a stored-value divergence
  from PostgreSQL's microseconds.

  **Residual, kept (2026-09-22, #1266).** TIMESTAMP has no infinity:
  `'infinity'` / `'-infinity'` text is 22007 and a binary parameter carrying
  PostgreSQL's infinity encoding (the int64 extremes) is 22023 at Bind,
  where PostgreSQL stores and returns both. The millisecond carrier has no
  value to hold it, and decoding the extremes as an instant named the year
  294247. Every other producer FLOORS sub-millisecond digits toward the
  past (UnixMilli, TimestampToEngineMillis), so "truncated" above means
  floored; the two differ only before 1970.

### E38

ADR lines 1460-1468. Catalog rows: r4. Stated in [Mechanisms](#mechanisms).

- **A temporal CAST over a box with no temporal reading keeps NULL.**
  (Added 2026-09-03, #836/#840.) `CAST(<text> AS DATE|TIMESTAMP)` raises
  22007/22008 for text naming no instant. Every OTHER box that fails to
  parse — a boolean, a container — keeps the NULL it had, because
  PostgreSQL answers those at PARSE time with **42846**
  (`cannot cast type boolean to date`): it is a TYPE-PAIR refusal, not a
  data exception, and minting 22007 for it would put a data-exception code
  on a type error. The boundary is attempted from the outside by
  `expr.TestCastTemporalRefusalStopsAtText`.

### E39

ADR lines 1469-1487. Catalog rows: r5, r6. Stated in [Mechanisms](#mechanisms).

- **An INTEGER cast to DATE is a wadjet SUPERSET, and its out-of-range
  refusal is `22003`, not `22008`.** (Added 2026-09-06, #911.) PostgreSQL
  has NO integer-to-date cast in any spelling — `0::date`, `2932896::date`,
  `3000000000::date` and `9223372036854775807::date` are all **42846**
  `cannot cast type integer|bigint to date`, measured on 17.11 — so there is
  no server answer to follow for the boundary, and the bound is the engine's
  own DATE carrier: a signed 32-bit day count.

  The class is the one this engine already gives for a number with no room
  in a 32-bit field, which is what `docs/data-types.md` said before the cast
  was fixed and what `batch.IntegerRangeError` raises at the store. `22008`
  — the class the arc brief proposed — is the DATE accept-set's answer for
  TEXT naming a calendar date that does not exist (`'2020-02-30'`), which is
  a different question with a different input; splitting the two by the
  SOURCE keeps `'…'::DATE` agreeing with PostgreSQL where PostgreSQL has an
  opinion, and keeps the superset's own refusal in the family its carrier
  belongs to. Gated on four arms by
  `coordinator.TestAnInt32DomainRefusalHoldsOnEveryArm` and at the door by
  `wadjet.TestAnOutOfRangeCastRefusesAtTheDoor`.

### E41

ADR lines 1658-1694. Catalog rows: r7. Stated in [Mechanisms](#mechanisms).

- **The CAST door reads the engine's ONE temporal accept-set, and inherits
  its refusals.** (Added 2026-09-03, #840; the accept-set is #639's and
  #641's. Amended 2026-09-04: #641 landed and the last residual here closed
  itself, which is what a shared accept-set is FOR.) `CAST(<text> AS DATE|TIMESTAMP)` takes both its VALUE and its
  refusal from `parquet.ParseDateDays` / `ParseTimestampMillis` — the same
  function the ingest boundary, the parquet writers, the row→batch builder
  and the filter kernel read — so a literal that STORES is a literal a
  predicate reads the same way and a CAST reads the same way. Three
  consequences follow, and all three are the accept-set's rather than the
  cast's:

  - A DMY spelling is **22007 here and 22008 in PostgreSQL**. Both engines
    REFUSE `'31/12/1996'`; PostgreSQL's DateStyle ISO, MDY reads the leading
    field as a month and calls month 31 a field-range failure, while wadjet
    refuses every spelling whose field ORDER DateStyle would decide, so it
    is "not a date" and the class differs.
  - `'0000-01-01'` and `'2024-001-01'` are **REFUSED, with PostgreSQL's own
    codes and messages** — 22008 for a year the calendar does not have
    (1 BC sits immediately before 1 AD there) and 22007 for a three-digit
    month, which PostgreSQL reads as a day-of-year and then rejects. This
    started as the entry's one open residual, and the way it closed is the
    point of the entry: the refusal was added ONCE, in
    `parquet.ParseDateDays` (**#641**, the storage arc), and the CAST door
    inherited it with no change in `expr` at all. **The two doors have
    converged**: the ingest boundary, `INSERT … VALUES`, a predicate and a
    CAST now give the same answer to "is this a date", including these two
    spellings. The pin that recorded the residual fired the day #641 landed
    and was deleted — that firing is the fix's proof. The cells live in
    `expr.TestCastToDateRaisesForTextThatNamesNoDay` now, with
    `expr.TestCastToDateStillReadsTheYearsAroundTheRefusal` asserting the
    boundary the new refusal must not cross: `0001-01-01`, `10000-01-01`
    and the compact `19960110` all still answer, as they do on 17.11.
  - The compact 8-digit form `'20240101'` is ACCEPTED, as PostgreSQL accepts
    it. It briefly RAISED — #836's first pass took only the error CODE from
    the shared parser and left the value to a narrower reading — which is
    the same two-answers-for-one-question defect pointing the other way: a
    cast refusing what the ingest boundary stores.

### E59

ADR lines 2424-2466. Catalog rows: r8. Stated in [Mechanisms](#mechanisms).

- **A timestamp-VALUED scalar function DECLARES text where PostgreSQL
  declares timestamp.** (Added 2026-09-04, from #544's residual; the
  rendering half CLOSED 2026-09-04.)

  `DATE_TRUNC('day', ts)` answered `2023-11-14T00:00:00Z` where PostgreSQL
  17.11 answers `2023-11-14 00:00:00`, and so did `NOW`,
  `CURRENT_TIMESTAMP`, `FROM_UNIXTIME`, `DATE_PARSE`, `TIMEZONE`,
  `PG_POSTMASTER_START_TIME` and interval arithmetic over a text operand.

  The first version of this entry said the fix had to wait for a
  TIMESTAMP-valued function result, because "the format is a per-function
  choice rather than a property of a type, and `batch.FormatTimestamp` has
  nothing to be called from". That was wrong on its own terms: a function
  that formats its own text can be given the ONE formatter to call, and the
  twelve `date_trunc` arms plus the six other sites now call
  `expr.formatInstant`, which is `batch.FormatTimestamp`. One renderer is
  exactly the property #544 states, and for every function whose result is
  derived from DATA — `date_trunc`, `from_unixtime`, `date_parse`,
  `timezone`, interval arithmetic over a text operand — the VALUE is
  PostgreSQL's byte for byte. The three CLOCK functions are the exception
  and have their own entry below.

  Two functions deliberately keep an ISO 8601 rendering: `TO_ISO8601`,
  whose name is its format contract, and `AT_TIMEZONE`, whose result is a
  wall clock in another zone that a zoneless rendering would publish as
  UTC — the same misreading `TIMEZONE` declines a non-UTC zone rather than
  commit.

  **What still diverges** is the DECLARATION. `date_trunc` RETURNS
  `timestamp` (OID 1114) on the server and `text` (OID 25) here: the scalar
  registry is `func([]any) any` with one static `Ret` per entry, so a
  function result cannot be TIMESTAMP-typed at all. A client that asks what
  the column IS gets the wrong answer, and no amount of formatting fixes
  that — which is why it is recorded rather than bandaged (protocol rule
  11). The structural fix is a type channel in the registry: ~14 entries
  return the engine's TIMESTAMP box and carry `parquet.TypeTimestamp`, and
  the planner's declared type follows.

  Gated by `wadjet.TestTimestampHasOneRenderingAtEverySite` — seven
  function-result value cells for the closed half, and the
  `residual_date_trunc_declares_string_not_timestamp` pin for the open one,
  which fails the day a timestamp-valued function declares TIMESTAMP.

### E60

ADR lines 2467-2530. Catalog rows: r9, r10, r11. Stated in [Mechanisms](#mechanisms).

- **`NOW`, `CURRENT_TIMESTAMP` and `PG_POSTMASTER_START_TIME` render a
  ZONELESS instant at MILLISECOND resolution where PostgreSQL renders a
  `timestamptz` with an offset at microsecond resolution.** (Added
  2026-09-04, from #544's second pass; round-2 review, B2r2.)

  Measured on PostgreSQL 17.11:

  ```
  SELECT now()::text                      2026-09-04 21:21:01.708284+00
  SELECT pg_typeof(now())                 timestamp with time zone
  SELECT pg_postmaster_start_time()::text 2026-08-31 20:43:01.076093+00
  ```

  and here: `2026-09-04 21:21:01.708`, declared `text`. Two facts differ,
  and both are structural rather than a formatting choice.

  **No `timestamptz`.** This engine has one TIMESTAMP type, an instant with
  no zone, so there is no offset to print. PostgreSQL's `timestamptz` is
  also an instant with no stored zone — it renders in the session's
  `TimeZone` — so the two hold the SAME value and disagree about what the
  text says about it.

  **Millisecond carrier.** A wadjet instant is epoch milliseconds
  (`batch.FormatTimestamp`), PostgreSQL's is microseconds. Three fractional
  digits is the most any rendering here can carry; see `docs/data-types.md`
  §Timestamp, "The resolution is the millisecond".

  Before #544's second pass these three answered RFC3339
  (`2026-09-04T21:01:38Z`), which named the zone and still was not
  PostgreSQL's text. Routing them through the one renderer is what "one
  rendering" requires — a second dialect for three functions is the thing
  it forbids — and the offset it costs is recorded here rather than traded
  for a second formatter. Closing it means a zone-aware timestamp type and
  a microsecond carrier, which is its own change.

  `expr.TestPgPostmasterStartTime` parses the engine's rendering and
  `pgwire.startupTimeIsThisProcess` asserts the value through the DataGrip
  opening sequence; neither pins the offset, because there is none to pin.

  The DECLARATION half closed in #868: all three declare `timestamp` (OID
  1114) now, as do `date_trunc`, `from_unixtime`, `date_parse` and
  `timezone`. What stays divergent is only what this entry is about — the
  zone and the sixth fractional digit, which the server's `timestamptz`
  carries and this engine's instant cannot.

  **One clock zone for every clock function: UTC.** (Added 2026-09-05,
  #870.) `NOW()` and `CURRENT_TIMESTAMP` render through
  `expr.formatInstant`, which normalizes to UTC because a wadjet TIMESTAMP
  is a zoneless instant with no offset to print. `CURRENT_DATE` formatted
  `time.Now()` in the MACHINE'S LOCAL zone, so on a host west of Greenwich
  the two named different DAYS for the hours between local midnight and UTC
  midnight — a landing battery at 20:00 ET had `CURRENT_DATE` on 2026-09-04
  and `NOW()` on 2026-09-05.

  PostgreSQL keeps them consistent through the SESSION's TimeZone
  (`current_date = now()::date` is TRUE there, measured under
  `TimeZone = UTC`), and this engine has no session zone to consult. UTC is
  the zone its rendering already commits to, so it is the zone its clock
  reads in; if a session TimeZone is ever implemented, both move together
  and this paragraph goes with the rest of the entry. `expr.clockNow` is the
  one clock, and `expr.SetClockForTest` exists because the defect is a
  CONDITION — the two functions agree for most of the day — so
  `wadjet.TestEveryClockFunctionReadsOneZone` mocks an instant straddling
  midnight UTC rather than trusting the machine's own hour.
