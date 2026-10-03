# ADR-0012 divergences: Parameters and the wire

The wire protocol: multi-statement strings without transactions, PORT/PROTOCOL/DURATION wire types, syntax errors without a POSITION field, and the type a bound parameter takes. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**A multi-statement string is a sequence, not a transaction** (E11, P008)
The engine has no transactions: BEGIN and COMMIT are accepted and ignored on every door. A simple-query string carrying several statements runs as a sequence with PostgreSQL's command tags and its stop-at-the-first-error rule, and a syntax error anywhere runs none of them; the statements that completed before a failure stay. This changes only if transactions are implemented, not through the statement splitter. Gated per door in the DML census and `internal/server/pgwire/multi_statement_test.go`.

**PORT and PROTOCOL declare int4 on the wire, DURATION int8 nanoseconds** (E37)
The three types are compared numerically, so they declare numeric OIDs (#834); their text form is unchanged (`443`, `6`, `1500000000`), `appendBinaryValue`'s int32/int64 arms write the bytes the OIDs promise, and `numericOID` in bindparams binds `WHERE port_col = $1` as a number. `kernel.NumericTypeName` renders `integer`/`bigint` in messages (`port_col = 'abc'` is 22P02 for type integer) and `pgFormatType` declares the same in the synthetic `pg_attribute`. DURATION as `interval` (OID 1186, microseconds) is the recorded alternative; it needs a unit decision first.
**A bound parameter is a literal of its type** (r5–r13)
A parameter's type is the OID the client declared at Parse, or — for OID 0 — the type of the position it occupies, decided as PostgreSQL decides it: the known peer's type beside a comparison, IN list, BETWEEN, CASE arm or COALESCE / NULLIF / GREATEST / LEAST argument (the planner's declaration walk over the columns in scope) — and beside arithmetic PostgreSQL's operator result type over the operands' types (`$1 = n + 1` integer, `$1 = n + f` double precision, `$1 = d - d` integer), never this engine's declaration of the result column, the target column's in INSERT, UPDATE and MERGE, bigint in a LIMIT / OFFSET / FETCH count, real in a TABLESAMPLE percentage, integer as a window function's offset, the target of a CAST, boolean as a predicate, and text as a bare select-list item or beside `||` / LIKE (`internal/server/pgwire/paramtypes.go`). The types are decided once, when the statement is parsed, against the catalog as it stands then, and kept on the statement: a DDL between Parse and Bind does not change them (PostgreSQL's rule), and a new Parse of the same text after a DDL on any connection types it again. The ParameterDescription reports it, and Bind splices the value as a literal OF it — `CAST('…' AS TIMESTAMP)`, `CAST(n AS BIGINT)` — so the operator rules a literal of that type meets apply to the parameter (a timestamp against a DATE column is compared as a timestamp, #1426); a NULL parameter, and the stand-in a statement Describe runs with, is a NULL of that type, so the Describe declares what the bound value declares — except an integer or smallint parameter bound a negative value, which executes as bigint (r12). Positions that read a constant only (a count, a window function's integer argument, LAG / LEAD's default, a MERGE action's whole value) take the constant spelling they read (`paramposition.go`). Where this engine has no type for the OID, or declares the expression around the parameter differently, the rows below say so. Gated by `pgwire.TestArcPWParameterTypesMatchPostgres` (the coverage table, PostgreSQL 17.11's answers pinned), `pgwire.TestArcPWPgxClientsBindAsPostgres` and `coordinator.TestArcPWParameterTypesOnTheCoordinatorDoor`.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `INSERT INTO mt VALUES (1); SELECT 1/0` | ERROR 22012; the implicit transaction rolls the INSERT back | INSERT 0 1 then ERROR 22012; the inserted row stays (measured) | 22012 | value divergence | 2026-09-03 · [E11](#e11), P008 | #711 | `internal/server/pgwire/multi_statement_test.go` |
| **r2** `BEGIN; INSERT INTO mt VALUES (2); ROLLBACK` | the row is gone | BEGIN and ROLLBACK are accepted and ignored; the row stays (measured) | — | documented gap | 2026-09-03 · [E11](#e11), P008 | #711 | — |
| **r3** `SELECT p, pr, d FROM np` | no PORT, PROTOCOL or DURATION type (the twin table declares integer, integer, bigint) | declares int4, int4, int8; 443, 6, 1500000000 (DURATION is nanoseconds, not interval; measured) | — | kept superset | 2026-09-03 · [E37](#e37) | #834 | — |
| **r4** `SELECT 1 +` | ERROR 42601 syntax error at end of input, with a POSITION field (psql LINE 1 and caret) | ERROR 42601 same sentence; no POSITION field (measured) | 42601 | documented gap | — · P106 | #1307 | — |
| **r5** `SELECT id FROM p WHERE n = $1` — `$1` declared text, bound `'7'` (n integer) | ERROR 42883 operator does not exist: integer = text | id 1: a text or varchar parameter is spliced as SQL's unknown literal, which the integer column's input reads (measured); `SELECT $1` declared varchar declares text (OID 25) where PostgreSQL declares varchar, and an undeclared parameter beside a VARCHAR column is typed text, as such a column declares here | — | kept superset | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (eq/n/25/\*, eq/d/25/\*, select/1043/\*, insert/VARCHAR/0/oid0) |
| **r6** `SELECT id FROM p WHERE n IN (SELECT $1)` — `$1` undeclared, bound `'7'` (likewise `n = (SELECT $1)`, `d IN (SELECT $1)`) | ERROR 42883 operator does not exist: integer = text (the subquery's unknown output is text) | id 1: the parser reads a FROM-less one-item subquery as its item, so the parameter is typed as the comparison's other side (integer, date) and answers (measured). `= ANY (SELECT $1 UNION ALL SELECT $2)` is text and 42883 on both | — | kept superset | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (infer/n IN (SELECT $1), infer/n = (SELECT $1), date/d IN (SELECT $1)/\*/unk) |
| **r7** `SELECT $1 IS NULL, $1` and `SELECT count(*) FROM p WHERE $1 IS NULL` — `$1` undeclared, bound NULL | ERROR 42P08 / 42P18 could not determine data type of parameter $1 | `t`, NULL declared text (OID 25), and 6: the select-list parameter is text, the `IS NULL` operand undetermined (OID 0) (measured) | — | kept superset | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (select-null/0, select-null/where/0) |
| **r8** `SELECT $1` — `$1` declared int2 | smallint (OID 21) | integer (OID 23): no smallint type (measured) | — | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (select/21/\*, select-null/21) |
| **r9** `SELECT $1` — `$1` declared timestamptz, bound `'2024-03-04 12:00:00+00'` | timestamptz (OID 1184) `2024-03-04 12:00:00+00` | timestamp (OID 1114) `2024-03-04 12:00:00`: the instant at UTC, the session TimeZone the server reports; an offset spelled `Z`, `±hh`, `±hh:mm`, `±hhmm` or `±hh:mm:ss` (a space before it or not) is applied — `ts = $1` bound `'2024-03-04 17:00:00+05'` or `'…+0530'` matches as on PostgreSQL — and a zone NAME (`UTC`, `EST`, `America/New_York`) is refused 22007 as the TIMESTAMP input refuses it ([temporal#r25](temporal.md#catalog); measured) | — | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1426 | `pgwire.TestArcPWParameterTypesMatchPostgres` (select/1184/\*, select-null/1184, eq/ts/1184/\*), `pgwire.TestArcPWRound2MatchesPostgres` (b3/ts >= $1 timestamptz/\*) |
| **r10** `SELECT $1` — `$1` declared bytea, int4[] or text[] | bytea (OID 17) `\x6869`; int4[] (1007); text[] (1009) | text (OID 25): `\x6869` for a text-format bytea and the bytes themselves (`hi`) for a binary one; `{1,2}`; an array parameter in binary format is refused 22023 (measured) | 22023 | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (select/17/\*, select/1007/\*, select/1009/\*) |
| **r11** `SELECT id FROM p WHERE bt = $1` — `$1` declared bytea, binary `00 ff 5c` | the row | ERROR 22P02: the bytes are spliced into a quoted literal that a BYTES comparison reads through bytea input again, and the backslash starts an escape (the same at v0.25.3; measured) | 22P02 | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (eq/bt/17/bin/backslash) |
| **r12** `SELECT $1 + 1` — `$1` undeclared, bound `'2'` | parameter integer, result integer | parameter integer, result bigint: the parameter takes PostgreSQL's type — beside arithmetic its operator result type, so `$1 = n * 2` is integer as on PostgreSQL — and the expression around it this engine's declaration: integer arithmetic ([numeric-decimal#r1](numeric-decimal.md#catalog)), an integer CAST (`SELECT CAST($1 AS INTEGER)`) and NTILE declare bigint, and `WHERE m = $1` over a derived `n * 2 AS m` types the parameter bigint where PostgreSQL types it integer (the derived column is this engine's declaration). `SELECT $1` (or `COALESCE($1, 0)`) declared integer and bound `-7`, text or binary, executes as bigint (a negative integer literal declares bigint here, and so does `CAST(-7 AS INTEGER)`) while the statement Describe declared integer: eight bytes in a binary row where PostgreSQL sends four, so a client that decodes binary rows by that Describe refuses the value; declared smallint it is r8's integer Describe with the same eight bytes (PostgreSQL: smallint, two). An integer or smallint parameter past its width raises 22003 as on PostgreSQL (measured) | — | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (infer/SELECT $1+1, infer/CAST int, infer/NTILE, infer/derived), `pgwire.TestArcPWNegativeIntegerOutputResidual`, `pgwire.TestArcPWRound2MatchesPostgres` (b2/\*, b4/\*) |
| **r13** `SELECT n FROM p GROUP BY n HAVING SUM(f) > $1` and `SELECT * FROM generate_series(1, $1)` — `$1` undeclared | parameter double precision / integer; one bigint column | parameter undetermined (OID 0), its text read by the position as before; the statement Describe of the table function over a parameter answers no columns (measured; the same at v0.25.3) | — | documented gap | 2026-10-02 · [PW](../0012-amendments.md#2026-10-02-a-bound-parameters-type-arc-pw-1426-1410) | #1410 | `pgwire.TestArcPWParameterTypesMatchPostgres` (infer/having, consumer/generate_series/\*) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E11

ADR lines 545-561. Catalog rows: r1, r2. Stated in [Mechanisms](#mechanisms).

- **A multi-statement simple-query string is not one transaction.**
  (Added 2026-09-03, #711.) PostgreSQL's simple query protocol wraps a
  string carrying several statements in an IMPLICIT TRANSACTION, so a
  failure in the third statement rolls the first two back — measured:
  `INSERT …; SELECT 1/0` leaves the table untouched. Wadjet runs the same
  string as a sequence with the same tags and the same stop-at-the-first-
  error rule, and the statements that already committed STAY.

  It is not a sequencing divergence and cannot be closed by the sequencing:
  wadjet has no transactions at all — `BEGIN` and `COMMIT` are accepted and
  ignored on every door — so there is nothing to roll back with. What
  changes if transactions are ever implemented is this entry, not the
  splitter. Recorded here so a future gate does not read it as undecided.
  The value half agrees: which statements run, in what order, with which
  command tags, and that a syntax error anywhere runs none of them, are
  all PostgreSQL's answers and are gated per door in the DML census and in
  `internal/server/pgwire/multi_statement_test.go`.

### E37

ADR lines 1419-1459. Catalog rows: r3. Stated in [Mechanisms](#mechanisms).

- **PORT and PROTOCOL are `int4` on the wire; DURATION is `int8`
  NANOSECONDS.** (Decided 2026-09-03, #834.) All three declared OID 25
  (`text`) while the engine compared them NUMERICALLY, which is item 2's
  exact shape: one type declared, another behaved as. Under `text`,
  `port > 5` is the operand pair #721 refuses and PostgreSQL rejects with
  42883 — yet wadjet answered it, and refusing would have broken a
  legitimate wadjet-native comparison. Declaring the numeric OID dissolves
  the asymmetry instead of choosing a side of it.

  The TEXT on the wire does not change: a PORT is a uint16 and a PROTOCOL a
  uint8, both stored in `Int32Data` and boxed as an int32, and a DURATION is
  an int64 — all three already rendered as plain integers (`443`, `6`,
  `1500000000`), which is exactly int4's and int8's text form.
  `appendBinaryValue`'s own int32/int64 arms already write the 4 and 8 bytes
  the OIDs promise, so no encoder arm was needed the way `date`, `numeric`
  and `uuid` needed one — those box as TEXT and these box as the number.
  `numericOID` in bindparams already covers int4 and int8, so an inbound
  `WHERE port_col = $1` binds as a bare SQL number rather than a quoted
  string.

  **DURATION is int8 and not `interval`, and that is the open alternative.**
  PostgreSQL's `interval` (OID 1186) is MICROSECOND precision with its own
  text and binary forms, so declaring it would change the RENDERING as well
  as the type — a wadjet DURATION counts nanoseconds, which is the unit
  `schema.go` defines and `Vector.GetValue` reads back. Moving to `interval`
  would need a unit decision first and is recorded here as the way out, not
  taken.

  The SQLSTATE messages moved with the declaration:
  `kernel.NumericTypeName` renders `integer` and `bigint` for these types,
  so `port_col = 'abc'` is `invalid input syntax for type integer: "abc"` —
  the name a client can look up in `pg_type`, where it used to say `port`,
  a type name nothing resolves. `pgFormatType` follows for the synthetic
  `pg_attribute` rows, because an introspecting client reads BOTH and a
  catalog saying `text` beside a RowDescription saying int4 is the same
  contradiction one layer down.

  Gated by the wire arm over `net_probe`, whose PostgreSQL twin declares
  `integer` / `integer` / `bigint`: the OIDs, the sizes, the text, the
  binary bytes, and an integer bound parameter against a PORT column in
  both the declared and the inferred spellings.
