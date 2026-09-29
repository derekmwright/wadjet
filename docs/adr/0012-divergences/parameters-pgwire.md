# ADR-0012 divergences: Parameters and the wire

The wire protocol: multi-statement strings without transactions, PORT/PROTOCOL/DURATION wire types, and syntax errors without a POSITION field. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**A multi-statement string is a sequence, not a transaction** (E11, P008)
The engine has no transactions: BEGIN and COMMIT are accepted and ignored on every door. A simple-query string carrying several statements runs as a sequence with PostgreSQL's command tags and its stop-at-the-first-error rule, and a syntax error anywhere runs none of them; the statements that completed before a failure stay. This changes only if transactions are implemented, not through the statement splitter. Gated per door in the DML census and `internal/server/pgwire/multi_statement_test.go`.

**PORT and PROTOCOL declare int4 on the wire, DURATION int8 nanoseconds** (E37)
The three types are compared numerically, so they declare numeric OIDs (#834); their text form is unchanged (`443`, `6`, `1500000000`), `appendBinaryValue`'s int32/int64 arms write the bytes the OIDs promise, and `numericOID` in bindparams binds `WHERE port_col = $1` as a number. `kernel.NumericTypeName` renders `integer`/`bigint` in messages (`port_col = 'abc'` is 22P02 for type integer) and `pgFormatType` declares the same in the synthetic `pg_attribute`. DURATION as `interval` (OID 1186, microseconds) is the recorded alternative; it needs a unit decision first.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `INSERT INTO mt VALUES (1); SELECT 1/0` | ERROR 22012; the implicit transaction rolls the INSERT back | INSERT 0 1 then ERROR 22012; the inserted row stays (measured) | 22012 | value divergence | 2026-09-03 · [E11](#e11), P008 | #711 | `internal/server/pgwire/multi_statement_test.go` |
| **r2** `BEGIN; INSERT INTO mt VALUES (2); ROLLBACK` | the row is gone | BEGIN and ROLLBACK are accepted and ignored; the row stays (measured) | — | documented gap | 2026-09-03 · [E11](#e11), P008 | #711 | — |
| **r3** `SELECT p, pr, d FROM np` | no PORT, PROTOCOL or DURATION type (the twin table declares integer, integer, bigint) | declares int4, int4, int8; 443, 6, 1500000000 (DURATION is nanoseconds, not interval; measured) | — | kept superset | 2026-09-03 · [E37](#e37) | #834 | — |
| **r4** `SELECT 1 +` | ERROR 42601 syntax error at end of input, with a POSITION field (psql LINE 1 and caret) | ERROR 42601 same sentence; no POSITION field (measured) | 42601 | documented gap | — · P106 | #1307 | — |

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
