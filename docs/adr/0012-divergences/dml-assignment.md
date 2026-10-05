# ADR-0012 divergences: DML, DDL and assignment

Assignment rounding and typing on every write door, DDL and CTAS result forms, and the DML and DDL statements that are refused. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**An integer assignment rounds by the source's PostgreSQL category** (E93)
The position is ADR-0024 §2c: a float-carried numeric (`5 / 2.0`, `SQRT(6.25)`, EXTRACT) keeps its FLOAT64 carrier and OID 701 and declares PostgreSQL's category beside it, and every write door (VALUES, INSERT … SELECT, UPDATE, MERGE) rounds an integer target by that category: half away from zero from a numeric, half to even from a float8, so `5 / 2.0` stores 3. A float4/float8 parameter binds as its own type (OID 700/701), as in PostgreSQL. An explicit `CAST` does not see the category, because the cast kernel sees only its compiled operand and a DAG stage boundary carries no category (#1392).

**The writes PostgreSQL refuses are refused before a row is written** (E93)
A MERGE action's expression forms are one table measured on 17.11 (`intround.MergeSetCells`): an aggregate is 42803 and a window function 42P20 on every door that evaluates one row, a target-correlated subquery under a reached WHEN NOT MATCHED clause is 42P01, an `ON` key naming a name a subquery source publishes twice is 42702, arithmetic between a bare text column and a number is 42883 (in a SELECT too), and a JSON field read (`j->>'k'`, `j->'k'`) into a non-text column is 42804 — the assignment half of "no text-to-integer superset" (arc VL). Six recorded exceptions remain, each with its page entry: the explicit cast (#1392), the JSON cast (#1406), the text parameter (#1408), text expressions (#1409), the unreached WHEN NOT MATCHED clause (#1043) and SMALLINT storage (#1407).

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT 5 / 2.0` | 2.5000000000000000, declared numeric (OID 1700) | 2.5, declared double precision (OID 701) (measured); INSERT INTO t VALUES (1, 5 / 2.0) stores 3 on both (measured) | — | value divergence | 2026-09-28 · [E93](#e93), P024 | #1353 | — |
| **r2** `SELECT CAST(s.x AS INTEGER) FROM (SELECT DISTINCT 5 / 2.0 + t.id * 0 AS x FROM ss_t t) s` | 3 | 2 (measured 2026-10-02 on every arm, roundOrigin/distinct; the same at c39858f3): a column a previous operator materialized is a float64 with no PostgreSQL category in the batch, so the cast rounds it half to even; the integer ASSIGNMENT of the same column rounds 3 (roundOrigin/insertDistinct). NARROWED 2026-10-02 (#1392): a cast whose operand computes the float-carried numeric itself rounds half away — `CAST(5 / 2.0 AS INTEGER)`, `CAST(SQRT(6.25) AS INTEGER)`, `CAST(POWER(2.5, 1) AS INTEGER)` are 3, negated -3, SMALLINT and BIGINT alike (2 and -2 at v0.25.3, issue/1392/*) | — | value divergence | 2026-09-28 · [E93](#e93), P015 | #1353, #1392 | `coordinator.TestArcNXNumericCarrierEveryArm` (kept roundOrigin/*) |
| **r3** `SELECT CAST(CAST('{"k": 2.5}' AS JSON)->>'k' AS INTEGER)` | ERROR 22P02 invalid input syntax for type integer: "2.5" | 2 (measured): the cast reads the JSON number; assigning j->>'k' to an integer column is 42804 on both (measured) | — | documented gap | 2026-09-28 · [E93](#e93), P016 | #1353, #1406 | — |
| **r4** `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = $1 -- $1 bound as text '2.5'` | ERROR 42804 (text is not assignable to integer without a cast) | ERROR 22P02: Bind renders the parameter as a quoted literal the column's input function reads; neither writes | 22P02 | value divergence | 2026-09-28 · [E93](#e93), P034 | #1353, #1408 | — |
| **r5** `SELECT UPPER(x) * 2 FROM s` | ERROR 42883 operator does not exist: text * integer | 24 over '12' (measured); -x answers -12; a bare text column, x * 2, is 42883 on both (measured) | — | documented gap | 2026-09-28 · [E93](#e93), P035 | #1353, #1409 | — |
| **r6** `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = UPPER(s.x) * 1` | ERROR 42883 | MERGE 2; n stores 12 (measured): every write door stores the evaluated number | — | documented gap | 2026-09-28 · [E93](#e93), P035 | #1353, #1409 | — |
| **r7** `MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, (SELECT s2.i FROM s s2 WHERE s2.id = t.id))` | ERROR 42P01 at parse: the target is not visible under WHEN NOT MATCHED | MERGE 0 when every source row matches (measured); 42P01 when a row reaches the clause | — | documented gap | 2026-09-28 · [E93](#e93), P036 | #1043 | — |
| **r8** `CREATE TABLE sm (n SMALLINT)` | creates an int2 column | ERROR 42704 type "SMALLINT" does not exist (measured): no int16 storage | 42704 | documented gap | 2026-09-28 · [E93](#e93), P104 | #1353, #1407 | — |
| **r9** `SELECT CAST(3 AS SMALLINT)` | 3, declared smallint | 3, declared bigint (measured) | — | value divergence | 2026-09-28 · [E93](#e93), P104 | #1407 | — |
| **r10** `CREATE TABLE t (id INT, n INT)` | command tag CREATE TABLE, no rows | one row, result = Table "t" created (measured) | — | value divergence | — · P011 | #1024 | — |
| **r11** `CREATE TABLE t (v NUMERIC); INSERT INTO t VALUES (0.00000000005), (12345678901234567890.123456789012)`; likewise `DECIMAL` with no precision and a CREATE TABLE AS column whose source is plain numeric (`CAST(b AS NUMERIC)`, `COALESCE(a, b)` over two scales, `SUM(n)`, a literal, a typed NULL) | unconstrained numeric column (typmod −1); stores 0.00000000005 and 12345678901234567890.123456789012 | DECIMAL(38, max(s, 10)) marked unconstrained (ADR-0024 §10): typmod −1, information_schema precision / scale NULL, values print without trailing zeros; a write past 10 fraction digits rounds to 10 (0.0000000001, 12345678901234567890.123456789), past 28 integer digits is 22003 (numeric-decimal r23); a source with more fraction digits keeps them (`n / m` is DECIMAL(38,15)). 8e681724 declared DDL `NUMERIC` DECIMAL(38,0) and stored 1.25 as 1, CTAS `CAST(b AS NUMERIC)` (38,0), a literal (3,2), `COALESCE` over numeric(10,2) and numeric(12,4) (12,4), a typed NULL double precision | 22003 | value divergence | 2026-10-05 · P031 | #1024, #1541 | `wadjet.TestArcUNUnconstrainedColumnEnumeration` |
| **r12** `CREATE TABLE IF NOT EXISTS ctas_t AS SELECT 1 AS c` | NOTICE relation "ctas_t" already exists, skipping; tag CREATE TABLE AS | tag CREATE TABLE AS, no notice (measured) | — | value divergence | — · P080 | #1024 | — |
| **r13** `INSERT INTO tt (s) SELECT DECODE('6869','hex')` | stores the bytea's text \x6869 | stores \x6869, as PostgreSQL does (amended 2026-10-05, arc BY round 2: a BYTES value is assigned to a text column as bytea's hex output; ERROR 42804 at c67ebf5b); a container or DURATION is ERROR 42804 column "s" is of type STRING (measured) | 42804 | refusal | — · P077 | #1024 | — |
| **r14** `INSERT INTO qa (a) SELECT '{1,2}'` | stores {1,2} through the array input function | ERROR 42804 column "a" is ARRAY and a string value is not an ARRAY (measured); BOOL and BYTES targets read the text (measured) | 42804 | refusal | — · P078 | #1088 | — |
| **r15** `INSERT INTO t VALUES (3, (SELECT max(n) FROM t))` | inserts the subquery's value | ERROR 0A000 a subquery in an INSERT ... VALUES expression is not supported (measured); MERGE's INSERT VALUES runs one | 0A000 | refusal | — · P108 | #1252 | — |
| **r16** `UPDATE t SET n = (SELECT max(i) FROM s)` | updates every row to the subquery's value | ERROR 0A000 SET n: a subquery in an UPDATE's SET list is not supported (measured); a MERGE SET answers one | 0A000 | refusal | — · P147 | — | — |
| **r17** `CREATE VIEW v AS SELECT id FROM t` | creates the view | ERROR 0A000 CREATE VIEW is not supported (measured) | 0A000 | documented gap | — · P144 | — | — |
| **r18** `DROP VIEW v` | drops the view | ERROR 0A000 DROP VIEW is not supported (measured) | 0A000 | documented gap | — · P146 | — | — |
| **r19** `ALTER TABLE t ADD COLUMN z INT` | adds the column | ERROR 0A000 ALTER TABLE is not supported (measured) | 0A000 | documented gap | — · P145 | — | — |
| **r20** `INSERT INTO t VALUES (4, 4) RETURNING id` | returns the inserted row | ERROR 0A000 RETURNING is not supported (measured); UPDATE, DELETE and MERGE likewise | 0A000 | documented gap | — · P148 | — | — |
| **r21** `MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE` | deletes the unmatched target rows | ERROR 0A000 MERGE: WHEN NOT MATCHED BY SOURCE is not supported (measured); BY TARGET likewise | 0A000 | documented gap | — · P149 | — | — |
| **r22** `MERGE INTO t USING s ON t.id < s.id WHEN MATCHED THEN UPDATE SET n = 0` | joins on the condition and updates | ERROR 0A000 MERGE ON supports only equality between the target and the source (measured) | 0A000 | refusal | — · P150 | — | — |
| **r23** `CREATE TABLE c AS SELECT id, CASE WHEN 1 = 1 THEN k ELSE n END AS x FROM t -- k numeric(5), n numeric(10,2); INSERT INTO c (id, x) VALUES (99, 1.255)`; likewise `CASE WHEN NULL THEN m ELSE n END`, `CASE 1 WHEN 1 THEN n ELSE m END`, `CASE WHEN id > 1 THEN n WHEN 1 = 1 THEN n ELSE m END` (m numeric(12,4)), `COALESCE(CAST(NULL AS NUMERIC(10,2)), k)`, and `+n` | numeric(5) — the planner folds the constant CASE to the arm it selects before the column is created — and stores 1; numeric(10,2) for the others; plain numeric for `+n` (1.255) | the column keeps the declaration the plan computes the expression at — the CASE's common type, numeric(10,2) (1.26), numeric(12,4) for the m pairs (1.2550) — as c23adbbb created it: the typmod PostgreSQL keeps after folding is not the (p,s) the plan carries, so the one rule (ADR-0024 §10, `physical.Planner.CreatedColumns`) answers "unknown" and leaves the plan's declaration; the parser drops a unary plus, so `+n` is the column n (numeric(10,2), 1.26). WITH NO DATA PostgreSQL does not fold, and both create plain numeric (measured 2026-10-05, round 3) | — | value divergence | 2026-10-05 · ADR-0024 §10 | #1541 | `wadjet.TestArcUNCreatedColumnKeepsPostgresTypmod` (=c23adbbb rows) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E93

ADR lines 4311-4351. Catalog rows: r1, r2, r3, r4, r5, r6, r7, r8, r9. Stated in [Mechanisms](#mechanisms).

- **An integer assignment rounds by the source's PostgreSQL type; the
  writes PostgreSQL refuses are refused before a row is written, with
  six recorded exceptions.** (Added 2026-09-28, arc IR, #1353.) The
  position is ADR-0024 §2c: a float-carried numeric (`5 / 2.0`,
  `SQRT(6.25)`, EXTRACT) keeps its FLOAT64 carrier and OID 701 and
  declares PostgreSQL's category beside it, and every write door —
  VALUES, INSERT … SELECT, UPDATE, MERGE — rounds an integer target by
  that category (half away from zero from a numeric, half to even from a
  float8; `5 / 2.0` stores 3). A MERGE action's expression forms are one
  table measured on PostgreSQL 17.11 (`intround.MergeSetCells`): an
  aggregate is 42803 and a window function 42P20 on every door that
  evaluates one row, a target-correlated subquery under a WHEN NOT
  MATCHED clause a row reaches is 42P01, an `ON` key naming a name a
  subquery source publishes twice is 42702, arithmetic between a text
  COLUMN and a number is 42883 (`SELECT x * 2` too), and a JSON field
  read (`j->>'k'`, `j->'k'`) into a non-text column is 42804 — the
  assignment half of this section's "no text→integer superset" (arc VL).
  A float4/float8 parameter binds as its own type (OID 700/701), as in
  PostgreSQL, where v0.25.1 read it as a numeric literal. Recorded, each
  with its differences-page entry:
  - `#1353-cast` — an explicit `CAST(<column> AS INTEGER)` over a column
    a previous operator materialized from a float-carried numeric (a
    derived table, DISTINCT, an aggregate, a CTE, a set operation,
    VALUES, a window, a join) rounds the double half to even (2 where
    PostgreSQL answers 3; on the stage DAG a derived table's or a CTE's
    such column reads NULL, recorded separately): the cast reads its operand's category from the
    operand's own expression over the input batch's columns, and a
    materialized float64 carries none (ADR-0024 §2c). A cast whose
    operand computes the value itself — `CAST(5 / 2.0 AS INTEGER)` —
    rounds half away, as PostgreSQL does, on every arm (#1392).
  - `#1353-json` — `CAST(j->>'k' AS INTEGER)` over `{"k": 2.5}` answers 2
    where PostgreSQL raises 22P02: the cast reads the JSON number, not
    `->>`'s text (#1406).
  - `#1353-param` — a text-typed (OID 25) parameter assigned to an
    integer column is 22P02 where PostgreSQL raises 42804: Bind renders
    it as a quoted literal, which the column's input function reads.
    Neither writes (#1408).
  - `#1353-text-expr` — arithmetic over a text EXPRESSION (`UPPER(x) *
    2`, `-x`, `CAST(n AS TEXT) * 2`, `(x || '') * 1`) is evaluated as its
    number, and every write stores it, where PostgreSQL raises 42883: the
    42883 rule reads a bare text column only (#1409).
  - `#1043` — a WHEN NOT MATCHED clause no row reaches is not resolved,
    so its target-correlated subquery answers `MERGE 0` where PostgreSQL
    raises 42P01 at parse; reached, it is 42P01 here too.
  - `#1353-int2` — `SMALLINT` / `INT2` columns are 42704: there is no
    int16 storage; `CAST(x AS SMALLINT)` answers, declared bigint (#1407).
