# ADR-0012 divergences: Access control, catalog and other

Casts to unknown or unconverted types, array casts and comparisons, reserved column names, catalog contents and access-filtered metadata. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**Metadata visibility follows table access** (E03, P049)
One decision, `auth.VisibleTables` / `auth.TableAccess`, implements it on every door: a relation an identity may not read is absent from SHOW TABLES and the listing endpoints, and DESCRIBE on it is 42501. A table's name is itself treated as sensitive; no PostgreSQL client behaviour depends on seeing a relation it cannot select. ADR-0034 carries the door table.

**A broken authentication configuration is an error, not a mode** (E04, E05)
`auth.enabled: true` with no usable credential mechanism exits at startup, and a reload with a broken auth configuration is refused with the running configuration kept (#931); PostgreSQL starts with whatever pg_hba.conf says. A policy condition attribute with no namespace is a load error naming the line (#930), because an attribute nothing populates never matches and beside a broad allow acts as a grant.

**A CAST names a type or refuses with 42704** (E58, P047, P125)
`expr.KnownCastDest` is the accept-set: every name `parquet.ParseTypeID` takes as a column type plus the PostgreSQL spellings `Cast.Eval` implements. A name outside it is 42704 on the CAST and CREATE TABLE doors; bytea, money and inet are refused there because the engine has no type for them and would render them differently. time, json and xml pass their text through under a text declaration. A DURATION destination still returns the operand unchanged, and a BYTES one reads a text operand through byteain (#1501); measured at the tip, VECTOR, container and network destinations now convert or refuse (22P02) rather than pass through, so the ADR's pass-through list is stale for them.

**The `__` column namespace is reserved** (E65, P076)
The planner materializes hidden columns (`__win_N`, `__sortkey_N`, `__gb_expr_N`, `__key_N`, the rest of `plansql.reservedSlotPrefixes`) and reads them by name, so a query that mints such a name (an alias, a derived table's or CTE's column list, DDL and ingest) is 42939. Reading a stored column of such a name is not minting: it stays readable and the planner renumbers its own slot around it.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `DESCRIBE classified_events` | listed in pg_catalog and information_schema and described to any connected role | ERROR 42501 for an identity that may not read it; omitted from SHOW TABLES and table listings | 42501 | refusal | 2026-09-06 · [E03](#e03), P049 | — | — |
| **r2** `SELECT CAST('abc' AS bytea)` | \x616263 (money and inet likewise answer: $1.50, 192.168.1.1/32) | ERROR 42704 type "bytea" does not exist; money and inet the same (measured) | 42704 | refusal | 2026-09-04 · [E58](#e58), P125 | #652 | `wadjet.TestUnknownCastDestinationIsUndefinedObject` |
| **r3** `SELECT CAST('12:34:56' AS time)` | 12:34:56 declared time | 12:34:56 declared text (OID 25); json and xml likewise pass their text through (measured) | — | value divergence | 2026-09-04 · [E58](#e58), P028 | #652 | `wadjet.TestUnknownCastDestinationIsUndefinedObject` |
| **r4** `SELECT CAST('abc' AS DURATION)` | no DURATION type | abc, the operand unchanged (measured); CAST('abc' AS BYTES) left this row 2026-10-05: it is byteain's bytes, `\x616263`, as `'abc'::bytea` is on PostgreSQL (#1501) | — | kept superset | 2026-09-04 · [E58](#e58), P047 | #652 | `wadjet.TestUnknownCastDestinationIsUndefinedObject` |
| **r5** `SELECT CAST(ARRAY[1,2] AS VECTOR(2))` | [1,2] declared vector (pgvector's cast) | [1,2] declared text (measured; a VECTOR column also declares text) | — | value divergence | — · P028, [E58](#e58) | — | — |
| **r6** `SELECT ARRAY[1.5] > ARRAY[1]` | ERROR 42883 (no numeric[] > integer[] operator) | true (compared by the numbers; measured) | — | kept superset | — · P028 | — | — |
| **r7** `SELECT CAST(ARRAY[1,2] AS JSON)` | ERROR 42846 | [1,2], to_json's text; json_array_length reads 2 (measured); other non-text container destinations 42846 | — | kept superset | — · P028 | — | — |
| **r8** `SELECT CASE WHEN true THEN ARRAY[1] ELSE ARRAY['a'] END` | ERROR 42804 (COALESCE 42804; ARRAY[1] = ARRAY['a'] 42883) | {1} as integer[]; COALESCE {1}; = answers false (measured); recorded for repair | — | documented gap | — · P028 | — | — |
| **r9** `SELECT ARRAY[1] UNION SELECT ARRAY['a']` | ERROR 42804 | ERROR 42000 cannot store string into INT32 vector (measured); 22P02 for a numeric array | 42000 | documented gap | — · P028 | — | — |
| **r10** `SELECT CAST(ARRAY[INTERVAL '1 hour'] AS TEXT)` | {01:00:00} | ERROR 0A000 a container element of type interval has no text form here (measured) | 0A000 | refusal | — · P028 | — | — |
| **r11** `SELECT ARRAY[INTERVAL '1 hour']` | {01:00:00} (interval[]) | {3600000000000} (DURATION nanoseconds; compares, groups and orders by value; measured) | — | value divergence | — · P028 | #351 | — |
| **r12** `SELECT amount AS __key_0 FROM rs` | 1 | ERROR 42939 reserved column namespace (measured); a stored column of such a name stays readable | 42939 | refusal | 2026-09-07 · [E65](#e65), P076 | #956 | — |
| **r13** `SELECT datname FROM pg_database` | postgres, template0, template1 and user databases | one row, wadjet (measured); pg_roles one non-superuser role | — | documented gap | — · P105 | #1251 | — |
| **r14** `SELECT CAST('rs' AS regclass)` | rs | the OID, 1314170258 (measured) | — | value divergence | — · P105 | #1251 | — |
| **r15** `SELECT current_schemas(false)` | declared name[] | declared text[] (measured); OID columns declare int8 | — | value divergence | — · P105 | #1251 | — |
| **r16** `SELECT json_build_object('o', x) FROM (SELECT json_build_object('a', 1) AS x) s` | {"o" : {"a" : 1}}, declared json (OID 114) | {"o" : "{\"a\" : 1}"} (measured): json_build_object declares text (OID 25), so a json value that reaches it declared text — a derived table's, a CTE's or a stored column, a scalar subquery over a table, a COALESCE with a text arm — is written as a JSON string; a FROM-less scalar subquery over its own `json_build_object` nests as an object; one its own argument expression produces (json_build_object, a CAST to json, a COALESCE or CASE of those) nests as an object. Being text, the object is also compared, grouped and md5'd where PostgreSQL raises 42883 for json | — | value divergence | 2026-10-02 · [RN](../0012-amendments.md#2026-10-02-the-scalar-renderers-write-a-value-as-its-declared-types-text-arc-rn-1474-1466-1467-1481) | #1470 | `pgwire.TestArcRNRenderersOnTheWire`, `coordinator.TestArcRNRendererTableEveryArm` jx/* |
| **r17** `SELECT count(*) FROM t TABLESAMPLE BERNOULLI (100) REPEATABLE (1)` | every row; the seed is any float8 (`REPEATABLE (NULL)` 2202G, `(1e400)` 22003) | ERROR 42601 syntax error: REPEATABLE is not parsed (measured) | 42601 | refusal | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` |
| **r18** `SELECT count(*) FROM t TABLESAMPLE BERNOULLI ((SELECT 50))` | a 50 % sample (the subquery is an InitPlan) | ERROR 0A000: the argument is evaluated at plan time, before any row exists (measured) | 0A000 | refusal | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` |
| **r19** `SELECT count(*) FROM t s TABLESAMPLE BERNOULLI (100)` (and `… FROM t TABLESAMPLE BERNOULLI (100) s`) | every row; the alias after the clause is ERROR 42601 | ERROR 42601 for the alias before the clause; every row for the alias after it (measured): this parser reads the alias after the TABLESAMPLE clause | 42601 | refusal | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` alias/\* |
| **r20** `SELECT count(*) FROM t TABLESAMPLE SYSTEM (50)` | heap pages sampled | blocks of 2048 rows sampled, as the scan reads them, each kept or dropped whole — on a cluster each worker's scan task samples its own files the same way; 0 and 100 are exact on both (measured) | — | value divergence | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` big/system\_\*, `exec.TestSampledSourceSystemSamplesBlocks` |
| **r21** `SELECT count(*) FROM e JOIN t TABLESAMPLE BERNOULLI (101) ON e.id = t.id` (e empty) and `SELECT count(*) FROM t WHERE id < 0 AND EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (101))` | the join 0 (its Nested Loop reads e first and never begins the sample; `FROM t TABLESAMPLE BERNOULLI (101) JOIN e …` is 2202H — the answer follows PostgreSQL's join order); the EXISTS 2202H (an InitPlan in a one-time filter) | the join ERROR 2202H in either order on the embedded engine (a coordinator arm raises 2202H or #1190's refusal of the empty relation); the EXISTS 0 on the embedded engine (the conjunction is evaluated per row and no row reaches the subquery), 2202H on a cluster (a conjunct that reads no row is evaluated once at plan time, as PostgreSQL's one-time filter is); the same over an empty table for every conjunct PostgreSQL evaluates that way — `WHERE EXISTS (…)`, `NOT EXISTS`, `NOT (…)`, `CASE WHEN EXISTS (…) THEN …`, `(EXISTS (…)) = true`, `HAVING EXISTS (…)`, `JOIN … ON … AND EXISTS (…)`: 0 on the embedded engine, the subquery's error on a cluster (measured). The semi-join spelling is the same class: `SELECT count(*) FROM e WHERE id IN (SELECT id FROM big TABLESAMPLE BERNOULLI (101))` and its `= ANY` form over the empty `e` answer 0 on PostgreSQL, which builds the set on the first probe, and raise 2202H on the embedded engine, which builds the set before it reads an outer row (measured) | 2202H | value divergence | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` empty\_join/\*, exists\_coded/id\_negative\_and\_exists; `coordinator.TestArcTBDeferredSubqueryFailureOnEveryArm` (the r21 pins) |
| **r22** `SELECT count(*) FROM t WHERE CASE WHEN id > 5 THEN EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (1e400)) ELSE true END` (t holds ids 1–3) | ERROR 22003: the subquery is planned with the statement and its argument's coercion to real fails there, though no row reaches the arm (under `CASE WHEN false` the arm is folded away before planning and the statement answers 3) | 3 on every arm (measured): a subquery's failure — its planning's as well as its run's — is raised when a row evaluates it, so an arm no row reaches answers; over ids 1–7, where rows reach the arm, 22003 as on PostgreSQL. The same for a SELECT-list `CASE WHEN id > 5 THEN EXISTS (…1e400…) ELSE false END`, and over an empty table for a conjunct that reads a row (`WHERE id = 1 OR EXISTS (…1e400…)`, `WHERE id IN (1, (SELECT … 1e400 …))`: 0 on the embedded engine and the asynchronous door, #1190's refusal on the DAG arms). The same for a subquery PostgreSQL fails while it plans the statement by folding a constant: `WHERE CASE WHEN id > 5 THEN EXISTS (SELECT 1 FROM t WHERE id = CAST('x' AS INT)) ELSE true END` and `WHERE id < 5 OR EXISTS (SELECT 1 FROM t WHERE id = CAST('x' AS INT))` over ids 1–3 are 22P02 on PostgreSQL and answer 3 on every arm (measured). And for a WHERE CASE over a join input: `SELECT count(*) FROM t JOIN t7 ON t.id = t7.id WHERE CASE WHEN t7.id > 5 THEN EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (101)) ELSE true END` (t7 holds ids 1–7) is 2202H on PostgreSQL, which pushes the condition to t7's scan where ids 6 and 7 reach the arm, and answers 3 on every arm here, where a condition holding a subquery is evaluated over the joined rows (measured); the unsampled `EXISTS (SELECT 1 FROM t WHERE 1/(id-id) > 0)` likewise (PostgreSQL 22012). Each answer is the value PostgreSQL's statement has without the raise | PG 22003 / 22P02 / 2202H / 22012 | kept superset | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBDeferredSubqueryFailureOnEveryArm` case\_col/E/22003/\*, select\_case\_col/E/22003/\* |
| **r23** `SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM big TABLESAMPLE BERNOULLI (101)) AND 1/0 = 1` | ERROR 22012: the constant `1/0` is folded, and raises, when the statement is planned, before any subquery runs | ERROR 2202H on every arm (measured): `1/0 = 1` is evaluated as a conjunct, after the sample's range check; both statements are refused, with a different SQLSTATE | 2202H | refusal | 2026-10-03 · [TB](../0012-amendments.md#2026-10-03-the-tablesample-argument-arc-tb-1411) | #1411 | `coordinator.TestArcTBTablesampleArgumentOnEveryArm` exists\_coded/exists\_and\_division\_by\_zero |
| **r24** `WITH s AS (SELECT id, random() AS r FROM tb_big WHERE id <= 50) SELECT count(*) FROM (SELECT id, r FROM s EXCEPT SELECT id, r FROM s) u` on a cluster (the stage DAG) | 0: the CTE is materialized once and both arms read it (`CTE Scan` per reference) | 50 on the three DAG arms (measured at e8ece057): the stage two consumers share is the body's SCAN and the volatile projection is computed in each consumer; likewise `(SELECT r FROM s) <> (SELECT r FROM s)` over `sum(random())` (3 rows) and an IN / NOT IN set or a CASE-wrapped subquery, which the coordinator executes at plan time apart from the FROM reference (`r NOT IN (SELECT r FROM s)` over `sum(random())`: 1). The embedded engine and the asynchronous door answer 0, and so does a statement the coordinator runs on its local pipeline (ADR-0021 §2d). A TABLESAMPLE body agrees where the sample is drawn in the shared scan stage | — | value divergence | 2026-10-05 · [CM](../0012-amendments.md#2026-10-05-a-volatile-cte-is-evaluated-once-arc-cm-1531) | #1531 | `coordinator.TestArcCMVolatileCTEReadTwiceIsEvaluatedOnce` (pins: ref2\_except\_arms, ref2\_union\_arms, ref2\_aggregate, ref2\_scalar\_where, issue/c1, ref2\_not\_in/aggrnd, join\_on\_two, exists\_two, distinct\_on\_union\_all, spill\_big\_scalar, toplevel\_except) |
| **r25** `WITH s AS MATERIALIZED (SELECT id, random() AS r FROM tb_big WHERE id <= 50) SELECT count(*) > 0 FROM s` (and `AS NOT MATERIALIZED`) | true; MATERIALIZED forces a `CTE Scan`, NOT MATERIALIZED inlines a non-volatile body — so `NOT MATERIALIZED` over a TABLESAMPLE body read twice makes PostgreSQL's own two references disagree (`(SELECT count(*) FROM s) - (SELECT count(*) FROM s)`: -166, 61, -177 in three runs; ADR-0013's legal nondeterminism) | ERROR 42601 on every arm (measured): the keywords are not parsed; a volatile body read more than once is evaluated once without them (ADR-0021 §2d) | 42601 | documented gap | 2026-10-05 · [CM](../0012-amendments.md#2026-10-05-a-volatile-cte-is-evaluated-once-arc-cm-1531) | — | `coordinator.TestArcCMVolatileCTEReadTwiceIsEvaluatedOnce` ref1\_from/matrnd, ref1\_from/nmatrnd |
| **r26** `WITH s AS (SELECT id, random() AS r FROM tb_big WHERE id <= 50) INSERT INTO cm_t SELECT id, CAST(r AS TEXT) FROM s UNION ALL SELECT id, CAST(r AS TEXT) FROM s` | inserts 100 rows, one r per id | ERROR 42601 on every arm (measured): a WITH list before INSERT is not parsed; `INSERT INTO cm_t WITH s AS (…) SELECT …` is, and stores one evaluation on the embedded engine | 42601 | documented gap | 2026-10-05 · [CM](../0012-amendments.md#2026-10-05-a-volatile-cte-is-evaluated-once-arc-cm-1531) | — | `coordinator.TestArcCMVolatileCTEReadTwiceIsEvaluatedOnce` dml\_withfirst/rnd |
| **r27** `WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50` (a CTE read ONCE, from a correlated subquery); and `SELECT count(DISTINCT (WITH s AS (SELECT random() r) SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50` (a WITH declared inside one) | 1: the CTE is materialized once and its tuplestore is kept across the subquery's rescans | 50 on the embedded arms (measured at c67ebf5b and at the round-3 tip): a CTE the statement reads once is inlined (ADR-0021 §2d), and the correlated subquery re-runs its body per outer row; a WITH inside the subquery's text is re-parsed per execution | — | value divergence | 2026-10-05 · [CM](../0012-amendments.md#2026-10-05-a-volatile-cte-read-more-than-once-is-filled-on-demand-arc-cm-round-3-1531) | — | `wadjet.TestArcCMCorrelatedReaderIsEvaluatedPerOuterRow` (pins R1, R3) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E03

ADR lines 372-384. Catalog rows: r1. Stated in [Mechanisms](#mechanisms).

- **Metadata visibility follows the effective table-access decision, where
  PostgreSQL shows it to anyone.** (Added 2026-09-06, ADR-0034.) PostgreSQL
  lets any role that can connect read `pg_catalog` and `information_schema`
  and run `\d`, whether or not it can SELECT the relation. Wadjet filters:
  a relation an identity may not read is not listed by `SHOW TABLES` or the
  table-listing endpoints, and DESCRIBE on it refuses with the same 42501
  the read would. The reasoning is the deployments this engine is built for
  — a table's NAME is itself sensitive there, `classified_events` names an
  operation whether or not its rows can be read — and no PostgreSQL client
  behaviour depends on seeing a relation it cannot select from. One
  decision implements it on every door, `auth.VisibleTables` /
  `auth.TableAccess`; see ADR-0034 for the full door table.

### E04

ADR lines 385-393. Stated in [Mechanisms](#mechanisms).

- **`auth.enabled: true` with no usable credential mechanism refuses
  startup, and a broken auth configuration refuses a hot reload.** (Added
  2026-09-06, #931, ADR-0034.) PostgreSQL starts with whatever
  `pg_hba.conf` says, `trust` included, and a bad reload leaves the old
  rules in place silently. Wadjet treats "the operator asked for
  authentication and it cannot be performed" as a configuration ERROR:
  startup exits with it, and a reload is refused with the running
  configuration kept. It is not a mode.

### E05

ADR lines 394-400. Stated in [Mechanisms](#mechanisms).

- **A policy condition attribute with no namespace is a load error.**
  (Added 2026-09-06, #930, ADR-0034.) PostgreSQL has no equivalent surface;
  this is Wadjet's own configuration. `attribute: role` used to be filed
  under subjects "by default", which read an attribute nothing populates —
  a rule that silently never matches, and beside a broad allow that is a
  grant. It refuses at load and names the line.

### E58

ADR lines 2373-2423. Catalog rows: r2, r3, r4, r5. Stated in [Mechanisms](#mechanisms).

- **A CAST to a type name this engine does not have is 42704, on both
  doors — including PostgreSQL type names it has no type for.** (Added
  2026-09-04, from #652.)

  `CAST(1 AS bogustype)` answered the string "1" under OID 25 and
  `CAST(<float column> AS bogustype)` answered its digits the same way:
  `expr.Cast.Eval`'s switch fell to `default: return v` and
  `physical.inferCastType`'s to `default: return TypeString`, so the two
  layers agreed with EACH OTHER about a column PostgreSQL 17.11 says cannot
  be described at all (`type "bogustype" does not exist`). That is item 9's
  class reached through the type system: a measurement published as text.

  `expr.KnownCastDest` is the accept-set, and it is the UNION of the two
  doors — every name `parquet.ParseTypeID` takes as a column type, plus the
  PostgreSQL spellings `Cast.Eval` implements that no column type answers
  to (`int4`, `smallint`, `real`, `double precision`, `signed`,
  `timestamptz`, …). One type name, one disposition, which is the property
  the length-modifier entry above already states.

  **What diverges**: three PostgreSQL type names this engine has no type
  for AND renders differently — `bytea`, `money`, `inet` — are refused where
  the server answers. The `CREATE TABLE` door has refused those all along
  (with no SQLSTATE at all until this change; it carries 42704 now, from
  `parquet.ParseTypeID`), and the alternative on the CAST door was a value
  under a `text` declaration that is not the server's: `abc` for
  `\x616263`, `1.5` for `$1.50`, `192.168.1.1` for `192.168.1.1/32`. Loud
  beats plausible THERE, and only there.

  **What does not**: `time`, `json` and `xml`. The first cut of this refused
  them too, and their text is what the server answers byte for byte —
  `CAST('12:34:56' AS time)` is `12:34:56` on both engines. Turning a right
  answer loud is the direction this list does not permit, and
  `expr.TestUnknownCastTypeIsRefusedAndKnownOnesStillAnswer` had been left
  in the tree saying so; it was passing only because it called `Cast.Eval`
  while the refusal had moved to the compile (round-1 review, B4). They are
  accepted, pass their text through, and are declared `text` — a
  DECLARATION divergence over a right value, the same class as every other
  pass-through destination.

  **What stays a superset**: a destination this engine HAS but does not
  CONVERT still returns its operand unchanged — the network types,
  `DURATION`, `BYTES`, `VECTOR(n)`, the containers, and non-address text
  cast to `IPV4`/`IPV6`/`CIDR`/`MACADDR`. This pass refuses names that name
  NOTHING, not casts that are unimplemented, and the boundary is asserted
  in both directions.

  Gated by `wadjet.TestUnknownCastDestinationIsUndefinedObject` (the
  refusals, 24 destination controls, and the unimplemented-but-named
  pass-throughs) and two `runWireErrors` entries. User-facing:
  `docs/sql-reference.md` §Casts and errors.

### E65

ADR lines 2683-2697. Catalog rows: r12. Stated in [Mechanisms](#mechanisms).

- **The `__`-prefixed column namespace is RESERVED, where PostgreSQL has
  no such namespace.** (Added 2026-09-07, arc J1 / #956.) The planner
  materializes its own values into hidden columns — `__win_N`,
  `__sortkey_N`, `__gb_expr_N`, `__key_N` and the rest of
  `plansql.reservedSlotPrefixes` — and every consumer reads one BY NAME. A
  query that MINTS such a name is refused, `42939`: an output alias
  (`SELECT amount AS __key_0`), a derived table's or CTE's column list, and
  the DDL / ingest doors. PostgreSQL answers all of them. The trade is
  stated in `reserved_slots.go` and it is the one this ADR's item 1 asks
  for: the alternative to refusing is not answering them, it is answering
  them WRONGLY — the user's column read where the planner's was meant, or
  the reverse (#694 under the slot's own name). READING is not minting: a
  stored column of such a name stays readable, star included, and the
  planner renumbers its own slot around it.
