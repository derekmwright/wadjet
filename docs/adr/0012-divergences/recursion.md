# ADR-0012 divergences: Recursion

Recursive CTEs: an iteration-bounded eager fixed point, WITH RECURSIVE list ordering, and the seed-typed recursive terms PostgreSQL refuses. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**A recursive CTE is materialized to its fixed point, bounded** (E92)
The engine evaluates a recursive CTE eagerly: the whole closure is built before the statement reads it, so a LIMIT in the reading query cannot stop it; the stop belongs in the recursive term's WHERE. The fixed point is bounded at 1,000,000 iterations (54000) and by the memory budget for one iteration (53200), and either error replaces the answer; the rows so far are never returned. Items of a WITH RECURSIVE list are resolved against the items before them only, so a forward reference and mutual recursion are 42P01. The recursive term is checked by the values it produces, and the seed's type decides the column: an integer term of another width is range-checked into the seed's width, a text term joins a varchar(n) seed's carrier, and a quoted seed is text. A float term under a fractional-literal seed is PostgreSQL's 42804 since the literal became numeric (arc VL). Gated in wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails and wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT count(*) FROM r` | no iteration limit; runs until statement_timeout or temp_file_limit ends it | ERROR 54000 at the 1,000,000th iteration (53200 for one iteration larger than the budget); no partial rows | 54000 | documented gap | 2026-09-22 · [E92](#e92), P156 | #1246 | `wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails` |
| **r2** `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5` | 1, 2, 3, 4, 5 (lazy evaluation) | ERROR 54000 (closure materialized before it is read) | 54000 | refusal | 2026-09-22 · [E92](#e92), P157 | — | `wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails` |
| **r3** `WITH RECURSIVE a(n) AS (SELECT n FROM b), b(n) AS (SELECT 1) SELECT n FROM a` | 1 | ERROR 42P01 relation "b" does not exist | 42P01 | refusal | 2026-09-22 · [E92](#e92), P158 | — | `wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails` |
| **r4** `WITH RECURSIVE a(n) AS (SELECT 1 UNION ALL SELECT n FROM b), b(n) AS (SELECT n FROM a) SELECT n FROM a` | ERROR 0A000 (mutual recursion) | ERROR 42P01 | 42P01 | documented gap | 2026-09-22 · [E92](#e92), P158 | — | `wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails` |
| **r5** `WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r` | 1, 2, 3, 4, 5 (duplicates removed at every step) | ERROR 0A000 recursive CTE written with UNION rather than UNION ALL is not supported (measured) | 0A000 | documented gap | — · P155 | — | — |
| **r6** `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3 ORDER BY 1) SELECT n FROM r` | ERROR 0A000 (ORDER BY or LIMIT on the whole recursive body) | answers 1, 2, 3 | — | kept superset | 2026-09-22 · [E92](#e92), P159 | — | `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType` |
| **r7** `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n + 1)::bigint FROM r WHERE n < 3) SELECT n FROM r` | ERROR 42804 | 1, 2, 3 (term range-checked into the seed's integer width; measured) | — | kept superset | 2026-09-22 · [E92](#e92), P159 | — | `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType` |
| **r8** `WITH RECURSIVE r(s) AS (SELECT CAST('a' AS VARCHAR(5)) UNION ALL SELECT s \|\| 'a' FROM r WHERE length(s) < 3) SELECT s FROM r` | ERROR 42804 (text term under a varchar(n) seed) | answers a, aa, aaa (one carrier) | — | kept superset | 2026-09-22 · [E92](#e92), P159 | — | `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType` |
| **r9** `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT CAST('x' AS TEXT) FROM r WHERE false) SELECT n FROM r` | ERROR 42804 at parse time | 1 (the term's produced values are checked; it produces none) | — | kept superset | 2026-09-22 · [E92](#e92), P159 | — | `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType` |
| **r10** `WITH RECURSIVE r(n) AS (SELECT '5' UNION ALL SELECT 2 FROM r WHERE n = '5') SELECT n FROM r` | answers (the quoted seed is resolved from the term) | ERROR 42804 (the quoted seed is text; a non-text term that produces rows is refused) | 42804 | refusal | 2026-09-22 · [E92](#e92), P159 | — | `wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType` |
| **r11** `WITH RECURSIVE r(k, v) AS (SELECT 1, (SELECT x.n FROM t x WHERE x.id = 1) UNION ALL SELECT k + 1, v + 1 FROM r WHERE k < 3) SELECT k, v FROM r` over a numeric(10,2) `x.n` | ERROR 42804 (numeric(10,2) in non-recursive term but type numeric overall) | 2.25, 3.25, 4.25: the subquery's answer is declared without the column's modifier, so it seeds an unconstrained column (likewise a correlated `(SELECT t.n …)`, a `CAST(x.n AS NUMERIC(10,2))` inside the subquery, and a numeric(5,2) answer under `v * 10`: 225.00, 2250.00, 22500.00); v0.25.3 raised 42804 (measured) | — | kept superset | 2026-10-01 · [SS](../0012-amendments.md#2026-09-30-a-scalar-subquerys-answer-is-a-typed-operand-arc-ss-1428-1431-1427-1422) | — | `pgwire.TestArcSSAuditConsumersOnTheWire` (r13/seed/subSeed*) |
| **r12** `WITH RECURSIVE r(k, v) AS (SELECT 1, CAST(1.5 AS NUMERIC(38,2)) UNION ALL SELECT k + 1, v + 1 FROM r WHERE k < 3) SELECT k, v FROM r` | ERROR 42804 (numeric(38,2) in non-recursive term but type numeric overall) | 1.50, 2.50, 3.50: a numeric(38,s) seed — a CAST or a column — reads as unconstrained (38 digits is the widest DECIMAL this engine holds); the same at v0.25.3 (measured); at precision 37 the seed is 42804 as on PostgreSQL | — | kept superset | 2026-10-01 · [SS](../0012-amendments.md#2026-09-30-a-scalar-subquerys-answer-is-a-typed-operand-arc-ss-1428-1431-1427-1422) | — | `pgwire.TestArcSSAuditConsumersOnTheWire` (r13/seed/n38*, n372cast) |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E92

ADR lines 4293-4310. Catalog rows: r1, r2, r3, r4, r6, r7, r8, r9, r10. Stated in [Mechanisms](#mechanisms).

- **A recursive CTE's fixed point is bounded, and a few recursive shapes
  PostgreSQL refuses are answered.** (Added 2026-09-22, arc RC; ADR-0021
  §1o-b.) PostgreSQL iterates without limit; this engine raises 54000 at
  1,000,000 iterations and 53200 for one iteration larger than the budget,
  and never returns the rows so far. PostgreSQL evaluates a recursive CTE
  lazily, so a LIMIT over a recursion with no stop answers there and is
  54000 here. A forward reference inside a `WITH RECURSIVE` list is 42P01
  here (PostgreSQL answers), and mutual recursion is 42P01 here where it is
  0A000 there. SUPERSET, kept: an ORDER BY or LIMIT on the whole recursive
  body, an integer term of another width than the seed (range-checked into
  the seed's width), a text term under a varchar(n) seed, and a mistyped
  term that never produces a row. (A float term under a fractional-literal
  seed was on this list while the literal was double precision here; since
  arc VL the literal is numeric and the cell is PostgreSQL's 42804.) A quoted seed is text here and resolved from the
  term there (42804 here for a non-text term). Gated in
  `wadjet.TestArcRCRecursiveCTEAnswersItsWholeClosureOrFails` and, cell by
  cell against PostgreSQL, `…SeedTypeDecidesAgainstEveryTermType`.
