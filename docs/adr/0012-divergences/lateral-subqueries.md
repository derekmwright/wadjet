# ADR-0012 divergences: LATERAL and subqueries

Correlated subqueries and LATERAL joins: shapes the per-row re-run or the join decorrelation cannot express are refused loudly; star naming over laterals. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**The per-row re-run rebuilds the subquery's text** (E74, E75, P154)
A correlated subquery this engine does not decorrelate is re-run per outer row by substituting the outer values as literals and rebuilding the statement (`plansql.RebuildSQL`). The substitution reaches the SELECT list, WHERE, HAVING, GROUP BY, ORDER BY and each JOIN's ON; a value in GROUP BY or ORDER BY is written as a CAST so it is a constant, not a position. Three shapes have no rendering and are refused 0A000 at compile time: a window call (`WindowFuncNode.String()` collapses the OVER clause; `plansql.HoldsWindowCall`), a set-operation body, and a SELECT item holding an aggregate beside a nested subquery that names the enclosing query. A value with no literal that reads back as itself (a non-finite FLOAT64) is refused the same way; a BYTES value is written as `CAST('\x<hex>' AS BYTES)` (`expr.BytesValueLiteral`, #1501). `walkForOuterRefs` reads window nodes and every clause of a block and of a set operation's arms, so an outer reference found only there is classified correlated rather than planned uncorrelated.

**PostgreSQL's aggregate level rule** (E49, E75)
An aggregate belongs to the level of the deepest variable in its arguments: `SELECT MAX((SELECT u.id)) FROM users u` is the enclosing query's (3 on both), `(SELECT SUM(x.visits + u.id) FROM x)` is the inner block's. This engine has no lowering for an aggregate level above the block it is written in, so an aggregate whose arguments name only the enclosing query is refused 0A000 rather than computed at the inner level. The plan-time rule of #809 refuses only an aggregate naming no relation outside the subquery's own FROM; an outer aggregate inside a subquery's WHERE reaches the standalone re-run and is 42803, PostgreSQL's SQLSTATE.

**An outer join's ON is evaluated at the join** (E28)
An outer join's ON is evaluated per probe row against each candidate build row, because a conjunct lifted above the join would delete the rows it preserves (ADR-0006, 2026-09-18). A subquery's value is not available there, so the construct is refused; an INNER join lifts the same ON into a filter above the join. Closing it needs a per-candidate subquery runner at the join.

**Unqualified outer names bind to the innermost scope that supplies them** (E76)
The decorrelation reads the body's own FROM namespace from the catalog and binds an unqualified name as PostgreSQL does, in every clause (ADR-0021 §1r). Two spellings remain loud: a derived table in the body whose own body names the enclosing row (0A000), and an unqualified enclosing name in the WHERE of a body reading a table function, whose columns the pass does not re-read.

**A LATERAL is decorrelated into a join** (E70, E71, E72, E73, E90)
A correlated LATERAL body becomes a join on its equality keys (inner-only expressions equal to bare outer columns). What that form cannot express is refused 0A000: an ON over an ungrouped aggregate's empty-input default that the pad would answer NULL for (`exec.LateralEmptyDefault` carries the default on the output column; an ON that provably rejects the padded row folds and answers); a window beside a non-equality correlated part or in an ungrouped aggregate body (the window is partitioned by the correlation keys, ADR-0021 §1s); a bound (LIMIT/OFFSET) over anything but an equality key, a DISTINCT other than exactly the key, a set operation or the body's own QUALIFY; a body condition naming a relation two levels out (`logical.refuseReferenceBeyondLateralScope`); and a correlated predicate reading the enclosing relation through a subquery whose FROM holds a LATERAL join (`logical.refuseOuterReferenceThroughLateralSubquery`, `logical.refuseLocalSubqueryWithLateral`). There is no general relation-valued per-row runner.

**A star over a LATERAL is expanded into the FROM arms' lists** (E66, E67, E71, E87, E88, E91, A16, A17)
A star over a LATERAL join is expanded into the FROM items' own lists, the lateral's read as `s.*` reads it, so the correlation slot the join drops (`Node.HiddenJoinCols`) and a key column carried for an expression key (`Node.StarLiftedRefCols`) are not published, and each item carries ADR-0026 §2's pair of names. A dependent join is not reorderable (ADR-0026 §8e), so the column sequence is PostgreSQL's. Two stars are not enumerated: a lateral list naming one column twice (`logical.RefuseStarPublishingLiftedSlot`, refused), and a body whose own list is a star, which is read off the join's stream and so qualifies a duplicate name by its alias. A qualified star over a derived table whose body is itself a star over a join is refused. The whole-join, column-order, source-name and `?column?` divergences these entries once recorded are closed.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT * FROM a LEFT JOIN b ON a.x = (SELECT max(y) FROM c)` | answers; the ON is evaluated per candidate row and unmatched a rows are padded | ERROR 42000 join ON residual on a left join is not evaluable at the join (measured); the INNER JOIN spelling answers | 42000 | refusal | 2026-09-18 · [E28](#e28), P124 | #1153 | — |
| **r2** `SELECT g FROM t GROUP BY g HAVING (SELECT MAX(k) FROM dim d WHERE d.k = SUM(t.g)) > 0` | answers; SUM(t.g) belongs to the enclosing grouped level | ERROR 42803 aggregate functions are not allowed in WHERE (measured) | 42803 | refusal | 2026-09-03 · [E49](#e49), P134 | #809 | `boundary_outer_level_aggregate_inside_a_subquery_is_refused` |
| **r3** `SELECT (SELECT MAX(u.id) FROM x) FROM users u` | one row, 3: the aggregate is promoted to the enclosing level | ERROR 0A000 no lowering for an aggregate level above the block it is written in (measured) | 0A000 | refusal | 2026-09-12 · [E75](#e75), P131 | #1044 | `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn` |
| **r4** `SELECT id, (SELECT u.id FROM x WHERE x.id=1 UNION ALL SELECT u.id FROM y WHERE y.id=99) FROM users u` | 1, 2, 3 | ERROR 0A000 correlated scalar subquery whose body is a SET OPERATION (measured) | 0A000 | refusal | 2026-09-12 · [E75](#e75), P131 | #1044 | `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn` |
| **r5** `SELECT u.id, (SELECT SUM(x.v) + (SELECT u.id) FROM x) FROM users u` | answers SUM(x.v) + u.id per row | ERROR 0A000 an aggregate beside a nested subquery naming the enclosing query has no plan-time type (measured) | 0A000 | refusal | 2026-09-12 · [E75](#e75), P131 | #1044 | `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn` |
| **r7** `SELECT id, (SELECT 1+SUM(u.id) OVER () FROM users x WHERE x.id=1) FROM users u` | 2, 3, 4 | ERROR 0A000 correlated scalar subquery holds a window function (measured) | 0A000 | refusal | 2026-09-12 · [E74](#e74), P130 | #1045 | `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn` |
| **r8** `SELECT id FROM o WHERE o.id IN (SELECT b.k FROM dc_in b JOIN generate_series(1, 9) g(x) ON g.x = b.k WHERE total > 100)` | answers; total binds to the enclosing row | ERROR 42000 filter column "total" does not exist in the input schema (measured); o.total answers | 42000 | refusal | 2026-09-22 · [E76](#e76), P132 | #1104 | `coordinator.TestArcDCAnUnqualifiedOuterReferenceBindsWhereTheBodyCannotSupplyIt` |
| **r9** `SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM (SELECT i.k FROM i WHERE i.k = o.k) d)` | answers the rows with a match | ERROR 0A000 a derived table in FROM that references "o" from an enclosing query is not supported (measured) | 0A000 | refusal | 2026-09-22 · [E76](#e76), P153 | #1104 | `coordinator.TestArcDCAnUnqualifiedOuterReferenceBindsWhereTheBodyCannotSupplyIt` |
| **r10** `WITH d AS (SELECT 1 AS k) SELECT o.id FROM o WHERE EXISTS (WITH d AS (SELECT 2 AS k) SELECT 1 FROM d WHERE d.k = o.k)` | reads the subquery's own d: the rows with k = 2 | ERROR 0A000 a WITH item inside a correlated subquery that shadows an outer WITH item is not supported (measured) | 0A000 | refusal | — · P133 | — | — |
| **r11** `SELECT id FROM o WHERE k < ALL (SELECT k FROM i)` | answers | ERROR 0A000 < ALL (subquery) is not supported; only = ANY and <> ALL are (measured; > ANY the same) | 0A000 | refusal | — · P152 | — | — |
| **r12** `SELECT c.id, (SELECT c.f + x.v FROM x WHERE x.id = 1) FROM c` | answers per row, Infinity included | ERROR 0A000 when a re-run outer value is a non-finite FLOAT64 (measured); a BYTES outer value is its typed hex value and answers, a NUL and invalid UTF-8 included (amended 2026-10-05, #1501; 0A000 at c67ebf5b); array expressions answer (`c.arr[1]`, `c.arr = ARRAY[…]`), and so does a subquery returning the array itself, `(SELECT c.arr …)`, declared as the column is (integer[] for an integer[] column, as on PostgreSQL) | 0A000 | refusal | 2026-09-30 · P154 | #1422 | `coordinator.TestArcSSScalarSubqueryTypedOperandEveryArm`, `pgwire.TestArcSSScalarSubqueryTypedOnTheWire` |
| **r13** `SELECT o.id, s.n FROM lat_ord o LEFT JOIN LATERAL (SELECT count(*) AS n FROM lat_item i WHERE i.order_id = o.id) s ON s.n = 0` | Carol, 0 for the order with no items | ERROR 0A000 naming the condition and pointing at WHERE (measured); ON s.n > 1 answers | 0A000 | refusal | 2026-09-07 · [E70](#e70), P135 | #977 | `coordinator.TestArcJ1AnOnConditionOverADefaultedColumnIsRightOrLoud` |
| **r14** `SELECT * FROM o JOIN LATERAL (SELECT i.id AS m, i.v AS m FROM i WHERE i.k = o.k - 0) s ON true` | answers, two columns named m | ERROR 0A000 the star could not be expanded (a list naming one column twice) (measured) | 0A000 | refusal | 2026-09-25 · [E71](#e71), P136 | #1302 | `coordinator.TestArcJP4RoutedLateralIsRightOnlyWhenSingleIsRight` |
| **r15** `SELECT * FROM lt_o o JOIN LATERAL (SELECT * FROM lt_i i WHERE i.k = o.k) s ON true` | columns named id, k, ..., id, k | the body's columns are named s.id and s.k (measured); values, types and positions agree | — | value divergence | — · P033 | #1126 | — |
| **r16** `SELECT x.* FROM (SELECT * FROM a JOIN b ON a.k = b.k) x` | answers every column of the join | ERROR 0A000 x.* expands only from a relation whose column list is known (measured); a bare * over the join answers | 0A000 | refusal | 2026-09-07 · [E87](#e87) | #979 | — |
| **r17** `SELECT o.id, s.* FROM o JOIN LATERAL (SELECT q.qid FROM q WHERE q.qk = o.k AND EXISTS (SELECT 1 FROM j JOIN LATERAL (SELECT x.v AS xv FROM x WHERE x.oid = j.oid) t ON true WHERE j.id = q.qid AND t.xv > o.id)) s ON true` | answers per outer row | ERROR 0A000 the predicate reads the enclosing relation inside a subquery whose FROM holds a LATERAL join (measured) | 0A000 | refusal | 2026-09-25 · [E72](#e72), P138 | — | — |
| **r18** `SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM j JOIN LATERAL (SELECT x.v AS xv FROM x WHERE x.id = j.id) t ON true WHERE j.k = o.k AND t.xv > 5)` | only the rows whose j match has xv > 5 (2) | admits rows the correlation rejects (1, 2 measured): the subquery does not keep its correlation; recorded for repair | — | documented gap | 2026-09-25 · [E72](#e72) | — | — |
| **r19** `SELECT o.id, s.* FROM o JOIN LATERAL (SELECT i.id FROM i JOIN LATERAL (SELECT j.k FROM i j WHERE j.k = o.k) t ON true) s ON true` | answers | ERROR 0A000 the condition names a relation neither the body's nor to its left (measured) | 0A000 | refusal | 2026-09-25 · [E72](#e72), P137 | — | — |
| **r20** `SELECT o.id, s.* FROM o JOIN LATERAL (SELECT i.v, row_number() OVER (ORDER BY i.v) FROM i WHERE i.k = o.k AND i.v < o.total) s ON true` | answers per outer row | ERROR 0A000 window beside a non-equality correlated condition (measured); the equality-only body answers | 0A000 | refusal | 2026-09-25 · [E73](#e73), P139 | — | — |
| **r21** `SELECT o.id, s.* FROM o JOIN LATERAL (SELECT count(*) AS c, rank() OVER (ORDER BY count(*)) AS r FROM i WHERE i.k = o.k) s ON true` | answers, c = 0 and r = 1 for an outer row with no matches | ERROR 0A000 window in an ungrouped aggregate body (measured) | 0A000 | refusal | 2026-09-25 · [E73](#e73), P139 | — | — |
| **r22** `SELECT o.id, s.v FROM o JOIN LATERAL (SELECT i.v FROM i WHERE i.k <= o.k ORDER BY i.v LIMIT 1) s ON true` | LIMIT applied per outer row | ERROR 0A000 the bound has no equality key to partition by (measured); i.k = o.k answers per outer row | 0A000 | refusal | 2026-09-24 · [E90](#e90), P141 | #1019 | `TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm` |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E28

ADR lines 1100-1110. Catalog rows: r1. Stated in [Mechanisms](#mechanisms).

- **A SUBQUERY inside an OUTER join's `ON` clause is refused.**
  (Added 2026-09-18, #1153.)

  An outer join's `ON` is evaluated AT the join, per probe row against each
  candidate build row, because a conjunct lifted above it would delete the
  rows the join preserves (ADR-0006's 2026-09-18 amendment). A subquery's
  value is not available there. The refusal NAMES the construct; an INNER
  join lifts the same `ON` into a filter above the join and answers it. What
  would close it is a per-candidate subquery runner at the join, which does
  not exist.

### E49

ADR lines 1974-1989. Catalog rows: r2. Stated in [Mechanisms](#mechanisms).

- **An aggregate of the OUTER level inside a subquery's WHERE is refused;
  PostgreSQL accepts it.** (Added 2026-09-03, #809.) `HAVING (SELECT MAX(k)
  FROM dim d WHERE d.k = SUM(t.g)) > 0` is legal SQL — the aggregate
  belongs to the enclosing query's grouped level — and PostgreSQL 17
  answers it. This engine has never answered it on any path: the subquery
  is re-run standalone and its own level-local placement rule fires, since
  nothing at that site holds the outer scope. It is a LOWERING gap and it
  is LOUD (42803, PostgreSQL's own SQLSTATE and message), not a wrong
  value, and it is recorded here so the next reader does not mistake the
  refusal for a decision. The plan-time rule added by #809 is deliberately
  written so it does NOT reach this shape — it refuses only an aggregate
  that names no relation outside the subquery's own FROM — so nothing that
  could one day answer is refused earlier because of it. Pinned as
  `boundary_outer_level_aggregate_inside_a_subquery_is_refused` in the
  correlation census; the day it answers, the pin fails.

### E66

ADR lines 2698-2710. Stated in [Mechanisms](#mechanisms).

- **A QUALIFIED star over a LATERAL join publishes the whole join.**
  (Added 2026-09-07; PRE-EXISTING, measured, not closed there.
  **CLOSED 2026-09-07, #979** — see the entry below.)
  `SELECT o.*` and `SELECT s.*` over `j1ord o JOIN LATERAL (…) s` both
  published every column of the join — four where PostgreSQL sends three
  and one. The unqualified `SELECT *` agrees with PostgreSQL as of this arc
  (the correlation key is dropped at the join, ADR-0026 §3c); the
  QUALIFIED spelling was not narrowed to its named relation at all, which
  is older and independent of the lateral. `SELECT o.*` publishes
  PostgreSQL's three columns now and `SELECT s.*` is a refusal;
  `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription` holds both, with no
  divergence recorded for either.

### E67

ADR lines 2711-2724. Stated in [Mechanisms](#mechanisms).

- **On the DISTRIBUTED arms, a star over a NON-aggregated LATERAL still
  publishes the inner correlation column under its SOURCE name.** (Added
  2026-09-07; PRE-EXISTING, not closed there. **CLOSED
  2026-09-07, #984** — the next entry is that closure, and the
  `wantDAG` that pinned this leak is deleted.) A lateral whose
  SELECT list is a bare projection emits no stage of its own, so the
  stage's stream carries the SCAN's column names and the materialized
  key's alias never lands — `order_id, amount, id, customer, total` on
  `dag`/`dagshuf` where the single-process arms and PostgreSQL publish
  four columns. The two arms therefore disagree about the column SET of
  one star, which is NOT in ADR-0013's list of legal nondeterminism; it is
  recorded here as a divergence with its mechanism, and closing it needs
  the lateral's own projection to be materialized onto its stage.

### E70

ADR lines 2882-2899. Catalog rows: r13. Stated in [Mechanisms](#mechanisms).

- **A written `ON` over an unrepaired LATERAL with an empty-input default
  is REFUSED (0A000) where PostgreSQL answers.** (Added 2026-09-07, the correlation key binding
  earlier implementation, #977.) PostgreSQL evaluates the lateral per outer row and applies
  the ON AFTER it, so an outer row the lateral matched nothing for still
  offers the ON a row carrying the item's empty-input value. This engine
  decorrelates into a join, which pads on the correlation BEFORE the ON is
  applied, so on the unrepaired path (an OUTER join carrying a written ON)
  it would answer NULL whatever the ON says. Where the ON provably REJECTS
  the padded row the two orders agree and the query answers — `ON s.n > 1`
  folds to `0 > 1`. Where it does not, the answer would be NULL where
  PostgreSQL prints a value (`ON s.n = 0` is PostgreSQL's `Carol, 0`;
  `ON o.id > 1` is too, and cannot be folded at all because it reads an
  OUTER column), so it is one sentence naming the condition and pointing at
  WHERE, which is evaluated after the default and is already right. A
  refusal is a divergence and is recorded as one; a wrong number is not an
  option. Gated in
  `coordinator.TestArcJ1AnOnConditionOverADefaultedColumnIsRightOrLoud`.

### E71

ADR lines 2900-2920. Catalog rows: r14. Stated in [Mechanisms](#mechanisms).

- **A bare `SELECT *` over a LATERAL whose correlated equality has an
  EXPRESSION on its outer side is REFUSED (0A000) where PostgreSQL
  answers.** (Added 2026-09-24, #1302.) `i.k = o.k - 0` is no hash
  key, so it is evaluated over the lateral join's OUTPUT and the join
  carries the body's key column there, hidden from a qualified star
  (`Node.StarLiftedRefCols`). A star over a LATERAL is not expanded into the
  arms' lists, so a BARE one publishes the join's output whole and would
  show that column; the refusal names the predicate and the two spellings
  that answer (a named list, `o.*, s.*`). It is the disposition a lifted
  non-equality predicate under a bare star already has (ADR-0021 §1s).
  Gated in `coordinator.TestArcJPALateralOuterExpressionKeyAnswersOnEveryArm`.
  **NARROWED 2026-09-25:** a bare star over a LATERAL
  join is expanded into the FROM arms' own lists, the lateral's read as
  `s.*` reads it, which hides the slot — so the star answers PostgreSQL's
  rows and names, and the lifted non-equality predicate under a bare star
  is no longer declined either. What remains refused is the star that
  cannot be expanded — a lateral list naming one column twice
  (`logical.RefuseStarPublishingLiftedSlot`, both paths). The twelve JPA
  refusals are deleted; `coordinator.TestArcJP4RoutedLateralIsRightOnlyWhenSingleIsRight`
  holds the 274-cell star census and the duplicate-name refusals.

### E72

ADR lines 2921-2940. Catalog rows: r17, r18, r19. Stated in [Mechanisms](#mechanisms).

- **A LATERAL body's correlated predicate that reads the enclosing
  relation through a subquery whose FROM holds a LATERAL join is REFUSED
  (0A000) where PostgreSQL answers.** (Added 2026-09-25.)
  A subquery with a LATERAL join does not keep its correlation with the
  query around it on any execution path — `EXISTS (SELECT 1 FROM j JOIN
  LATERAL (…) t ON true WHERE j.id = q.qid AND t.xv > 5)` admits every
  row even at top level (a wrong value, recorded for repair, not a
  divergence) — so a body's `t.xv > o.id` inside it is not evaluated per
  outer row, and the lateral answered every pair. Base refused it by
  accident (a text split at the first `=` inside the EXISTS); the
  property is refused now (`logical.refuseOuterReferenceThroughLateralSubquery`).
  **Amended 2026-09-25:** the same subquery in a LOCAL
  condition of the body (reading only the body's relation, `j.id = q.qid`)
  is refused too (`logical.refuseLocalSubqueryWithLateral`) — the text
  path had refused it by accident, and the local condition now reaches the
  filter as parsed nodes; an uncorrelated one answers. And a body
  condition naming a relation that is neither the body's nor to its left
  (a LATERAL nested in another naming the outermost relation) is refused
  0A000 (`logical.refuseReferenceBeyondLateralScope`; it was 42000).

### E73

ADR lines 2941-2949. Catalog rows: r20, r21. Stated in [Mechanisms](#mechanisms).

- **A window in a correlated LATERAL body beside a NON-EQUALITY correlated
  condition, or in an UNGROUPED aggregate body, is REFUSED (0A000) where
  PostgreSQL answers.** (Added 2026-09-25.) The window is
  evaluated per outer row by partitioning it by the correlation keys
  (ADR-0021 §1s earlier implementation), which is exact only when every correlated part is
  an equality key; a non-key part is a filter over the join, applied after
  the window numbered the rows, and an ungrouped aggregate's no-match row
  is the join's default pad, whose window value the pad cannot know.

### E74

ADR lines 2950-2981. Catalog rows: r7. Stated in [Mechanisms](#mechanisms).

- **A CORRELATED subquery holding a WINDOW CALL is REFUSED (0A000) where
  PostgreSQL answers.** (Added 2026-09-12, #1045.) A correlated
  subquery this engine does not decorrelate is re-run per outer row by
  substituting the outer values into its WHERE clause and REBUILDING the
  statement around them (`plansql.RebuildSQL`), re-emitting every other
  clause as the text the parser recorded. A window call's recorded text is
  `<func>(<args>) OVER (...)` — `WindowFuncNode.String()` collapses the OVER
  clause deliberately, which is why `plansql.ReplaceWindowFuncs` matches
  window nodes by pointer — so the rebuilt statement does not parse. That
  was already the answer, as `expected ')' after OVER clause` from a runner
  re-reading a statement nobody wrote; it is a named refusal now, raised at
  compile time by all three correlated constructs through
  `plansql.HoldsWindowCall`.

  What made it a DIVERGENCE to record rather than a message improvement is
  the silent half. `walkForOuterRefs` had no case for a window node, so an
  outer reference inside one was invisible to the classifier and to
  `DanglingTableRefs` alike, and `SELECT id, (SELECT 1+SUM(u.id) OVER ()
  FROM users x WHERE x.id=1) FROM users u` was planned UNCORRELATED, ran
  once, and answered 2, 2, 2 for PostgreSQL 17.11's 2, 3, 4 — the qualifier
  strip rebinding `u.id` to the inner relation's own `id`. Two shapes that
  were RIGHT before the walk was repaired move to the refusal with it
  (`PARTITION BY u.id` and `ORDER BY u.id` over a ONE-row inner relation),
  and they were right because one row makes any partitioning of it the same
  partition: the same shapes over a TWO-row inner answered 1, 1, 1 for
  PostgreSQL's 3, 3, 3. Both pairs are cells of
  `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn`, which carries
  PostgreSQL's value beside each refusal so the day the shape is executable
  the cell fails and is rewritten to that value. Executing it needs the
  re-run to substitute OUTSIDE the WHERE clause, which needs a faithful
  rendering of the OVER clause; ADR-0021 §1m states what that costs.

### E75

ADR lines 2982-3034. Catalog rows: r3, r4, r5. Stated in [Mechanisms](#mechanisms).

- **A correlated subquery whose body is a SET OPERATION or a LATERAL body,
  and one holding an aggregate the ENCLOSING query owns, are REFUSED
  (0A000) where PostgreSQL answers.** (Added 2026-09-12,
  #1044; narrowed by the subsequent measurements, which left the GROUP BY / ORDER BY
  half with no members.) A correlated subquery this engine does not
  decorrelate is re-run per outer row by substituting the outer values into
  its text. The substitution reaches the SELECT list, the WHERE, the
  HAVING, the GROUP BY, the ORDER BY and each JOIN's ON condition, every
  one of which the rebuild renders from its own tree. In a GROUP BY or an
  ORDER BY one RENDERING cannot be written — a bare numeric literal, which
  both engines read as the FIRST SELECT ITEM rather than as the number one
  — and the value is written as a CAST instead (`ORDER BY CAST(1 AS
  BIGINT)`), which is a constant expression on PostgreSQL 17.11 and here
  and a position on neither. Nothing in those two clauses is refused for
  that reason any more: refusing on the PRESENCE of an outer reference took
  four shapes main answers exactly as PostgreSQL does, and refusing on the
  rendering took three more (`(SELECT COUNT(*) FROM x GROUP BY u.id, x.id
  ORDER BY x.id LIMIT 1)` is 1, 1, 1 on both). The refusal survives as the
  rendering's post-condition. The rebuild has no arm at all for a body that is a SET
  OPERATION (`(SELECT u.id FROM x WHERE x.id=1 UNION ALL SELECT u.id FROM y
  WHERE y.id=99)` is 0A000 where PostgreSQL answers 1, 2, 3). A SELECT item
  holding an aggregate beside a NESTED subquery THAT NAMES THE ENCLOSING
  QUERY is refused for a third reason: the nested subquery's value is the
  outer row's, so the item has no plan-time type. That refusal is on the
  SHAPE and not on the declaration the item would get — an exact
  accumulator's DECIMAL and a string MIN cannot be stored in the FLOAT64
  the plan allocated, a COUNT would fit and answer, and all of them are
  refused together; an UNCORRELATED nested subquery beside an aggregate is
  NOT refused and is answered as `bf99c56c` answers it, under a
  declaration that is the FLOAT64 default where PostgreSQL 17.11 declares
  `numeric` (#1018 / ADR-0024's family, pinned fail-on-agree on the wire).
  Since earlier implementation the classifier reads
  every clause of the block and a set operation's arms, so the refusal
  reaches a subquery whose ONLY outer reference is in one of them — which
  before answered the qualifier strip's constant in silence. A LATERAL body
  is refused on the same ground — it is decorrelated into a join and the
  projection has nothing to respell.

  The aggregate half is PostgreSQL's LEVEL rule, measured on 17.11: an
  aggregate belongs to the level of the deepest variable in its arguments,
  so `SELECT MAX((SELECT u.id)) FROM users u` is the ENCLOSING query's and
  answers one row (3), while `SELECT id, (SELECT MAX(u.id) FROM x) FROM
  users u` is 42803 because promoted to the outer query it leaves `id`
  ungrouped. `(SELECT SUM(x.visits + u.id) FROM x)` names an inner variable
  too and is the inner block's — 345, 348, 351 — and is not refused. This
  engine has no lowering for an aggregate level above the block it is
  written in, and a per-row re-run would compute it at the INNER level and
  answer a number PostgreSQL does not give, so the shape is refused rather
  than answered. Every refusing cell carries PostgreSQL's value beside it in
  `coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn`, so the day a
  shape becomes executable the cell fails and is rewritten to that value.
  ADR-0021 §1l states what closing each one costs.

### E76

ADR lines 3035-3051. Catalog rows: r8, r9. Stated in [Mechanisms](#mechanisms).

- **An UNQUALIFIED outer reference inside a correlated subquery's body is
  bound as PostgreSQL binds it, except where the body's namespace cannot
  be named.** (Added 2026-09-20, #1104; narrowed 2026-09-22, arc
  DC earlier implementation.) PostgreSQL binds an unqualified name to the innermost scope
  that supplies it, so `total` in a body over `dc_in` is the enclosing
  row's. The decorrelation reads the body's own FROM namespace from the
  catalog and binds the same way in every clause. Two spellings remain
  boundaries, both LOUD: a derived table in the body whose own body names
  the enclosing row (the standing 0A000 class, unqualified or not), and an
  unqualified enclosing name in the `WHERE` of a body that reads a table
  function, whose columns this pass does not re-read — that one fails with
  `filter column "…" does not exist in the input schema` where PostgreSQL
  answers; the qualified spelling answers. Pinned with PostgreSQL's row set
  beside each in
  `coordinator.TestArcDCAnUnqualifiedOuterReferenceBindsWhereTheBodyCannotSupplyIt`;
  ADR-0021 §1r states the rule.

### E86

ADR lines 4161-4168. Moved to the log: [A16](../0012-amendments.md#a16).

  *(Moved to the amendment log: [A16](../0012-amendments.md#a16).)*

### E87

ADR lines 4169-4201. Catalog rows: r16. Stated in [Mechanisms](#mechanisms). Moved to the log: [A17](../0012-amendments.md#a17).

- **A qualified star ALONE over a lateral join publishes the whole join.**
  (Amended 2026-09-07. CLOSED 2026-09-07,
  #979, and it was never about laterals.) `SELECT o.*` with nothing beside
  it published the whole join — over a LATERAL four columns for
  PostgreSQL's three, and over a PLAIN join seven for three, one of them
  literally named `o.id`. `logical.isStarOnly` read a QUALIFIED star as the
  identity of its input, which only a BARE star is, so the list built no
  projection and the expansion never saw the star. It builds one now and
  the two spellings agree.

  What is NOT expanded is unchanged and still refused rather than guessed:
  a bare `*` over a JOIN (the entry below), and a derived table whose body
  is itself such a star.

  *(Moved to the amendment log: [A17](../0012-amendments.md#a17).)*

### E88

ADR lines 4202-4237. Stated in [Mechanisms](#mechanisms).

- **The published NAME of an UNALIASED item inside a block a LATERAL reads
  is its expression text, where PostgreSQL publishes `?column?`.** (Added
  2026-09-13; PRE-EXISTING, measured byte-identical at `0193c4e9`.
  NARROWED 2026-09-14 — the JOIN half is CLOSED. **CLOSED
  2026-09-25**: a star over a LATERAL join is expanded
  into the arms' own lists like a star over any join, each item ADR-0026
  §2's pair, and the six `lateral/*` pins in
  `coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList` are
  deleted.) `SELECT * FROM
  lat_ord o JOIN LATERAL (SELECT order_id, i.amount + 1 FROM lat_item i
  WHERE i.order_id = o.id) s ON true` sends `i.amount + 1` in
  `RowDescription` where PostgreSQL sends `?column?`. A column has two names
  (ADR-0026 §2) and the decorrelated body's stream carries the RESOLUTION
  one; the published name is applied where the block IS the statement's
  output projection, and a star over the lateral's join reads the stream
  instead. Values, types and positions agree on all five arms — only the
  name differs. Making the stream carry `?column?` would give two unaliased
  items ONE name, after which every by-name lookup between the block and the
  client reads the first of them, so closing it needs the published list
  travelling BESIDE the stream by POSITION
  (`physical.ProjectExprSpec.SourceSlot` one relation out). Pinned per arm
  in `coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList`, six cells.

  THE JOIN SPELLING IS CLOSED (#997/#1012): a star over a join
  MINTS the projection that publishes it, and each item carries ADR-0026
  §9's pair — it references the producer's spelling (`amount + 1`) and
  publishes PostgreSQL's name (`?column?`), so no by-name lookup is asked to
  serve both. `SELECT * FROM (SELECT order_id, amount + 1 FROM lat_item) x
  JOIN lat_ord o ON true` now declares `?column?` on all five arms and on
  the wire, with the value and OID PostgreSQL sends. The six `joined/*` pins
  that recorded it are deleted, and the ITEM-KIND row of
  `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` (unaliased
  expression, aggregate, SUM, literal, CAST, each through an inline derived
  arm, a CTE arm and a derived block) is the gate, with
  `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms` for the OIDs.

### E90

ADR lines 4262-4279. Catalog rows: r22. Stated in [Mechanisms](#mechanisms).

- **Some bounded correlated LATERAL bodies are refused.**
  (Amended 2026-09-24, #1019; supersedes the 2026-09-13
  qualified-star refusal.) An equality-keyed body applies its bound per
  outer row, including a qualified star. Keys are inner-only expressions
  equal to bare outer columns. A bound over an inequality, a mixed
  inner/outer expression, an outer-side expression, a DISTINCT body other
  than exactly the key, a set operation or the body's own QUALIFY raises
  `0A000`. There is no general relation-valued per-row runner. ADR-0021
  §1s records the decision and
  `TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm` gates it.

  *History (2026-09-13):* the first cut refused every bounded
  spelling on the bound's existence; whether a bound binds is a property
  of the data, so a non-binding `LIMIT 10` (right on five arms at
  `0193c4e9`) and `OFFSET 0` became errors — a new refusal on a shape that
  answered correctly is a regression. It was narrowed to the qualified
  star the same day; the per-key lateral bound superseded it.

### E91

ADR lines 4280-4292. Stated in [Mechanisms](#mechanisms).

- **A star over a NON-aggregated LATERAL publishes PostgreSQL's columns in
  a different ORDER.** (Added 2026-09-07; PRE-EXISTING.
  **CLOSED by #1008.**) `SELECT * FROM o JOIN LATERAL (SELECT amount …) li`
  published `amount, id, customer, total` on the single-process arms where
  PostgreSQL publishes `id, customer, total, amount`: a join emits its
  PROBE side first, and `reorderJoins` was making the lateral the probe on
  an estimated row count — a cost decision changing what a star publishes.
  A DEPENDENT join is not reorderable (ADR-0026 §8e), so every arm now
  publishes PostgreSQL's sequence and the divergence is GONE rather than
  pinned: `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription`'s
  `star_over_a_non_aggregated_lateral` cell wants
  `{id, customer, total, amount}` with no divergence beside it.
