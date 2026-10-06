# ADR-0012 divergences: Set operations

UNION, INTERSECT and EXCEPT: VECTOR widths, declared decimal and integer carriers, refused type pairs and literals, and ORDER BY qualification. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**A set operation over VECTOR columns cannot carry two widths** (E06)
VECTOR storage is fixed-width per column and `setOpColType` in `internal/planner/physical/set_op_schema.go` holds a TypeID and a DECIMAL's (p,s) only, so there is no carrier for a mixed-width result. UNION and UNION ALL refuse a value that would be materialized at another arm's width (22000) rather than truncate it, which the engine did before. INTERSECT and EXCEPT emit left-arm values only and answer. Closing it needs a set-op type ladder that carries a VECTOR's width and a mixed-width carrier; PostgreSQL's pgvector answer for INTERSECT/EXCEPT is not measured.

**A set operation publishes its leftmost arm's names** (E09)
Closed 2026-09-18 (#1079). `publishedOutputProjectionNode` passes a set operation to its leftmost arm for the names the client reads, and the DAG carries the same rule as a gather rename (`dagplan.setOpPublishedRenames`), so `SELECT g + 1 FROM t UNION ALL …` publishes `?column?` as PostgreSQL does. Gated by `coordinator.TestSRAStarPublishesItsArmsOwnColumns` (`naming/*`) and `pgwire.TestSRTheWireDeclaresAStarsOwnArms`.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT v FROM v2 UNION ALL SELECT v FROM v3` | both rows (pgvector: vector(2) and vector(3) are one type; the width typmod is dropped) | ERROR 22000 expected 2 dimensions, not 3 (either arm order, UNION too; measured) | 22000 | refusal | 2026-09-05 · [E06](#e06), P050 | #900 | `wadjet.TestASetOperationOverTwoVectorWidths` |
| **r2** `SELECT v FROM v2 EXCEPT SELECT v FROM v3` | not measured (no vector extension on the oracle; pgvector comparisons raise on differing dimensions) | [1,2] (the left row); INTERSECT of the same arms 0 rows (measured) | — | documented gap | 2026-09-05 · [E06](#e06), P050 | #900 | `wadjet.TestASetOperationOverTwoVectorWidths` |
| **r3** `SELECT a.id FROM a UNION ALL SELECT a.id FROM a ORDER BY a.id DESC` | ERROR 42P01 | answers, ordered by the first arm's id (measured); any other qualifier 42P01, ORDER BY "ID" over id 42703 | — | kept superset | 2026-09-22 · [E01](comparison-membership.md#e01), P095 | #1236 | — |
| **r5** `SELECT p FROM a UNION ALL SELECT i FROM a` | no PORT type; an int4 pair stays int4 (OID 23) | declared bigint (OID 20) for PORT with INTEGER (measured) | — | value divergence | — · P030 | — | — |
| **r6** `SELECT d FROM a UNION ALL SELECT x FROM b` | answers | distributed arm refuses when an arm's decimal type or scale is unresolved; the single-process arm answers | — | documented gap | — · P114 | #551 | — |
| **r7** `SELECT DATE '2024-01-01' UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00'` | two timestamp rows | ERROR 0A000 no common carrier for DATE and TIMESTAMP (measured); INTERSECT, EXCEPT and a membership body mixing the two the same | 0A000 | refusal | — · P127 | #1378, #1430 | `coordinator.TestArcDTDateTimestampEveryArm` |
| **r8** `SELECT TRUE UNION ALL SELECT 'true'` | t, t (the literal is typed from the other arm) | ERROR 0A000 cannot build a BOOL value from a quoted literal's text (measured) | 0A000 | refusal | — · P128 | — | — |
| **r9** `SELECT CAST(12345678901234567890123456789012345678 AS DECIMAL(38,0)) UNION ALL SELECT 0.5` | answers (numeric is unconstrained) | ERROR 22003 (the common scale leaves too few integer digits) | 22003 | refusal | — · P129 | — | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E06

ADR lines 401-435. Catalog rows: r1, r2. Stated in [Mechanisms](#mechanisms).

- **A set operation over two VECTOR columns of different declared widths
  is REFUSED, where PostgreSQL answers.** (Added 2026-09-05, #900's
  earlier measurement.) PostgreSQL's `vector` extension makes `vector(2)` and
  `vector(3)` ONE type carrying a width typmod, so a union of the two drops
  the typmod and returns both rows. Wadjet's VECTOR storage is fixed-width
  PER COLUMN and has no carrier for a mixed-width result, so a value that
  would have to be materialized at another arm's width raises 22000
  (`expected N dimensions, not M`) instead. Before that refusal the wider
  arm's value was silently TRUNCATED into the first arm's width and the
  query answered, which is the one disposition neither engine allows.

  `INTERSECT` and `EXCEPT` are unaffected and answer, and that is not an
  inconsistency: both emit values from the LEFT arm only, so nothing is ever
  materialized at a foreign width. Measured on this engine, in both arm
  orders: `EXCEPT` returns the left arm's row, `INTERSECT` returns no rows,
  and `UNION`/`UNION ALL` refuse with the arm-appropriate width.

  **What PostgreSQL answers for those two is NOT measured.** The shared
  oracle server carries no `vector` extension (`pg_available_extensions`
  lists none), so the comparison cannot be made here, and pgvector's
  comparison operators are documented to RAISE on differing dimensions
  rather than report "not equal" — which would make PostgreSQL error where
  wadjet answers, the opposite of a first reading. Nothing is claimed about
  agreement until it is measured against an image carrying the extension.
  What IS settled is the disposition above: refusing a materialization at a
  foreign width beats truncating it, and refusing INTERSECT/EXCEPT for
  symmetry would turn an answer into an error for no measured reason.

  Closing it means a set-op type ladder that carries a VECTOR's width
  (`setOpColType` in `internal/planner/physical/set_op_schema.go` holds a
  TypeID and a DECIMAL's (p,s) and nothing else) plus a mixed-width carrier
  to resolve the pair INTO. Until then the refusal is the honest answer.
  Gate: `wadjet.TestASetOperationOverTwoVectorWidths`, both arm orders,
  all four operators.

### E09

ADR lines 511-535. Stated in [Mechanisms](#mechanisms).

- **A SET OPERATION does not take PostgreSQL's output-column names.**
  (Added 2026-09-05, #732. **CLOSED 2026-09-18, #1079;
  the pins are deleted and that is the proof.**) The naming rule is applied
  at the two places a query's values leave the engine — the collecting sink
  and the gather's rename target — and both were reached through the
  statement's OUTPUT PROJECTION. A set-op root has none
  (`findOutputProjectionNode` answers nil for it), so
  `SELECT g + 1 FROM t UNION ALL SELECT g + 2 FROM t` published `g + 1`
  where PostgreSQL publishes `?column?`, and `SELECT CAST(g AS BIGINT) …
  UNION ALL …` published the cast's text where PostgreSQL publishes `g`.

  It publishes its LEFTMOST arm's names now — PostgreSQL's own rule and
  §8b's — on both engines at once. `publishedOutputProjectionNode` answers
  the question "whose names does the CLIENT read" and passes a set
  operation to its leftmost arm; `findOutputProjectionNode` keeps its own
  answer for every consumer that asks where the pipeline's output
  projection IS. The DAG carries it as a gather rename of the same node's
  visible items (`dagplan.setOpPublishedRenames`) — names only, one per
  item, emitted only where the two names differ — because a copy of the
  rule in one engine is how the two would drift (§2b). The two
  `732/set-op-*` pins in
  `coordinator.TestArcF4BoundariesArePinned` assert PostgreSQL's names
  now, and the gate is `coordinator.TestSRAStarPublishesItsArmsOwnColumns`'s
  `naming/*` cells with `pgwire.TestSRTheWireDeclaresAStarsOwnArms` on the
  wire.
