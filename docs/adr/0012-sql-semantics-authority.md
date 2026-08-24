# ADR-0012: PostgreSQL is the SQL semantics authority; DuckDB is the performance goal and an oracle

Status: Accepted (2026-08-19)

## Context

Wadjet ships the PostgreSQL wire protocol, and DuckDB serves as both the
performance bar and the differential correctness oracle. Those two facts
pull in different directions the moment the engines disagree about what a
query *means* rather than how fast it runs.

SQL leaves a surprising amount implementation-defined, and the two engines
resolve it differently. Default NULL placement is the worked example:
PostgreSQL sorts NULLS LAST for ASC and NULLS FIRST for DESC; DuckDB's
`default_null_order` is NULLS LAST in both directions. Neither is wrong.

The question surfaced during the 2026-08-18/19 correctness work and was
escalated for a decision, which exposed the real problem: **the answer was
already implied by a commitment nobody had written down.** CLAUDE.md states
pgwire compatibility is non-negotiable because Superset, psql and JDBC depend
on it; once that holds, semantics follow. ADR-0001 exists because settled
questions were getting re-asked, and this was one.

Escalation is not a neutral cost here. A semantics question presented as an
open preference arrives stripped of the commitment that settles it, so it can
be answered wrongly by someone without that context — and the gates then
*encode* the wrong answer. The stored DuckDB baseline is regenerated from
whatever the oracle says, so a wrong choice becomes self-verifying within
minutes, and the eventual correction shows up red. That is the poisoned-
baseline failure mode (`baseline-local-small.json` held a signature captured
from a broken engine, so a *correct* engine failed our own gate) one level up.

## Decision

1. **PostgreSQL decides semantics.** Where PostgreSQL and DuckDB disagree
   about the meaning of a query — result values, NULL handling, ordering
   placement, type coercion, error-vs-not, catalog introspection — wadjet
   follows PostgreSQL. The wire protocol is a behavioral contract, not just a
   byte format: clients and the tools above them encode PostgreSQL's behavior
   in their query generation.

2. **DuckDB remains the performance goal and a correctness oracle.** It finds
   real bugs (dozens during the 2026-08 correctness arc) and sets the speed
   bar. Neither role makes it the semantic authority.

3. **On a semantic divergence, configure the oracle — do not exempt the
   entry.** An exemption blinds the gate permanently to real bugs in exactly
   the queries most likely to have them; a configured oracle still compares
   every row. The differential gate runs DuckDB with
   `default_null_order='nulls_last_on_asc_first_on_desc'`.

4. **A decision a gate encodes must be asserted where regeneration cannot
   rewrite it.** `TestOracleIsConfiguredForPostgresSemantics` fails if either
   oracle invocation loses that setting, because otherwise deleting one line
   silently changes what "ground truth" means.

5. **Deliberate divergences from PostgreSQL.** (Amended 2026-08-23: collation
   was not the only one — the other three below were never PostgreSQL's call
   to begin with, and are recorded here so a future gate does not mistake
   them for undecided.)

   - **Collation.** Wadjet compares and sorts strings with BINARY collation,
     not PostgreSQL's locale-dependent collation. Locale-sensitive comparison
     costs real work on every string compare and sort, surprises analysts
     more than it helps, and no BI client depends on it. Where the oracle
     needs to agree, use a `C`-collation database rather than exempting
     string ordering.
   - **MIN/MAX over BOOL.** PostgreSQL has no `min(boolean)`/`max(boolean)`
     aggregate (verified against live PostgreSQL: it errors, "function
     min(boolean) does not exist") — `bool_and`/`bool_or` are its idiom for
     the same question. Wadjet supports MIN/MAX over BOOL as a deliberate
     extension, not a divergence PostgreSQL took a position on; `bool_and`/
     `bool_or` remain available and are still the PostgreSQL-idiomatic
     spelling.
   - **MAP and VECTOR ordering.** Both are wadjet-only types — PostgreSQL has
     neither — so their total orders (`internal/engine/exec/kernel/
     container_sort.go`) are wadjet-defined, not a choice against a
     PostgreSQL answer.
   - **MIN(bytes) declares BYTEA, not STRING.** The output type follows the
     input type, matching PostgreSQL's own `min(bytea)`; no divergence, noted
     here only because early declared-schema code guessed STRING for every
     MIN/MAX before the input-typed fix.

6. **A numeric literal's carrier is its TEXT, not a float64.** (Added
   2026-08-23, from #452.) PostgreSQL types an unsuffixed decimal literal as
   `numeric` and compares it at full precision, so `WHERE d = 493827160549382.7160549350`
   must find the row holding that value. A float64 carries ~15-16 significant
   decimal digits and a `DECIMAL(38,10)` carries 38, so the box the compiler
   builds for arithmetic cannot also be the record of which number was
   written: it is a different number by the time it meets the column, and the
   damage is not uniform — the rounded literal landed just BELOW the stored
   value, so `=` matched nothing, `>` gained a row, `<>` gained it back, and
   `>=` and `<` agreed by luck. Four operators agreeing is not partly right.

   Three rules follow. They hold for a bare DECIMAL column compared, matched
   against an IN list, or bounded by BETWEEN, against a numeric literal — the
   vectorized kernel, the row-at-a-time expression, the raw-text predicate,
   and the row-group prune all bind that one shape. They do NOT yet reach
   every site that compares a DECIMAL column to a literal: an
   arithmetic-wrapped operand (`d + 0 = lit`), `CASE d WHEN lit`,
   `d IS DISTINCT FROM lit`, and `GREATEST`/`LEAST` still fall through to the
   generic float64 comparison and can reproduce the same failure. Tracked in
   #465 rather than fixed here.

   - The literal's source text travels with its box (`expr.Lit.Text`,
     `logical.Predicate.ValueText`, `exec.KernelFilter.LitText`) and is what
     a DECIMAL comparison converts, at the COLUMN's scale.
   - A literal the column's scale cannot hold exactly — `0.255` against a
     `DECIMAL(9,2)` — equals nothing, and still has its place in the ORDER:
     it sits strictly between two representable values, so `> 0.255` excludes
     the row holding `0.25` that `>= 0.25` admits. Truncating the literal
     instead would answer a different question; the residual of the discarded
     digits is carried rather than dropped.
   - One conversion serves the prune and the filter
     (`kernel.StatsDomainValue`, `kernel.decimalLiteralAt`), because a prune
     that reads the predicate differently from the filter deletes rows the
     filter would have kept.

   Arithmetic over DECIMAL still goes through float64, and so do MIN/MAX/SUM
   over a DECIMAL column. That is a separate, visible limit — comparison is
   where a rounded value silently changes the ROW SET, which is why it is
   settled here first.

7. **Semantics decisions are technical, not product.** They are made and
   executed, then reported — not escalated. An existing project commitment
   settles everything downstream of it; check for the commitment before
   drafting the question.

8. **Float ordering follows PostgreSQL, not IEEE754, in every ORDER/
   PARTITION/peer/key context; a boxed value's comparison order follows the
   column's declaration, not the box's Go type.** (Added 2026-08-23, #444/
   #446 follow-up.)

   - **Float order.** PostgreSQL's `float8_cmp_internal`/
     `float4_cmp_internal` give FLOAT a total order that IEEE754's own
     comparison operators do not: NaN sorts ABOVE every other value and
     equals itself, and -0.0 equals +0.0. `ORDER BY`, `GROUP BY`'s peer
     grouping, window PARTITION/peer groups, and any key built to represent
     "the same value for merge/comparison purposes" now apply that rule —
     `kernel.CompareFloat64`/`CompareFloat32`
     (`internal/engine/exec/kernel/float_order.go`) is the one place the
     rule is stated, every scalar FLOAT32/FLOAT64 comparator and the
     VECTOR/ARRAY(FLOAT) element comparators are built on it, and the boxed
     k-way MERGE key a spilled aggregate's drain step reifies
     (`appendKeyValue`/`keyFloat32bits`/`keyFloat64bits`,
     `internal/engine/exec/sort.go`) is canonicalized to agree: two values
     the comparator calls equal must also serialize alike, or a query's
     answer depends on how much memory it had (the same failure mode
     `appendKeyValue`'s BYTES/ARRAY/ROW fix addressed for a different type
     class).
   - **A boxed value's order is the column's DECLARATION, not a property of
     its Go box.** `Vector.GetValue` erases declaration order — a ROW boxes
     as `map[string]any`, which has none — so a comparator that dispatches
     on the BOX's own Go type (a `map[string]any`'s keys, sorted
     alphabetically) can disagree with the COLUMNAR comparator, which reads
     the real declared field order. `internal/engine/exec/compare_boxed.go`
     resolves the boxed comparator FROM the declaration (a closure built
     once per column), so both paths order a ROW's fields positionally and a
     DECIMAL numerically, matching PostgreSQL's `record_cmp`. The dynamic
     fallback (`compareAny`, used only when no declaration is available)
     still orders a ROW by field name — no production path reaches it — and
     is not addressed by this decision.
   - **What this does NOT yet cover.** The predicate kernels (`=`, `>`, `IN`
     — `internal/engine/expr/expr.go`'s `cmpFloat64Op`/`cmpFloat32Op`,
     `internal/engine/exec/kernel/compare.go`'s `ResolveFilterKernel`), the
     PRIMARY (non-spilled) GROUP BY/DISTINCT hash key
     (`internal/engine/exec/aggregate.go`'s `typedRowHash`/
     `serializeGroupKey`/`appendColumnValue`), and the hash-join key
     (`internal/engine/exec/join.go`'s `buildKeyFromBatch`/`buildProbeKey`)
     still compare/hash raw IEEE754 bits — a `WHERE f = f` over a NaN row,
     or a `GROUP BY`/`DISTINCT`/hash-join over `{-0.0, 0.0}` in the common
     (non-spilling) case, still disagrees with PostgreSQL. Tracked as #459.
     MIN/MAX over a NaN column is the same gap in the aggregate kernels,
     tracked as #457.

## Consequences

- `ORDER BY x DESC` places NULLs first (changed 2026-08-19). The default had
  been unreachable before that: the DESC comparator negated the kernel's null
  handling along with its values, so the code *declared* PostgreSQL's rule and
  *emitted* DuckDB's.
- The differential gate keeps full strength across the ordering corpus rather
  than carrying exemptions.
- A DECIMAL predicate answers the same on the kernel path and the row-at-a-time
  path (changed 2026-08-23). The row path had compared a DECIMAL column's
  RENDERED TEXT against a float64 literal and, finding no numeric reading of
  that pair, fell through to a lexicographic string comparison — so
  `CASE WHEN d = 1339815.97` was false for the row holding exactly that value
  even at a scale a float64 represents perfectly.
- Open questions in the same territory, to be decided by this ADR's rule and
  recorded when they are: integer overflow behavior, `timestamptz` and the
  session TimeZone GUC, empty-string versus NULL, identifier case folding.
- DuckDB cannot adjudicate "would a PostgreSQL client expect this," so a
  PostgreSQL differential arm (small scale, plus a pgwire protocol comparison)
  is the natural completion of this decision. See ADR-0013.

## Related

- ADR-0001 (record architecture decisions — the re-asked-question problem)
- ADR-0013 (correctness gates and their deliberate boundaries)
- CLAUDE.md, "Don't skip pgwire compatibility"
- `benchmarks/tpch/oracle_semantics_test.go`, `internal/planner/physical/plan.go`
  (`resolveNullsLast`), `internal/distributed/messages.go` (`PlaceNullsLast`)
- `internal/engine/exec/kernel/decimal_literal.go` (the literal, resolved at a
  column's scale), `internal/engine/expr/decimal_literal.go` (the row path's
  binding), `wadjet/decimal_literal_test.go` (the operator sweep at three
  scales, both paths)
- #444 (boxed ROW comparator ordered fields by name, not declared position),
  #446 (VECTOR/ARRAY(FLOAT) comparators not transitive under NaN) — the work
  item 8 above records the settled position for
- #459 (predicate kernels, the primary GROUP BY/DISTINCT hash key, and
  hash-join keys still compare floats as raw IEEE754), #457 (MIN/MAX over a
  NaN column) — item 8's open remainder
- `internal/engine/exec/kernel/float_order.go`, `internal/engine/exec/
  compare_boxed.go`, `internal/engine/exec/kernel/
  container_order_property_test.go` (the P1-P4 total-order property test)
