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
   and the row-group prune all bind that one shape. (Amended 2026-08-24,
   #465: `CASE d WHEN lit`, `d IS DISTINCT FROM lit` and
   `GREATEST`/`LEAST(d, lit)` now hold them too. Those three compare through
   the BOXED path, where the column is rendered text and the literal is the
   float64 box, so they carry the literal's `Text` into the comparison and
   order the two exact decimals — `expr.compareWithText`,
   `batch.CompareDecimalTexts`. An arithmetic-wrapped operand — `d + 0 = lit`
   — still does not: arithmetic over DECIMAL goes through float64 before any
   comparison sees it, which is the separate limit recorded at the end of this
   item.)

   - The literal's source text travels with its box (`expr.Lit.Text`,
     `logical.Predicate.ValueText`, `exec.KernelFilter.LitText`) and is what
     a DECIMAL comparison converts, at the COLUMN's scale.
   - A literal the column's scale cannot hold exactly — `0.255` against a
     `DECIMAL(9,2)` — equals nothing, and still has its place in the ORDER:
     it sits strictly between two representable values, so `> 0.255` excludes
     the row holding `0.25` that `>= 0.25` admits. Truncating the literal
     instead would answer a different question; the residual of the discarded
     digits is carried rather than dropped.
   - A literal the CARRIER cannot hold at that scale — `1e39`, or `10^30`
     against a `DECIMAL(38,10)`, whose unscaled integer needs more than the
     128 bits `Int128` has — keeps its place in the order by SATURATING: it
     compares strictly greater (or strictly less, when negative) than every
     value the column can hold, which is what it is. It never wraps and never
     errors. Narrowing it two's-complement instead put it back INSIDE the
     ordinary range as a plausible number of either sign, so `WHERE d < 1e39`
     — true of every row — selected none of them (#462). The order this
     produces is the order of the exact rationals, saturation included, which
     `batch.TestScaledDecimalOrderIsTransitiveAtTheBoundary` asserts over
     every triple of stored values and constants either side of the ends.
   - One conversion serves the prune and the filter
     (`kernel.StatsDomainValue`, `kernel.decimalLiteralAt`), because a prune
     that reads the predicate differently from the filter deletes rows the
     filter would have kept.
   - **A DECIMAL meeting a value of another type is compared by VALUE, and
     the rule is the other type's.** (Added 2026-08-24, #476/#477.) A DECIMAL
     column boxes as its rendered TEXT, so every boxed comparison against it
     used to fall through to a LEXICOGRAPHIC one, where "9" sorts above "10".
     Against an INTEGER the comparison is exact (`expr.decimalTextOrder`, the
     same `ScaledDecimal` carrier); against a FLOAT it is a float64
     comparison, because PostgreSQL's `numeric <op> double precision` casts
     the numeric; against another DECIMAL it is the unscaled Int128s at their
     two scales (`kernel.CompareDecimalValues`), which no box can be
     dispatched on — two rendered DECIMALs are indistinguishable from two
     strings — so that pair is bound from the column DECLARATIONS, per item 8.

     **Known residual: that declaration-bound pair is wired at exactly one
     site.** `expr.bindDecimalCols` is called from `NewCmp` alone, so a
     direct `d1 op d2` gets the exact Int128 comparison but the SAME two
     columns at a BOXED site — a simple `CASE d1 WHEN d2 THEN ...`,
     `d1 IS DISTINCT FROM d2`, `GREATEST(d1, d2)` — still fall through
     `compare()`'s two-rendered-strings path and compare LEXICOGRAPHICALLY,
     the same defect #477 fixed for the direct comparison. Measured on the
     `declit` fixture (`wadjet/decimal_literal_test.go`), where `d_2` and
     `d_4` are numerically equal at exactly one row: `d_2 = d_4` (bound)
     answers 1, matching PostgreSQL; `CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1`
     answers 0. `GREATEST(d_2, d_4) = d_4` and `d_2 IS DISTINCT FROM d_4`
     diverge the same way, by construction rather than by one unlucky row.
     Closing this means extending `#465`'s literal-side `compareWithText`
     carry-through to a column-side counterpart, not a new comparison rule —
     tracked as #506 rather than folded into this fix, since `#465`'s three
     sites need their OWN column-pair binding, not the literal one they
     already carry.
   - **A constant that is not a number is a query ERROR, never a value.**
     (Added 2026-08-24, #463.) The conversion used to answer ZERO for
     anything it could not parse, so `WHERE d = 'abc'` — and `WHERE d = 1e400`,
     which the float64 expansion could not read either — matched every row
     holding zero. PostgreSQL refuses both spellings of "this is not a
     number" with SQLSTATE 22P02, so wadjet raises the same, from the
     vectorized filter (`exec.decimalConstError`) and from the row-at-a-time
     comparison (`expr.raiseInvalidTextRepresentation`) alike: one path
     erroring while the other answers is the two-path defect class. Exponent
     form is not that case — `1e400` IS a number, and is now read as one, by
     folding the exponent into the scaling instead of through
     `strconv.ParseFloat`.

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
   - **What is now covered, and what is left.** (Updated 2026-08-24, #459's
     close.) The predicate kernels (`=`, `>`, `IN` — `internal/engine/expr/
     expr.go`'s `cmpFloat64Op`/`cmpFloat32Op`, `internal/engine/exec/kernel/
     compare.go`'s `ResolveFilterKernel`), the PRIMARY (non-spilled) GROUP
     BY/DISTINCT hash key (`internal/engine/exec/aggregate.go`'s
     `typedRowHash`/`serializeGroupKey`/`appendColumnValue`), and the
     hash-join key (`internal/engine/exec/join.go`'s `buildKeyFromBatch`/
     `buildProbeKey`) now compare/hash the canonical bits — a `WHERE f = f`
     over a NaN row, and a `GROUP BY`/`DISTINCT`/hash-join over `{-0.0,
     0.0}`, agree with PostgreSQL in the single-process engine. MIN/MAX over
     a NaN column was fixed earlier and separately (`kernel.CompareFloat64`
     in the accumulator loop, #457). The DISTRIBUTED half of the same rule
     closed alongside: `hashRowsIntoPartitions`
     (`internal/worker/partitioned_shuffle_sink.go`) is the shuffle's own
     router, keyed independently of the in-process hash above, and its
     scalar FLOAT32/FLOAT64 arms moved with #459 — its VECTOR arm did not,
     because a VECTOR element's canonicalization lives in a different
     function (`appendVectorKey`, aggregate.go) that #459 did not touch;
     the router disagreeing with that key for one type was the same
     defect class one type over (`hashVectorValue` too, kept in step per
     its own comment requiring the two hash the same byte stream), closed
     in the same fold-in that closed #459. Nothing named in this item's
     original list remains open. Three findings adjacent to it — not float
     ordering — surfaced during the same work and are tracked separately
     rather than folded in: RIGHT/FULL joins losing a NULL-keyed BUILD row
     on the integer key paths (#496), `BuildFromRows` routing a dual-int key
     join down the string branch (#498), and cross-scale DECIMAL set
     operations not deduplicating (#499). A float row-group statistics bound
     can also HIDE a NaN
     that this order says must have kept the row group in a `>`/`>=`/`<>`
     prune — that is a pruning-input question, not an ordering one, and is
     recorded in ADR-0018's territory instead (its §5).

9. **Exact numeric aggregates: what MIN/MAX/SUM/AVG over a DECIMAL answer.**
   (Added 2026-08-23, #455.) PostgreSQL's `min`/`max`/`sum`/`avg` over
   `numeric` are exact and answer in `numeric`. Wadjet's were answering in
   `float64`: the accumulators were already exact Int128 at the column's
   scale, but the declared OUTPUT type was a double, so everything past ~16
   significant digits was gone before any consumer saw it —
   `MAX(numeric(38,10))` returned `9.777777778877776e+14` for
   `977777777887777.7577887713`, and `HAVING MAX(d) = <that value>` therefore
   matched nothing. The contract now:

   - **MIN/MAX(DECIMAL(p,s)) → DECIMAL(p,s).** The answer is a value the
     column holds, so it keeps the column's own precision and scale. Exact,
     and identical to PostgreSQL.
   - **SUM(DECIMAL(p,s)) → DECIMAL(38,s).** Exact, accumulated in Int128 at
     the input's scale. The declared precision is the carrier's full width
     rather than the input's, because a sum genuinely exceeds its column's
     precision and a narrower declaration would hand the parquet writer a
     leaf too small for the value.
   - **SUM overflow is an ERROR, not a wrapped total.** PostgreSQL's numeric
     is unbounded; wadjet's exact carrier is 128 bits, which holds every
     DECIMAL(38) value but not every sum of them (two values near 10^38
     suffice). A wrapped sum is a different number wearing the right type, so
     the query fails with a message naming the aggregate. This is a
     deliberate, documented limit of the carrier — not a semantic
     disagreement with PostgreSQL. The flag is STICKY, so a running total
     that leaves the range and comes back — `+9e37, +9e37, -9e37`, whose
     exact total is representable — also fails; refusing a sum we did carry
     exactly is the conservative side of a limit whose other side is a
     wrapped number nobody can see is wrong.
   - **AVG(DECIMAL(p,s)) → DECIMAL(38, min(s+4, 38))**, computed as exact
     division of the Int128 sum by the row count, rounded half away from
     zero. This is a **deliberate divergence in the number of digits kept**:
     PostgreSQL's numeric division picks a scale giving at least 16
     significant digits (and never below the dividend's scale), so its answer
     may carry more or fewer fractional digits than wadjet's. Both are exact
     to the digits they keep and agree to `min(both scales)`. A fixed
     increment — the Spark and SQL Server rule — is the honest choice for a
     128-bit carrier: the digits kept do not depend on the magnitude of the
     answer, so the same query over more rows cannot silently change the
     scale of its own output column. An average with no exact 128-bit value
     is an error, for SUM overflow's reason.
   - **STDDEV / VARIANCE / CORR / COVAR / MEDIAN / PERCENTILE over a DECIMAL
     stay float64.** PostgreSQL answers those in `numeric` too. This is a
     KNOWN, deliberate deviation, recorded rather than hidden: they need
     square roots and running means, which an exact fixed-point tower does
     not provide, and the oracle's float tolerance covers the difference.
     Reopening it means building the tower, not widening an accumulator.
   - **Both execution paths and the DAG's partial/final merge answer the
     same thing.** The partial ships SUM as an Int128 DECIMAL plus a COUNT,
     and the final divides — `internal/worker/avg_fold.go`. A DECIMAL
     aggregate that answered exactly in one process and approximately across
     three workers would be the two-path defect class all over again
     (ADR-0018 §3).
   - **The oracle compares these entries EXACTLY.** MIN/MAX/SUM over a
     DECIMAL are compared digit for digit on both engines
     (`pgCase.exactNumeric`), because a float-rendered comparison is what let
     the defect ship green. AVG keeps the float comparison, for the scale
     contract above and for no other reason.

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
  bindings and the boxed comparisons), `wadjet/decimal_literal_test.go` (the
  operator sweep at three scales, both paths)
- `internal/engine/batch/decimal.go` (`ScaledDecimal`, `DecimalTextAt`,
  `CompareDecimalTexts` — one carrier and one text comparison for every
  DECIMAL predicate), `internal/engine/exec/kernel/compare.go`
  (`colColFilterDecimal`, `DecimalConstText`),
  `internal/engine/exec/kernel/sort.go` (`CompareDecimalValues`)
- #462 (a literal past the carrier wrapped two's complement), #463 (an
  unreadable literal answered ZERO), #465 (CASE / IS DISTINCT FROM /
  GREATEST / LEAST did not carry the literal's text), #476 (a boxed DECIMAL
  against a number compared lexicographically), #477 (two DECIMAL columns had
  no kernel at all) — the work items 6's amendments record
- #444 (boxed ROW comparator ordered fields by name, not declared position),
  #446 (VECTOR/ARRAY(FLOAT) comparators not transitive under NaN) — the work
  item 8 above records the settled position for
- #459 (predicate kernels, the primary GROUP BY/DISTINCT hash key, and
  hash-join keys compared floats as raw IEEE754 — closed), #457 (MIN/MAX over
  a NaN column — closed) — item 8's remainder, now closed; see "What is now
  covered, and what is left" above for the distributed VECTOR-router
  follow-on that closed alongside
- `internal/engine/exec/kernel/float_order.go`, `internal/engine/exec/
  compare_boxed.go`, `internal/engine/exec/kernel/
  container_order_property_test.go` (the P1-P4 total-order property test)
- Exact numeric aggregates (item 9): `internal/engine/batch/decimal.go`
  (`AddChecked`, `AvgScale`, `DecimalAvg`), `internal/engine/exec/kernel/types.go`
  (`Accumulator.FinalSum`/`FinalAvg`/`FinalMin`/`FinalMax`),
  `internal/engine/exec/aggregate.go` (`outputSchema`, `minMaxOutputType`),
  `internal/planner/physical/plan.go` (`aggSpecOutputType`),
  `internal/worker/avg_fold.go`
