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

5. **Deliberate divergences from PostgreSQL: the rule, and where they are
   recorded.** Where PostgreSQL has a spelling, this engine answers what
   PostgreSQL answers or refuses loudly, never a plausible different value:
   *loud beats plausible*. A shape the engine cannot compute PostgreSQL's way
   is refused with PostgreSQL's SQLSTATE and sentence where PostgreSQL has
   one. A superset (an answer where PostgreSQL refuses by type or grammar, or
   a spelling PostgreSQL lacks, from DuckDB, Trino or an extension's own
   specification) is KEPT only when its value is meaningful and identical on
   every arm; otherwise it is REFUSED with PostgreSQL's code. That is the
   *arm rule*: a difference that exists on one arm (the single-process
   engine, the local fast path, a DAG arm) and not on another is a defect of
   that arm, labeled `engine` or `distributed`, never a position. A kept
   superset, a refusal, a value divergence and a documented gap are all
   recorded, and a gate that meets one configures its oracle to the recorded
   answer (item 3). It never exempts the query, and it keeps comparing
   everything else the row carries.

   The divergences are the [divergence catalog](0012-divergences/README.md):
   one file per family, one row per cell (the SQL, PostgreSQL 17.11's answer,
   this engine's answer, the SQLSTATE, the disposition, the date, the issue
   and the gate that pins it), with each family's mechanism stated above its
   table. The dated history (additions, amendments, closures, withdrawals,
   corrections) is the [amendment log](0012-amendments.md). The user-facing
   rendering is the [differences page](../postgres-differences.md), which
   carries every `kept superset` and `value divergence` row. A new divergence
   is a catalog row, a log entry and, for those two dispositions, a
   differences-page entry. Changing a row's disposition is an amendment.
   (Amended 2026-09-29, arc DS: this item held the entries themselves as
   4,295 lines of prose. They moved to the catalog and the log verbatim, and
   the catalog's README counts them.)

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
   order the two exact decimals — `expr.boxedPair`'s literal arm and
   `batch.CompareDecimalTexts`. (That carry-through was `expr.compareWithText`
   until #504 replaced it: it applied the exact reading to ANY string box
   against a literal's text, which read a genuine STRING column numerically.
   The rule it applied is unchanged; what selects it is now the operands'
   declared kinds.) An arithmetic-wrapped operand — `d + 0 = lit`
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

     **Every boxed site is now bound from the declarations, not from one
     call site.** (Amended 2026-08-24, #506.) `expr.bindDecimalCols` used to
     be called from `NewCmp` alone, so a direct `d1 op d2` got the exact
     Int128 comparison while the SAME two columns at a BOXED site — a simple
     `CASE d1 WHEN d2 THEN ...`, `d1 IS DISTINCT FROM d2`,
     `GREATEST(d1, d2)`, and a Cmp whose operand is a COMPOSITE like
     `GREATEST(d1, d2) = d2` — fell through `compare()`'s two-rendered-strings
     path and compared LEXICOGRAPHICALLY, the same defect #477 fixed for the
     direct comparison. Verified against live postgres:17-alpine on the
     `declit` fixture: `CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1` answered 0
     against PostgreSQL's 1, `GREATEST(d_2, d_4) = d_4` answered 167 against
     101, `LEAST(d_2, d_4) = d_2` 164 against 98, and
     `d_2 IS DISTINCT FROM d_4` 199 against 198.

     `expr.boxedPair` (`internal/engine/expr/boxed_pair.go`) is the binding.
     It classifies each operand's DECLARED kind — DECIMAL, a non-DECIMAL
     number, genuine text, or unknown — resolving a bare column from the
     batch and a composite (`GREATEST`/`LEAST`, `CASE`, `COALESCE`) from the
     join of the alternatives one of its values comes from, and then applies
     one rule per kind PAIR. The kinds are cached once settled, and a pair
     whose declarations can select no rule disarms itself, so the generic
     path's per-row cost stays one atomic load. Four sites hold one:
     `Cmp`'s generic arm, `Case`'s simple-CASE arm (one per WHEN),
     `IsDistinctFrom`, and `pickExtremum` (one operand per ARGUMENT, since
     the best-so-far moves between iterations). Nothing in it reads a box to
     decide which RULE applies — only to decide which side of the chosen rule
     holds this row's value, which is what item 8's boxed-value rule requires
     generally.

     **The classification covers BOTH arithmetic nodes, not only the typed
     one.** (Amended 2026-09-04, #849 residual / #555.) Exact
     fixed-point arithmetic boxes its result exactly as a DECIMAL COLUMN
     boxes one — the value's rendered text — so an arithmetic node is a
     `boxDecimal` operand whenever its exact arm resolved. `expr.BinOpNumeric`
     answered that from its mode; the GENERIC `expr.BinOp` did not, and it is
     the node that needed to, because it is where every operand with no typed
     protocol arrives: a negated column, a CAST, a scalar function, and a
     CHOOSING construct, none of which satisfies `Float64Expr` for
     `compileBinOp` to build the typed node from. So `(COALESCE(a, 0) + 1) > 1`
     compared `"1.00"` against `"1"` by BYTES and admitted the rows whose
     value is exactly 1 — 8 rows where PostgreSQL 17.11 answers 5 — and
     `GREATEST(COALESCE(a, 0) + 1, 2)` picked 2 over 13.75. Every boxed
     consumer was affected alike (`IN`, `BETWEEN`, `IS DISTINCT FROM`, a
     simple CASE, GREATEST/LEAST), and every producer that reaches this node:
     `-a + 1`, `CAST(a AS DECIMAL(9,2)) + 1` and `ABS(a) + 1` had the exact
     arm since #555 and were wrong at those sites for as long; the choosing
     constructs joined them when `0214d48b` gave them the exact kernel.
     The node's INT mode answers `boxNumber` for the same reason its typed
     sibling does, and its remaining modes stay unclassified on purpose: this
     node also evaluates date ± interval, and a shifted date is not a number,
     so declaring one would be a WRONG declaration rather than a missing one.
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
   - **The #463 refusal reaches the #465 boxed sites and a negated string
     literal.** (Added 2026-08-24, #505.) `CASE d WHEN 'abc'`,
     `d IS DISTINCT FROM 'abc'` and `GREATEST`/`LEAST(d, 'abc')` answered
     instead of erroring: the boxed sites' exact-text arm only fires when the
     literal already looks numeric (`Lit.Text` set by `compileLit`), so a
     non-numeric string never reached ANY refusal on these three sites and
     fell through to `compare()`'s ordinary string comparison instead.
     `expr.refuseArm` closes it: whichever operand resolves, in THIS batch,
     to a materialized DECIMAL column is checked against the other operand's
     literal text before the comparison runs, raising the same 22P02
     `decimalLitCmp.order` already raises for the direct-comparison shapes —
     so the refusal depends only on the column's REAL type, never on the
     operand's Go box, the way item 8's boxed-value rule requires generally.
     (#517 lifted the same question to bind time, where it also stops
     depending on a row and on operand order; this runtime half remains for
     the shapes the binder cannot prove.)

     `d = -'abc'` and `d = -'1e400'` were the same failure mode wearing a
     `UnaryOp`: a unary minus over a STRING literal was deliberately left
     unfolded and evaluated through the generic numeric-coercion path,
     which reads anything unparseable as the float64 zero — both matched
     the row holding 0.00, #463's exact failure mode on the one shape #463
     never touched. Compile-time folding (`compileWithCtx`'s `UnaryOp` case,
     `expr/compile.go`) now treats a QUOTED string literal the way an
     UNQUOTED one already was: a numeric-looking string folds into a
     negated `Lit` carrying its exact text — so `d = -'5.00'` enters the
     same `DecimalLiteral` path `d = -5.00` does, saturating past the
     carrier exactly as this item already specifies rather than matching
     the wrong row — and a non-numeric string is refused at COMPILE time.

     (Corrected 2026-08-24. That last sentence used to continue "before any
     row, batch, or short-circuiting conjunct exists to hide it", which
     described the refusal and not the QUERY. The refusal was raised at
     compile time and then SWALLOWED: the physical planner's six compile
     sites fall back to copying an input column when an expression will not
     compile, and only `expr.IsUnknownFunc` was exempt, so `SELECT -'abc'`
     came back as `column "-'abc'" does not exist in the input schema` with
     no SQLSTATE. A refused literal is now its own error TYPE
     (`expr.InvalidLiteralError`, SQLSTATE 22P02) and
     `expr.IsCompileRefusal` names both classes the planner must propagate;
     `TestRefusedLiteralReachesTheClientAsItsOwnError` gates the SELECT,
     WHERE, ORDER BY, GROUP BY and aggregate-argument forms. With that, the
     sentence is true of the fold — and of nothing else.)

     **The refusal is a PLAN-TIME check now, not a per-row one.** (Amended
     2026-08-24, #517.) It used to live inside the comparison, so it
     depended on two things a type rule may not depend on:

     - **A row.** `d = 'abc'` against an EMPTY table, or behind a conjunct
       no row survives to (`k > 100000 AND d IS DISTINCT FROM 'abc'`),
       answered zero rows instead of erroring, on this shape and on the
       original #463 shape alike: the comparison — and therefore the
       refusal — never ran.
     - **Which operand won.** `GREATEST`/`LEAST` compare (best-so-far,
       candidate) pairs, and a pair refused only when a DECIMAL column was on
       one side and the bad literal on the other. Which argument is the
       running best depends on the VALUES, so the SAME three arguments
       refused under one and answered under the other: `GREATEST(k, 'abc',
       d)` raised and `LEAST(k, 'abc', d)` returned a row.

     PostgreSQL resolves an unknown-typed literal's type from the column's
     DECLARATION and refuses at parse/bind time, independent of any row
     existing and of any operand order — verified live for all nine shapes,
     `=`, `<>`, IN, BETWEEN, the three boxed sites, and both extremum
     orders, each with the same 22P02 and the same message wadjet raises.

     `physical.checkLiteralTypes` is the check, in the AST binder
     (`validate.go`) that already resolves every column reference against the
     catalog before a plan exists. `colScope` carries which columns a BASE
     TABLE declares DECIMAL, and the refusal fires only when the column
     PROVABLY resolves to one in a CLOSED scope — the same conservatism the
     rest of the binder is built on, since a false positive breaks a working
     query. The runtime refusals STAY, for the shapes the binder cannot
     prove (an open scope, a derived table or CTE column, an expression it
     does not parse); both call `expr.IsNumericLiteralText`, so the two
     cannot disagree about which strings are numbers.

     **What this does NOT cover, deliberately.** The same silent reading of
     an unparseable constant as the type's ZERO is still live for the
     INTEGER and FLOAT families — `WHERE k = 'abc'` over a BIGINT column
     matches the rows holding 0, where PostgreSQL raises 22P02 "invalid
     input syntax for type bigint". That is #463's failure mode on the
     families #463 never covered, tracked as #536 and pinned in the wire
     corpus, and closing it means carrying the destination TYPE into the
     refusal rather than extending its timing. `'NaN'` is a third case:
     PostgreSQL's numeric HAS a NaN and wadjet's exact carrier does not, so
     `d = 'NaN'` is refused here and answered there (#534).

     **The refusal's cost is a plan-time cost, not a per-row one.**
     (Added 2026-08-24, from the #505 measurement.) The first version asked both
     of the refusal's questions on EVERY ROW — allocating a
     `kernel.DecimalLiteral` and re-walking the literal's digits to answer
     "is this a number", then re-resolving the column to answer "is this a
     DECIMAL" — which cost +42% on a simple CASE over a DECIMAL column,
     +37% on IS DISTINCT FROM, and +69% with 7x the bytes on an
     exponent-form literal. Both answers are fixed for the query's lifetime,
     which is why `decimalLitCmp.numeric` was already a cached slice rather
     than a per-row `Numeric()` call, and the same discipline now applies at
     the three boxed sites (`expr.refuseArm`, `caseArms`,
     `extremumRefusal`): the literal is judged once when the node is first
     evaluated, and the column's answer is cached the way
     `decimalLitCmp.notDecimal` caches its own. Residual overhead against
     removing the check entirely is inside the noise. A correctness fix that
     re-introduces a documented performance regression has not finished.

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
     expr_compare.go`'s `cmpFloat64Op`/`cmpFloat32Op`, `internal/engine/exec/kernel/
     compare.go`'s `ResolveFilterKernel`), the PRIMARY (non-spilled) GROUP
     BY/DISTINCT hash key (`internal/engine/exec/agg_group_key.go` and
     `internal/engine/exec/agg_key_encoding.go`'s
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
     function (`appendVectorKey`, agg_key_encoding.go) that #459 did not touch;
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

9. **Exact numeric aggregates: what MIN/MAX/SUM/AVG over a DECIMAL — and
   over an INTEGER — answer.** (Added 2026-08-23, #455; the integer half added
   2026-09-02, #784.) PostgreSQL's `min`/`max`/`sum`/`avg` over
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
   - **INTEGER inputs answer PostgreSQL's own result types, with one
     narrowing and one carrier limit.** (#784.) Taken from the live server:
     `pg_typeof(sum(int4))` is `bigint`, `pg_typeof(sum(int8))` is `numeric`,
     and `pg_typeof(avg(int2|int4|int8))` is `numeric`. Wadjet answered
     `double precision` for all of them, so `SUM` over a BIGINT column past
     2^53 lost integer digits before any client saw them. The rules now:

     - `SUM(int4-class) → BIGINT`; `SUM(int8) → DECIMAL(38,0)`, accumulated in
       the same exact Int128 carrier a DECIMAL sum uses; `AVG(any integer) →
       DECIMAL(38, AvgScale(0))`.
     - **The AVG scale is the DECIMAL rule's, not PostgreSQL's**, and for the
       DECIMAL rule's reason: PostgreSQL's numeric division picks a
       magnitude-dependent scale — sixteen fractional digits over
       `nation.n_regionkey`, none at all over a quotient already carrying
       nineteen integer digits — and a scale that depends on the values makes
       the same query over more rows change the type of its own output
       column. Wadjet answers at the fixed `batch.AvgScale(0) = 4`. Both are
       exact to the digits they keep and agree to `min(both scales)`: the
       same class as the DECIMAL AVG bullet above, and what the wire oracle's
       `SumAvgOverInteger` float-render pin cites.

       **RE-AFFIRMED 2026-09-02** after the arc-A earlier implementation pass re-opened it
       as a candidate defect and measured PostgreSQL 17 directly. The rule
       there is `select_div_scale`: at least sixteen SIGNIFICANT digits, so
       the FRACTIONAL digit count moves with the magnitude of the answer, and
       the output column's declared typmod is **−1** — unconstrained, no
       `(p,s)` at all. Measured: `AVG(c_i32)` over 5 000 rows answers
       `7497.6449875724937862` (16 fractional digits), `AVG(c_i64)` answers
       `2499158148.41289523` (8), `AVG` of three 1s answers
       `1.00000000000000000000` (20), and `AVG(numeric(18,4))` of 1 and 2
       answers `1.5000000000000000` (16) — so the INPUT scale is not what
       decides either. Wadjet answers `7497.6450`, `2499158148.4129`,
       `1.0000`, `1.5000` at its fixed scale 4.
       This is **not representable** on wadjet's model, not merely
       unimplemented: a column carries ONE declared scale on a 38-digit
       carrier, and sixteen fractional digits over a quotient with twenty-three
       integer digits needs thirty-nine. Adopting PostgreSQL's rule is a
       carrier-and-column-model change, not a constant. The piece that IS a
       real metadata divergence is the unconstrained numeric DECLARATION on
       the wire, which this ADR already tracks as #542. If anything here is
       worth doing it is RAISING `batch.AvgScaleIncrement` — a bigger fixed
       number of fractional digits, still fixed — and that is a benchmarked
       type-width change rather than a correctness fix.

     - **`(ohlcv(...)).vwap` takes AVG's scale, so it is NOT the written-out
       `SUM(price*volume)/SUM(volume)` digit for digit.** (#965, 2026-09-08.)
       The bar's `vwap` is a weighted MEAN and declares what `AVG(price)`
       declares; the hand-written quotient beside it declares what DIVISION
       declares. Measured on this engine over an INT8 price:
       `(b).vwap` = `14.5833` and `SUM(p*v)/SUM(v)` = `14.583333`; over
       `DECIMAL(18,4)`, `14.58333333` against `14.583333`. Over INTEGER
       columns the written-out spelling is INTEGER division and answers `14` —
       on PostgreSQL 17 too, measured, so that half is not a divergence from
       the server but a difference between two functions.

       Against the SERVER the divergence is the AVG bullet above and nothing
       more: PostgreSQL's `sum(v*w)/sum(w)` over the same rows answers
       `14.5833333333333333` (`select_div_scale`, re-measured 2026-09-08
       alongside `175::numeric/12` = `14.5833333333333333`,
       `1::numeric/3` = `0.33333333333333333333` and
       `1000000::numeric/3` = `333333.333333333333` — sixteen SIGNIFICANT
       digits, so the fractional count moves with the magnitude). Wadjet's
       `vwap` keeps AVG's fixed digits, and the two agree to
       `min(both scales)`.

       ADR-0035's first statement of this claimed "digit for digit" and was
       wrong; the corrected relation, and the gate that asserts it on int4,
       int8 and three DECIMAL prices rather than only on float8, are in
       ADR-0035 §Decision 6.

     - **A bar FIELD sends its real numeric typmod where PostgreSQL sends
       −1.** (#965, 2026-09-08.) Measured on 17.11 over the same rows:

           SELECT px FROM t                                  -> numeric(9,2)
           SELECT min(px) FROM t                             -> numeric
           SELECT date_bin(…) g, min(px) o FROM t GROUP BY 1  -> numeric
           SELECT (r).f1 FROM (SELECT date_bin(…) g,
                               ROW(min(px)) r … GROUP BY 1)   -> numeric

       PostgreSQL keeps a numeric's typmod for a BARE COLUMN REFERENCE and
       drops it for anything an aggregate produced — grouped or not, through a
       composite field path or not. Wadjet sends `(b).open` over a
       DECIMAL(9,2) price as typmod 589830 (= `numeric(9,2)`), in the grouped
       and the ungrouped spelling alike.

       It is the same family as #457/#458 and #542 and stops one step short of
       them: an aggregate's OWN DECIMAL result is marked `WireUnconstrained`
       here and sends −1 as the server does, but a FIELD PATH over a bar is
       not an aggregate call, so it keeps the declaration the plan derived.
       The VALUE is right on every path and the OID is PostgreSQL's; what
       differs is a modifier that says MORE than the server says, never less.

       Recorded rather than changed, and the reason is worth stating: the
       modifier is right about the data, and moving it to −1 is a change to
       what every ROW field path declares — not to this function. Gated as
       what this engine does in `pgwire.TestPGWireDeclaresABarFieldTheSameWith
       RowsAndWithout` and `server.TestTheBarDeclaresTheSameThingOnBothWire
       Doors`, so a move toward the server's answer moves those lines and this
       entry together.

       One consequence worth naming: because `pgwire.TypeMod` answers −1 for
       `Precision <= 0`, a LOST precision and an honestly-unconstrained
       numeric send the same four bytes. That is why earlier implementation's grouped-bar
       divergence — DECIMAL(0,2) on the DAG arms — was invisible on the wire
       and visible only in `wadjet.ColumnMeta.Precision`, and why the arm
       census asserts `(type, precision, scale)` rather than the typmod.
     - **A COMPUTED integer argument is declared by its own WIDTH**, the way a
       bare column is. (Amended 2026-09-03, #841; this bullet used to read
       "declared BIGINT, not numeric".) Wadjet declares every integer
       expression INT64 (ADR-0024's recorded divergence), so the declared
       TypeID cannot tell int4 from int8 — but the AST plus the column
       declarations can, and `physical.aggInputIsWideInteger` reads them: an
       expression that provably carries an int8-domain operand (an INT64
       column, an INT64 ROW field, an integer literal outside int4's range, or
       any of those inside arithmetic, a CASE arm or a choice function) gets
       PostgreSQL's `sum(int8) → numeric`; everything else keeps `bigint`.
       That keeps `SUM(CASE WHEN … THEN 1 ELSE 0 END)` — TPC-H Q12's shape and
       a BI staple, `bigint` on the live server — where it was, and closes the
       residual the next bullet used to record. A shape the walk cannot see
       through keeps the int4 reading, so nothing moves on a shape nobody can
       point at. A bare column is unaffected either way: it is typed from the
       column's real width.
     - **What is left of the narrowing's residual is an ERROR, not a wrapped
       total**, by the SUM-overflow rule three bullets up and for its exact
       reason. Since #841 the shapes whose width can be READ answer
       PostgreSQL's exact numeric instead — `SUM(-int8_col)` and
       `SUM(CASE … ELSE int8_col END)` were pinned as refusals and now answer
       — so what remains is a computed integer sum past 2^63 that the width
       walk could not see through. It has no int64 to land in; PostgreSQL's
       numeric answers it and wadjet's declared BIGINT cannot, so the query
       fails with SQLSTATE 22003 naming the aggregate. It was silent until 2026-09-02:
       over a column whose total is exactly 2^64, `SUM(b)` answered
       18446744073709551616 and `SUM(-b)` answered 0 — one question, two
       spellings, and no way to see which one was lying. Gated on all five
       execution arms by
       `coordinator.TestIntegerSumOverflowIsLoudOnEveryArm`, which asserts
       both halves: the wrapping spellings refuse AND the exact ones still
       answer.

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

10. **A network-literal comparison follows the ADDRESS's own order and
    PostgreSQL's `inet` rules — item 8's boxed-value rule applied to IPv6
    and CIDR — and a literal that names no address is a query ERROR.**
    (Added 2026-08-24, #492. Rewritten 2026-08-24: the first
    pass got the ordering RULE wrong for CIDR and invented a match-nothing
    answer for a literal it could not parse.) `tryNetworkLit`/
    `CmpNetworkLit` (`internal/engine/expr`) already pre-parsed an IPv4 or
    MAC literal into its column's raw int64 encoding at compile time, so
    ordering compared the address numerically. IPv6 and CIDR literals had
    no such preparser: `compileCmp` fell back to a plain `*expr.Cmp`, whose
    generic path compares the column's RENDERED TEXT lexically —
    `"2001:db8::9" > "2001:db8::10"` as text (`'1' < '9'` byte-wise), the
    opposite of the numeric truth, and not even a total order (a value can
    fail BOTH `<` and `>` against the same literal). CIDR was worse: even
    the KERNEL's scan-pushdown path (`ResolveFilterKernel`'s `TypeCIDR`
    case) was lexical, because the column stores CIDR as plain text
    (`parquet/schema.go`) with no raw-byte form to fall back on the way
    IPv6 already had.

    **The order is PostgreSQL's `inet`, not its `cidr`.** Those are two
    types there and only one of them can hold what wadjet's column holds:
    `'10.0.0.1/8'::cidr` is an ERROR in PostgreSQL ("Value has bits set to
    right of mask") while `'10.0.0.1/8'::inet` is an ordinary value, and
    wadjet's CIDR column is unvalidated text (`internal/storage/ingest`)
    into which host-bearing prefixes are routinely written — they are what
    most network telemetry carries. Choosing `cidr`'s semantics would mean
    declaring most of the real data invalid; choosing `inet`'s means every
    value has a place in the order. `inet` it is.

    `network_cmp_internal` (`src/backend/utils/adt/network.c`) compares, in
    order: the address FAMILY; the common bits under the SMALLER of the two
    prefix lengths; the prefix length; the FULL, UNMASKED address. Three
    consequences a simpler rule gets wrong, all verified against live
    PostgreSQL 17:

	'9.255.255.255/32' < '10.0.0.0/8'    — common bits decide before the mask
	'192.168.1.5/24'   < '192.168.1.0/32' — the MASK outranks the address
	'10.0.0.0/8'       < '10.0.0.1/8'    — host bits are KEPT, ordered last

    `kernel.CidrSortKey` is that order as a byte string —
    `[family][address masked to its own prefix][prefix length][full unmasked
    address]` — and is the ONE implementation both the kernel's
    scan-pushdown path and `expr.CmpNetworkLit`'s generic-evaluation path
    call, exported for exactly the reason its own doc comment gives: two
    structural parsers maintained separately is the two-path defect class
    this item closes, not a shape to reintroduce by duplicating it.
    `TestCidrSortKeyMatchesPostgresInetOrder` pins the whole order against a
    PostgreSQL-derived table of host-bearing and canonical values, v4 and
    v6, at mixed prefix lengths.

    **The first pass keyed the MASKED network alone**, which threw the host
    bits away: `10.0.0.1/8` and `10.0.0.0/8` became one value, so
    `WHERE c_cidr = '10.0.0.1/8'` answered a row holding a DIFFERENT
    address. Every value in the corpus was a canonical `192.168.N.0/24`, so
    no gate could see it; the fixture now mixes canonical and host-bearing
    prefixes at four mask lengths, with host-bearing addresses INSIDE
    networks the fixture also holds (`typematrix.cidrValue`).

    **A BARE address is a /32 or /128 host route**, which is what
    PostgreSQL's inet does with the same input (`'10.0.0.1'::inet =
    '10.0.0.1/32'::inet` is true). The first pass could not parse one, and
    answered `c_cidr = '10.0.0.1'` with a match-nothing kernel through the
    scan while the row-at-a-time path compared the text — so a WHERE clause
    and a SELECT list disagreed about the same row.

    **A literal that names NO address is a query ERROR — SQLSTATE 22P02 —
    never a value and never a match-nothing kernel.** This is #463's rule
    for DECIMAL, one type family over, and for the same reason: a
    match-nothing answer to `c_cidr <> 'garbage'` deletes every row of a
    query that cannot mean anything, silently. Both paths raise it: the
    kernel returns no kernel and `exec.networkConstError` turns that into
    the error (the mechanism `compareFilterDecimal` already used), and the
    row path raises from `CmpNetworkLit`'s CIDR/IPv6 arms for a literal that
    parses as some OTHER address kind, or from the binding `Cmp` already
    carries for a literal that parses as nothing at all. The lexical
    `genericFallback` is gone from both arms. The comment that claimed
    `parseIPv4ToInt64`/`parseIPv6ToRawString` already answered such a
    literal with a match-nothing sentinel was simply wrong — the first
    returns 0, which MATCHES the rows holding `0.0.0.0` — and TypeIPv4,
    TypeMAC and TypeUUID still take that silent path. That is the same
    defect one type over, filed as #519 rather than widened into this fix.

    **A malformed STORED value is UNKNOWN, not an error and not a text
    comparison.** The column is unvalidated, so a row can hold something
    that is not an address. It matches nothing for every operator, `<>`
    included — the answer a NULL row gets — on both paths. Raising instead
    would fail a whole query over one bad row in a column the engine never
    promised to validate; falling back to a lexical comparison on one path
    only (what `evalCIDR` did) is how one bad row split the two paths apart.

    **That UNKNOWN rule is a PREDICATE rule, not a value rule.** A malformed
    row is invisible to `WHERE c_cidr = anything` and `WHERE c_cidr <>
    anything`, but it is still an ORDINARY value everywhere a query retains
    or orders values rather than testing them against one: GROUP BY gives it
    its own group, ORDER BY gives it a definite position, and MIN/MAX can
    return it. `kernel.CidrOrderKey` is exactly this split, by construction —
    it is `CidrSortKey`'s `ok` collapsed to always succeed, falling back to
    the row's own raw text when the value does not parse, so "no defined
    place in `WHERE`'s order" becomes "the row's own bytes decide its place"
    the moment the consumer is a key or a comparator instead of a predicate.
    One consequence worth naming because it looks like a bug and is not:
    `MAX(c_cidr)` can return a value that no `WHERE c_cidr = '<that text>'`
    will ever match — a malformed value sorts (as its raw text, which a
    real address's inet-ordered key does not collide with except by exact
    byte match) and predicates refuse it. PostgreSQL has no equivalent case
    to verify against, because its `inet` input function rejects a
    malformed literal before a row can ever hold one; wadjet's column is
    unvalidated text, so this asymmetry is the shape the "don't fail a whole
    query over one bad row" decision above takes once #520 gave GROUP BY/
    ORDER BY/MIN-MAX their own inet-ordered key.

    **IPv6 against a v4-shaped literal is a FAMILY comparison.**
    PostgreSQL's inet puts every v4 address below every v6 one
    (`'255.255.255.255'::inet < '::'::inet`), including below a v4-MAPPED v6
    address, which it still calls family 6. `kernel.IPv6LitKey` keys a v4
    literal to the EMPTY string — shorter than, and a prefix of, every
    16-byte row value, so it compares strictly below all of them with no
    per-row re-keying. The kernel used to read that literal as its v4-mapped
    16 bytes, landing it in the MIDDLE of the v6 range, while the expr path
    fell through to a lexical text compare: two paths, two orders, neither
    PostgreSQL's.

    IPv6's ordinary case needed the same preparse CIDR did: the literal is
    the address's raw 16 bytes, a fixed-width big-endian encoding, so Go's
    own string ordering IS the address's numeric order.

    UUID needed no fix: its literal always zero-pads to a fixed 32-hex-digit
    form, so lexical order of that FIXED-WIDTH text happens to equal the
    address's own byte order — an accident of representation pinned by a
    test (`TestUUIDOrderingIsCorrectByHexAccident`) rather than relied on
    silently, because it does not generalize to IPv6's variable-width
    `::`-compressed form or CIDR's variable-width prefix notation, which is
    exactly why those two needed a real fix and UUID did not.

    **One key, every consumer of the comparison.** `IN` shares it
    (`kernel.inFilterKeyed`), because `c = 'X'` and `c IN ('X')` answering
    differently is the two-kernel version of the same defect. The row-group
    PRUNE does not, and cannot: the footer bounds are the address TEXT's
    extremes and the inet-order extremes are different rows, so TypeCIDR is
    withheld from pruning entirely — ADR-0018 §6, which is where that
    reasoning lives; restoring it needs an inet-ordered bound written at
    WRITE time, filed as #523.

    **Residual, closed 2026-08-25 (#520): `ORDER BY`, `GROUP BY`, DISTINCT,
    COUNT(DISTINCT), MIN/MAX and hash-join keys over a CIDR column used TEXT
    order.** The two engines agreed with each other there, so no two-path
    gate saw it, but PostgreSQL sorts those by inet order too — '10.0.0.1'
    and '10.0.0.1/32' are one value there, so text-order GROUP BY/DISTINCT
    answered TWO groups/values for a pair `=` already called one. Closing it
    needed an inet-ordered comparator in the sort kernels, the group/hash
    key (`appendColumnValue` and the spill/boxed key path), the
    declaration-resolved boxed comparator, MIN/MAX (both the batch kernel
    and `Accumulator.Merge`, which combines partials across a scan-batch or
    parallel-worker boundary and had the identical TEXT-order bug one level
    up), and BOTH partition routers — the distributed shuffle sink and a
    second, LOCAL one (`legacyCompositeHash`, the morsel-parallel
    aggregation router), which was the hardest of the sites to find because
    it only misroutes rows across a `workers > 1` boundary. `kernel.CidrOrderKey`
    is the one implementation every site now calls, the same breadth item
    8's float rule needed for its own primitive (`kernel.KeyFloat64Bits`).

    **Residual, closed 2026-08-25 (#546): the single-process SET OPERATION
    dedupped a CIDR column by its raw stored TEXT** while the stage DAG —
    which lowers a set operation to a `GroupByAll` aggregate, so #520's key
    already reached it — dedupped it by inet. The identical `UNION` answered
    4 rows locally and 3 on the DAG. `physical.keyValueText`'s
    `parquet.TypeCIDR` arm is the missing consumer, and it takes
    `CidrOrderKey` rather than `CidrSortKey` for the reason the split above
    gives: this is a KEY, so a value that names no address needs a position
    rather than a refusal, and discarding those to the empty string would
    dedup three DISTINCT malformed values into one member.

    **Residual, closed 2026-08-25 (#565): a column-to-COLUMN comparison was
    inet-ordered on the VECTORIZED kernel alone.** `WHERE c = d` answered
    PostgreSQL's 2 in process and 0 on the stage DAG, and — the sharper
    form, needing no second engine — one process selected two rows through
    the kernel and then said `SELECT c = d` was FALSE about those same rows.
    `expr.compare()`'s both-string fast path is what a projection and a later
    DAG stage's re-parsed filter reach, and it compared the stored texts.

    The fix is item 8's boxed-value rule applied to this family: the pair is
    bound from the two operands' DECLARATIONS (`expr/boxed_pair.go`'s
    `boxCidr`/`boxIPv6` kinds), which is the only place the answer exists,
    since a CIDR value and a STRING value are the same Go box. That reaches
    the three sites #506 needed for DECIMAL — a simple `CASE`'s operand, `IS
    DISTINCT FROM`, `GREATEST`/`LEAST` — which were wrong on BOTH arms and
    so invisible to every two-path gate.

    Two findings from sweeping the other network types on that shape:

    - **IPv6 column-to-column ORDERING had the identical split**, and is
      fixed with it. The kernel compares the stored raw 16 bytes;
      `ColRef.Eval` boxes the RENDERED text, and "2001:db8::9" sorts above
      "2001:db8::10" as text and below it as an address.
      `kernel.IPv6RowKey` re-keys the rendering back to the stored bytes —
      exactly, v4-MAPPED addresses included, which is why it is NOT
      `IPv6LitKey`: a LITERAL dotted quad is a v4 address and keys below
      every v6 row by family, while a STORED one is a v4-mapped v6 address
      and keys among them. IPv4, MAC and UUID were checked on the same shape
      and AGREE — the first two box the raw encoded int64, UUID by the
      fixed-width-hex accident recorded above — and are pinned that way.
    - **A malformed STORED value was compared by TEXT at both col-col
      sites**, the kernel included, contradicting this item's own UNKNOWN
      rule. `colColFilterCidr` keyed through `CidrOrderKey`, whose text
      fallback is right for a KEY and wrong for a PREDICATE — the split this
      item draws, missed at one site — so `WHERE c = 'garbage'` and
      `WHERE c = d` followed two different rules about the same bad row.
      Both take `CidrSortKey` and UNKNOWN now.

    **Open residuals, filed 2026-08-25.** Three places a CIDR value still
    does not get the order this item specifies. None is a predicate, which is
    why none of them is fixed here:

    - **#568: a ROW FIELD PATH is declared STRING, so `ORDER BY rw.c` sorts
      by text** — `9.0.0.0/8` LAST, where `ORDER BY rw` over the same values
      puts it FIRST. The sort is not what is wrong: `colRefDeclaredType`
      resolves the field name against the input's own COLUMNS, where it is
      not one, and never consults the parent's `parquet.Column.Fields`, so
      the projection keeps its STRING default and `ResolveSortCompare` is
      handed a STRING column. It is not a CIDR defect — `ORDER BY rw.n`
      sorts an INT64 field as text by the same mechanism — which is why it
      is its own issue rather than a line in this list. **CLOSED** (#568): a
      field path carries the field's declared type now, and the pin that
      recorded the wrong answer is replaced by
      `wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType`, which compares
      each field against a sibling FLAT column of the same type.
    - **#569: windowed MIN/MAX declares FLOAT64 and fails the query** for
      CIDR and seven other types, where the plain aggregate answers
      correctly. `exec.WindowMinMaxType`'s allow-list is deliberate — the
      window picks its answer with `compareAny`, which dispatches on the Go
      box and has no type tag to route a CIDR to `CidrOrderKey`, so the
      FLOAT64 declaration turns a silently wrong ordering into a loud
      failure. **CLOSED** (#569): the comparison was given the declaration
      (`exec.newBoxedCompare`), which is this item's rule again at a site
      that never had it, and `exec.WindowMinMaxType` now names every one of
      the 22 types — exactly as `exec.minMaxOutputType` does for the grouped
      form, which is what this ADR's item 9 already says of it.
    - **#523: the row-group PRUNE**, unchanged and recorded above.

    The two-path divergence this item closes was reproducible directly: the
    single-process engine answered 16 rows and the stage DAG answered 2 for
    the same `WHERE c_ipv6 < '2001:db8::10'` — both engines compile the
    predicate through the identical `expr.Compile` (the worker's
    `compileFilterExprs` calls it too, never a separate re-implementation),
    so the fix lives entirely in `internal/engine/expr` and
    `internal/engine/exec/kernel` and reaches both paths through the shape
    both already shared. `internal/oracle/typematrix`'s corpus, which had
    deliberately excluded an ordering literal comparison against IPv6/CIDR
    to avoid gating an already-known bug, now includes one
    (`litcmp_ord_c_ipv6`, `litcmp_ord_c_cidr`) plus the shapes the second
    pass needed (`litcmp_bare_c_cidr`, `litcmp_hostbits_c_cidr`,
    `litcmp_xfamily_c_ipv6` and their ordering forms).

11. **LIKE renders the column to TEXT for every type — the value's own
    printed form, identical at both evaluation sites — rather than refusing
    the way PostgreSQL does.** (Added 2026-08-24, #497. Scoped and corrected
    2026-08-24: the original claimed the rendering matches
    `CAST AS STRING` across all 22 types, which is true of the seven types
    the fix was about and false of DATE.)

    PostgreSQL's `inet`/`cidr`/`macaddr` — and its `date`, `numeric`,
    `integer` and `boolean` — refuse LIKE outright (verified live:
    `'10.0.0.1'::inet LIKE '10.%'` raises "operator does not exist: inet ~~
    unknown"). That is not a semantics PostgreSQL DECIDED for wadjet's own
    network types to disagree with (item 1's territory); it is PostgreSQL
    not having the "these types are text everywhere" contract #484 already
    built for them. **Wadjet renders and matches.** The reasons, in order:
    the rendering contract already exists for the six network types and
    UUID; the engine has answered `int_col LIKE '1%'` since before there was
    a decision to make, and turning that into an error is a breaking change
    with no correctness payoff; and the invariant that actually failed here
    was never "does this operator exist" but "does it read the right backing
    store, and does it answer the same at both sites".

    **What the text is.** The value's own printed form — `Vector.GetValue`'s
    rendering, which is what the projection shows: `2011-02-02` for a DATE,
    `10.0.0.1` for an IPv4, `1.0001` for a DECIMAL, epoch MILLISECONDS for a
    TIMESTAMP, `0.14285715` for a FLOAT32 (float32 shortest round-trip, not
    its float64 widening). For the six network types and UUID this is also
    exactly what `CAST AS STRING` and every scalar function argument produce.
    (Amended 2026-08-25, #521: it did NOT hold for DATE — `CAST(c_date AS
    STRING)` answered the epoch DAY, `15007`, where the projection and LIKE
    answered `2011-02-02` — and the measurement that closed it found the identical
    gap for FLOAT32, where CAST answered the float64-widened digits instead
    of the float32-shortest-round-trip form. Both were `expr.Cast.Eval`'s
    string-family case reading the operand through `ColRef.Eval`'s raw-box
    fast path (epoch-day int32 for DATE, float64-widened for FLOAT32)
    instead of the column's own rendering — the identical mechanism LIKE's
    operand had via `likeOperand`, which #497's measurement had already fixed
    for these same two types on the LIKE side and left CAST's separate,
    narrower `networkOperand` (IPv4/MAC only) unfixed. The two resolvers
    are now one function, `boxedTextOperand`, shared by both call sites —
    the two-implementation drift this ADR calls out elsewhere (`CidrSortKey`,
    `appendColumnValue`) for the same reason: two structural renderers
    maintained separately is how one gets fixed and the other doesn't. The
    claim now holds for every flat type PostgreSQL gives wadjet a printed
    form to agree on, verified against live PostgreSQL 17: `date::text` and
    a `real` column's `::text` match wadjet's rendering exactly.
    `wadjet.TestCastStringAgreesWithPostgresAcrossFixture` sweeps every flat
    type across the type-matrix fixture; item 11 below records LIKE's own
    sweep, `wadjet.TestLikeAnswersTheSameAtBothSites`, which already covered
    every flat type and is what caught FLOAT32 for LIKE in the first place.)

    **A FLOAT32 concatenated as TEXT is rendered at FLOAT64 WIDTH, and that
    is a recorded divergence.** (Added 2026-09-02, #609's measurement.) The
    paragraph above says a FLOAT32's text is `0.14285715`, its shortest
    round-trip, and that is what `CAST` and `LIKE` answer — they share
    `boxedTextOperand`, whose type list has FLOAT32 on it since #521. The
    FUNCTION-ARGUMENT path does not: `expr.ColRef.Eval` widens a FLOAT32 to
    float64 on the way out and the text kernels stringify that, so
    `CONCAT(c_f32, 'x')` and `c_f32 || 'x'` both answer
    `0.1428571492433548x` where PostgreSQL 17 answers `0.14285715x`
    (`(1.0::real/7.0::real)::text || 'x'`, verified live).

    It is recorded rather than left to be re-found because it is one type's
    text having TWO renderings inside one engine — the shape this item exists
    to close — and because the #609 split of `||` from `CONCAT` is where a
    reader will look for it. BOTH spellings answer alike, which is precisely
    what says the split did not cause it: the divergence is older than the
    split and belongs to the float32 renderer on the function-argument path,
    not to either concatenation kernel. Pinned as the arc-A census cell
    `#609/boundary_float32_concat_widens_on_both_spellings`, which asserts
    the two spellings AGREE and carries PostgreSQL's answer beside them, so
    the day either moves the cell fails. The fix is to render a FLOAT32
    argument at its own width where every other boxed type is already
    rendered (`FuncCall.Eval`'s `stringInputFuncs` rewrite, beside the
    IPv4/MAC/DATE arms), not in `fnConcat` or `fnConcatOp`.

    **Both sites must render alike, and two did not.** `ResolveLikeFilterKernel`
    (`internal/engine/exec/kernel/compare.go`) unconditionally read the
    column's `BytesData`, with no per-type dispatch at all — the same shape
    of gap `ResolveFilterKernel` (the `=`/`<`/`>` kernel) is explicitly built
    to avoid. TypeIPv4/TypeMAC/TypePort/TypeProtocol store into
    `Int64Data`/`Int32Data`, so this INDEXED AN EMPTY BACKING STORE — a panic
    that is not the one deliberate `FatalEvalPanic` shape the pipeline
    drivers convert back into a query error, so it re-raised untouched all
    the way up: a process killer, not a wrong answer. TypeIPv6/TypeUUID
    store into `BytesData` but as the address's RAW binary form, so the
    pattern silently matched nothing. `likeTextRenderer` now resolves a
    per-type row-to-text function once per column — the same
    resolve-once-dispatch-in-the-loop discipline every other kernel here
    follows — covering every one of the 22 types (a default arm renders any
    other type's own boxed value, never indexing `BytesData` on a column
    that does not have it).

    The row-at-a-time `expr.Like` path had the same class of gap for the
    four types `ColRef.Eval` boxes differently from `GetValue`. #497 closed
    two of them (TypeIPv4/TypeMAC, through the shared `networkOperand`
    resolver `Cast`'s string-family case already used) and left two open,
    which the measurement found: `c_date LIKE '20%'` matched 4949 rows through
    the scan and 83 through a projection — the epoch DAY, not the date —
    and `c_f32 LIKE '%1%'` differed by 237 rows, a float64-widened rendering
    of a float32. `expr.likeOperand` closes both, and
    `wadjet.TestLikeAnswersTheSameAtBothSites` sweeps EVERY flat type
    through both sites so a fifth type that starts boxing differently is a
    failing test rather than another quiet divergence. An enumerated list of
    "types that box differently" is the same shape of gap #497 was filed
    for; the sweep is what makes the list checkable.

    **Containers refuse, closed 2026-08-25 (#522).** ARRAY, ROW, MAP and
    VECTOR used to reach the default arm and match Go's own `fmt.Sprint` of
    the boxed value (`[1 2 3]`, `map[k0:0]`) — both sites agreed on it, so it
    was not a two-path divergence, but it was not a text form the engine
    produces anywhere else and not a contract: it would have changed if the
    boxing did. Of the two honest answers the open question named — define a
    real container text, or refuse the way PostgreSQL refuses its own
    composite/array types — the decision is REFUSE, on rule 1 above: this is
    not one of the seven types wadjet has its own reason to diverge from
    PostgreSQL for (they have no PostgreSQL equivalent at all), it is an
    ordinary composite/array value, and PostgreSQL's answer for `LIKE`
    against one is unambiguous (verified live: `ARRAY[1,2,3] LIKE '1'` and a
    composite-typed value both raise "operator does not exist: <type> ~~
    unknown", SQLSTATE 42883/undefined_function). Inventing a text form
    wadjet has never committed to anywhere else would be manufacturing a
    contract this ADR's own rules argue against creating casually.
    `kernel.ResolveLikeFilterKernel` returns nil for the four container
    types (`exec.LikeFilter` turns that into the 42883, the same shape
    `decimalConstError`/`networkConstError` already use for a different type
    family) and `expr.Like.EvalBoolNull` raises the same error from the row
    path (`containerLikeKind`/`raiseNoLikeOperator`) — both verified against
    live PostgreSQL 17, and `wadjet.TestLikeAnswersTheSameAtBothSites` (no
    longer skipping non-flat columns) and
    `wadjet.TestLikeAgainstContainerRefusesWithPostgresErrorCode` pin the
    SQLSTATE at both sites.

    **Both refusals are RUNTIME, not plan-time — the same open question
    #517 tracks for DECIMAL, one type family over.** `WHERE id > 100000 AND
    col = 'garbage'` answers 0 rows SILENTLY rather than raising, when no row
    satisfies `id > 100000`: row-group pruning (or simply zero matching
    rows) means the scan never delivers a batch to the `col = 'garbage'`
    filter at all, so `KernelFilter.Execute`/`expr.Like.EvalBoolNull` — and
    item 10's own `networkConstError`/`CmpNetworkLit` raise for a literal
    that names no address — never RUN for that column, and a refusal that
    only fires when data flows through it is not the same guarantee as one
    PostgreSQL's planner gives by type-checking the literal against the
    column's declared type before any row is read. #517 names the identical
    mechanism for DECIMAL's boxed CASE/IS DISTINCT FROM/GREATEST sites —
    those still ANSWER for a malformed literal reaching them, the same way a
    container or a malformed network literal still answers through a
    conjunct no row survives to. Neither this pair nor #517 is fixed here;
    #517's own "fix direction" (a plan-time check against the column's
    declaration, before any row is scanned) is the shape a fix for any of
    these would need to take, and it is one check for all of them precisely
    because the column's declaration is known before any type's differing
    runtime code path matters. (Unrelated to this pair, and mentioned only
    because it was found alongside them: #544, where `CAST(timestamp AS
    STRING)` and LIKE rendered epoch milliseconds rather than the instant
    pgwire renders. That one is FIXED — see the amendment at the end of item
    11 — and this sentence is left in place only so the pairing that found it
    still reads.)

    **Item 6's PLAN-TIME refusal has the identical gap, for the identical
    structural reason.** (Added 2026-08-25, #579.) `checkLiteralTypes`/
    `refuseLiteralAgainstColumn` (`internal/planner/physical/
    validate_literal.go`) ask `colScope` whether a column is DECIMAL — a
    plain BOOLEAN (`colScope.addQualifiedTyped`'s `isDecimal bool`,
    `internal/planner/physical/validate.go`), not the column's declared
    `parquet.TypeID` — so there is no way to ask the same question for CIDR
    or any other network type even in principle; this is not a rule that
    excludes them, it is a bool that only ever meant one type. A non-numeric
    literal against a CIDR column therefore never reaches item 6's #517
    fix and falls all the way back to THIS item's runtime-only story: it is
    evaluated as ordinary text at the three boxed sites (CASE, IS DISTINCT
    FROM, GREATEST/LEAST) and only refuses at the scan-filter runtime layer,
    and only once a row actually reaches it. Verified live against
    postgres:17 on a `cidrtest(id BIGINT, c CIDR)` fixture: `WHERE id >
    100000 AND c = 'zzz'` raises 22P02 there unconditionally and here only
    when a row satisfies `id > 100000`; `CASE c WHEN 'zzz' THEN 1 ELSE 0
    END = 1`, `c IS DISTINCT FROM 'zzz'` and `GREATEST(c, 'zzz')` all answer
    here and all raise there. Widening `colScope` to carry the column's
    declared type (or an enum covering DECIMAL/network/neither) instead of
    a bare bool, with a per-type literal validator alongside
    `expr.IsNumericLiteralText`, is the fix shape #579 records.

    **BYTES is bytea, and it is the one type where LIKE and CAST
    deliberately disagree.** (Added 2026-08-25, #570. Recorded as an open
    divergence when this item was written; closed the same day.)

    PostgreSQL's `bytea::text` is `\x` followed by lowercase hex, under the
    default `bytea_output = hex` — a setting `expr.pgcompat` already reports
    to a client that asks for it. Wadjet had THREE renderings of one BYTES
    value and none of them was that: the embedded projection handed back a
    `[]byte` (`Vector.GetValue`'s TypeBytes arm), `CAST AS STRING` and LIKE
    handed back the RAW BYTES as a Go string, and the WIRE handed back Go's
    `%v`, `[255 254 0 65]`, under OID **25** because `pgwire.pgTypeOID` had
    no BYTES case and `formatPgValueTyped` no `[]byte` case. `oidBytea = 17`
    existed in the tree and was used only for inbound Bind parameters and
    the `pg_type` catalog row.

    The CAST rendering was not only wrong, it was a HAZARD. For the four
    bytes `0xff 0xfe 0x00 0x41` the raw form is invalid UTF-8 and holds an
    embedded NUL. PostgreSQL cannot represent that in a text-format field at
    all — `text` rejects `\0`, so no PG server ever puts one inside a
    DataRow text field — and libpq TRUNCATES at it: `PQgetvalue` returns a
    NUL-terminated `char*`, so a length-aware client (pgx, JDBC) read four
    bytes and a strlen-based one read two. One query, two answers, decided
    by the client library.

    **What landed.** Rule 1 decides all of it, because PostgreSQL does give
    BYTES a printed form:

    - `pgTypeOID` returns **17 (bytea)**, `pgTypeSize` -1 and `pgTypeMod` -1
      (bytea is varlena and takes no modifier), and `pgFormatType` answers
      `bytea` so the catalog stops contradicting the wire — the same pairing
      #454 had to restore for DECIMAL.
    - `formatPgValueTyped` renders `\x` + lowercase hex, and
      `appendBinaryValue` writes the raw bytes, which IS bytea's binary form
      (`byteasend`). The binary arm is what the OID change makes
      load-bearing: under OID 25 the `%v` fallback was at least
      self-consistent, because the binary form of a text column is its
      bytes.
    - `CAST(b AS STRING)` renders the same `\x` hex, which removes the
      embedded-NUL hazard for free — hex is pure ASCII. This REVERSES the
      direction of the change that made CAST agree with `likeTextRenderer`
      by handing back the raw bytes; agreeing with each other was the right
      half, agreeing on the raw bytes was not.
    - A bytea **Bind parameter** decodes to BYTES. pgwire has no
      bound-parameter path below the parser, so Bind renders each parameter
      as a LITERAL, and it was writing the SPELLING of the bytes
      (`'\x6869'`) — a ten-character string compared against a two-byte
      column, matching nothing. `bindparams.decodeByteaText` reads both of
      `byteain`'s spellings (hex, and the escape form with `\\` and
      `\ooo`) and the binary form is the bytes themselves; a malformed
      spelling is an error, never a fallback to the raw characters.

    **LIKE does NOT follow the CAST, and that is the PostgreSQL answer.**
    `~~` over bytea EXISTS there and is BYTEWISE — verified live,
    `'\xfffe0041'::bytea LIKE '%A%'` is TRUE, matching the 0x41 BYTE and not
    a digit of the hex spelling. So PostgreSQL itself renders one way and
    matches another for this type, and `kernel.likeTextRenderer` keeps
    matching the raw bytes. The "identical at both evaluation sites" claim
    this item makes is about LIKE's two sites (the kernel and the row path),
    which still agree; the additional claim that CAST agrees with LIKE holds
    for every flat type EXCEPT this one, and only because PostgreSQL breaks
    it first. `wadjet.TestCastStringPinsPostgresRenderingPerType` pins the
    hex spelling and `TestCastStringAgreesWithPostgresAcrossFixture` builds
    the BYTES expectation from the projected bytes rather than from CAST, so
    an implementation and a comparator agreeing on the same wrong hex still
    fail.

    **The engine's own answers were already PostgreSQL's**, which is why the
    fix moved the wire and the CAST and nothing else: comparison and
    ordering over BYTES are bytewise, an unknown-typed literal beside a
    BYTES column is read as its bytes (`b = 'hi'` finds the same row on both
    engines), `LENGTH`/`OCTET_LENGTH` count octets, and GROUP BY, DISTINCT,
    joins and NULL handling all agree. That is a finding of the coverage
    below, not an assumption.

    **The coverage hole is closed, and it found a second defect.** `bytea`
    appeared NOWHERE in `benchmarks/` or `internal/oracle/` — the pg-oracle
    ran only over the TPC-H fixture, which has no BYTES column, so
    `EngineSemantics` had never compared the value and `WireProtocol` had
    never compared the OID. The fixture now has a `bytea_probe` table
    (`postgres_oracle_test.go`, PG side `bytea`, wadjet side BYTES) holding
    the empty value, four NULs, the invalid-UTF-8-with-embedded-NUL value
    above, ASCII text, single high bytes, a prefix pair, and NULLs. Over it
    run 33 gated semantics entries (plus the one pin below), 10 wire-metadata
    entries and 2 in the wire error list; the DuckDB arm loads the SAME rows
    — not a second copy of "the same" fixture — for 13 more BLOB entries. Standing that
    up immediately failed a query the type matrix had never asked:
    `ResolveColColFilterKernel`'s string arm listed STRING, IPv6 and UUID
    but not BYTES, and because two BYTES columns share a TypeID the
    mixed-type row-at-a-time fallback did not apply either — so
    `WHERE b_val = b_other` came back as "could not resolve kernel", the
    identical shape #477 found for two DECIMALs. This is the coverage-matrix
    argument in concrete form: the gap was not in what the gates ASSERT but
    in which types they had a value for.

    **What is not fixed, and is pinned rather than forgotten.** A BYTES
    LITERAL does not exist: `b_val = '\x6869'` in a hand-written statement
    compares six characters, where PostgreSQL's byteain reads two bytes
    (#582, `BytesEqHexSpelledLiteral`). The Bind path escapes this only
    because it has a declared parameter OID to decode against. And the
    scalar function layer has no BYTES notion at all — every function reads
    its operand through `expr.toString`, so `UPPER(b)` ANSWERS where
    PostgreSQL raises 42883 (#583, `ByteaTextFunctionOverBytes`).

    (Corrected 2026-09-18, arc EX.) The second half is no longer true:
    `UPPER(b)` is 42883 here now, from a plan-time check over the argument's
    declared type, and `ByteaTextFunctionOverBytes`'s pin in the wire arm's
    error list is deleted. The BYTES LITERAL half stands.

    (Corrected 2026-09-05, #583's second pass.) Two claims in this paragraph
    are no longer true and were stale when the fix landed: `b || b` returns
    **bytea under OID 17** now, not TEXT, and `OCTET_LENGTH(b)` declares
    **OID 23** (integer), not float8 — both measured at the tip on the wire.
    The raw-bytes-under-OID-25 hazard survives one shape over, `text || bytea`,
    which IS text on both engines and whose VALUE differs; that is recorded
    with its own measured table in the #583 entry of the divergence catalog
    ([text, bytes and collation](0012-divergences/text-collation.md)).

    **A TIMESTAMP renders its INSTANT through CAST and LIKE.** (Amended
    2026-09-02, #544.) The paragraph above says the text is "the value's own
    printed form — `Vector.GetValue`'s rendering … epoch MILLISECONDS for a
    TIMESTAMP". That sentence recorded the defect as the rule. `GetValue`
    keeps a TIMESTAMP as its raw epoch-ms int64 deliberately — it is also
    the GROUP BY key, the aggregate and window spill row encoding, the
    window comparator, and the row map an UPDATE re-ingests — but that box
    is a COMPUTE form, not a display one, and pgwire has converted it to
    PostgreSQL's `timestamp` on the way out since #321. So one column
    answered `2023-11-14 22:13:20` when projected over the wire and
    `1700000000000` when CAST, on one connection. Both text sites now render
    from the DECLARED type instead: `expr.boxedTextOperand` (CAST and the
    row-path LIKE) and `kernel.likeTextRenderer` (the single-process LIKE
    kernel, whose default arm was that `GetValue` call — which is why
    `c_ts LIKE '2023%'` was false on the single-process path and true on the
    DAG). `Vector.GetValue` is unchanged, and so is the embedded API's int64
    box for a projected TIMESTAMP: that is a third question — what the Go
    API hands back — bounded by the same five consumers, and not this one.

    **A SUB-SECOND timestamp prints the MINIMAL fraction, as PostgreSQL
    does.** (Recorded as a divergence 2026-09-02 by the measurement of #544;
    CLOSED 2026-09-04.) The server prints `2023-11-14 22:13:20.5` for `.500`
    and `.25` for `.250` — verified live — and `batch.FormatTimestamp` printed
    three digits always. It is the same function pgwire's send path calls, so
    the padding was what a CLIENT saw and not only what CAST answered; one
    change to that one renderer closed both doors, which is the property #544
    is about. `batch.TestFormatTimestamp` carries six fraction cells including
    the ones a trim could break (`.001`, `.999`, and every whole second, so a
    trim that left a bare point behind fails), and `pgTextOf` in
    `wadjet/cast_string_rendering_test.go` still answers PostgreSQL's rule
    written from the server rather than from this function, so the sweep's
    reference stays independent of the implementation.

    **DURATION is NOT fixed, and the reason is a type-model gap rather than
    an omission.** `CAST(c_dur AS STRING)` answers the raw nanosecond count
    (`1000000`) where PostgreSQL's `interval` — the type wadjet's DDL accepts
    as a synonym for DURATION — answers `00:00:00.001`. It is left alone
    because wadjet has no canonical TEXT for the type for a CAST to agree
    with: `pgTypeOID` maps DURATION to OID 25 (text) and the send path writes
    the nanosecond count, `docs/sql-reference.md` documents the type as
    "integer nanoseconds", and the INSERT coercion reads one. Rendering only
    the CAST would make the same column answer two ways again — precisely the
    defect this amendment closes for TIMESTAMP, re-opened one type over.
    Closing it means choosing DURATION's text form ONCE and moving the wire
    (OID 1186), the send path, the row reader, the INSERT coercion and the
    reference doc together. That is a lead, not a CAST-site fix.

12. **A set operation's result type is the COMMON type of its arms, and every
    arm is MOVED into it — never reinterpreted.** (Added 2026-08-25, #533.)

    The numeric ladder, verified against live postgres:17-alpine and pinned
    by `physical.TestSetOpWidenLadder`:

        INT32 → INT64 → DECIMAL → FLOAT64

    `numeric UNION ALL bigint` is **numeric** (an integer converts to numeric
    implicitly and not back); `numeric UNION ALL double precision` is
    **double precision** (float8 is the PREFERRED type of PostgreSQL's
    numeric category). Arm ORDER changes neither, there or here. Nothing
    outside the numeric family widens: rendering a number as text to make two
    arms line up would answer a different query.

    **A pair with NO common type is REFUSED at PLAN time, with SQLSTATE
    42804 and PostgreSQL's own sentence, on both execution paths and in
    either arm order.** (Amended 2026-09-04, #648.)

        UNION types numeric and text cannot be matched: result column "a"

    `UNION` there whatever the `ALL`, and `INTERSECT` / `EXCEPT` under their
    own names; measured live on 17.11 for numeric ∪ text, bigint ∪ text,
    double precision ∪ text, boolean ∪ bigint, uuid ∪ text, timestamp ∪ text
    and text ∪ bytea. Before this the DAG refused with a message of its own
    carrying no SQLSTATE and the single-process path let the arms meet at
    RUNTIME: `text UNION ALL numeric` answered a STRING column of rendered
    decimals silently, the same pair the other way round failed mid-execution
    with 22P02 on the first row of text that is not a number, and `numeric
    INTERSECT text` answered one row by comparing decimals against text as
    text. The column NAME rides after PostgreSQL's sentence rather than
    inside it: a set operation's arms correspond by POSITION, and the
    position is the localization PostgreSQL's own message does not carry.

    **The question asked is PostgreSQL's ALGORITHM, not a list of pairs.**
    (Rewritten 2026-09-04, earlier measurement of #648.) The first draft of this
    refusal exempted a pair only when the numeric ladder widened it, when the
    two mapped to the same `pgTypeName`, or when it was DATE/TIMESTAMP — and
    that hand-written list refused pairs PostgreSQL MATCHES: thirteen ordered
    column pairs and eight literal idioms that ANSWERED on the single-process
    path (which is also the coordinator's local fast path, so the default for
    a small query) became a plan-time 42804. The rule is the documented one
    ("Type Conversion → UNION, CASE, and Related Constructs"):

    1. every arm the same type → that type;
    2. an arm whose select item is an UNKNOWN-typed literal — a quoted string
       or `NULL` — has no type of its own and takes the others'. `SELECT
       c_ipv4 … UNION ALL SELECT '10.0.0.9'` is inet, `c_mac ∪ 'aa:bb:…'`
       macaddr, `c_date ∪ '2010-01-01'` date, `c_uuid ∪ '000…'` uuid,
       `c_dec ∪ '0'` numeric, `'1.5' ∪ numeric` numeric, `c_ipv4 ∪ NULL`
       inet, and two unknown literals resolve to text — all measured live on
       17.11;
    3. arms whose types are in different CATEGORIES have no common type, and
       the statement is 42804 in either order and for the whole node;
    4. within a category the result is the type every arm implicitly casts to.

    **The unknown-literal rule is applied at every door that DECLARES the
    column, not only at the one that executes it.** (Amended 2026-09-04,
    earlier measurement of #648.) There are three: the stage DAG's arm projection
    (`reconcileSetOpArmTypes` stamps the resolved type on the literal arm's
    spec), the DAG's declared output schema (`setOpDeclaredOutputSchema` skips
    unknown arms in its fold) and the single-process path's schema
    reconciliation (`setOpResolveUnknownLiteralArms`, ahead of
    `unifySetOpSchemas`). With the rule on the first two only, the same
    statement declared two different types depending on which door answered —
    which is the fast-path byte threshold, not anything the query says:

        SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL SELECT a FROM decpair
          single : STRING       (OID 25)    rendering 1.5
          DAG    : DECIMAL(9,2) (OID 1700)  rendering 1.50
          PostgreSQL 17.11: numeric

    A fourth door types a NESTED set-operation arm
    (`setOpNodeResultTypes`), which is what three arms with the literal FIRST
    lower to: without the mask it counted the literal's STRING as a type of its
    own, returned "unknown" for the whole inner node, and the DAG then REFUSED
    a query the single-process path answered.

    **A QUOTED literal whose resolved type cannot be built from TEXT is
    refused at plan time with 0A000.** (Amended 2026-09-04, earlier measurement of
    #648.) PostgreSQL parses such a literal with the resolved type's own input
    function, so `c_ts ∪ '2010-01-01 00:00:00'` is timestamp, `c_bool ∪ 'true'`
    boolean and `c_port ∪ 'notaport'` is 22P02 — all measured live on 17.11.
    Wadjet's literal arm produces the constant as a STRING box and that box
    reaches the result column's vector unchanged, so this works for exactly the
    types whose vector has a text arm. `batch.VectorAcceptsText` names them —
    STRING, BYTES, IPV4, IPV6, CIDR, MAC, UUID, DATE, DECIMAL — and it is held
    to what `SetValue` actually does by
    `batch.TestVectorAcceptsTextIsWhatSetValueDoes`, which writes a string into
    a vector of every one of the 22 types and compares; a list somebody keeps
    by hand is what cost this rule a measurement round already.

    The other nine — BOOL, INT32, INT64, FLOAT32, FLOAT64, TIMESTAMP, PORT,
    PROTOCOL, DURATION — failed the #361 silent-write guard with NO SQLSTATE
    ("batch: cannot store string into TIMESTAMP vector", which the pgwire door
    reports as XX000, "the server broke") mid-execution on the single-process
    path, and on the stage DAG after THREE retries of a deterministic parse
    failure. They are refused at PLAN time now, with the 0A000 the carrier gap
    takes and for the same reason: PostgreSQL answers the query and this engine
    does not yet. A bare NULL is unaffected — it has no text to parse and every
    vector takes one — and so is an UNQUOTED literal.

    Closing it means giving the literal its resolved type at PLAN time rather
    than at the vector: parse the text into the target type's own box in the
    arm's rows (the single path, beside `setOpLiteralRows`) and rewrite the
    arm's projection expression to the target's literal spelling (the DAG,
    beside `reconcileSetOpArmTypes`' stamp) — which also makes unparseable text
    22P02 the way PostgreSQL reports it, rather than a refusal of the whole
    statement.

    **The result's TYPMOD is the typed arm's, where PostgreSQL's is
    unconstrained.** `'1.5' ∪ numeric(9,2)` is `numeric` with typmod −1 there,
    so PostgreSQL renders the literal `1.5` beside the column's `2.00`; wadjet
    declares DECIMAL(9,2) and renders `1.50`. A wadjet DECIMAL vector has ONE
    scale — "unconstrained" is not a carrier it has — and the alternative,
    resolving one arm's scale away, is #532's truncation. It is the same answer
    `c_dec ∪ '0'` already gives with the typed arm on the LEFT. A declared-TYPMOD
    divergence, listed here with the declared-WIDTH one below.

    **The category table**, wadjet's declared type to the category PostgreSQL
    puts its WIRE type in, measured live:

    | category | wadjet types | note |
    |---|---|---|
    | N numeric | INT32 INT64 FLOAT32 FLOAT64 DECIMAL **PORT PROTOCOL DURATION** | PORT/PROTOCOL declare int4 and DURATION int8 (#834), so `c_port ∪ c_i64` is bigint ∪ integer there and answers |
    | S string | STRING | |
    | B boolean | BOOL | |
    | D datetime | DATE TIMESTAMP | `date ∪ timestamp` → timestamp, both orders |
    | I network | IPV4 IPV6 CIDR | `inet ∪ inet` → inet, `inet ∪ cidr` → inet, both orders, values preserved |
    | U other | BYTES UUID MAC ARRAY ROW MAP VECTOR | PostgreSQL puts bytea, uuid and macaddr in one category too, and with no implicit conversion between them its step 6 still fails — `uuid ∪ bytea` is "UNION could not convert type bytea to uuid" — so each matches only itself here |

    Two members of the int4 family that are not the same TypeID (a PORT beside
    an INT32, a PORT beside a PROTOCOL) resolve to **bigint** here where
    PostgreSQL resolves int4. No value moves — no integer this engine stores in
    an int4 carrier is outside int8 — and the engine has no CAST spelling that
    produces an INT32 carrier, so declaring int4 would put the type on a box
    that is not one. A declared-WIDTH divergence, listed here with the rest.

    **A pair PostgreSQL RESOLVES that this engine cannot CARRY is refused
    LOUDLY, with SQLSTATE 0A000 and never with 42804.** The refusal is this
    engine saying what it does not do yet — `feature_not_supported`, the class
    the order-by-unselected-aggregate refusal and ADR-0021 §1e's container
    refusals already use — because PostgreSQL ANSWERS the query. Unclassified it
    reached a client as XX000, which says the server broke.

    The pairs, in full — there are 14 ordered ones. The list is COMPUTED from
    the predicate by `physical.PlanContext.SetOpCarrierGapPairs`, and
    `coordinator.TestTheCarrierGapListIsTheCodes` READS THIS PARAGRAPH: it
    parses the count in the sentence you are reading and the type names in the
    table below, and fails when either disagrees with the computed set. An
    earlier draft named two pairs while the code refused twenty, and the draft
    after it said EIGHT while its own table listed fourteen — a paragraph a test
    does not read is a paragraph nothing keeps true.

    | pair | PostgreSQL resolves | why there is no carrier |
    |---|---|---|
    | DATE ∪ TIMESTAMP, both orders | timestamp | no DATE → TIMESTAMP promotion in the arm coercion |
    | IPV4 ∪ IPV6, IPV4 ∪ CIDR, IPV6 ∪ CIDR, all orders | inet | no inet-family carrier that holds two of them |
    | PORT ∪ DECIMAL, PROTOCOL ∪ DECIMAL, DURATION ∪ DECIMAL, both orders | numeric | DecimalCoercion reads an INT32/INT64 unscaled carrier and setOpDecimalTarget has no digit count for these |

    The INTEGER and FLOAT rungs DO carry PORT, PROTOCOL and DURATION — an
    earlier draft refused REAL beside them, which turned three shapes that
    answered PostgreSQL's `real` rows into hard errors. The single-process path used to
    ANSWER them, and the answer was CORRUPT: measured, `c_date ∪ c_ts` rendered
    every timestamp as `-2207656-04-19`, and `c_ipv4 ∪ c_ipv6` and
    `c_ipv4 ∪ c_cidr` rendered every row of the second arm as `0.0.0.0`. Both
    paths refuse now, with a message naming the two CARRIERS and saying that
    PostgreSQL resolves the pair and wadjet does not yet — silent wrong → loud,
    and never PostgreSQL's SQLSTATE for a query PostgreSQL answers. Closing it
    means a real DATE → TIMESTAMP promotion and an inet-family carrier, which
    is a typing feature and not a repair.

    **PostgreSQL's refusal wins over wadjet's, and every column is resolved
    before either is reported.** A column with no common type is a fact about
    the QUERY; a column whose common type this engine cannot carry is a fact
    about this engine. Reporting whichever the walk met first made the
    disposition depend on column ORDER — `SELECT c_date, c_dec … UNION ALL
    SELECT c_ts, c_str …` failed mid-execution with 22P02 where PostgreSQL is
    42804, while the same query with its two columns swapped refused at plan
    time.
    **When the common type is DECIMAL, the (p,s) is:**

    - **scale = max over the arms.** The only choice that moves no value; a
      narrower one DROPS digits the wider arm holds.
    - **precision = max over the arms of (precision − scale), plus that
      scale**, capped at 38. Rebuilt from the widest INTEGER part rather than
      taken as max(precision), because max(precision) is not a bound on the
      widened values: `DECIMAL(18,2)` alongside `DECIMAL(9,4)` needs 16
      integer digits at scale 4 — 20 — where max(precision) would declare 18
      and hand the parquet writer a leaf too small for its own values
      (ADR-0018 §4's encoding rule keys off precision). For every pair whose
      integer parts order the same way as their precisions the two rules
      agree, `(9,2)/(18,4)/(38,10)` included.
    - An INTEGER arm contributes its whole range's digits (10 for INT32, 19
      for INT64) at scale 0.

    **The arms are moved, and that is the whole point.** A DECIMAL value is
    an unscaled integer plus the column's declared scale (ADR-0018 §4), and on
    the stage DAG the two travel apart: each arm's task writes its own `.wshf`
    file carrying its own scale in the header, and the task that reads several
    such files writes ONE file under the schema of the first batch it saw.
    Reconciling only the `TypeID` reconciles nothing for two DECIMALs — they
    ARE the same TypeID — so the wider arm's unscaled integer was read at the
    narrower arm's scale and every value from it came back 100× too large,
    silently (#533). `exec.DecimalCoerce` multiplies the carrier instead, in
    the union arm's own fragment, before the rows meet. A CAST cannot do this
    job: the cast evaluator's DECIMAL destination produces a float64, which is
    the precision loss the exact carrier exists to prevent.

    **A value with no exact carrier at the output scale is an ERROR.** Same
    rule and same reason as SUM's overflow in item 9: a wrapped value is a
    different number wearing the right type, and nothing downstream can see
    that it is wrong. The bound checked is the DECLARED PRECISION (`10^p`),
    not the Int128 carrier's — they are different bounds and only the first is
    the type. A DECIMAL(38,2) whose unscaled value lands in `[10^38, 2^127-1]`
    fits the carrier and not the declaration, and admitting it would write a
    number the declared type cannot hold into a column the parquet writer
    sizes from that precision (ADR-0018 §4).

    **The 38-digit cap is a RANGE REDUCTION, and it costs answers** (#552).
    The cap can pull the output precision below what an arm's own values need,
    so a value BOTH arms held before the union becomes a hard failure after
    it: `DECIMAL(38,0)` holding `10^30` beside `DECIMAL(11,10)` resolves to
    `DECIMAL(38,10)`, and `10^30` at scale 10 needs `10^40`. PostgreSQL's
    numeric is unbounded and never reaches this. Worse, a filter or a `LIMIT`
    above the union does NOT rescue the query — the coercion runs in the arm's
    own fragment, ahead of the post-filter and ahead of the Singleton `LIMIT`
    stage — so `… WHERE v < 100` over that union fails on a row it does not
    want. This is the honest side of a 128-bit carrier and it is RECORDED
    rather than hidden; `TestSetOpDecimalCapIsARangeReduction` pins all three
    shapes so a future widening shows up as that test failing.

    **A computed DECIMAL expression is typed DECIMAL** (#555, closed across
    ADR-0024 and #695/#724). `d + d`, `COALESCE(d, d)`, `CAST(d AS DECIMAL)`
    and now arithmetic OVER a choice (`COALESCE(d, 0) * 2`) all declare and
    execute as exact fixed-point. The paragraph that stood here recorded the
    opposite and was right when it was written: those expressions resolved to
    FLOAT64 and answered float-rounded values where PostgreSQL answers
    `numeric`, and `COALESCE` over two DECIMALs failed outright at #361's
    store guard.

    **What is left is the CARRIER, and it is a recorded divergence rather
    than a gap** (ADR-0024 item 1). PostgreSQL's `numeric` is unbounded and
    has `NaN`/`±Infinity`; wadjet's is 38 digits of Int128 with neither. So a
    fold that lands on DECIMAL and meets a value outside it —
    `GREATEST(numeric(15,2), '1e39')`, or the same with `'NaN'` or
    `'Infinity'` — raises 22003 where PostgreSQL answers. 84 such composites
    are LISTED, with PostgreSQL's answer beside each, in
    `coordinator.nfCarrierRefusals`, and the gate asserts each still refuses,
    so the list fails if the carrier ever grows. Most of them appeared to
    agree with PostgreSQL before #724 for the wrong reason: the quoted
    literal made the whole call declare `text`, so its own characters went out
    unread.

    **The dedup key is the columnar one.** UNION, INTERSECT and EXCEPT decide
    membership by EQUALITY, so their key must agree with the comparator. On
    the DAG they already do — the operation lowers to a hash aggregate whose
    DECIMAL key is `batch.AppendDecimalKey` at the column's scale (#474), the
    canonical minimal-scale form — and after this the arms reach it at one
    scale anyway. The single-process path keys a boxed row by its RENDERED
    TEXT and needs its own fix (#499); the two paths are held to one answer by
    `internal/coordinator/setop_decimal_scale_two_path_test.go`.

    **Where an arm cannot be resolved, the query is REFUSED at plan time,
    naming the column.** (Amended 2026-08-29, #551.) Leaving every arm as
    written was the earlier answer and it is a SILENT WRONG ANSWER: an earlier
    draft said `shuffleWriter.writeChunk`'s scale check turns that residual
    into a failed task, and it does not — that check sees a SINGLE writer
    handed two scales, while in a union stage each arm writes its own
    consistent file and the reinterpretation happens in the downstream stage
    that reads several of them (ADR-0010 carries the corrected statement).

    The refusal covers TWO conditions, because the resolution can fail in two
    places: an arm typed DECIMAL whose `(p,s)` nothing resolved (#458's
    "unconstrained" sentinel is the reachable one), and an arm with NO resolved
    type at all sitting beside a DECIMAL arm — which is the condition the SQL
    shapes actually take, and which the first draft of this amendment did not
    cover. Both are witnessed end-to-end by
    `TestSetOpUnresolvableDecimalArmIsRefused`; an assertion over a hand-built
    arm state is not evidence that a refusal is reachable.

    **The single-process path ANSWERS what the DAG refuses here.** It re-reads
    each row's rendered text under a `max(scale)` fallback, which moves no
    value on that path, so an unresolvable arm is a divergence in WHICH
    ANSWER EXISTS, not in what the answer is. PostgreSQL answers too. That is
    the price of the refusal and it is recorded rather than hidden.

    Most of the shapes that used to reach it are resolved instead.
    `physical.setOpArmDecls` is the set operation's own view of an arm's
    columns:

    - A JOIN keeps a PER-SIDE answer. `inputColTypes` / `inputColDecimal`
      merge the two sides and delete any name they disagree about — right for
      a TypeID and exactly wrong here, since that disagreement IS the fact
      being reconciled — so each side's columns are keyed under its own
      relation names and the QUALIFIED spelling the projection carries
      resolves against the right one (#551). A DERIVED TABLE or CTE on one
      side keys under its SCOPE name the same way: without that its Project
      emitted only bare names, the merge deleted the contested one, and
      `SUM(v)` over `(SELECT s.dx FROM (SELECT id, dx FROM b) s JOIN a …
      UNION ALL SELECT dx FROM a)` answered 5151.0000 where PostgreSQL
      answers 102.0000.
    - A PROJECT is descended INTO, so a DERIVED-TABLE arm resolves through the
      names its subplan emits (#554), with `setOpArmComputedSource` rewriting
      a forwarded COMPUTED column into the expression that builds it. A nested
      SET OPERATION behind such a Project reads its own reconciled result
      types.
    - A ROW FIELD PATH resolves through the FIELD's declaration on ADR-0022's
      terms, and the spec carries the type because nothing downstream resolves
      a field path by name — it is materialized the way a computed expression
      is.
    - A numeric LITERAL arm takes its spelling's `(p,s)`. PostgreSQL types a
      constant numeric whenever it carries a decimal point OR an exponent
      (`1.23456`, `1.`, `1e2`, `1.5e-2` — verified live against 17.11 with
      `pg_typeof`; there is no float8 constant syntax) and types an INTEGER
      constant numeric once no integer type holds it. The arm's expression is
      REWRITTEN to the literal's plain decimal TEXT, because the evaluator
      folds a numeric literal into a float64 and `1234567890123456.78` is not
      one: declaring DECIMAL over that box would put an exact type on an
      already-rounded number. `litDeclType` is scoped to the arm, because the
      type of a literal inside an ARITHMETIC expression is decided with
      ADR-0024 item 3's decimal arithmetic. The single-process path builds the
      literal arm's vector from the declared-type layer and so still resolves
      float8 there — the values agree wherever float64 holds them, the wire
      OID does not, and closing that half is the literal `(p,s)` work in
      `expr`.

      **Amended 2026-09-04 (#665, #683): the single-process path resolves it
      too.** The set-operation adapter restates a literal column as the DECIMAL
      its spelling names and replaces its box with the literal's plain decimal
      TEXT before the arms meet, which is the shape every reader below already
      expects from a DECIMAL — `batch.FromRowsChecked` parses it at the
      resolved scale and `setOpCheckedDecimalText` range-checks it, so this
      path also raises the 22003 the DAG raises for a literal the union's own
      type cannot hold (item 7). `SELECT a FROM t UNION ALL SELECT
      1234567890123456.78` came back exact from the DAG and
      `1.2345678901234568e+15` here; both are exact now. What is still float8
      is a literal typed by something OTHER than the arm — inside a DERIVED
      TABLE, in arithmetic — which is the declared-type layer's rule and is
      pinned in `coordinator.TestANumericLiteralSetOperationArmIsExactOnBothPaths`.
      A literal NO DECIMAL this engine declares can hold — more than 38 digits
      — is **22003** beside an arm whose values are exact, and not the float8
      it used to silently fold to: `SELECT a FROM t UNION ALL SELECT
      123456789012345678901234567890123456789.5` answered
      `1.2345678901234568e+38`, a rounded number under an exact type, on both
      paths. Beside a FLOAT arm PostgreSQL resolves double precision and that
      float8 IS the answer, so the refusal is scoped to an exact result
      (earlier measurement of #683).

    **A computed DECIMAL expression does NOT reach the refusal**, and saying it
    did was wrong in both directions. `d + d` and `COALESCE(d, d)` are declared
    FLOAT64 by the arithmetic rule that has not landed yet, so the pair
    resolves FLOAT64 and the query answers with float-rounded values;
    `CAST(d AS DECIMAL(12,3))` is declared STRING by `inferCastType` and meets
    the LADDER's refusal ("the arms disagree on the type … and neither widens
    into the other"), not this one. Both are #555's typing gap, and the rung
    is not what has to change.

    **Two carrier properties are deliberate divergences, not defects.** A
    wadjet DECIMAL column has ONE declared scale, so the narrow arm's rows
    render with the unified scale's trailing zeros (`12.7500` beside
    `12.7501`) where PostgreSQL's variable-scale numeric prints `12.75` — the
    same number, the same row set, the class item 9 already records for AVG's
    digits. And PostgreSQL declares a cross-scale set operation's numeric
    result UNCONSTRAINED on the wire (`\gdesc` says `numeric`, typmod −1,
    where a single-arm `SELECT` of the same column says `numeric(9,2)`);
    wadjet still declares a real (p,s) there. That is wire METADATA only and
    is tracked separately (#542), not a value difference.

    **The single-process path does not yet resolve two different TypeIDs**
    (#541): it builds the result under the FIRST arm's schema, so an integer
    arm under a DECIMAL first arm reads as an unscaled carrier (1 becomes
    0.0001) and a DECIMAL arm under a FLOAT64 first arm fails the store
    outright. `unifySetOpSchemas` should delegate to `setOpWiden` and
    `setOpDecimalTarget` rather than keep its own rule, so the two paths
    cannot drift on what the output type IS. It also SATURATES rather than
    failing when the value it re-reads has no Int128 at the result scale
    (#553) — `batch.DecimalTextAt` returns `Int128Max` with a `Sat` flag the
    caller discards, so `10^30` comes back as
    `17014118346046923173168730371.5884105727`. Saturation is right where it
    was built, in #462's COMPARISON path, where an out-of-range literal
    genuinely orders above every value the column holds; as a stored VALUE it
    is a lie, and the rule above (a value with no exact carrier is an error)
    is what the value-producing callers owe.

13. **A QUOTED literal meeting a NUMERIC column is coerced with THAT COLUMN'S
    OWN INPUT FUNCTION — one rule, parameterized by the column's TypeID, at
    every comparison site and on both execution paths.** (Added 2026-08-29,
    #646; closes #634 for the integer family.)

    PostgreSQL types an unknown-typed literal FROM the operand it meets and
    then runs that type's input function over the text. There is no widening
    anywhere in it. Read off `EXPLAIN VERBOSE` on postgres:17-alpine over a
    `real` column:

        r = '3.1'                  ->  (r = '3.1'::real)
        r IN ('3.1')               ->  (r = '3.1'::real)
        r IN ('3.1','7.1')         ->  (r = ANY ('{3.1,7.1}'::real[]))
        r BETWEEN '3.1' AND '100'  ->  (r >= '3.1'::real) AND (r <= '100'::real)
        CASE WHEN r < '3.1'        ->  (r < '3.1'::real)
        CASE r WHEN '3.1'          ->  CASE r WHEN '3.1'::real
        GREATEST(r, '3.1')         ->  GREATEST(r, '3.1'::real)
        NULLIF(r, '3.1')           ->  NULLIF(r, '3.1'::real)
        r IS DISTINCT FROM '3.1'   ->  (r IS DISTINCT FROM '3.1'::real)

    **That is the OPPOSITE direction from item 8's unquoted literal, and both
    are PostgreSQL's.** An unsuffixed decimal constant is `numeric`, which has
    no `real <op> numeric` operator to resolve to, so the comparison goes
    through float8 and the COLUMN moves (#631). A quoted one is `unknown`,
    which is coerced directly, so the LITERAL moves. Over a column holding
    `real(3.1)`, `r = 3.1` selects nothing and `r = '3.1'` selects that row —
    one number, two spellings, two predicates. Both spellings are in the
    oracle corpus side by side for exactly that reason, and the two kernels
    stay separate: `kernel.ResolveFilterKernel` picks between them on the
    constant's Go box, a `string` for the quoted spelling and a float64/int64
    for the numeric one. Reading the box is right HERE and nowhere else — item
    8's caution is about a VALUE's ORDER, where a box cannot tell a DECIMAL
    from a STRING; "which literal did the user write" is the one thing the box
    does carry and the declaration does not.

    **What this replaces is a silent zero.** `kernel.toFloat64` has no string
    arm at all, so every quoted constant against a FLOAT column read as 0.0:
    `real = '3.1'` matched the row holding 0.0, `real = 'abc'` matched it too,
    `real IN ('3.1','7.1')` matched nothing, and `f > '-Infinity'` asked
    `> 0.0` and dropped every negative row. That is #463's failure mode on the
    type family #536 (integers) and #574 (BOOL) had already closed, and the
    boxed sites — `NULLIF(int_col,'abc')`, `int_col IS DISTINCT FROM 'NaN'`,
    `CASE WHEN int_col < 'NaN'`, `GREATEST(int_col,'NaN')` — answered every row
    for the integer family too, because `expr.refuseArm` and
    `extremumRefusal` tested `batch.TypeDecimal` alone.

    **The accept-sets, live from postgres:17-alpine.** They differ, and every
    difference is observable:

        text      bigint            real                numeric
        '3.1'     22P02             3.1                 3.1
        '1_000'   1000              22P02               1000  (wadjet 22P02, #634)
        '0x1A'    26                26                  26    (wadjet 22P02, #634)
        '0o17'    15                22P02               15    (wadjet 22P02, #634)
        '0x1p3'   22P02             8                   22P02 (wadjet 22P02)
        'NaN'     22P02             NaN                 a BOUND (ADR-0024 item 6)
        '+NaN'    22P02             NaN                 22P02
        '1e400'   22P02             22003               a very large number
        '1e39'    22P02             22003               a number
        '7e-46'   22P02             22003 (underflow)   a number
        ''        22P02             22P02               22P02

    (Corrected 2026-09-28: the `'0o17'` numeric cell read 22P02; PostgreSQL
    17.11 answers 15, `'0o14'::numeric` 12, measured by arc SM.)

    - **INTEGER** is PostgreSQL 16's `pg_strtoint*`: C whitespace trimmed, an
      optional sign, `0x`/`0o`/`0b` radix prefixes, underscore separators
      between digits, and leading-zero DECIMAL (`'007'` is seven, not fifteen).
      Neither Go base matches it — base 10 refuses the radix forms and base 0
      reads `'017'` as octal — so `kernel.parseIntText` is a dedicated parser.
      Its one asymmetry is PostgreSQL's: an underscore may not be first in a
      decimal (`'_1000'` is 22P02) but MAY be first after a radix prefix
      (`'0x_1A'` is 26), because PostgreSQL's own source puts the
      "not first" check in the decimal branch alone. Refusing these was a
      PG-superset regression (#634); it is closed.
    - **FLOAT** is `float4in`/`float8in`, which are `strtod` plus PostgreSQL's
      special spellings. So C99 HEX floats are values (`'0x10'` is 16,
      `'0x1p3'` is 8, `'0x.8p1'` is 1) and underscores are NOT (`'1_000'` is
      22P02 there even though the integer and numeric inputs take it).
      Go's `ParseFloat` disagrees on both and on a third point — it is silent
      about UNDERFLOW, answering a plain 0 for `'1e-400'` where PostgreSQL
      raises 22003 — so `kernel.FloatLitText` supplies the binary exponent Go's
      hex syntax requires, refuses underscores, and decides underflow from the
      DIGITS. `real`'s range boundary is its smallest DENORMAL, PostgreSQL's
      own: `'1e-45'` is a value and `'7e-46'` is 22003.
    - **DECIMAL** keeps its own grammar (ADR-0024 item 6) and is the only one
      with no RANGE failure: a literal past the Int128 carrier SATURATES into
      its place in the order rather than erroring (#462).
    - **PORT and PROTOCOL** read their OWN input function, not the `integer`
      they declare on the wire. (Added 2026-09-18, #1137, closing arc NT's
      deferral 1.) PROTOCOL takes the IANA NAME `protocol_name()` prints —
      `'udp'` is 17 — and neither takes int4's radix prefixes or underscores,
      which the writer refuses; the range is the TYPE's, 0..65535 and 0..255.
      They sat in the int32 arm because they are CARRIED in an int32, which is
      a storage fact and not a grammar one, so `WHERE c_proto = 'udp'` was
      22P02 while `CAST('udp' AS PROTOCOL)` answered 17, and
      `WHERE c_port = '0x1bb'` MATCHED port 443. `kernel.NetworkIntLitText` is
      the one reader, over `parquet.NetworkTextValue` — the same function the
      CAST and every writer door read, which is what makes the one-grammar
      claim hold at the comparison door as well.

      The refusal NAMES `integer` at every one of the twelve doors, which is
      the type these columns declare on the wire (OID 23, #834) and the only
      name a client can resolve in pg_type; `port` and `protocol` resolve to
      nothing there. The RANGE refusal deliberately keeps its own sentence —
      `PORT value 70000 out of range [0, 65535]` — which names the bound and
      says more than PostgreSQL's shape would. Arc EX's earlier measurement (N1)
      measured the split: eleven doors said `integer` and the writer said
      `port`.

    **Two SQLSTATEs, and they are different answers.** 22P02 for text that
    names no value, 22003 for a number the type cannot carry. The wording
    differs by family too, and it is reproduced rather than tidied: the integer
    inputs prefix the literal (`value "3000000000" is out of range for type
    integer`) and the float ones do not (`"1e400" is out of range for type
    real`). A QUOTED literal's message names its TEXT VERBATIM, where a
    numeric->real cast names the numeric's DIGITS — so `real IN ('1e40',3.1)`
    says `"1e40"` and `real IN (1e40,3.1)` says the forty-one digits, both
    verified live.

    **A CALL is resolved by its NAME and its ARGUMENTS, and every failure of
    that is 42883.** (Added 2026-09-18, #1053 / #1056 / #583.) PostgreSQL
    reports the wrong COUNT, a number in a text position and a bytea in a
    text-only position with the identical code and message shape —
    `function upper(unknown, unknown) does not exist`,
    `function upper(integer) does not exist`,
    `function upper(bytea) does not exist` — because they are one failure:
    no overload takes these arguments. This engine answered all three. The
    registry recorded no arity, so every body read `args[i]` defensively and
    `semver_cmp('1.0.0')` answered NULL while `upper('a','b')` answered 'A'
    with the extra argument dropped; a numeric literal reached a STRING vec
    kernel with no text arena and took the query to XX000; and a BYTES operand
    was read through `expr.toString` as whatever text those bytes spell. Where
    the check lives, and what keeps the table honest, is
    [ADR-0038](0038-a-call-is-resolved-by-its-name-and-its-arguments.md).

    `expr.Signature` declares the arity of every registered builtin — the
    table is closed against the registry in BOTH directions — and the argument
    DOMAIN wherever PostgreSQL restricts one, in three values: any, text, text
    or bytes. `expr.RefuseUnresolvableCall` is the check, run from the binder's
    own walk so every arm reaches ONE answer, with
    `expr.compileFuncCallNamed` as the backstop for the doors that walk does
    not see.

    **What is deliberately NOT refused, and why.** A COLUMN of a non-text type
    in a text position: a DATE, a TIMESTAMP, an IPV4, a MAC and an integer
    column are RENDERED as their text before a string function reads them
    (`expr.stringInputFuncs`, #273/#500/#544/#568), so `UPPER(mac_col)` and
    `SUBSTR(date_col, 1, 4)` answer here where PostgreSQL raises 42883. That is
    the SUPERSET class this ADR already records, and it is what the
    network-analytics shapes are built out of; narrowing it would delete a
    documented feature rather than close a defect. A numeric LITERAL in the
    same position is 42883 on both engines, because there the engine answered
    an internal error rather than a value. BYTES is the one declared type that
    IS refused, because reading bytes as text is a wrong VALUE with #570's
    embedded-NUL hazard behind it.

    A second exclusion is mechanical: `batch.TypeBool` is ZERO, so a
    `parquet.Column` carrying no type information is indistinguishable from one
    declaring boolean, and a declaration layer that answers Decided from such a
    column would hand this check a boolean for every column it knows only the
    name of. The exclusion is therefore the SHAPE of the column-side test in
    `expr.RefuseUnresolvableCall`: it refuses on a POSITIVE identification —
    `ArgText` meeting `batch.TypeBytes`, or `ArgBytes` meeting
    `batch.TypeString` — and never on "this is not text", so a column that
    answers `Decided(bool)` is never refused. `UPPER(bool_col)` answers and
    `UPPER(TRUE)` is 42883, because a LITERAL's type is syntactic. (This
    paragraph named `expr.trustedArgType` until 2026-09-18; no such symbol
    exists, and arc EX's earlier measurement caught it — N2.)

    **The CAST to an integer type is TWO casts, and the operand's DECLARATION
    chooses.** (Added 2026-09-18, #1141.) `'2.5'::integer` is 22P02 — int4in
    has no fractional part, and so is `'2.0'`, `'26.7'`, `'-0.4'` and `'1e3'`,
    measured — while `numeric_col::integer` ROUNDS half away from zero and
    `float8_col::integer` rounds half to EVEN. A DECIMAL and a STRING arrive at
    the evaluator in the SAME Go box, so choosing from the box gave every
    quoted fractional literal the rounding cast: `CAST('2.5' AS INTEGER)`
    answered 3 and reached REST that way through `INSERT … SELECT`.
    `expr.castOperandDeclaresText` decides from the EXPRESSION — a quoted
    literal, a STRING column, a cast to a text type, a call whose registered
    return type is fixed STRING, or a CASE/COALESCE all of whose arms are one
    of those — and a text operand reads `castTextToInt`, the destination's
    input function and nothing else. There is no float fallback: the one that
    stood there claimed the server rounds `'26.7'::integer` to 27.

    **One predicate serves every site**, which is the property rather than the
    coverage: `kernel.QuotedLitStatus(typ, text)` is read by the plan-time
    refusal (`physical.refuseLiteralForType` via `expr.RefuseNumericLiteral`),
    the vectorized kernel's scalar and IN arms, the row-at-a-time
    `exec.ColumnCompareLit`, the boxed sites' refusal masks
    (`expr.litRefusalMask`), and the row-group prune
    (`kernel.StatsDomainValue`, which now converts a quoted literal at the
    column's own width instead of handing the prune layer a Go string it
    declines). A query refused at one site and answered at another is the
    two-path defect class the refusal exists to close.

    **The refusal is not a property of a row.** PostgreSQL coerces at parse
    analysis, so an unreachable conjunct (`r_key < 0 AND r_val = 'abc'`) and
    one that only ever meets NULLs still error. That is why the plan-time
    binder carries it and the runtime refusals are the backstop for the shapes
    the binder cannot prove — #517's rule, one type family wider. NULLIF joins
    GREATEST/LEAST at that binder, because its equality test is the same
    question their ordering is.

    **Inside a COMPOSITE the type is the CALL'S, folded once over every
    argument.** (Added 2026-08-29 from this item's own measurement.)
    GREATEST/LEAST, CASE and COALESCE resolve one type through
    `select_common_type` and coerce the unknown-typed literal to THAT, so
    `GREATEST(bigint, '3.1', double precision)` is a double comparison and
    answers where the bigint input function would raise 22P02. Three things
    have to read that one fold or they disagree with each other: the REFUSAL
    (`extremumArms.checkRefusal`), the COMPARISON of every (best, candidate)
    pair — which is pairwise and must NOT use the pair's own types — and the
    VALUE, because the argument that wins comes back at the call's type and a
    quoted literal arrives as a Go string. Reading each argument's own type
    instead produced all three failures at once: a PG-superset regression
    (`GREATEST(k, '3.1', d)` refused), a silent width error
    (`GREATEST(r, '16777217', d)` read at real width), and a crash (the
    literal's string stored into a FLOAT64 vector).

    A kind that has NO declared type must answer "no rule" rather than fall
    back to the ROW'S BOX. `COALESCE(real_col, 0)` boxes an int64 on the row
    where the column is NULL and a float64 on every other, so a box-driven
    reading coerced the literal with the INTEGER input function on one row and
    the double one on the next — `COALESCE(r,0) = '3.1'` raised 22P02 with the
    NULL row present and answered zero rows without it, where PostgreSQL
    resolves real once and answers one row. Where the fold cannot be made —
    an argument this layer cannot type — only a literal EVERY numeric type
    refuses may raise, because a partial fold is a LOWER BOUND and a lower
    bound is what refuses at the wrong width.

    **DECIMAL is a rung of that fold, and a NUMERIC CONSTANT arm carries its
    own type into it.** (Added 2026-08-29 from this item's second measurement.) The
    ladder is `select_common_type`'s, the same one item 12 pins for set
    operations, with `float4` where live `pg_typeof` puts it:

        INT32 < INT64 < DECIMAL < FLOAT32 < FLOAT64

    Leaving DECIMAL off it was one defect with two faces. A composite holding a
    DECIMAL column could not fold at all, so each pair kept its own type and
    `GREATEST(bigint, '3.1', numeric)` asked BIGINT's input function for a
    literal PostgreSQL reads as numeric — a refusal where PostgreSQL answers.
    And a NUMERIC-typed CONSTANT arm — an unsuffixed literal with a point or an
    exponent, which PostgreSQL types `numeric` — made the join answer "no kind
    at all", so `COALESCE(real_col, 0.0) > '9'` fell through to the generic
    comparison and ordered the rendered number against the literal BYTEWISE.
    The two predicates the fold has to keep apart are visible in one pair:
    `COALESCE(real, 0.0)` is REAL and `COALESCE(bigint, 0.0)` is NUMERIC.

    Only the operand FACING the quoted literal is retyped to the fold. A pair
    of typed operands already has each side's own rule, and retyping both sent
    an integer box and a DECIMAL's text to the two-DECIMALs comparison, which
    needs two strings — so it declined and the extremum picked its winner by
    byte order instead.

    **The fold decides the READING, not only the literal's grammar.** (Added
    2026-08-30 from this item's fourth measurement.) The kind and the type are two
    answers, and applying the second to the literal alone left the comparison
    on the first. A composite whose kind is DECIMAL and whose fold is float8
    took the DECIMAL arm on every row the decimal arm supplied — read with the
    DECIMAL grammar, at DECIMAL width, before the literal was parsed at all —
    and that is three wrong answers wearing one cause:

    - a literal only the fold's grammar reads. `COALESCE(numeric, float8) =
      '0xC.C'` answered none where PostgreSQL answers 4: the float input
      function reads that hex float as 12.75 and the numeric one refuses it.
    - the wrong WIDTH. `= '12.750000000000000001'` answered none where
      PostgreSQL answers 4, because float8 rounds the literal onto the value
      and an exact decimal comparison does not.
    - a ROW-DEPENDENT refusal, which this item says is closed. `= 'abc'`
      raised only on the rows the FLOAT arm supplied.

    So every QUOTED pair is routed through the fold's rung first, and the
    DECIMAL arms own a pair only when the fold is itself DECIMAL. A CAST to a
    numeric type is a typed operand for the same fold (`castNumericKind`);
    saying otherwise made `COALESCE(numeric, CAST(k AS DOUBLE PRECISION))`
    unresolvable and sent it back to the DECIMAL rung.

    **And the refusal is decided at PLAN time.** A composite is typed there by
    the fold over its arms (`physical.foldArgTypes`), so the literal is parsed
    once against the fold's grammar before any row exists — which is the only
    way `WHERE id > 100 AND COALESCE(numeric, float8) = 'abc'` raises, as
    PostgreSQL does, over a range holding no rows at all. The binder recurses
    CHILDREN FIRST so the innermost failing coercion is the one reported, which
    is the order PostgreSQL analyses in.

    A gate can pass for a reason that does not generalise, and one here did:
    the row-independence test used GREATEST, which evaluates its float operand
    as a candidate on every row, so it refused whatever the range. COALESCE and
    CASE take their value from ONE arm and did not. Both, and an empty range,
    are gated now.

    **The DECLARED type of such a call was `decided[0]`, not the fold**
    (`expr.CommonDeclType`), so a projection of `GREATEST(real, …, double)`
    narrowed the double answer back into a real vector. The cause was one line
    further up: `physical.nodeDeclaredType` typed a QUOTED string literal as
    `Decl(TypeString), Decided`, so a call holding one had a NON-NUMERIC
    decider and `CommonDeclType` fell back to `decided[0]` instead of folding.
    PostgreSQL types that literal `unknown` and resolves the call from the
    other arguments. **CLOSED by #724**: the declaration moved, and the six
    cells below answer PostgreSQL's own text on every arm.

    That deferral does not merely narrow — it WRAPS. A folded double `1e39`
    stored into an INT64 vector is int64's MINIMUM, and so is a NaN: #462's
    failure mode, which item 6 forbids and ADR-0024 item 4 makes a 22003, and
    it reaches GROUP BY keys built from the same projection. It cannot be
    fixed from the comparison layer — the materialized value feeds the
    COMPARISON as often as it feeds a store, so narrowing or refusing there
    answers a different predicate than PostgreSQL's — so the six shapes were
    pinned LOUDLY, with PostgreSQL's answer recorded beside each, and filed as
    #724. Deleting those pins was the declaration fix's proof, and it is done:
    the six cells live in `coordinator.TestRealComparisonWidthTwoPath`'s
    `runExtremumWinnerMaterialization` block
    (`internal/coordinator/real_width_two_path_test.go`) and every `want` is
    now PostgreSQL 17.11's own text rather than this engine's wrong answer.

    **The boxed layer resolves the column's WIDTH from its DECLARATION.**
    `expr.ColRef.Eval` widens on the way out — a FLOAT32 column boxes as
    float64 and an INT32 one as int64 — so a box-driven rule would compare
    `r < '3.1'` at double width and skip int4's range check. `boxKind` carries
    `boxInt32`/`boxInt64`/`boxFloat32`/`boxFloat64` for exactly this, with
    `boxNumber` kept for an operand whose declaration this layer cannot read;
    the widening is exact and order-preserving, so narrowing the box back
    inside the real arm recovers the stored value bit for bit.

    **Residuals, named rather than claimed.** DECIMAL still refuses `'1_000'`,
    `'0x1A'` and the other PostgreSQL 16 numeric-input forms that this closes
    for the integer family — that half of #634 needs `parquet.DecimalTextParts`
    to move, which is ADR-0024 item 6's grammar and its own change. The NETWORK
    types are still not wired into the plan-time refusal (#627): their parsers
    are STRICTER than PostgreSQL's, so refusing on them would reject PG-valid
    input. And TIMESTAMP deliberately keeps its own string grammar — a quoted
    string against a TIMESTAMP column is a timestamp, not a number (#493).

  - **`INSERT INTO … SELECT` assigns within the NUMERIC family and refuses
    outside it.** (Added 2026-09-12, #1024; narrowed 2026-09-13 after the
    earlier measurement.) PostgreSQL's `transformAssignedExpr` coerces each query
    item to its target column's type, so `INSERT INTO t (text_col) SELECT
    bigint_col` succeeds there (measured on 17.11). This engine converts every
    cell through its OWN assignment converter — `assignEvaluatedValue`, the one
    `INSERT … VALUES` and `UPDATE … SET` have used since #647/#678 — so every
    pair that converter has a rule for is assigned, and the value stored is the
    value the VALUES door stores for the same number: the whole numeric family,
    in every direction, including a numeric into a bigint (rounded, as
    PostgreSQL rounds) and a `DECIMAL(12,3)` into a `DECIMAL(18,4)` (rescaled,
    1.5000).

    What is refused is a pair OUTSIDE that family — an integer into a STRING, a
    timestamp into a bigint — with `42804` and PostgreSQL's own hint. PostgreSQL
    assignment-casts the first of those and refuses the second. The divergence
    is therefore in the REFUSING direction and it is narrow.

    The earlier implementation record here was much wider: it refused an integer into a DECIMAL
    and a DECIMAL into anything, because the door handed the query's BOX to the
    writer and `parquet.DecimalValueFromBox` reads an integer box as the
    already-UNSCALED carrier (ADR-0018 §4) — a BIGINT 5 into `DECIMAL(18,4)`
    stored **0.0005**, and `PORT 500000` landed in a uint16 column. Both were
    the same defect, which was not the pair but the missing conversion.
    `wadjet.TestBothWriteDoorsStoreTheSameNumber` and
    `TestADecimalSourceIsAssignedAtItsValue` compare the two doors' stored
    VALUES pair by pair, against each other and against PostgreSQL's measured
    answer.

    **Amended 2026-09-24 (arc VL, #1252 #1254): ONE assignment table
    for every write, and it is PostgreSQL's.** `ingest.AssignableToColumn` is
    asked by `INSERT … VALUES`, `INSERT … SELECT`, `UPDATE … SET` and `MERGE`
    alike, from the source's DECLARED type, before a row is read. It assigns
    every scalar into TEXT (rendered as PostgreSQL's I/O cast renders it — an
    inet host as `10.0.0.1/32`) and DATE↔TIMESTAMP, which closes the refusing
    divergence above for those pairs; it refuses TEXT into every non-text
    type (42804), which VALUES and SET used to ACCEPT by reading the text as a
    number — a superset no entry here kept, so it is closed rather than
    recorded. Kept: BYTES, the containers and DURATION into TEXT refuse
    (refusing direction), and ONE superset — a call the registry declares
    TEXT for a network or UUID value (`expr.DeclaresTextForTypedValue`:
    `uuid()`, `int_to_ip`, …) is read by the target's input function like an
    unknown-typed literal. The table is only as right as the declarations it
    reads, so the same change made every TEMPORAL declaration true: a
    date/time function or operator declares PostgreSQL's type and its kernel
    produces that type's box (int64 epoch days / epoch milliseconds), the
    unit of a box is read from its PRODUCER (`expr.producedTemporal`) at every
    consumer, and `date ± n` is typed by the operand's declared type, never
    its spelling. Gates: `expr.TestRegistryDeclaredTypeIsTheProducedType`
    (census, every registry entry), `physical.TestTemporalArithmeticDeclares
    WhatItProduces`, `wadjet.TestOneAssignmentTableOnEveryDoor` (580 cells ×
    four doors against PostgreSQL 17.11's measured answers).

    **Amended 2026-09-24 (arc VL): one assignment FUNCTION, and a
    door-diff gate.** earlier implementation's one table sat under two converters — INSERT …
    SELECT assigned a constant from the value its select list had evaluated
    (a decimal literal is a double there, ADR-0024), so `SELECT 2.50` into
    TEXT stored `2.5` there and `2.50` on every other door. Every door now
    classifies its source through one function (a constant by its spelling,
    a typed-text call, or the declared type) and calls one check and one
    converter. `wadjet.TestAssignmentDoorsAgree` proves, for 560 source ×
    target cells (constants on all five write doors — VALUES, INSERT …
    SELECT, UPDATE SET, MERGE SET, MERGE INSERT — and column expressions on
    the four with a FROM) and 42 CTAS cells, that no two doors store a
    different value or raise a different SQLSTATE, and that the common
    answer is PostgreSQL 17.11's; the only listed differences are CTAS
    column TYPES (a decimal literal was double precision by ADR-0024's
    literal rule — closed in earlier implementation below; integer arithmetic and a negated
    integer literal declare bigint),
    never a stored value another door disagrees with. The same change put
    PostgreSQL's DATE / TIMESTAMP range (22008) at the one place a temporal
    value is constructed (#911's family), and the date/timestamp operator
    refusal (42883) into expression typing on every DML door.

    **Amended 2026-09-24 (arc VL): the door-diff gate gets its
    SOURCE axis and its PostgreSQL column.** earlier implementation's gate proved no two
    doors differ; it could not see every door being equally wrong, and they
    were: a decimal constant one expression deeper (`CASE WHEN true THEN 2.50
    END`, `COALESCE(2.50, 1)`, a CTE's, a derived table's, a VALUES list's)
    was double precision and stored `2.5` into TEXT and 2 into INTEGER. A
    fractional literal now declares its spelling's numeric wherever it sits
    (ADR-0024's 2026-09-24 amendment), and
    `wadjet.TestAssignmentExpressionSourcesAgreeWithPostgreSQL` proves, for
    49 constant-expression sources × 5 targets × 9 doors (VALUES, INSERT …
    SELECT directly and through a CTE, a derived table and a VALUES list,
    UPDATE SET, MERGE SET, MERGE USING (SELECT …), MERGE INSERT), zero door
    splits and zero unlisted differences from PostgreSQL 17.11; the three
    listed cells are the #764 trailing-zero class (a choice over constants of
    different scales prints at one scale). The CTAS decimal-literal type
    divergences are closed. The same change: a quoted operand beside a DATE or
    TIMESTAMP is typed by PostgreSQL's operator resolution in every statement
    (`date + '…'` 42725; the literal of `date - '…'`, `ts - '…'`, `ts + '…'`
    read as a date, a timestamp, an interval); an INTERVAL's clock part is
    summed in checked seconds, so the 22008 range rule holds for it too
    (PostgreSQL raises 22015 at the literal for a field past its own range;
    here the shift is 22008); and the writer's temporal box normalisation —
    the embedded ingester API's door — asks the same range question.
    INTERVAL itself stays what it was before arc VL except where that was a
    wrong value (see postgres-differences, "INTERVAL").

  - **A CTAS over a star of a self join answered where PostgreSQL refuses —
    CLOSED 2026-09-13 by arc O1 (#997, #1012).** (Added 2026-09-12, #1024.)
    `CREATE TABLE t AS SELECT * FROM s a JOIN s b ON b.id = a.id` is `42701`
    on PostgreSQL 17.11 — `column "id" specified more than once` — because
    both sides publish `id` and a relation cannot hold two columns of one
    name. This engine's join published the probe's columns bare and every
    DUPLICATE build column QUALIFIED by its owning alias, so the declared
    output had no duplicate at all and the table was created with `b.id` as a
    column name: a strict superset, allowed by rule 5.

    A star over a join now publishes the FROM clause's arms with duplicates
    kept BY POSITION (ADR-0026 §9), so the duplicate is real, and the door
    that already refuses a repeated name refuses this one — same SQLSTATE,
    same sentence, same shape as PostgreSQL.
    `wadjet.TestAQuerySourcedWriteRefusesWhatPostgresRefuses/StarOverASelfJoinIsPostgresRefusal`
    asserts the refusal and keeps the column-list spelling, which both engines
    accept, beside it.

  - **A CTAS column that PostgreSQL declares UNCONSTRAINED `numeric` is
    declared `DECIMAL(p,s)` here.** (Added 2026-09-13, #1024.)
    `COALESCE(numeric(15,2), numeric(38,10))` folds to unconstrained `numeric`
    on PostgreSQL 17.11, so `CREATE TABLE t AS SELECT …` gives a column whose
    `format_type` is `numeric` and whose values keep each row's own scale —
    12.75 renders `12.75` (measured). This engine has no unconstrained DECIMAL a
    table column can carry: a stored column needs a (precision, scale), so the
    CTAS stores the fold's declaration, `DECIMAL(38,10)`, and the same number
    renders `12.7500000000`.

    The NUMBER is identical, and that is what keeps it off item 6's list: the
    stored-position census (`coordinator.TestNumericFoldStoredPosition`)
    compares both sides through `math/big` and every one of its ten composites
    agrees on both arms, so `nfStoredPins` is empty. What differs is the
    DECLARATION a client reads — the same unconstrained-numeric difference
    ADR-0024 already carries on the wire for an aggregate's result
    (`ColumnMeta.WireUnconstrained`), now visible in a table's schema because a
    query's declared output has become one.

  - **`CREATE TABLE IF NOT EXISTS … AS SELECT` over a taken name sends no
    NOTICE.** (Added 2026-09-12, #1024.) PostgreSQL emits
    `NOTICE: relation "t" already exists, skipping` and the `CREATE TABLE AS`
    tag; this engine sends the tag and no NOTICE, because it has no NOTICE
    channel on every door and a message only pgwire could carry would be a
    fourth answer to one statement. For the two `CREATE TABLE … AS SELECT`
    forms and for `INSERT INTO … SELECT` the TAG is what a client branches on
    and it is IDENTICAL to PostgreSQL's — `CREATE TABLE AS` for a skip or a
    `WITH NO DATA`, `SELECT <n>`, `INSERT 0 <n>`, all measured on the wire —
    so for those the divergence is only in what a human sees in `psql`.

    The DECLARED `CREATE TABLE IF NOT EXISTS` (and `DROP TABLE IF EXISTS`) skip
    the same way, but their tag is NOT PostgreSQL's: this engine answers every
    declared DDL statement with `SELECT 1` over a one-row result, a fresh
    `CREATE TABLE` included, where PostgreSQL sends `CREATE TABLE` and no rows.
    That is a pre-existing property of declared DDL on this wire and not of the
    clause; the skip is consistent with the form it belongs to.

  - **The SQL-standard spellings, and what this engine does NOT read of
    them.** (Added 2026-09-18, arc PT / #1169, #1168, #1179, #1180, #1183.)
    `SUBSTRING(s FROM n FOR m)`, `SUBSTRING(s FROM pattern)`, `OVERLAY(s
    PLACING r FROM n [FOR m])`, `NORMALIZE(s [, NFC|NFD|NFKC|NFKD])`,
    `LOCALTIMESTAMP [(p)]`, `LIKE|ILIKE|SIMILAR TO … ESCAPE`, `LEFT`/`RIGHT`
    as function names and the `#` integer XOR operator answer PostgreSQL
    17.11's values, measured form by form. Six things around them do not, and
    each is on the differences page:

    `SUBSTRING(text SIMILAR pattern ESCAPE escape)` is refused with `0A000`
    naming the construct — the `#"` capture markers have no expression in the
    SIMILAR TO translation this engine does, and a loud refusal beats a
    plausible substring. `LOCALTIME`, `'x' IS NORMALIZED` and the `U&'…'`
    literal have no grammar at all (a TIME type, a postfix predicate and a
    lexer form this engine does not have). `NORMALIZE(s, 'NFC')` with a QUOTED
    form answers here and is a syntax error there — a superset, kept.
    `#` accepts operands PostgreSQL refuses — a non-integer one answers where
    the server raises 42883/42725 — and its declared WIDTH agrees with the
    server's: `int4 # int4` is integer and a bigint operand makes it bigint,
    measured on four of four widths through the wire. And the pattern language's own two:
    `LIKE` does not honour the DEFAULT backslash escape (`'a%b' LIKE 'a\%b'`
    is `f` where PostgreSQL answers `t`) while the explicit `ESCAPE` clause is
    read exactly as PostgreSQL reads it; a SIMILAR TO pattern whose
    translation cannot compile is `2201B` on both engines with different
    message text.

    The ESCAPE clause is a CALL (`like_escape`, `similar_to`) rather than a
    field on the LIKE node, because seven rewriters rebuild that node and a
    rebuild that dropped the escape would answer the UNESCAPED pattern — the
    same reasoning that expands `BETWEEN SYMMETRIC` and `ILIKE` at parse time.

  - **A truth context types EVERY expression kind, and a DML predicate is a
    truth context.** (Added 2026-09-19, arc PT / #1179.) A `WHERE`, a
    `HAVING`, a `JOIN … ON`, the operands of `NOT`/`AND`/`OR`, a searched
    `CASE`'s `WHEN`, a `DELETE`'s and an `UPDATE`'s `WHERE` and a `MERGE`'s
    `WHEN … AND` all require a boolean, and the type is proved from whatever
    the parser produced: a literal, a column, a call whose declaration is
    FIXED, a call whose declaration is POLYMORPHIC (through the argument it
    mirrors), a `CASE` (its branch results), a `CAST` (its declared target),
    arithmetic, an `ARRAY` constructor, an `INTERVAL` literal and a scalar
    subquery. What cannot be proved is left alone, which is the rule's bound:
    a derived table's or a CTE's column, and a polymorphic call whose mirrored
    argument this layer cannot type.

    The reason it is a POSITION and not an implementation note: the DML
    predicate is compiled and never planned (ADR-0031), and its per-row
    closure reads a non-boolean as false only at the TOP of the clause — so an
    integer under an `AND` matched every row and `DELETE FROM t WHERE id > 0
    AND CASE WHEN id > 0 THEN 1 ELSE 0 END` EMPTIED a table PostgreSQL 17.11
    leaves untouched, while the same predicate selected zero rows through
    SELECT. One statement, two answers, and the destructive one was the DML
    door. An aggregate in such a clause is 42803 and a window function 42P20,
    both refused before column resolution, which is the order the server
    reports in.

  - **A table function's column-alias list is applied at its SOURCE.**
    (Added 2026-09-18, arc PT / #1184; narrowed 2026-09-19, arc TF / #1210.)
    `FROM read_json(…) [AS] f(k, v)` now renames positionally like every other
    FROM item. The rename happens where the relation's WIDTH is known, and
    since arc FR (2026-09-20, #1230) that is PLAN time for a LOCAL file
    reader: `42P10` is raised before anything runs, like PostgreSQL's. It is
    still EXECUTION for an `http(s)` source and for the database readers,
    whose inputs the planner does not read (ADR-0039 §3), and one of those
    that produces no batch at all is never measured against its list. For
    `generate_series` and `unnest` the width is a function of the CALL. A
    repeated name in the list is `42701` at the list, the same narrower
    refusal arc PS recorded for a base table.

  - **A TABLE FUNCTION IN FROM IS A RELATION, AND WHERE ITS COLUMNS COME FROM
    DECIDES WHERE A MISSING ONE IS REFUSED.** (Added 2026-09-19, arc TF /
    #1210 #1203 #1211 #1202; the position is ADR-0039.) A reference to a
    column a table function does not publish is `42703` naming the column, as
    it is over a base table — where it used to answer NULL for every row.
    `generate_series` and `unnest` declare their columns from the call, so
    their refusal is made at plan time — and since arc FR (2026-09-20,
    #1229/#1230/#1231) so is a LOCAL FILE reader's. The ordering that stopped
    it is fixed rather than worked around: the table-function capability is
    authorized FIRST, before the statement binds, so the planner may read the
    input — a Parquet FOOTER, or ONE BATCH of a JSON or CSV file through the
    same reader the query uses, and only when the input is a REGULAR file it
    can open twice — without reading it for an identity that may not be
    allowed to (ADR-0034's amendment). The JSON and CSV inference is the
    readers' own 100-ROW SAMPLE and describes the whole file; a row past it
    with a non-NULL value that does not fit refuses with COPY's SQLSTATE for
    the field (`22P02`, `22003`, `22007`), naming the reader, file, row,
    column and types (arc RP, #1242, #1243); a CSV field is read with
    PostgreSQL's bigint/float8/bool input functions. Divergences: a timestamp
    column takes only the sample's spellings and an inet column no prefix
    length (both on the differences page). A key first
    seen past the sample is refused too (`22P04`, a nested field `22P02`),
    and `sample_size = -1` types the columns from every row (ADR-0039 §3,
    arc RD, 2026-10-03). With a column list the reader is
    an ordinary relation: an unknown column is `42703` at plan time through
    ANY path, `f.*` expands, and `SUM` over a whole-number column is `numeric`
    and `MIN`/`MAX` keep its width, which is what PostgreSQL declares for the
    same `bigint` column.

    The divergences that remain, each on the differences page: an `http(s)`
    source and a database connector are NOT read at plan time (a plan-time
    fetch is a second request for every statement and would make `EXPLAIN`
    reach the network), so for those the `42703` and the `42P10` are made at
    the FIRST BATCH, one that produces no batch answers zero rows where
    PostgreSQL raises, `EXPLAIN` over such a statement does not refuse, an
    aggregate over their column declares `double precision` and a qualified
    star over one is `0A000`; and `generate_series(…) WITH ORDINALITY`
    publishes one column where PostgreSQL publishes two. Amended 2026-09-24
    for arc PC: a FROM alias now names a single-column function's output,
    as PostgreSQL does (`SELECT g FROM generate_series(1,2) AS g`).

  - **A FILE READER WHOSE INPUT DECLARES NO COLUMNS IS `0A000`, WHERE
    POSTGRESQL HAS A ZERO-COLUMN RELATION.** (Added 2026-09-20, arc FR /
    #1230.) `SELECT * FROM read_json('<zero-byte file>')` raises `0A000 the
    table function "read_json" published no columns: its input … is empty`.
    PostgreSQL permits a relation with zero columns — `CREATE TABLE t ();
    SELECT * FROM t` answers zero rows of zero columns, measured on 17.11 —
    and this engine does not, at ANY door: a result that declares no columns
    is not an answer it has. It used to reach the door as `XX000 the result
    has no columns at all`, the engine reporting an internal invariant for a
    file the caller can see is empty; the class and the sentence are the
    change. A Parquet file carries its schema in the footer and a CSV in its
    header row, so an empty file of either kind is an ordinary empty relation:
    zero rows, columns declared.

    TWO SHAPES THAT WERE SILENT WRONG VALUES ARE CLOSED by the same arc.
    A BARE reference to an unknown column in a join holding two readers
    answered NULL for every row and is `42703` now, through the direct
    spelling, the comma spelling, a `LEFT JOIN`, a CTE over either arm and a
    derived table over either arm. And an `ON` that names the RIGHT arm's
    column first dropped the condition and answered the CROSS PRODUCT — eight
    rows over a four-row and a two-row file where PostgreSQL answers two; the
    key's side is its QUALIFIER now, which holds even for a relation with no
    plan-time schema at all (ADR-0039 §9). Neither is a divergence any more
    and #1229's pins are deleted rather than re-pinned.

    The declared functions' own value rules follow PostgreSQL
    exactly: the step is never flipped for the caller, `generate_series(5,1)`
    is an empty relation, a zero step is `22023`, a series ends at the 64-bit
    carrier's edge rather than wrapping, and the column is `integer` for
    arguments that fit int4 and `bigint` otherwise — so its `SUM` is `bigint`
    and not a float8 that loses the value.

  - **`read_csv` READS THE GRAMMAR OF `COPY … (FORMAT csv)`, WITH FIVE
    DIFFERENCES.** (Added 2026-09-23, arc FR2 / #1248 #1259.) A field is NULL
    only when it is empty and no part of it was quoted, so `""` is the empty
    string; a quote opens anywhere in a field; whitespace is data; and an
    unterminated quote and a record of the wrong width are `22P04`
    bad_copy_file_format naming the line — each measured against 17.11's
    `COPY`. A SHORT record is refused although base NULL-padded it, because
    a stray unquoted line break splits one record into two short ones and
    padding them answers rows the file does not hold (loud beats plausible;
    a `\.` line in a multi-column file is such a short record); a record
    with a value past the header's last column is refused because base
    dropped that value. The differences:
    - A trailing delimiter whose extra fields are EMPTY and unquoted (`1,x,`)
      reads as the record without them, where `COPY` raises `22P04 extra
      data after last expected column`: base answered it on every path and
      no value is lost; exporters write it.
    - A BLANK line in a file of more than one column is SKIPPED, a trailing
      one at the end of the file above all, where `COPY` refuses it (`22P04
      missing data`). This is the superset rule: base 962117da skipped it
      identically on every read path and schema path, and nearly every
      exported CSV ends with one. In a one-column file a blank line is that
      column's NULL, as `COPY` reads it. A blank line inside a quoted field
      is data.
    - LF, CR and CRLF line endings may be MIXED in one file, where `COPY`
      fixes the first one and refuses another (`22P04 unquoted carriage
      return / newline found in data`). Same rule: base read LF and CRLF
      mixed as record ends on every path (and DuckDB 1.5.5 accepts both). A
      CR inside an unquoted field therefore ends the record, which is then
      short and `22P04`; base kept the field with the CR dropped.
    - A line holding `\.` is DATA, not PostgreSQL 17's end-of-data marker
      (which silently drops every later row, and which PostgreSQL 18 no
      longer honours in a file either).
    - A UTF-8 byte-order mark at the start of a file is skipped — with
      `header=false` from the first data value — where `COPY` keeps it in the
      first field.
    Nor are a field's bytes checked against the encoding: `COPY` refuses a
    NUL byte with `22021`, and this reader stores it.

  - **A FILE READER'S INPUT THAT CANNOT BE OPENED IS REFUSED WITH `COPY`'S
    SQLSTATE, AND `EXPLAIN` OVER IT IS REFUSED TOO.** (Added 2026-09-23, arc
    FR2 / #1245.) `58P01` for an input that does not exist (a glob matching
    no file included), `42501` for one that may not be read, `42809` for a
    directory — the classes 17.11's `COPY FROM` and `pg_read_file` raise for
    the same path. The planner knows it without reading the input, so the
    statement and `EXPLAIN` over it are refused at plan time, where
    PostgreSQL's `EXPLAIN` over a relation that does not exist is `42P01`: the
    class is the input's, not the catalog's. An `http(s)` source is not
    reached at plan time (ADR-0039 §3), so its `58P01` (a 404) is raised at
    the first batch and `EXPLAIN` over it prints a plan.

  - **`QUALIFY` has no PostgreSQL, and DUCKDB 1.1.3 IS THE ORACLE FOR IT.**
    (Added 2026-09-14, #1076.) The clause originates in Snowflake/BigQuery and
    has a second, checkable implementation in DuckDB; PostgreSQL has none, so
    rule 1 has nothing to say and rule 2's "oracle" role is the whole of the
    authority here. Every fact in `logical/qualify.go` was measured on DuckDB
    1.1.3 over rows identical to this repository's `lat_ord` / `lat_item`
    fixtures, and
    `coordinator.TestArcL1QualifyAnswersDuckDBOnEveryArm` holds 31 cells of it
    on five arms.

    Four of those facts are decisions a second implementation could have made
    differently, so they are recorded rather than left implicit: the clause
    filters AFTER window evaluation and BELOW the projection (so it may name a
    column the SELECT list does not publish); a window call written inside it
    is evaluated and not projected; a BARE name binds the INPUT relation's
    column first and a SELECT-list alias second (`SELECT i.amount AS id …
    QUALIFY … AND id > 60` answers ZERO rows in DuckDB — `id` is the table's);
    and a `QUALIFY` no window function reaches is an ERROR, not a `WHERE`
    under another name.

    This is the only clause in the supported language whose oracle is not
    PostgreSQL, and the reason is stated so the next reader does not take it
    as a precedent: PostgreSQL cannot parse the statement at all. Where
    PostgreSQL has a spelling, it decides.

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
  no kernel at all), #505 (the #463 refusal did not reach the #465 boxed
  sites or a negated string literal), #506 (two DECIMAL columns still compare
  lexically at the same three boxed sites — open), #517 (the boxed-site
  refusal is per-row, not plan-time — open) — the work items 6's amendments
  record
- #492 (IPv6/CIDR literal ordering was lexical-text, not numeric/structural,
  and disagreed between the single-process engine and the stage DAG) and
  #497 (LIKE against a network-native type or UUID panicked or silently
  matched nothing) — item 10 and item 11 above record the settled position
  for; `internal/engine/exec/kernel/compare.go` (`CidrSortKey`,
  `likeTextRenderer`), `internal/engine/expr/compile.go` (`tryNetworkLit`,
  `kernel.IPv6LitKey`), `internal/engine/expr/expr_compare.go` (`CmpNetworkLit`),
  `internal/engine/expr/expr_special.go` (`Like.EvalBoolNull`), `internal/oracle/typematrix/typematrix.go`
  (`networkOrdLit`)
- #521 (CAST AS STRING did not render DATE or FLOAT32 the way the projection
  and LIKE already did — closed, item 11's amendment above) and #520 (ORDER
  BY/GROUP BY/DISTINCT/COUNT(DISTINCT)/MIN/MAX/hash-join keys over a CIDR
  column still used TEXT order — item 10's own "known residual," now
  closed) — `internal/engine/expr/expr_cast.go` (`boxedTextOperand`),
  `internal/engine/exec/kernel/compare.go` (`CidrOrderKey`),
  `internal/engine/exec/kernel/sort.go`, `internal/engine/exec/kernel/agg.go`,
  `internal/engine/exec/kernel/types.go` (`Accumulator.Merge`),
  `internal/engine/exec/agg_key_encoding.go` (`appendColumnValue`),
  `internal/engine/exec/partitioned_agg.go` (`legacyCompositeHash`),
  `internal/worker/partitioned_shuffle_sink.go`
- #546 (the single-process set operation dedupped CIDR by stored TEXT while
  the stage DAG dedupped it by inet — closed) and #565 (a column-to-COLUMN
  CIDR comparison, and IPv6's ordering, were inet-ordered on the vectorized
  kernel only, so a WHERE clause and a projection of the same comparison
  disagreed inside one process — closed) — `internal/planner/physical/
  set_op_key.go` (`keyValueText`), `internal/engine/expr/boxed_pair.go`
  (`boxCidr`, `boxIPv6`, `netOrder`, `netKeyFor`, `compareNull`),
  `internal/engine/exec/kernel/compare.go` (`IPv6RowKey`,
  `colColFilterCidr`), `internal/coordinator/cidr_col_col_two_path_test.go`,
  `internal/engine/expr/network_col_col_test.go`
- #568 (a ROW field path was declared STRING, so `ORDER BY rw.c` sorted CIDR
  by text and `ORDER BY rw.n` sorted an INT64 by text, while `ORDER BY rw` was
  correct — CLOSED, gated by
  `wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType`), #569 (windowed
  MIN/MAX declared FLOAT64 for eight types and failed the query where the
  plain aggregate answers — CLOSED, `exec.WindowMinMaxType` names all 22
  types), #570 (BYTES was not `bytea` on the wire — CLOSED the same day it
  was recorded, `pgTypeOID` answers 17; see item 11) — items 10 and 11's
  residual lists, now all closed
- #566 (a GROUP BY over an ARRAY/ROW/MAP/VECTOR column failed the query past a
  partial-aggregate spill — CLOSED, and the pin that recorded the failure is
  replaced by `exec.TestContainerGroupByAcrossASpillMatchesMemory`), and the
  merge key underneath
  it: `internal/engine/exec/sort.go` (`appendKeyElemWithMeta` — a container's
  elements were written in the TOP-LEVEL encoding on the meta path, so
  ARRAY[1,23] and ARRAY[12,3] were one key)
- #522 (LIKE against a container column matched Go's own `fmt.Sprint` of the
  boxed value, an unspecified text form — item 11's own open question,
  settled as a refusal, closed) — `internal/engine/exec/kernel/compare.go`
  (`ResolveLikeFilterKernel`), `internal/engine/exec/filter.go`
  (`likeConstError`), `internal/engine/expr/expr_special.go` (`containerLikeKind`),
  `internal/engine/expr/fatal.go` (`raiseNoLikeOperator`)
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
  `internal/engine/exec/agg_output.go` (`outputSchema`, `minMaxOutputType`),
  `internal/planner/physical/aggregate_declared_output.go` (`aggSpecOutputType`),
  `internal/worker/avg_fold.go`
