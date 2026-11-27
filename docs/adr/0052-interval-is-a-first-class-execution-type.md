# ADR-0052: INTERVAL is a first-class type, in execution first

- Status: **Direction accepted** by the maintainer (2026-11-26); **stage A (declarations and guards, nothing admitted) is build-ready**; stages B to F are defined here and are NOT build-ready until the two rulings below are made and each stage's brief has been judged.
  - Accepted (2026-11-26), in substance: a first-class interval is the long-term architecture, with an execution-only implementation following the interim refusals. Separate months, days and microseconds. Typed identity carried through vectors, hashing, comparison, grouping, sorting, aggregation, spill and distributed shuffle. Equality, hashing, ordering, calendar arithmetic, normalization and output types are defined against PostgreSQL 17.11 before implementation. SQL-equal intervals hash identically. SQL comparison conventions are distinct from calendar arithmetic. Unsupported paths stay fail-loud throughout the transition.
  - Not authorized in this stage, to return with a separate compatibility contract: persistent INTERVAL storage, format migration, wire-protocol changes (type OID 1186, binary encoding). No hidden text-side key as a permanent workaround.
  - **Pending with the maintainer (ruling 10):** where the type identity lives. Recommended: one execution-only type ID appended to the shared type enum, with the conditions of §6. No shared type ID is added and no codec is extended until this is ruled.
  - **Pending with the maintainer (ruling 11):** overflow of interval `sum` / `avg` under parallel execution. Recommended: PostgreSQL's checked, finite-width behaviour, accepting an order-dependent 22008. Stage E does not start until this is ruled.
- Deciders: Derek Wright
- Related: [ADR-0012](0012-sql-semantics-authority.md) (PostgreSQL decides; its temporal divergence rows r16, r20, r28, r30, r31 record the interim); [ADR-0013](0013-correctness-gates-and-their-boundaries.md) (legal nondeterminism; SQL ties versus representation); [ADR-0024](0024-decimal-is-finite-fixed-point-with-postgres-result-types.md) §12 (the representative among SQL-equal values); [ADR-0006](0006-never-oom-memory-model.md) (single-owner accounting); epic #1680; #1713.
- The definition, with the measurement behind every statement: [`docs/design/interval-execution-type.md`](../design/interval-execution-type.md). This ADR states the positions; that record holds the algorithms, the complete engine boundary inventory, the stage manifest and the corpus references. Where the two disagree, the record's measured cell wins and this ADR is corrected.
- Evidence: the corpus `internal/coordinator/testdata/arc_1680b/` (PostgreSQL 17.11's answers with result types and settings, beside the engine's disposition on six arms) and its tests `internal/coordinator/arc_1680b_*_test.go`; three independent judge rounds and the architect's rulings, kept with the crew notes of epic #1680 (`1680b_*`).

## Context

An interval existed here in three forms, none of them a type: a boxed expression value with one populated field, text after any materialization, and DURATION (bigint nanoseconds) behind an `INTERVAL` column declaration. Measured against PostgreSQL 17.11 this gave silently wrong answers wherever an interval was a key, an ordering, an aggregate argument or a published value, and a timestamp difference was a count of milliseconds. The interim (landed as 67a02a0f, #1713) refuses all of that by provenance: an interval-valued expression is consumed inline or refused, a new INTERVAL column is refused, and no execution path turns a timestamp difference into a number.

The interim withholds real functionality. This ADR defines the type that returns it, and the order in which it may return without ever passing through a silently wrong state. The earlier attempt to make interval arithmetic correct before its consumers could type the result leaked for three review rounds; the staging below exists because of that.

## Decision

### 1. Representation

An interval is the triple (months int32, days int32, microseconds int64), plus NULL. PostgreSQL's two reserved triples are positive and negative infinity. The three fields are kept as given: they are never normalized on input, on arithmetic or on selection. The exact bounds, the reserved triples and what each constructor does with them are in the record §1.

### 2. Equality, ordering and hashing are by the normalized value; everything else is by the fields

- Comparison, equality, ordering, grouping, DISTINCT, set operations, join keys, window partitions and peers all use PostgreSQL's comparison value N = ((months × 30) + days) × 86 400 000 000 + microseconds. N needs more than 64 bits; the engine carries it in 128.
- The hash of an interval is a function of N alone. SQL-equal intervals therefore hash identically, on every path that hashes: aggregate, join, distinct, set operation, spill partitioning, shuffle partitioning, bloom filters. This is held by a property test over generated values.
- A byte-comparable key, where one is used, is N as fixed-width big-endian with the sign bit flipped.
- Printing, arithmetic and calendar arithmetic use the fields. `1 mon`, `30 days` and `720:00:00` are equal and are three different values to add to a timestamp. `EXTRACT(epoch ...)` uses a 365.25-day year and is not N.

### 3. The printed member of a group of SQL-equal intervals

By ADR-0024 §12, extended: the representative is the minimum under ascending lexicographic order over (months, days, microseconds), selected over the WHOLE key tuple, with set-operation eligibility decided before the comparison. It is always an actual member of the input. PostgreSQL's own choice is plan-dependent (the corpus shows four different answers across hash and sort plans and input orders), so matching one PostgreSQL plan is not the contract.

The representative order is never SQL order. `ORDER BY iv, id`, window peers, ranks and tied `LIMIT` use N only (ADR-0013).

Because the representative changes later calendar arithmetic, it is selected once, before any downstream computation, and carried.

Refused in this program: `min` / `max` and any other aggregate over an interval with an explicit `ORDER BY` inside it (the user stated an order the rule would override); `sum(DISTINCT iv)` and `avg(DISTINCT iv)`. `count(DISTINCT iv)` is allowed.

### 4. Arithmetic and calendar arithmetic are PostgreSQL's algorithms, not equivalences

The record §4 gives each as an algorithm with its discriminating cells: multiplication truncates months and days separately, rounds the remainders, carries whole remainder-days and then rounds microseconds; division divides each component and is not multiplication by a reciprocal; `justify_interval` is its own algorithm; `age` borrows from the earlier date's month; unit fractions and clock fractions round by different rules; a fractional week passes through days; typmods change interpretation as well as truncation; opposing infinities raise 22008. Calendar arithmetic applies months (with day clamping), then days, then microseconds.

A result PostgreSQL returns as numeric infinity is refused where the finite DECIMAL carrier cannot hold it.

### 5. What a client sees: a bare interval result stays refused

With no wire change authorized, an interval is displayed only through an explicit scalar cast to a character type. Describe, execution, every door and zero-row results obey the same contract. Implicit conversion to text (concatenation included) is refused. Text and NULL interval parameters keep their existing behaviour; binary interval parameters stay refused. First stage output is `IntervalStyle = postgres` only; other styles, typmod-qualified declarations the engine cannot implement exactly, and every time-zone-dependent interval operation are refused until their own items.

Where an interval operation would produce or consume a value through one of the pre-existing wrong TIME or timestamptz paths (record §11), the statement is refused. Those wrong answers themselves are lines of epic #1680 and are not repaired by this program.

### 6. Type identity (ruling 10, PENDING; written to the recommended option)

Recommended: one execution-only type ID appended to the shared enum. Existing IDs and DURATION's meaning do not change. Conditions, all stage-B obligations:

- The ID can never be written to Parquet, Iceberg, the catalog or the wire as a column type in this stage; the Parquet writer's column validation rejects it explicitly, nested declarations included.
- Every transient stream reader (the shuffle format, the partial-aggregate spill, the columnar spill format, which gains a per-file schema header for this) validates each column's type byte when it reads the HEADER, empty streams included, and refuses an unknown ID with a coded error.
- Distributed admission of the type requires an enforced same-version or capability contract covering every worker eligible for dispatch throughout the query, retries and replacements included: a new reader cannot protect an already-deployed old one.

The alternative is a separate execution-type layer with adapters over the existing physical codecs: the shared enum stays untouched, at the cost of a translation at every boundary. Spill is process- and query-local; shuffle files and stage outputs are query-scoped but their cleanup is best effort, and shuffle input can remain in a worker's cache after its query (record §10).

### 7. Overflow of `sum` / `avg` (ruling 11, PENDING; written to the recommended option)

Recommended: PostgreSQL's checked finite-width state; overflow raises 22008. Under parallel execution which inputs overflow can depend on the order partial states combine, so for an UNORDERED aggregate either PostgreSQL's 22008 or the finite sum is a legal answer where a checked combination order can produce it, recorded as a class under ADR-0013. A prescribed order keeps one answer. The alternative, a wider state that never overflows where PostgreSQL would, is a deliberate divergence and is not taken.

### 8. Staging: every intermediate state is correct or refusing

| Stage | Admits | Carries |
|---|---|---|
| A: declarations and guards | nothing | every interval producer and typed slot is recognized by declared lineage, not syntax; every unsupported consumer is refused before rewrites and at run time; bare-result and write refusals enforced from here on |
| B: carrier and transport | nothing | the triple and NULL round-trip through selection, copy, pool reuse, spill, shuffle and gather; unknown IDs fail loudly; memory is accounted to one owner. Needs ruling 10 |
| C: inline expressions | scalar operations, one proven operation at a time | every result has its declared type; no timestamp difference reaches a numeric path |
| D: internal columns and keys | an explicit OPERATOR ALLOWLIST (carry and evaluate stated separately per operator) | no interval-typed vector reaches an operator outside the allowlist, checked on optimized plans, stage plans and at runtime batch entry, with a negative control that removes one check |
| E: aggregates and windows | one family at a time | typed transition, combine, inverse and final states agree across local, clone, spill, precombine and distributed paths. Needs ruling 11 |
| F: publication | nothing new | audits that Describe, execution, every door and zero-row results obey §5 |

A stage is landed when every corpus cell it owns meets its manifest outcome, which is one of exactly four: supported (value AND type equal PostgreSQL's), PostgreSQL's own error (same SQLSTATE), an intentional capability refusal (0A000 with its NAMED reason), or the approved representative divergence of §3. An internal guard error, a panic, an uncoded error or a wrong SQLSTATE meets none of them. A cell belongs to the earliest stage at which everything it needs has been admitted.

Typed lineage (record §9.1) is the contract that replaces the interim's assumption that a column is never an interval: the type lives in the bound output slot, is forwarded through aliases, projections, CTEs, derived tables and subqueries, is reconciled positionally across set-operation arms, is carried and validated in stage and transport schemas, and a declaration that is unresolved or disagrees with a runtime batch is a loud refusal.

## Consequences

- Functionality withheld by the interim returns in stages C to E. Until then the interim's refusals stand, and stage A adds refusals (the TIME and timestamptz intersections, implicit text conversion) that turn present wrong answers into loud ones.
- A bare interval result, an INTERVAL column, interval binary parameters and non-`postgres` interval styles stay refused after stage F, until the storage and wire contract is decided separately.
- The engine deliberately prints a different member than a given PostgreSQL plan would for a group of SQL-equal intervals. It is always a member PostgreSQL could have printed.
- Unordered parallel `sum` / `avg` over intervals may raise 22008 on one execution and answer on another, if ruling 11 goes as recommended.
- Mixed-version clusters cannot run interval-typed stages, if ruling 10 goes as recommended.

## Alternatives rejected

- **Keep text and add a hidden normalized key.** Needs the same typed lineage through every boundary as a real type and silently reverts to text semantics wherever the lineage is lost. Ruled out by the maintainer as a permanent workaround.
- **Canonicalize equal intervals to one spelling at materialization.** Destroys field-preserving printing, EXTRACT and calendar arithmetic, and can print a value absent from the input.
- **Refuse only mixed spellings at run time.** Leaves ordering of unequal values wrong (`25:00:00` against `10 days`) and approaches the global key machinery of a real type while answering less.
- **Lift the interim refusals operation by operation as each expression is fixed.** This is what leaked: a correct interval value reached consumers that still declared the old type. The stage order above admits a value only after every consumer downstream of that admission handles it or refuses it.
