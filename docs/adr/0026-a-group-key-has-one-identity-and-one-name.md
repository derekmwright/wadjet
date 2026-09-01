# ADR-0026: A GROUP BY key has one identity and one published name

Status: Accepted (2026-08-30, #720 / #723 / #725; amended three times the same day after review — one identity, one SLOT, one published name, one ALLOCATOR per aggregate, and a NAME never re-read as structure; amended again 2026-09-01 for #737 and #759 — a WINDOW above the aggregate is spelled against what it publishes, and the allocator's per-aggregate SCOPE is a boundary with a fixture that attempts it)

## Context

A grouped query says the same expression more than once. `GROUP BY g + 1`
names the key; `SELECT g + 1 AS gk` asks for its value; `HAVING g + 1 > 2`
filters on it; `ORDER BY g + 1` sorts by it; an outer query references it
through a CTE or a derived table. Everything above the aggregate has to
answer one question about each of those: *which key is this?*

Below the aggregate `g + 1` is arithmetic over a column. Above it, the
aggregate has already consumed `g` and emits ONE column carrying the
computed value, so `g + 1` is a NAME. Every consumer that re-reads it as
arithmetic looks for `g`, finds nothing, and answers NULL for every row —
or, in a filter, UNKNOWN for every row, which admits nothing.

Each site had grown its own answer to "is this the same expression":

| site | its rule |
|---|---|
| `physical.buildProject`'s `gbExprToSyn` | exact AST rendering |
| `physical.aggregateOutputName` | `strings.EqualFold` — case-insensitive, paren-sensitive |
| `physical.validate.groupTermKey` | lower-cased, OUTER parentheses stripped only |
| `physical.aggregateProjectionSource` | the projection's own `Expr`/`Column` text |
| `logical.sortTermResolvesOverAggregate` | `strings.EqualFold` on `String()` |
| `sql.resolveGroupByAliasRef` | "contains no `.`, space or parenthesis" |
| HAVING | no rule at all |

So which SPELLING the query used decided which execution path answered.
Against live PostgreSQL 17 over `internal/oracle/typematrix` — one table,
`g = i % 7` with a NULL every thirteenth row, 5000 rows — PostgreSQL
answers eight rows (keys 1–7 and a NULL group) for every one of these, and
wadjet answered:

| SQL | single-process | stage DAG |
|---|---|---|
| `SELECT (g + 1) AS gk … GROUP BY g + 1` | 8 rows, gk **all NULL** | correct |
| `SELECT G + 1 AS gk … GROUP BY g + 1` | 8 rows, gk **all NULL** | correct |
| `SELECT g + 1 AS gk … GROUP BY (g + 1)` | 8 rows, gk **all NULL** | 8 rows, gk **all NULL** |
| `SELECT g AS gk … GROUP BY (g)` | **all NULL** | **all NULL** |
| `SELECT ((g) + 1) … GROUP BY g + 1` | **42803** | **42803** |
| `SELECT (g+1)+2 … GROUP BY g+1+2` | **42803** | **42803** |
| `ORDER BY (g + 1)` over `GROUP BY g + 1` | **refused** | **refused** |
| `HAVING g + 1 > 2` | **0 rows** | **0 rows** |
| `SELECT "g + 1" … GROUP BY "g + 1"` | correct | **loud**: `GROUP BY key "\"g + 1\"" is not a column of its input` |
| `SELECT (g + 1) * 2 … GROUP BY g + 1` | **all NULL** | **all NULL** |
| `SELECT (g + 1) + COUNT(*)` | correct | **all NULL** |

None of these errors except where marked. A grouped query returning the
right number of rows with a NULL key column is the most expensive kind of
wrong answer this repository has: it survives every row-count gate, every
"both paths agree" gate where both paths are wrong together, and it looks
like data.

## Decision

**A GROUP BY key has exactly one IDENTITY and exactly one published NAME.
Every consumer resolves through them; no site compares spellings.**

Three parts.

### 1. `plansql.ExprIdentity` is the identity, and it erases only spelling

`ExprIdentity` (`internal/planner/sql/canonical.go`) renders an expression
with three differences erased and no others:

- **parentheses** — the parse tree already records the grouping;
- **identifier case** — the way PostgreSQL folds an unquoted identifier;
- **whitespace** — which the AST rendering already gave.

The rendering is fully parenthesised at every INFIX node. That is the part
that matters: dropping `ParenNode` and printing `a * b + c` for
`a * (b + c)` would make two DIFFERENT expressions share an identity, which
is wrong in the more dangerous direction than the defect being fixed.
`g - 1 - 2` and `g - (1 - 2)` keep different identities, and stay two group
keys, as PostgreSQL has them. A node kind the canonicaliser does not know
keeps its own `String()` — the behaviour every caller had before.

### 2. `plansql.GroupKeyName` is the published name; the SLOT is where the value lives

A derived key is not a column of the aggregate's input, so one of the two
engines has to materialize it. **It is materialized into a hidden slot —
`SlotName(SlotGroupKey, N)`, i.e. `__gb_expr_N` — and PUBLISHED under its
canonical text by a rename at the aggregate's output.** The name a consumer
uses and the name the value is stored under are two different names on
purpose.

The first version of this ADR materialized the key under its own canonical
text and called the resulting collision "possible and accepted". That was
wrong twice over. It is not rare — any relation carrying a column spelled
like the key produces it, including one a query mints itself with
`SELECT c AS "g + 1"` — and the two engines do not even fail the same way,
so "both behave the same" was false. Measured against PostgreSQL 17 over a
derived table that renames a column to `"g + 1"`:

| | PG 17 | single-process | stage DAG |
|---|---|---|---|
| `SELECT g + 1 AS k, COUNT(*) … GROUP BY g + 1` | 8 rows | **4829 rows**, grouped by the COLUMN | **4829 rows** |
| `… MAX("g + 1")` | 8 rows | wrong | **loud** |
| `COUNT(*) AS "G + 1"` beside a `g + 1` key | 8 rows | correct | **wrong** |

The pre-aggregate projection APPENDS on the single-process path and
`batch.RecordBatch.ColumnIndex` answers with the FIRST exact match, so the
input column won and the query grouped by it; the worker's projection
NARROWS, so the key won and shadowed the column an aggregate needed. One
name, two operators, two different wrong answers.

### 2a. A slot is ALLOCATED, never merely named

`SlotName(family, n)` is a namer. Naming is not enough, because a slot is
safe only when NOTHING else answers to it, and three different things can:

- a name already **in scope** — a table may legitimately store a column
  called `__gb_expr_0`, and it is never refused at read;
- a name the query **binds** elsewhere — another group key, an aggregate's
  argument or output, a filter column;
- a slot the same query has **already issued**.

A per-key namer sees only the first, and that is a wrong answer, not a
missed optimisation. Over a table carrying `__gb_expr_0`, two derived keys
landed in one column: key 0 stepped off the stored name onto `__gb_expr_1`,
key 1 started at its own index, found `__gb_expr_1` free, and took it — so
the second key silently carried the first's value and a twelve-group query
answered three. The DAG had the mirror image: `SUM(__gb_expr_0)` beside
`GROUP BY g + 1` was answered from the key's slot, because the worker's
projection narrows and the key had claimed the name first — right keys,
right row count, the sum of a group key.

**A slot is therefore ALLOCATED from a per-aggregate (planner) or
per-fragment (worker) allocator that excludes all three sets, and
termination is bounded by their size rather than assumed.** Two authors hit
this same bug independently, which is the evidence that the shared API
needs an allocator and not only a namer.

### The SCOPE is the AGGREGATE, and that is a boundary, not an oversight (2026-09-01, #759)

`__win_N`'s scope had to become the QUERY (ADR-0025 §"A scope is the QUERY,
not the block"), because two sibling subqueries minting one window slot
carried a column of that one name into a join and the projection above it
published one window's value twice. The group-key allocator is per AGGREGATE
and #759 was filed against it as the same defect one slot family over.

It is not, and the difference is what the two slots ARE. A window slot is an
OUTPUT column: `exec.Window` appends it to its input and it travels up through
every consumer, a join included. A group-key slot is a column of its
aggregate's OWN INPUT — the pre-aggregate projection materializes it, the
aggregate groups on it, and the aggregate publishes the value under
`plansql.GroupKeyName`. The slot never leaves the operator that minted it, and
two sibling aggregates have disjoint input streams, so one name in both is two
different columns that never meet.

Method 10 does not accept that as an argument, only as a claim with a fixture
attempting it. `ctl/SiblingAggregatesEachMintingASlot` is the attempt, over
`collslot` — whose two STORED slot-family columns push both aggregates' first
allocation to the same `__gb_expr_2`, so the shapes really do mint one name
twice. Five spellings: two siblings joined, a sibling NESTED in a sibling (the
shape that broke the window family on the single path), each sibling minting
TWO slots, both stored slots read as aggregate ARGUMENTS at once, and the
UNION ALL form. Every one is asserted on the KEY and on BOTH siblings'
aggregate values, which are far apart on purpose — a count of 80 beside a sum
in the thousands — and every one agrees with PostgreSQL 17 on all arms.

The claim this records is therefore narrow: the per-aggregate scope is
sufficient *because the slot is not published*, and the day a group-key slot
becomes visible above its aggregate — a stage that ships `__gb_expr_N` as its
output name would be one — the scope has to widen with it and these fixtures
are what says so.

`__gb_expr_` is in the RESERVED namespace (`planner/physical/reserved_slots.go`):
a user column, derived-table output or SELECT alias spelled inside it is
refused with 42601 naming the family, so no query can put a value there and
the collision is **impossible**, not accepted. That reservation is a
deliberate divergence from PostgreSQL, which has no reserved column
namespace — recorded in ADR-0025 — and it is the right trade, because the
alternative to refusing those queries is not answering them but answering
them wrongly.

Two keys are NOT materialized, and both matter:

- a key whose expression is a bare column of the input — it is already
  there, which is every ordinary `GROUP BY c`;
- a key an aggregate DIRECTLY BELOW already publishes under the same name
  and identity. `SELECT DISTINCT g + 1 AS k … GROUP BY g + 1` lowers to two
  aggregates keyed alike, and the outer one reads the inner one's OUTPUT:
  its recorded expression still says `g + 1` over a `g` that is no longer
  in scope, so materializing it would evaluate that `g` against a schema
  without one and collapse the table into a single NULL group. Only an
  aggregate below counts — a derived table that merely has a column SPELLED
  like the key carries a different value under that name, which is the
  collision above.

`groupKeyByIdentity` therefore indexes a key whenever a consumer cannot
simply NAME it: derived, elided-literal, or published under a text no column
reference can spell.

### 2b. `plansql.GroupKeyName` is the published name, and both engines use it

A bare column reference is published under its own name with any delimiters
stripped: `GROUP BY "g + 1"` names the column `g + 1`, not the four tokens
its quoted spelling lexes into. Anything else is published under its own
rendered text with redundant OUTER parentheses removed, so
`GROUP BY (g + 1)` and `GROUP BY g + 1` publish one name.

Case is PRESERVED here, because a batch column is matched by BYTES
(`batch.RecordBatch.ColumnIndex`). The identity folds case because it is
only ever compared; the name does not, because it has to be found.

The single-process pre-aggregate projection now materializes a derived key
under that same name instead of a synthetic `__gb_expr_N`. The two engines'
aggregate output schemas are therefore IDENTICAL, which is what lets one
logical rewrite — the HAVING respelling below — be evaluable on both. A
LITERAL key keeps the synthetic name: it is elided from the key set and
re-attached as a constant, no consumer resolves a constant by expression,
and `1` or `'x'` makes a poor column name.

`physical.groupKeyOutputs` states the naming rule once
(`internal/planner/physical/group_key_identity.go`), and
`aggregateOutputNames`, the pre-aggregate projection, the projection above
the aggregate and the gather's rename all read it from there.

### 2c. Only a column REFERENCE is resolved as a column name

A key's recorded TEXT cannot say whether the query wrote a name or an
expression: a delimited identifier's quotes are not part of its name, so
`GROUP BY "g + 1"` and `GROUP BY g + 1` are both recorded as `g + 1`.
Asking the text whether some column answers to it bound the ARITHMETIC key
to a delimited column of that spelling — PostgreSQL answers five groups of
the sum, both DAG arms answered nine groups of the column, silently.

**The parsed form decides.** A term that is not a `*ColRef` is never
resolved as a name; only its LEAVES are. PostgreSQL's rule is that unquoted
`g + 1` is arithmetic, full stop.

The same rule settles what a NAME may travel through. A key defined by a
rename Project is re-spelled into SOURCE columns for DISPATCH only
(`aggStageDispatchKey`), because the DAG flattens that Project;
`aggregateOutputName` does not take that path, since it answers what a SORT
KEY names and a sort key is resolved on both engines.

And a key so re-spelled is TYPED below the rename chain, which is #387's
own rule: `inputColDecls` stops at a Project, so a derived key over a
renamed DECIMAL column had no declared type, fell to the float rule, and
handed exact fixed point to a FLOAT64 vector.

### 2d. A name is never re-read as structure — in EITHER direction

§2c settles which way a key BINDS. The same confusion runs the other way, in
the rule that decides whether a query is legal at all.

`GROUP BY "g + 1"` groups by one COLUMN and says nothing about `g`, so
`SELECT g + 1` beside it reads an ungrouped column and PostgreSQL refuses it
with 42803. The grouping-coverage walk recorded each term's *recorded text*
as well as its parsed form — and since #725 that text is the key's published
NAME with the delimiters stripped, so re-parsing it read a column as
arithmetic and marked `g` grouped. The query then answered: 60 rows with a
NULL key on the single-process path, 3 rows on the DAG. Two engines
disagreeing about a query neither should answer.

`"g plus 1"` did the same with no operator in it at all — it parsed to just
`g` and marked THAT grouped — which is why the repair is to stop reading text
as structure rather than to special-case operators.

**The grouped terms are read from their PARSED forms and from nothing else.**

## The impossibilities, and the fixtures that attempt them

Method 10 of the correctness-fix protocol: a claim of the form *X cannot
happen* is exactly where the next regression lives, and it is invisible to
every gate until a fixture contains X. Each claim above is listed here with
the fixture that tries it.

| claim | fixture that attempts it |
|---|---|
| a slot is a name no query can spell | `collslot` STORES `__gb_expr_0` and `__gb_expr_1` — admitted through the catalog, because the DDL door refuses them; `ctl/TheStoredColumnIsStillGroupable` reads and groups by one |
| …including one the query itself mints | `SlotCollidesWithAStoredColumn/MintedByADerivedAliasIsRefused` — `1 AS "__gb_expr_0"` is REFUSED at the alias door (#694's reservation), which is stronger than allocating around it; a STORED column of that name is still admitted and read, and the rows above are that case |
| the slot never shadows an aggregate's argument | `AggregateOverTheStoredColumn` — `SUM`/`MAX` of the stored column, asserted on the VALUE |
| two keys never share a slot | `TwoDerivedKeys`, `ReversedKeyOrder`, `ThreeDerivedKeys`, `WithHaving`, all with the stored slot present |
| an arithmetic key and a delimited column of that text are different things | `ArithmeticKeyBesideADelimitedColumnOfThatText` — both directions, 5 rows against 9 |
| a delimited term does not group what its name spells | `gcov` carries `"g + 1"` and `"g plus 1"`; `GroupingCoverageUnderADelimitedTerm` asserts the 42803 AND the answering direction, plus three controls that must keep refusing and one that must keep answering |
| two SIBLING aggregates never share a slot | `SiblingAggregatesEachMintingASlot` — five spellings over `collslot`, whose stored slot columns make both aggregates allocate the same `__gb_expr_2`; joined, nested, two keys each, over the stored slots, and UNION ALL, asserted on both siblings' aggregate VALUES (#759) |

### 3. HAVING is spelled against what the aggregate publishes

The predicate is rewritten in the LOGICAL plan
(`plansql.ReplaceGroupKeyRefs`, called from `logical.BuildFromSelect`), so
both engines receive one they can evaluate. The walk is TOP-DOWN and stops
at the first whole-term match, so the LARGEST expression that is a key is
the one replaced rather than descending to a column the aggregate does not
emit. It never enters an aggregate call: inside `SUM(g + 1)` the expression
is evaluated over the aggregate's INPUT rows, where `g` is exactly the
column that does exist.

Bare column keys are deliberately absent from the identity→name map. Their
value is published under the input column's own name and every consumer
already reads it there; a mapping for them would only re-route a resolution
that works.

### 4. A WINDOW above the aggregate is spelled against what it publishes (2026-09-01, #737)

`SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) FROM t GROUP BY g + 1`
answered the right eight rows with the key NULL on every one of them, on all
five arms. Two consumers of the same fact, each reading `g + 1` as arithmetic
where it is a NAME:

- **the SELECT item.** The walk that re-points a select item at its key's
  column stops at any node that does not leave the aggregate's own columns
  visible, and a `NodeWindow` was on none of their lists. It belongs there —
  `exec.Window` APPENDS its output and renames nothing, which is the same
  answer `scopePreservingWrapper` gives for the relation-scope question.
  `logical.AggScopePreservingWrapper` states the list once and **all FIVE**
  walks read it — `physical.aggregateUnderOutput` for the gather,
  `physical.findAggregateAncestor` for the single-process projection,
  `physical.groupKeysPublishedBelow`, which decides whether an aggregate
  DIRECTLY BELOW already publishes the key, `logical.AggregateOverGroupRows`,
  which decides whether a Project's INPUT rows are one per GROUP and therefore
  whether a predicate above it may be substituted below, and
  `physical.aggregateOutputNames`, which answers what the aggregate below
  PUBLISHES so each projection can be pinned to the physical slot its
  provenance names.

  The count in this section has been wrong four times, and the sequence is the
  point: "both walks read one list" (two), then three, then four, then five.
  Each miss cost the same kind of answer. The third kept its own hardcoded
  Filter/Sort/Limit list, so `SELECT DISTINCT g + 1 … GROUP BY g + 1` with a
  window between its two aggregates re-materialized a key the inner one had
  already published and collapsed the table into ONE NULL group. The fourth
  asked its question through `logical.AggregateBelowProject`, whose list is
  Filter-ONLY, so a WHERE on the key applied above a window substituted `k`
  away to `(g + 1)`, met a schema with no `g`, and admitted no row at all on any
  arm (#774). The fifth descended NodeFilter alone while the call site that
  guards it (`findAggregateAncestor`) read the full list, so with a WINDOW
  between them #575's duplicate-name slot pinning was skipped and
  `SELECT COUNT(*) AS g, g AS x, ROW_NUMBER() OVER (ORDER BY g) … GROUP BY g`
  published the KEY's value under the aggregate's alias on the single-process
  path.

  The list therefore lives in `logical` and not in `physical`: the fourth
  reader is in that package, `physical` imports it and not the reverse, and a
  COPY is exactly what let the two disagree. `physical.aggScopePreservingWrapper`
  is now a delegation.

  `logical.AggregateBelowProject` keeps a narrower, Filter-only list ON PURPOSE,
  and says so at its definition. Its two callers —
  `physical.aggregateProjectionTarget` and `physical.aggregateGroupKeyName` —
  map a Project's SELECT list onto the aggregate's own STAGE, and a Sort, a
  LIMIT or a WINDOW between the two emits a stage of ITS own that the projection
  would be carried past. "Are these rows one per group" and "which stage does
  this Project sit on" are two questions, and conflating them is how #774 was
  written.

  `TestAggScopePreservingWrapperIsReadByEveryWalk` states exactly what is
  checked: **these five NAMED readers agree with the list, and the list covers
  every node type the logical package declares.** It cannot discover a SIXTH —
  it drives the five by name, and a review proved the point by adding a walk
  with its own list and watching the test pass. Saying otherwise is an
  overclaim this ADR has now made twice.

  A source-level guard was considered and rejected, and the number that
  justified the rejection was wrong. This ADR said "twelve functions in the
  package carry a `NodeFilter, NodeSort, NodeLimit` case". The census is:
  **29 functions in `internal/planner/physical` carry a literal case naming at
  least one of the three** (17 on the strictest reading, a clause containing all
  three), plus **6 more** that walk the same kinds through the shared predicate,
  and **15** in `internal/planner/logical`. So an allowlist would be closer to
  thirty entries than to eleven, and the argument against it is stronger than
  the one originally made — but it is an argument against a GUARD, not against
  a RECORD, so here is the record. Every candidate that is even arguably asking
  this section's question, with its decision:

  | function | list | decision |
  |---|---|---|
  | `physical.aggregateUnderOutput` | shared | reader 1 |
  | `physical.findAggregateAncestor` | shared | reader 2 |
  | `physical.groupKeysPublishedBelow` | shared | reader 3 |
  | `logical.AggregateOverGroupRows` | shared | reader 4 (#774) |
  | `physical.aggregateOutputNames` | shared | reader 5 (#575 under a window) |
  | `physical.wrapsAWindow` | shared | a REFINEMENT of the question — "is one of the wrappers specifically a Window" — used to keep the projection-elision decision from looking through one, because a window ADDS a column and elision needs the node's WHOLE output |
  | `logical.AggregateBelowProject` | Filter only | deliberately narrower; its callers map a SELECT list onto the aggregate's own STAGE and a Sort/LIMIT/window emits a stage of its own. Documented at its definition |
  | `physical.scopePreservingWrapper` | Filter/Sort/Limit/Distinct/**Window** | the RELATION-scope twin of this question, already has Window |
  | `physical.resolveSortKeyColumn` | Filter/Limit/Sort/Distinct, no Window | MEASURED, left alone. `SELECT g AS k, ROW_NUMBER() OVER (…) … GROUP BY g ORDER BY k DESC` and six siblings answer PostgreSQL's order on all four arms — the call site's `producerMaterializesName` reset already covers it, and its mirror walk `derivedAliasSourceColumn` also excludes Window, so changing one alone is the ADR-0025 out-of-step shape |
  | `physical.aggregateUnderWindow` | Filter/Project/Sort/Limit/Distinct, no Window | MEASURED, left alone. Five stacked-window-over-aggregate shapes over a DECIMAL(18,4) column — the type question it exists for — answer PostgreSQL's values on single and on both DAG arms. Adding Window would also need `windowSpecOutputType` layered for the skipped window, so it is a change with its own gap and no defect to justify it |
  | `logical.aggregateBelow` | Filter/Project/Window/Sort/Limit — its own, WIDER | a sixth de-facto reader, filed as **#787**; wider than the shared list by `NodeProject`, so it is a different question or a bug, and either way not settled here |

  The schema walks (`inputColTypes`, `emittedColTypes`, `emittedColDecimal`,
  `inputColDecimal`, `strictIntArithCols`, …) are NOT candidates: they ask what
  a node EMITS, and most already carry their own `NodeWindow` arm that ADDS the
  window's outputs, which is the correct answer to a different question. The
  set-operation walks and the locator walks are likewise their own questions.

  What finds the next reader is a review counting them, which is how the third,
  the fourth and the fifth were each found.
  On the DAG the SELECT list is attached to the WINDOW stage's fragment
  (ADR-0025 shape g), and that projection is respelled over the producer's
  emitted columns by the same `respellSpecsOverProducerOutput` the
  `StageProject` branch uses.

- **the window's OWN spec.** Its argument, its PARTITION BY and its ORDER BY
  keys are evaluated over the aggregate's OUTPUT too. `ORDER BY g + 1` reached
  `resolveWindowKeys` as arithmetic, was materialized by EVALUATING it against
  a schema with no `g`, and ordered by NULL on every row — the right rows in an
  arbitrary sequence, which no row-count or key-set assertion can see. It is
  respelled in the LOGICAL plan, where §3 already respells HAVING for the same
  reason, and rendered as a DELIMITED identifier so the key resolver reads it
  as the NAME it is rather than re-parsing it as structure (§2c, in the
  direction that decides how a key BINDS).

An AGGREGATE inside a window's spec — `SUM(COUNT(*)) OVER ()`, `ORDER BY
COUNT(*)` — is the same rule for the other kind of published column: it names
the aggregate's own output, is REUSED when the SELECT list already computes it
and hoisted into the nested-aggregate slot family otherwise, which is what
HAVING has done since it grew `__having_N`.

### 4a. A key that NAMES a window's output is REFUSED, because the question is about a stage (2026-09-01, #777)

The other direction of §4: not a key read above a window, but a WINDOW OUTPUT
read as a key. `SELECT x.id, x.w, COUNT(*) FROM (SELECT id, SUM(a) OVER () + 0
AS w FROM decpair) x LEFT JOIN decpair z ON x.id = z.id GROUP BY x.id, x.w`
answered `w = 52.99` on the single-process path and NULL on every row on both
DAG arms.

`aggStageGroupKey` answers a key that names a derived table's COMPUTED alias
with the alias's DEFINING EXPRESSION, and for a wrapped window that expression
is `__win_0 + 0` — a SLOT the join does not carry, because the window arm's own
projection (`absorbWindowArmProjection`, ADR-0025 shape g) already renamed it
away to `w`.

The obvious repair is to answer the ALIAS instead, and it is **not available at
that point in the plan**. `walkStages` emits `GroupByCols` BEFORE
`attachScanSelectProjections` and `absorbWindowArmProjection` run, and those are
the passes that decide whether any fragment publishes the alias at all —
`absorbWindowArmProjection` fires only from the join-input-projection pass, for
a join arm's child with exactly one window stage, empty ProjectExprs and its
expression columns available. Both candidate spellings are therefore right on
some plan shapes and wrong on others, and nothing at stage-emission time knows
which.

Three attempts to infer it from NODE KINDS were each wrong in a different
direction, and this ADR records them because the shape of the mistake is more
useful than the fix:

- the whole subtree below the aggregate ("any join, sort, LIMIT, window or
  DISTINCT") turned two CORRECT DAG answers into `stage scan-0: column "w" does
  not exist`, over a derived table with an ORDER BY or a LIMIT in it;
- the producer DIRECTLY below the defining Project turned another correct
  answer loud for `id % 7 AS k` beside a window — arithmetic over a scan column
  that nothing materializes (`TestWindowPartitionKeyTwoPath` caught it);
- that plus `referencesSyntheticWindow` still over-fired with NO join above:
  nothing attaches the arm projection there, so the key dispatched as the bare
  alias and `hash_aggregate` bound whatever the batch carried. LOUD where the
  alias names nothing — and SILENT where it SHADOWS a base column.
  `(SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) x GROUP BY g` answered
  three groups keyed by the SCAN's `g` where PostgreSQL answers one group of
  240, on both DAG arms, turning a right answer into a wrong one. No fixture in
  the tree contained a window alias that shadows a base column, which is
  method 10 turned against the predicate itself.

So the cell is REFUSED — `refuseUnstageableGroupKey` condition (3) — and routed
to the coordinator-local pipeline, where the derived table's Project is a real
operator and the alias is a real column. That answers PostgreSQL's rows for
every shape in the cell INCLUDING the ones the DAG previously got right by
luck, which is the only disposition that is base-or-better everywhere.

The refusal is a placeholder for a stage-level answer, not a settled position.
The real fix re-spells `GroupByCols` over the producing fragment's
actually-emitted columns — the `emittedThroughPassThrough` /
`respellSpecsOverProducerOutput` machinery `absorbWindowArmProjection` itself
uses — as a pass that runs AFTER the projection passes. When that exists,
condition (3) is deleted and `TestWindowOutputAsAGroupKeyMatchesPostgres`'s
`routed` flags flip to false; every entry there asserts BOTH the rows and
whether the refusal fired, so neither half can move in silence.

The same one-field problem in a shape with no window in it — a computed alias
over a BARE SCAN, and its aggregate-wrapped spelling — is **#781**, pinned in
that gate. The discarded wide predicate would not have fixed the
aggregate-wrapped spelling either: `COUNT(*) + 0 AS w` references no `__win_N`
slot, so no window-shaped condition can see it. That is the clearest evidence
that the question is about what a STAGE emits and not about what kind of node
produced it.

Both halves were needed. With only the walks widened, `SELECT g + 1, COUNT(*),
SUM(COUNT(*)) OVER ()` stopped failing loudly and started answering NULL, and
`ORDER BY COUNT(*)` started answering in an arbitrary order — a LOUD failure
turned SILENT, which is a regression in kind (protocol item 8) even though the
loud failure was itself an accident of a different column's projection.

## Consequences

- A site that has to ask "is this the same expression as that group key"
  calls `ExprIdentity`. A site that has to name the key's column calls
  `GroupKeyName`, or reads `groupKeyOutputs`. Adding a new consumer without
  one of them is the way this class comes back.
- The identity is a MAP KEY that crosses package boundaries and is
  recovered by re-parsing a stage's published name, so its exact rendering
  is pinned by `TestExprIdentityIsStable` and its idempotence by
  `TestExprIdentityIsIdempotent`. Changing the rendering is changing what
  two planners agree on.
- A COLLISION is IMPOSSIBLE, not accepted. The materialized value lives in
  the reserved namespace, which no query can spell, so nothing the user
  writes can be mistaken for it or hidden by it. The earlier draft of this
  ADR accepted the collision; the review refuted that with a 5-arm matrix
  (single-process, single-process spilled, DAG, DAG broadcast, DAG spilled)
  and the position is corrected above.
- A key a rename Project defines is re-spelled into SOURCE columns for
  DISPATCH only (`aggStageDispatchKey`): the DAG flattens a rename Project,
  so `a_b + 1` over `(SELECT c_i32 AS a_b …)` reached the worker spelled
  over a column the scan does not emit and collapsed the table into ONE
  NULL group. `aggregateOutputName` deliberately does NOT take that path —
  it answers what a SORT KEY names, and a sort key is resolved on both
  engines, of which only one has a dispatch spelling.
- Not decided here, and filed instead:
  - **#731** — an unquoted identifier is never folded to lower case
    anywhere in the engine, so `GROUP BY G + 1` still computes a key from a
    name nothing resolves and collapses the table into one NULL group.
    That is column RESOLUTION, not key matching: `SELECT G FROM t` with no
    GROUP BY is wrong the same way. Pinned in the two-path corpus.
  - **#732** — an unaliased expression is named after its own text where
    PostgreSQL names it `?column?`. Both paths agree with each other; the
    rule is about naming a select ITEM, not resolving a KEY.
  - **#729** — the DAG declared FLOAT64 for arithmetic whose value is an
    exact DECIMAL. CLOSED for a WINDOW's output (2026-09-01): `inputColTypes`
    and `inputColDecimal` stopped at a `NodeWindow`, so the slot had no
    declared type and the float rule stood; both walks now add the window's
    own slots from `windowSpecOutputType`, which `emittedColDecimal` has read
    since #586. What remains is the AGGREGATE's output declaration —
    `SUM(w * 2)` over a DECIMAL window output still fails the #361 store guard
    at `final_aggregate` on both DAG arms, byte-identical to `de95b3b5`,
    because `AggSpec` has an OutputType and no (p,s) (ADR-0024 item 2). Filed
    as **#775**.
  - **#774** — CLOSED 2026-09-01, and it was §4's own defect one reader over
    rather than a third consumer of the HAVING respelling. A WHERE on the key
    applied ABOVE the window admitted no row at all, on every arm: the outer
    predicate is pushed below the derived table's Project and `k` substituted
    away to `(g + 1)`, which above the aggregate is a NAME and not arithmetic,
    so the filter was UNKNOWN on every row and a filter admits only TRUE. The
    substitution is DECLINED for a definition that is not evaluable over group
    rows — `projRefs.overAgg` — and that flag was read off
    `AggregateBelowProject`, the Filter-only walk. It now reads
    `AggregateOverGroupRows`, the fourth reader of §4's list. Nothing about the
    predicate's spelling changed; what changed is that it stays where the query
    wrote it. Gated on nine spellings plus the SUM and COUNT faces in
    `R4/…/WhereOnTheKeyAboveAWindow`, with the four controls that make the cell
    exactly this one.
  - **#749** — `DECIMAL(38,10)` arithmetic keeps too few decimal places
    (`d + 1` is `201.000000013` where PostgreSQL says `201.0000000125`), on
    every arm of every tree. Inherited by the group-key family for the same
    reason: the key reaches the arithmetic now.
  - **#736** — CLOSED 2026-09-01. What remained of the DAG half after the
    reference rule above closed the collision shapes was re-measured over
    `typemx` on four arms (single, single spilled at 512 KiB, DAG, DAG
    `BroadcastBytesOverride=1`) against a live PostgreSQL 17: eight of the
    eleven shapes the issue tabulates already agreed everywhere, and the
    remaining three were three DIFFERENT DAG mechanisms, none of them the
    reference rule.

    One was a naming asymmetry and is fixed outright. An aggregate whose
    ARGUMENT is spelled like the key failed LOUDLY —
    `column "\"g + 1\"" does not exist` — because the parser records a
    delimited argument WITH its quotes (`ColRef.String()` re-delimits), the
    single-process path strips them with `NormalizeIdentRef` and the DAG
    carried the spelling verbatim, so the alias lookup missed and the argument
    was never re-spelled to its source column. A delimited identifier's quotes
    are not part of its NAME (§2b), and that is as true of an aggregate's
    argument as of a key.

    The other two need the SEPARATION §2 is about, and a `Stage` has one field
    where the single-process path has two. `Stage.GroupByCols` is
    simultaneously what the worker computes the key FROM and what it publishes
    it AS; the worker re-derives "is this key derived?" by PARSING that text
    and mints its own slot. So a key an aggregate DIRECTLY BELOW already
    publishes was recomputed against a schema that no longer has its leaves
    (the DISTINCT lowering — ONE NULL group), and a derived key sharing its
    published name with one of the aggregate's own outputs won every by-name
    lookup above it, because a batch resolves a name to its FIRST column and
    the aggregate emits keys before outputs (the KEY's value under the
    aggregate's alias).

    Both are REFUSED and ROUTED (`ErrGroupKeyDistributed` →
    `runGroupKeyLocal`) rather than fixed on the DAG, and that is a
    deliberate, narrow choice rather than the end of the story. The fix this
    ADR sketches — the source-spelled expression on the stage beside the
    published name — is a wire change through `distributed.OpSpec` and the
    worker's aggregate builder, and the v0.18.8 author probed one shape of it
    (a `groupKeysPublishedBelow` consultation at stage emission) and reverted
    it when it never fired. The refusal makes the two shapes ANSWER
    PostgreSQL's rows on every arm today, and its predicate is exactly the
    condition `groupKeyOutputs` already computes, so the day a Stage carries
    two names the refusal is deleted with the same one-line test.

    The narrowness is asserted, not asserted-to:
    `ctl/an-ordinary-computed-key-still-runs-on-the-dag` fails if the refusal
    starts swallowing plain `GROUP BY g + 1`.

    "All on the DAG arms alone" was written here and is FALSE with a WINDOW
    present: `SELECT DISTINCT g + 1 AS k, ROW_NUMBER() OVER (…) … GROUP BY
    g + 1` was one NULL group on the SINGLE-process path too, for §4's third
    walk rather than for #736. Both halves now answer.

    The refusal's own boundary is measured, and it is where the next finding
    lives. Its first cut asked `!Derived && !nameIsPlainColumn(Name)`, which is
    true of `GROUP BY n1.n_name` — a key that is not derived because it IS a
    column, whose qualified name is not "plain" by that predicate. TPC-H Q07
    was refused and routed local, and `TestTPCHStageDumpGolden` is what said
    so. `groupKeyOutputs` now records `PublishedBelow` — WOULD be derived, but
    an aggregate below already publishes it — and the refusal reads that, so
    the two reasons a key needs no materializing cannot be confused again.

    That narrowing also decided **#781**, filed from the same matrix while
    gating #777: a computed alias over a BARE SCAN used as a key through a
    join or a DISTINCT. The wide predicate answered it, the narrow one does
    not, and the wide one is not available — so #781 stays open and pinned.
    Its key IS a column of the aggregate's input by every logical-plan test;
    only the STAGE spelling is wrong (`aggStageGroupKey` dispatches the
    defining expression), so the site is `assertGroupKeysResolve` — the
    GROUP-BY twin of `assertAggregateInputsResolve`, which does not exist. One
    was written and discarded here: its input modelling excludes a JOIN, which
    is the producer under every #781 shape, so it fired on no fixture at all,
    and a guard no fixture reaches is untested code on the default path
    (method 10).

    Also still open in the family: **#785**, an aggregate aliased like the key
    BESIDE a HAVING on it, which answers zero rows on every arm and is
    therefore not the DAG's.

A related naming rule, settled here because two of the four review findings
turned on it: **an ALIAS is a name, and its case is part of it.** A
delimited alias is published as written on every path, and a positional
`ORDER BY` resolves to the select ITEM, never to the alias's text re-parsed
as an expression.

## Gates

- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/SlotCollidesWithAStoredColumn` — the allocator, over a fixture that
  STORES a column named like the slot: two and three derived keys, either
  order, with HAVING, minted by a derived alias with no stored column at
  all, and an aggregate over the stored column asserted on its VALUE.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/ArithmeticKeyBesideADelimitedColumnOfThatText` and
  `R4/…/DelimitedAliasUnderPositionalOrderBy`.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/AGroupKeyIsResolvedByIdentityNotBySpelling` — the whole spelling
  matrix on both arms, asserted as ordered rows with per-key counts
  computed from the fixture generator, plus the two controls
  (`ctl/AssociativityIsNotSpelling`, `DelimitedIdentifierKey`) that fail if
  the identity is made too coarse.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `N3/HavingOnAComputedGroupKey` — #720's five rows, and the refusal
  PostgreSQL gives for a select-list alias in HAVING.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/WindowAboveTheAggregate` and its four siblings — the ordered answer,
  key sequence and window value per row, over the repro, the same query
  ordered BY the window's output, PARTITION BY the key, a HAVING beside it,
  and a BARE key as the control that needs no materialization; plus
  `WindowOverAnAggregateOutput` in the reuse and the hoist spellings (#737).
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/SlotCollidesWithAStoredColumn/SiblingAggregatesEachMintingASlot` —
  the fixture §2a's scope claim needs (#759).
- `tpch.TestPostgresOracle` § `groupKeySpellingCases` — the same matrix
  asked of live PostgreSQL 17 over TPC-H, including the DECIMAL key under
  `TPCH_DECIMAL=1`, where the comparison is digit for digit.
- `sql.TestExprIdentityErasesOnlySpelling` — the identity's contract in
  both directions: spellings that must collapse, and expressions that must
  not.

Both halves were verified able to fail: stubbing `ExprIdentity` to
`n.String()` fails the paren-nested, identifier-case and ORDER BY entries;
stubbing `GroupKeyName` the same way fails every delimited-identifier
entry.
