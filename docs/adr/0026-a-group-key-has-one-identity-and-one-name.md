# ADR-0026: A GROUP BY key has one identity and one published name

Status: Accepted (2026-08-30, #720 / #723 / #725; amended three times the same day after review — one identity, one SLOT, one published name, one ALLOCATOR per aggregate, and a NAME never re-read as structure)

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
| a slot is a name no query can spell | `collslot` STORES `__gb_expr_0` and `__gb_expr_1`; `ctl/TheStoredColumnIsStillGroupable` reads and groups by one |
| …including one the query itself mints | `SlotCollidesWithAStoredColumn/MintedByADerivedAlias` — `1 AS "__gb_expr_0"`, no DDL |
| the slot never shadows an aggregate's argument | `AggregateOverTheStoredColumn` — `SUM`/`MAX` of the stored column, asserted on the VALUE |
| two keys never share a slot | `TwoDerivedKeys`, `ReversedKeyOrder`, `ThreeDerivedKeys`, `WithHaving`, all with the stored slot present |
| an arithmetic key and a delimited column of that text are different things | `ArithmeticKeyBesideADelimitedColumnOfThatText` — both directions, 5 rows against 9 |
| a delimited term does not group what its name spells | `gcov` carries `"g + 1"` and `"g plus 1"`; `GroupingCoverageUnderADelimitedTerm` asserts the 42803 AND the answering direction, plus three controls that must keep refusing and one that must keep answering |

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
  - **#729** — the DAG declares FLOAT64 for arithmetic whose value is an
    exact DECIMAL. The derived-key typing above makes exact DECIMAL
    arithmetic REACH that path where it used to fail to type at all, so a
    fractional literal in a key, and an outer aggregate over one, now fail
    loudly on the DAG where the single-process path answers.
  - **#749** — `DECIMAL(38,10)` arithmetic keeps too few decimal places
    (`d + 1` is `201.000000013` where PostgreSQL says `201.0000000125`), on
    every arm of every tree. Inherited by the group-key family for the same
    reason: the key reaches the arithmetic now.
  - **#736** — what remains of the DAG half after the reference rule above
    closed the collision shapes: an aggregate whose ARGUMENT is spelled
    like the key (loud), an aggregate ALIASED like the key, and DISTINCT
    over a derived key.

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
