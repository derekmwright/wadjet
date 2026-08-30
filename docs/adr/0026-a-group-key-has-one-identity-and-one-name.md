# ADR-0026: A GROUP BY key has one identity and one published name

Status: Accepted (2026-08-30, #720 / #723 / #725)

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

### 2. `plansql.GroupKeyName` is the published name, and both engines use it

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
- A COLLISION is possible and accepted: a table with a column literally
  named `g + 1` in a query that also groups by the arithmetic `g + 1`
  publishes both under one name, and the first wins. Both engines behave
  the same way, which is the property that matters; the DAG has had this
  ambiguity since it began naming keys by their text.
- Not decided here, and filed instead:
  - **#731** — an unquoted identifier is never folded to lower case
    anywhere in the engine, so `GROUP BY G + 1` still computes a key from a
    name nothing resolves and collapses the table into one NULL group.
    That is column RESOLUTION, not key matching: `SELECT G FROM t` with no
    GROUP BY is wrong the same way. Pinned in the two-path corpus.
  - **#732** — an unaliased expression is named after its own text where
    PostgreSQL names it `?column?`. Both paths agree with each other; the
    rule is about naming a select ITEM, not resolving a KEY.

## Gates

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
