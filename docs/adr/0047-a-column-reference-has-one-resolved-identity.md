# ADR-0047: A column reference has one resolved identity

- Status: Accepted (2026-10-06), stages 1 and 2.
- Deciders: Derek Wright
- Related: [ADR-0026](0026-a-group-key-has-one-identity-and-one-name.md), [names-scopes r10](0012-divergences/names-scopes.md#catalog).

## Decision

On the single-process engine, match a GROUP BY term by its resolved column
bindings when the block is bound in full. Keep the spelling comparison for
unbound blocks. The five-arm gate is
`coordinator.TestArcGKGroupKeySpellingEveryArm`: `pair/*`, `shape/*`, `j/*`,
`ci1/innerJoin*` and `g/grouping*` measure the supported comparisons;
`qd/*`, `j/using*` and `corr/*` record the retained limits.

The DAG arms keep their base answers until stage 5. In particular,
`boundary_qualified_key_bare_select_item` in
`coordinator.TestTheIdentityErasesAQualifierAndATypeSynonym` and the group-key
gate's `ci1/innerJoin*` rows keep 42803, as catalogued in names-scopes r10.

## Measured scope

| Rule | Gate rows |
|---|---|
| A qualified and a bare reference to one input column match in a bound block | `pair/numeric/itemQual/ordinalUnaliased`, `pair/numeric/keyQual/ordinalUnaliased`, `ci1/innerJoin*` |
| References to different join inputs remain different | `j/selfOther*`, `j/ambig*` |
| Bare ORDER BY names prefer outputs; qualified names refer to inputs | `alias/negOrdQual`, `alias/negOrdQualKeyQual` |
| An output alias does not cover an ungrouped input in HAVING | `collide/havingQual/sel` |
| Commuted operands and constant-folded expressions remain different terms | `spell/commutedBare/sel`, `spell/constFold/sel` |
| DISTINCT can read an already published key under its other spelling | `pair/int/keyQual/distinct` |
| A subquery's reference to this block needs a grouped column, even when its body repeats the key expression | `corr/subqInSelOuterKeyBare`, `corr/subqInSelOuterKeyCtl`, `corr/subqInHaving`, `corrMatrix/*` |

These are rows of `TestArcGKGroupKeySpellingEveryArm`. The correlated rows
check the single-process refusal's SQLSTATE and message class; DAG rows keep
the measured base answer. A retained refusal is compared with the base by
SQLSTATE, separately from the message check. Grouped-column controls that
need unsupported subquery execution keep their measured base refusal.

## Binding and window terms

`plansql.ColRef.Bound` carries the binder's column binding. The census in
`wadjet.TestArcCI1BindingCensusOverTheGroupKeyTable` checks the recorded
blocks for mixed bindings, the observed comparisons for bound/unbound
mixtures, and the compared relation outputs for ordinal disagreement.
`tpch.TestCI1BindingCensusTPCH` makes the same checks on its TPC-H corpus.
These assertions describe those corpora and observed comparison sites.

A window term over an aggregate is still substituted by
`logical.respellOverAggregate` in `internal/planner/logical/builder.go`.
The bound tree decides the match; the substitution's output text is not what
that match reads. The group-key gate's `pair/*/window` rows and the binding
census measure the result and the compared trees.

Expression-subquery and LATERAL bodies retain the spelling comparison in
stage 1 (`corr/innerShadow`, `corr/lateralGrouped`, `corr/inSubqGrouped`).
The grouped check separately uses the binder's body scopes to judge a
subquery reference to the containing block (`corrMatrix/*`). Dotted names
and unfolded FROM-less forms retain the gate's recorded answers (`qd/*`,
`corr/fromlessSubq`, `corr/fromlessSubqKeyQual`).

## Other gates

- `server.TestArcCI1GroupKeyByBindingNeverPublishesAPolicedValue`, rows
  `maskedKeyKeyQual`, `maskedKeyOverJoin`, `rowFilteredKeyOverJoin`: masked
  key values and stored-column row filtering across nine doors.
- `pgwire.TestArcGKGroupKeySpellingOnTheWire` and
  `wadjet.TestArcGKEmbeddedGroupKeySpelling`: declared types, CTAS and
  INSERT SELECT for their group-key statements.
- `wadjet.TestArcGKGroupKeyMatchPlanningBound`: depth 16, 200 items over
  50 keys and a twelve-way join, each under the gate's two-second bound.
- `go run ./tools/licensecheck .` and `TestAGPLPhysicalMemberBudget`:
  the existing package and physical-member budgets.

## Stages

| Stage | State | What reads the binding | What stays keyed by name, and where it moves |
|---|---|---|---|
| 1 | done (2026-10-06, #1524) | the single-process GROUP BY term match | — |
| 2 | done (#1393) | the single-process declaration walk: a node's output is an ordered identity list (`logical.Node.OutputColumns` / `OutputIDs`, from the instance the binder records on the FROM item, `plansql.TableRef.Rel`); `physical.declWalk.outputs` declares each position once per walk; a bound reference — a projection's leaf, a GROUP BY key, LAG's default — is declared by the position its binding names; a GROUP BY key is read from its tree, never its text | the PostgreSQL category (`ColDecls.pgCat`) and the strict-integer set, by name; the rename chase that re-spells a computed key for execution (`resolveAggInputName`, stage 6); a node a rewrite rebuilt without its instance answers by name; the stage DAG's per-stage declarations (`GroupByTypes`, `GroupByDecimal`, stages 5 and 7) |

Stage 2's gates: `coordinator.TestArcCI2DeclaredOutputByIdentityEveryArm`
(1,573 cells × five arms: eight origins of a relation that publishes another
relation's names at other types, nine expressions, thirteen consumer sites;
the stage-DAG arms keep their base answers as kept lines),
`pgwire.TestArcCI2DeclaredOutputOnTheWire` (#1393's statements and the
grouped forms, text and binary, INSERT … SELECT *, CREATE TABLE … AS and
MERGE), `wadjet.TestArcCI2DeclarationByIdentityCensus` (every reference
declared by its binding is also asked by name; zero disagreements wherever
the name rules answer) and `wadjet.TestArcCI2DeclarationPlanningBound`
(derived tables nested 16 and 64 deep, 200 items over 50 keys, a twelve-way
join, each under two seconds).

The name lookups the walk keeps for unbound references resolve a qualified
reference by its PARSED qualifier and column (`physical.lookupColRef`):
cutting an expression's text at its last dot read `x + a.k` as the column
`k`, which is #1393.

## Measurement record

Across the 1,182 rows of `TestArcGKGroupKeySpellingEveryArm`, zero base
SQLSTATEs that agree with PostgreSQL move away on any arm. The gate asserts
this for each row, using the base states recorded in
`testdata/arc_ci1_base_sqlstates.tsv`, independently of diagnostic text.

Historical prototype counts and hook timings are retained in the arc's
`ci1_landing_notes.md`. Current SQLSTATE comparisons are recorded in its
`SUMMARY ROUND 2` and `ci1_author/r2/` tables.
