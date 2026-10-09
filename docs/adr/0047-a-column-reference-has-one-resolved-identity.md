# ADR-0047: A column reference has one resolved identity

- Status: Accepted (2026-10-06), stages 1 and 2; stage 3 2026-10-09.
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

A window's terms — its arguments, PARTITION BY and ORDER BY keys and frame
offsets — are the select item's own `plansql.WindowFuncNode`; `plansql.WindowSpec`
carries only the function's name, the alias and the frame. The binder stamps
the terms in the block's input scope, so an output alias is not visible there
(`SELECT i AS k, sum(i) OVER (PARTITION BY k) …` is 42703, as on PostgreSQL).
Above a GROUP BY, `logical.respellWindowTerm` substitutes a term over the
aggregate's outputs by matching that tree: a group key by binding
(`plansql.ReplaceGroupKeyRefs`), an aggregate call by the call's text, as
HAVING and the select list match theirs. The rewritten trees travel beside the
published text (`WindowExpr.ArgExprs`, `PartitionByExprs`, `OrderByExprs`),
and the column pruner and the argument's declaration read them. The grouped
check judges every window term, including a window call in QUALIFY or in
the ORDER BY clause, with the select item's rules: a key or an expression
over keys passes, an aggregate call passes, any other column is 42803; a
window call's terms resolve against the block's input wherever the call
sits, so an output alias is not visible inside OVER (…) in those clauses
either. Every node above the window — the projection, and QUALIFY reading
an item by its alias — reads an item holding both an aggregate and a
window call with both replaced by their slots (`logical.itemRewrites`), and
QUALIFY's own aggregates outside its window calls are computed by the
aggregate. In a block the binder does not bind, a window term also passes when it
spells a key with its qualifiers erased, which is the term the substitution
replaces with the key. Gates: `coordinator.TestArcCWWindowTermsEveryArm`
(328 cells × five arms, `qualify/*` and `qualifyMixed/*` among them),
`pgwire.TestArcCWWindowTermsOnTheWire`, the group-key gate's `pair/*/window`, `term/winAggArg` and `ci1/joinWindow*`
rows, and the binding census.

Since stage 3 an UNCORRELATED expression subquery's body is the node's
memoized parse, bound with the enclosing scope and planned from that tree, so
its terms match their keys by binding on the single-process arms
(`corr/innerShadow`, `c738/outerSameAliasInner`, `p/inSubqJoin/ord`,
`p/scalarDirectItem/sel`, `p/scalarDirectJoin/sel`, PostgreSQL's answers). A
CORRELATED body is run per outer row from its substituted text and
decorrelated from a private parse, neither of which carries a binding, so the
binder clears its bindings and judges it by spelling (`origin/outerRef/sel`,
`corr/existsOuterGrouped`, `p/correlatedJoin/ord` keep 42803); so does a body
the binder cannot classify — one holding a derived table, a WITH or a LATERAL
item (`p/scalarSubqJoin/sel`) — and a LATERAL body keeps the spelling
comparison (`corr/lateralGrouped`, `p/lateralBodyJoin`, stage 4).
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
| 1 (window terms) | done (2026-10-09, #1651, #1646; carrier C27) | a window's terms, read from the item's bound `WindowFuncNode`: the over-aggregate substitution, the column pruner, the argument's declaration and the grouped check | an aggregate call inside a window term is matched to its aggregate by its text; the stage DAG's window terms are the published text the logical plan writes (stages 5 and 6) |
| 2 | done (#1393) | the single-process declaration walk: a node's output is an ordered identity list (`logical.Node.OutputColumns` / `OutputIDs`, from the instance the binder records on the FROM item, `plansql.TableRef.Rel`); `physical.declWalk.outputs` declares each position once per walk; a bound reference — a projection's leaf, a bound GROUP BY key, LAG's default, an aggregate's argument — is declared by the position its binding names. Planning time stays in its measured degree: derived-table depth 16 / 32 / 64 / 128 plan in 8.9 / 34.5 / 247 / 1,689 ms at a0f0c322 and 10.3 / 39.7 / 303 / 2,291 ms here (fitted exponent 2.55 and 2.63; `wadjet.TestArcCI2DeclarationPlanningBound` holds depth 128 under three seconds) | an unbound GROUP BY key (a re-parse of its text) and an unbound aggregate argument (the stage DAG's re-spelled names, typed from the scans below first, `aggInputColumnType`); the PostgreSQL category (`ColDecls.pgCat`) and the strict-integer set, by name; the rename chase that re-spells a computed key for execution (`resolveAggInputName`, stage 6); a node a rewrite rebuilt without its instance answers by name; a dotted relation alias, which the binder does not bind (#1650); the stage DAG's per-stage declarations (`GroupByTypes`, `GroupByDecimal`, stages 5 and 7) |
| 3 | done (2026-10-09; #1602, #1603, #1606, #1599) | an expression subquery's body: `SubqueryNode.Select` / `ExistsNode.Select` memoize it on the node with the WITH chain in scope where it is written (`CTEScope`); the binder validates it with the enclosing scope, records the references whose binding reaches the enclosing query (`SetOuterRefs`, the correlation classifier ADR-0021 §1k now reads, `plansql.CorrelatedRefsOf`), and keeps an uncorrelated body's bindings; the physical runner, declaration, column count and inner scope of each subquery are a child planner over the node's chain that plans its memoized body (`physical.subqueryScopingIn`, `memoBodyFor`); the declaration pass and the declaration walk ask a subquery by its node (`subqueryNodeDeclIn`, `ColDecls.subqueryNode`); a WITH item binds the innermost item of its name and a CTE materialization answers by the item's identity (`logical.Node.CTEIdent`, `Planner.cteCacheFor`); a block run more than once reads a volatile WITH item's one evaluation (`plansql.ReadPerRun`); the stage DAG plans eager runs, IN sets and producer stages in the node's chain from its memo; the per-build policy binder resolves the body's WITH items against that chain (`SelectInfo.SetEnclosingCTEs`). The body is parsed ONCE per statement — the statement reader's syntax parse seeds the memo, and every requester reads it (ADR-0032's 2026-10-09 amendment; `wadjet.TestArcCI3SubqueryBodyParsesOnce`) — and a declaration plans the body once: the answer column, integer width and PostgreSQL category come from one plan (three plans, each re-declaring the nested bodies, planned a body at depth k 3^k times). Planning time over nesting depth (the closure review's shape A, a derived table holding its own WITH and a scalar per level, EXPLAIN best of 3): depth 1 / 4 / 8 plan in 0.39 / 4.11 / 61.7 ms at 542b4f37 and 0.70 / 4.48 / 53.6 ms here, ×2.04 and ×1.81 per level (×3.11 at the round-1 tip, 6.6 s at depth 8; `wadjet.TestArcCI3SubqueryNestingPlanningBound`). TPC-H planning time (EXPLAIN, best of 5, SF0.01, minimum of six interleaved runs, ms, 542b4f37 → here): Q02 2.98 → 3.42, Q04 0.64 → 0.82, Q17 1.09 → 1.24, Q20 3.22 → 3.01, Q21 2.81 → 3.30, Q22 0.92 → 0.90 — the binder's walk of each body with a copy of the enclosing scope, a constant per body | a CORRELATED body (run per outer row from substituted text, stage 8) and the decorrelated join keys a body yields (stage 4, `repairDecorrelatedSpelling`); a body holding a derived table, a WITH or a LATERAL item, which the binder does not classify (names, as before); the declaration stamp keyed by a subquery's text (`SubqueryColDecls`) for the walks that hold no Planner; the stage DAG's filter subqueries the plan carries only as text, and an aggregate whose input holds a subquery, carried by its spelling and re-parsed by four DAG readers (`sum((SELECT max(id) FROM t))`: the body parses 5 times on the DAG arms, once on the embedded door); the recursive term's arms, split from the body's text (ADR-0032) |

A SELECT item that is a GROUP BY key by its binding but spelled apart from it
(`t.i + 1` over `GROUP BY i + 1`) is declared by the key's published column
(`declWalk.boundKeyNamesBelow`), as the projection the planner builds reads
it: the zero-row RowDescription of `SELECT t.i + 1 FROM ss_t t WHERE false
GROUP BY i + 1`, a CREATE TABLE AS from it and a scalar subquery over it were
double precision at 542b4f37 and are the key's integer now.

Stage 2's gates: `coordinator.TestArcCI2DeclaredOutputByIdentityEveryArm`
(1,662 cells × five arms: eight origins of a relation that publishes another
relation's names at other types, nine expressions, thirteen consumer sites,
and nine aggregates over a derived column that shadows a scan column of
another type; the stage-DAG arms keep their base answers as kept lines),
`pgwire.TestArcCI2DeclaredOutputOnTheWire` (#1393's statements, the
grouped forms and the aggregate arguments, text and binary, INSERT … SELECT *, CREATE TABLE … AS and
MERGE), `wadjet.TestArcCI2DeclarationByIdentityCensus` (every reference
declared by its binding is also asked by name; zero disagreements wherever
the name rules answer) and `wadjet.TestArcCI2DeclarationPlanningBound`
(derived tables nested 16 and 64 deep, 200 items over 50 keys, a twelve-way
join, each under two seconds; depth 128 under three).

The name lookups the walk keeps for unbound references — `physical.lookupColRef`
and the aggregate argument's scan lookup (`scanColumnType`,
`scanColumnDecimal`) — resolve a qualified reference by its PARSED qualifier
and column: cutting an expression's text at its last dot read `x + a.k` as the
column `k`, which is #1393 (`pgwire.TestArcCI2DeclaredOutputOnTheWire` grp/*;
agg/* for the aggregate argument).

## Measurement record

Across the 1,182 rows of `TestArcGKGroupKeySpellingEveryArm`, zero base
SQLSTATEs that agree with PostgreSQL move away on any arm. The gate asserts
this for each row, using the base states recorded in
`testdata/arc_ci1_base_sqlstates.tsv`, independently of diagnostic text.

The base SQLSTATE of every row is the gate's own record,
`testdata/arc_ci1_base_sqlstates.tsv`; the prototype counts and hook timings
measured while the stage was built are not carried in the repository.
