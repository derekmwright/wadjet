# ADR-0021: A decorrelated subquery's names are resolved from the plan, and the sets it cannot join are materialized

Status: Accepted (2026-08-25; §1a added the same day after a derived-table; §1b added for the CTE and recursive-CTE remainder
inner was found to reach the executor as a scan of a nonexistent table; §3
added the same day (#562))

## Context

`IN (SELECT …)`, `NOT IN (SELECT …)` and correlated `EXISTS` are all lowered
the same way: the logical optimizer rewrites them into a semi or anti join
whose **build side is the subquery's own plan** — `Scan → [Join …] → [Filter]
→ [Aggregate]`, and never a `Project`. That shape is the source of two
separate classes of defect, and between 2026-08-24 and 08-25 both were
measured against live PostgreSQL 17.

**Names.** Because there is no Project, the build side carries the SOURCE
column names of the relations it reads, and the rewrite has to name its
build-side references the way that side emits them. With ONE inner relation
the bottom Scan emits every column bare, so the rewrite can strip a qualifier
and be provably right (#516). With a JOIN it cannot: a join emits its PROBE
side's columns bare and qualifies a BUILD column only where the bare name
collides (`exec.joinOutputSchemaWithMapping`), and which side is which is
decided by `reorderJoins` from **estimated row counts** at `Optimize` step 73
— long after the rewrites run at steps 35/36.

Both available answers are wrong half the time. Keeping the qualifier is
wrong when the join emits the column bare; stripping it is wrong when the join
qualifies it. #516 shipped the strip and had to scope it back within a day
(#526, #527) precisely because each was right for one case and silently wrong
for the other. "Silently" is the operative word: the physical planner splits
the condition literally, `exec.HashJoin.FixKeyAssignment` swaps the pair on the
premise that a left key resolvable in the build must be misassigned, and the
join then matches nothing — `IN` answers zero rows, `NOT IN` answers every row.

**Sets.** Three guards in `tryDecorrelateInSubquery` DECLINE the rewrite: a
subquery carrying `LIMIT`/`OFFSET` (#482 — decorrelating matched the FULL
unbounded set), an ungrouped aggregate item, and a computed item (#516 — the
key named nothing). Declining is right; each of those was a wrong answer
before. But a declined IN stays a subquery PREDICATE, and the stage DAG had
nothing to execute one with. `Planner.resolveSubqueryAST` handled a scalar
`SubqueryNode` and fell through `default:` for `InExpr`, so the filter shipped
to the worker verbatim and failed with *"IN subquery requires a
SubqueryRunner"* (#524). The single-process path answered every one of those
correctly, which made it a two-path divergence where the distributed side
errored.

**Narrowing.** A third class sits beside the other two and is reached through
the same shape. `dedupSemiAntiBuildSide` PROJECTS the build side down to the
join keys, so how it reads the condition decides which columns still exist by
the time the join compares them. It read the condition as TEXT, split on
`" and "` — and a decorrelation renders `" AND "`, so a two-key correlation
lost its second key and the join matched nothing (#562). Nothing in any corpus
here correlated on more than one column, in this project or in the fuzzer, so
the shape had never been asked.

## Decision

### 1. Record what a reference MEANS; settle the text after `reorderJoins`

The decorrelations no longer choose a spelling. They record the relation
qualifier and source column as the subquery wrote them
(`logical.InnerKeyRef`), and `repairDecorrelatedSpelling` — a pass that runs
immediately after `reorderJoins` — turns each reference back into text by
modelling what the build subtree actually emits
(`logical/inner_key_spelling.go`).

The model mirrors `exec.joinOutputSchemaWithMapping`: probe columns verbatim,
then build columns qualified by their owning relation exactly where the bare
name already occurs on the probe side. It also mirrors
`exec.HashAggregate.outputSchema`, which is a SECOND renaming and was got
wrong first time round: a group key READS one name and EMITS another, because
the aggregate strips the qualifier off its output column unless stripping
would make two keys collide. Modelling the output as the key's own text left
a semi join over a grouped inner naming `c.x` while the aggregate emitted `x`
— the same wrong answer one node higher. The pass walks bottom-up so those
output names are settled before the join above resolves a key against them.

Three properties make this safe to adopt everywhere rather than case by case:

- **It is a no-op where the old rule was provable.** A single-relation inner
  resolves to the same bare column it always did.
- **A reference it cannot resolve keeps the spelling the rewrite wrote**, so a
  plan that never reaches the repair (an un-annotated Scan, no catalog) reads
  exactly as it did before.
- **It applies to every site with the same exposure**, not only the reported
  one: the IN key, the GROUP BY term of a grouped inner (and the aggregate's
  own output renaming above it), a correlated EXISTS's equality key, its
  non-equality JoinFilter term, and the inner-only WHERE conditions — which
  had the same premise and a worse failure, since a stripped
  `c.n_nationkey < 3` over `nation c JOIN nation b` is *pushed to the wrong
  relation* and changes the membership set outright.

One inner-only shape has no spelling at all and is DECLINED rather than
guessed: a condition naming MORE THAN ONE inner relation. Stripped, pushdown
lands `c.x > b.x` on one scan as `x > x`, which compares that relation's
column against itself; qualified, it stays above the join, where one side's
column is emitted bare and the qualified spelling names nothing. The IN stays
a subquery predicate, executed as written — which §2 now lets the stage DAG do
too.

Two consequences are accepted. `dedupSemiAntiBuildSide` has to run before
`reorderJoins` for its NDV bound to reach the cost model — which is before the
spelling exists — so it DEFERS exactly the shape in doubt (a qualified
reference over a joined build) and the repair re-applies it there, later than
the reorderer's costing saw it. And the physical layer had to stop stripping
qualifiers too: `BuildSemiAntiFilter`, `extractFilterBuildColumns` and
`ParseSemiAntiNE` keep the qualified spelling and resolve it with the same
bare-name fallback the key index uses, because the strip there was the same
defect one layer down.

### 1a. A relation the rewrite cannot BUILD is declined too

Naming is not the only thing the rewrites get from the subquery. Each of them
turns the subquery's FROM/JOIN list into Scan nodes directly —
`NewScan(info.Tables[0].Name, …)` — and a DERIVED TABLE is not a name a Scan
can hold. The parser keeps a FROM-subquery as a table whose NAME is its own
SQL text, `(SELECT …)`, and the plan BUILDER recognises that prefix and
recurses into it. The rewrites did not, so the semi/anti join's build side
became a scan of a table the catalog has never heard of.

That scan does not fail. It yields ZERO batches, so the build side was empty
and `IN` answered nothing while `NOT IN` answered every row — on both paths,
with no error anywhere (#571). Two further defects were reachable only through
it: the runtime key repair then fires on a key-only build and wipes NOT IN's
NULL poison (#572), and the subquery-predicate route the decline now takes had
never stated the empty-set boundary at all, dropping a NULL-keyed probe row
where `x NOT IN ()` is TRUE.

Building the derived plan here instead was rejected on §1's own terms: the
rewrite would then have to NAME the derived side's columns, and
`emittedColumns` has no model for a derived scan — it reads `ScanColumns`,
which the catalog annotation fills and a derived table has no catalog entry
for. `spellInner` reports "unresolved" and the caller keeps the rewrite's
pre-reorder guess, which is exactly the silent wrong answer §1 exists to
remove. A CTE name has the same exposure by a different spelling; §1b covers it.

### 1b. A CTE name is declined too, and a recursive CTE is refused downstream

A CTE reference is `NewScan(cteName)` — a table the catalog has never heard of,
the same empty-build failure §1a describes, but spelled as a bare identifier
rather than `(SELECT …)`. The decline could not see it because the layer had
no CTE list. It does now: `Optimize` threads the enclosing `WITH`
(`plan.CTEs`) into the three decorrelations, and `innerRelationsAreScannable`
declines a FROM/JOIN item whose name is a CTE — from the enclosing statement or
one the subquery declares itself — exactly as it declines a derived table
(#535, #581, the build side). The declined subquery is executed as written, and
the routes resolve the CTE: `buildSubqueryPipeline` merges the enclosing `WITH`
before building, so the materialized IN-set and the local pipeline both see it.

A RECURSIVE CTE is declined by the same rule, but the materialized-set route
has a hole the decline makes reachable: `buildSubqueryPipeline` has no
fixed-point cache, so it reads a recursive reference as ZERO rows with no
error, and §2's `len(rows)==0` branch would take that for a genuine empty set —
`IN` answered 0 and `NOT IN` every row on the DAG. So `materializeInSubquery`
now REFUSES a subquery whose FROM reads a recursive CTE (at any nesting,
through a derived wrapper, and through the subquery's own `WITH`) and routes it
to the coordinator-local pipeline, which materializes the recursive CTE and
answers it. Building the CTE plan here instead was not chosen, for §1a's
reason and for consistency with the derived-table decline; a CTE feeding a
subquery is a slower right answer on the local/materialize route, which the
maintainer's own "a slower right answer beats a wrong one" settles.

Two shapes remain PINNED, tracked and gated as divergences: a derived table's
column-alias LIST `(…) AS b(kk,nn)`, which the builder drops so the aliased
names resolve to nothing (#613), and a CTE on the PROBE side, which is not
decorrelated at all because its outer-scope collectors lack the CTE's alias
(#535). Both reproduce with one key and are the corpus's `derived_*_colalias`
and `cte_probe_base_build` / `cte_referenced_twice` entries.

### 2. An IN-subquery the join cannot express is a SET, and the coordinator materializes it

`resolveSubqueryAST` gains an `InExpr` case. An uncorrelated IN-subquery is
executed once on the coordinator and its rows become the literal list the
expression layer already evaluates — including NOT IN's three-valued rule over
a NULL in the list (#370), which is the same rule #507 gave the semi-join
lowering. The subquery runs AS WRITTEN, so its `LIMIT`, `OFFSET` and `ORDER
BY` mean what they say, which is exactly why #482 made the rewrite decline.

Two bounds, and crossing either is a typed refusal rather than a guess:

- **The set must fit.** A declined shape can be unbounded, and inlining a
  million literals into a filter expression is not a plan. The bound is a
  plan-TEXT budget (the expression is serialized into every task), default
  10,000 rows, `WADJET_IN_SET_MAX` to override and `=0` to disable
  materialization entirely. It is a bound on what gets INLINED, not on what
  gets read: `executeSubquery` collects the whole result before the count is
  checked, so an unbounded subquery is materialized in coordinator memory once
  and then refused — and executed a SECOND time on the local route. Capping
  the sink so the read stops at the bound is the honest fix and is not done
  here; the row count is the only thing the refusal currently protects.
- **Every value must have a literal spelling that survives the round trip
  through the filter's text, AND a kernel that compares it the way the engine
  stores it.** Integers, finite float64s (round-trip checked), strings,
  booleans and NULL qualify. NaN and the infinities have no numeric literal in
  this dialect. FLOAT32 is refused for a different reason: the IN-literal-list
  kernel compares a float32 column in float64 space while `=` narrows the
  literal, so a set of eight values that eight rows satisfy under an
  OR-of-equals matches none of them through IN (#549). Inlining there would
  turn a loud failure into a silent wrong answer, which is the one trade this
  whole change exists to avoid.

`ErrInSubqueryDistributed` routes the query to the coordinator-local
single-process pipeline, where `expr.InSubquery` resolves the set once under
`resolveMu` and caches it — the same handoff #359 makes for correlated
subqueries and #466 for an unstageable DISTINCT. A slower right answer beats
an error, and both beat a different one.

An EMPTY set is a real answer and not an absence: `x IN ()` is FALSE for every
row and `x NOT IN ()` is TRUE for every row, including a row whose key is
NULL, because an empty set has nothing to be UNKNOWN about. Neither renders as
an empty value list, so both render as the constant they are.

### 3. A build-side narrowing is all-or-nothing, and the condition is read STRUCTURALLY

`dedupSemiAntiBuildSide` narrows a semi/anti join's build side to
`Project(keys) → Distinct` so the hash table is sized to NDV rather than to
raw rows. The caller projects the build down to exactly the key list
`extractRightJoinKeys` returns, which makes that list a correctness interface
and not a hint: a list short by one conjunct DELETES a column the join still
compares, and the join then matches nothing — the semi answers zero rows and
the anti answers every row, in silence.

It read the condition by splitting the TEXT on `" and "` and then on the first
`"="`. A decorrelation renders its condition with `" AND "`
(`renderDecorrelatedKeys`), which that split does not see: a two-key
correlation arrived as ONE part whose right operand was the literal text
`o_custkey AND o_orderstatus = o_orderstatus`, only the first conjunct's key
survived, and every two-column correlated `EXISTS` answered 0 (#562). It is
the same lexical-where-the-condition-is-structural defect
`physical.parseJoinKeys` was rewritten for in #351, one layer up, and it had
been unreachable only because nothing in any corpus correlated on two columns.

So the same rule applies at both layers: parse, flatten the top-level ANDs,
require each conjunct to be an equality between two bare column references,
and DECLINE the whole condition on anything else. FOUR declines. The first
three are because the narrowing cannot attribute the key:

- a conjunct that is not an equality of two columns (a literal operand, the
  `1 = 1` ON-TRUE sentinel, an expression) names no build column;
- a name that resolves on BOTH sides — a self-join's `k = k` — is not
  attributable from the condition alone, which is what it always did;
- two keys whose BARE names collide, because the Project aliases each key to
  its bare name and the second would then read the first's column.

The fourth is about the Project rather than the condition, and it is the one
the first cut of this decision got wrong. The Project aliases EVERY key to its
bare name (`Projection{Column: k, Alias: stripQualifier(k)}`), so a key the
condition spells QUALIFIED is renamed out from under the condition still
asking for it: the build emits `q_s` while the join looks up `b1.q_s`, which
resolves to index -1 and matches nothing. That is reachable whenever the build
subtree has two arms sharing a bare name and `reorderJoins` therefore
qualifies one side — an `EXISTS` whose inner self-joins. Aliasing the key to
its qualified text instead is not available: the spelling is settled by the
model in §1 and the narrowing does not get to re-decide it. So:

- a key whose text is not already its bare name declines.

The NDV bound is a performance optimization and the key list is not, so a
decline costs a bigger hash table and nothing else. `dedupSemiAntiBuildSide`
is registered in `internal/optswitch` as `WADJET_SEMIANTI_BUILD_DEDUP`
(#287): it changes the row set in both directions when it is wrong — a semi
join answers nothing, an anti join answers everything — which is exactly the
class the invariance oracle enumerates, and the oracle would have reported
#562 as a divergence the first time a two-key correlation entered any corpus.

### 4. What was rejected

- **Projecting the inner plan explicitly** so the build side has a schema the
  rewrite chose. It moves the problem rather than solving it: the Project's
  own expression has to name a column the join below it emits, which is the
  same question.
- **Pinning the inner join's order** so `Tables[0]` is provably probe-most.
  It answers "which side" and not "does the bare name collide", so the
  cross-relation shape with no collision stays wrong — and it costs the
  estimator's choice on every joined inner to fix a naming problem.
- **A set-valued producer STAGE** for the materialized IN. It is the better
  answer for a large set and it is what removes the plan-time execution cost,
  but it needs a placeholder kind the coordinator can substitute a LIST into,
  which is machinery the literal list does not need. Left as the follow-up the
  row bound's refusal covers in the meantime — imperfectly, since the refusal
  reads the result before it counts it.
- **Refusing every declined IN-subquery** (#524's cheaper option). Correct,
  but it sends the whole query single-process for a bounded subquery the DAG
  could otherwise run in full — the outer query is the expensive half.

## Consequences

- The logical optimizer gained a pass whose correctness depends on a MODEL of
  another package's behavior. That model is pinned by value, not by reading:
  the answers it decides are asserted against live PostgreSQL over fixtures
  that put each relation on the probe in turn
  (`wadjet.TestInSubqueryOverAJoinedInnerAgreesWithPostgres`,
  `TestCorrelatedExistsInequalityOverAJoinedInnerAgreesWithPostgres`), and per
  type through `internal/oracle/typematrix`'s `semijoin_join_*` /
  `notin_join_` families.
- A materialized IN-set is executed at PLAN time on the coordinator. For the
  bounded shapes this exists for that is a small read. For anything larger the
  read still happens in full before the bound refuses it, and the local route
  then executes the subquery again — two executions and one full result in
  coordinator memory is the cost of a refusal today.
- A multi-column correlation is now a gated shape rather than an unreachable
  one. `internal/oracle/multikey` carries the fixture and the corpus, in TWO
  arms that differ in one thing: whether the narrowing can attribute the keys.
  The shared-schema arm's relations carry one schema, so every conjunct reads
  `s = s` and the pass DECLINES; the distinct-name arm gives them `p_`/`q_`/
  `w_` prefixes, so it FIRES. An arm with only the first gates the decline and
  never runs the narrowing — which is how the qualified-key defect above
  reached a green branch. The corpus spans
  EXISTS / NOT EXISTS / IN / NOT IN on two and three keys, over STRING+INT64,
  DECIMAL+DATE and CIDR+UUID, with the inner-only predicate on either side of
  the correlations, with NULLs in one key on each side, with the estimator's
  semi/anti swap engaged and declined, and with the inner a join or a derived
  table. Every expected count is live PostgreSQL 17's, re-derived by the
  oracle arm rather than recorded once.
- The two-path gate asserts the IN-subquery refusal fires for NOTHING in its
  corpus. An entry taking that route means the materialization declined a set
  it should have inlined — the answer stays right while distributed execution
  quietly stops, so only the counter can see it.

## Related

- ADR-0012 (PostgreSQL decides semantics), ADR-0013 (the gates and their pins)
- #516, #526, #527 (naming), #482, #524 (sets), #507 (three-valued NOT IN)
- #571 (derived-table inner), #572 (the key repair it reached), #535 (CTE)
- #562 (the multi-key narrowing), #351 (the same lexical split one layer down)
- Reachable from #562's corpus and tracked separately, because they reproduce
  with ONE key: #577 (a semi/anti join whose build side is a derived table
  matches nothing), #578 (a CORRELATED `NOT IN` answers its `NOT EXISTS`
  twin — #507's remainder) and #584 (an unqualified outer conjunct pushed onto
  a decorrelated EXISTS's subquery scan)
- `internal/planner/logical/inner_key_spelling.go`,
  `internal/planner/logical/semi_anti_dedup.go`,
  `internal/planner/physical/in_subquery_set.go`,
  `internal/oracle/multikey` (the fixture and corpus, answers pinned to live
  PostgreSQL 17),
  `docs/internals/native-dag-execution.md` §Correlated subqueries
