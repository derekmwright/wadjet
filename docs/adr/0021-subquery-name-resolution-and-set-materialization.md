# ADR-0021: A decorrelated subquery's names are resolved from the plan, and the sets it cannot join are materialized

Status: Accepted (2026-08-25)

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

### 3. What was rejected

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
- The two-path gate asserts the IN-subquery refusal fires for NOTHING in its
  corpus. An entry taking that route means the materialization declined a set
  it should have inlined — the answer stays right while distributed execution
  quietly stops, so only the counter can see it.

## Related

- ADR-0012 (PostgreSQL decides semantics), ADR-0013 (the gates and their pins)
- #516, #526, #527 (naming), #482, #524 (sets), #507 (three-valued NOT IN)
- `internal/planner/logical/inner_key_spelling.go`,
  `internal/planner/physical/in_subquery_set.go`,
  `docs/internals/native-dag-execution.md` §Correlated subqueries
