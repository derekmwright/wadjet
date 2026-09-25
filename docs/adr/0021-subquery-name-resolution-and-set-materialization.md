# ADR-0021: A decorrelated subquery's names are resolved from the plan, and the sets it cannot join are materialized

Status: Accepted (2026-08-25). §1a was added the same day, after a
derived-table inner was found to reach the executor as a scan of a nonexistent
table; §1b for the CTE and recursive-CTE remainder; §3 the same day (#562).
§1c (2026-09-02) made a subquery that cannot be RUN fail instead of answering
a constant. §1d–§1i (2026-09-03) are the PRODUCER half §1c said was still
short: the CTE scope the collectors could not see (§1d), the re-run's typed
outer values (§1e), the correlated NOT IN an anti join cannot express (§1f),
the aggregate argument that asked for no outer scope (§1g), the LATERAL whose
empty input still answers (§1h), and what that arc measured and did not move
(§1i). §1j (2026-09-04) gives the answer §1a and §1b declined to give — the
build side is the subquery's own FROM clause as a plan — and SUPERSEDES both
declines for every relation but a recursive CTE (#852, #616). §5 (2026-09-04)
settles what "scalar" means — at most ONE row, and the second row is 21000 —
and §5a closes §1c's named boundary: an alias hides its table name.
§2a (2026-09-04) gives a SELECT-LIST scalar subquery the producer stage a
predicate's has had since Q11 (#659), and §1f gains the other half of #539:
where a null-aware anti join's replicated build is DECLARED.
§1k (2026-09-07) settles WHICH SCOPE an unqualified name inside a subquery
binds to — the inner relation's SCHEMA decides, whatever kind of relation it is
— and §2b gives an uncorrelated EXISTS the plan-time evaluation an
uncorrelated scalar subquery and an IN-subquery already had (#955).
§1p (2026-09-14) corrects §1's own premise about the PROBE side, gives a block
its own WITH scope in the parse, the build and the rebuild, makes the nested
correlation walk the same walk as the top-level one, and gives a recursive CTE
reference the column list a join key needs (#1098, #1067, #1072, #1066).
§1o-b (2026-09-22, arc RC) settles how a recursive CTE's fixed point ENDS — at
the fixed point or with an error, never with the rows so far — and what types
and names it publishes (#1246, #1041, #1074, #1193).
§1r (2026-09-20) is the half those sections assumed: WHICH references the
rewrite finds. A body's JOIN ON was never read, and a condition naming only
the outer row was stripped or dropped — so the outer references are now a SET
collected over the whole body, each one carried or declined (#1232, #1104).

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

(SUPERSEDED by §1j, 2026-09-04: the rewrite now BUILDS it. This section
records why the decline was right while the build side was assembled out of
`NewScan`, and the defect that made the decline necessary.)

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

(SUPERSEDED IN PART by §1j, 2026-09-04: an ordinary CTE reference is now
BUILT into the build side. The RECURSIVE half stands unchanged — it is the
one relation §1j still declines, and the reason is this section's.)

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

### 1c. A subquery that cannot be RUN is not a subquery that is FALSE

(Added 2026-09-02, #734 / #679 / #535.)

The declines above are the PRODUCER half — which subqueries this engine turns
into a join, and which it leaves as a per-row predicate. What a subquery that
was left behind then ANSWERS is the consumer half, and it used to answer a
constant.

Three evaluators folded a run-time failure into a value, and folded it three
different ways for one event: `CorrelatedExistsSubquery` read
`runErr == nil && len(rows) > 0`, so a failed re-run was "does not exist";
`CorrelatedScalarSubquery` returned NULL; `CorrelatedInSubquery` returned
`e.Not`. None reached the client. And the three UNCORRELATED evaluators —
which run their subquery ONCE, query-wide, and memoize — were handed
subqueries that were correlated and had not been recognized as such, so they
ran text still naming the outer relation. Standalone that does not fail
either: `expr.ResolveColumnRef` STRIPS the qualifier and retries the bare
name, so `sub.g = typemx.g` rebinds to the inner relation's own column and
reads constant TRUE, while `y.id = x.id * 2` — where nothing rebinds — reads
constant FALSE. One misclassification, two different confident wrong answers,
decided by whether the two relations happen to share a column name.

**The rule now: a subquery this engine cannot run, or one it is about to run
standalone whose text still names a relation it does not read, FAILS the
query.** Both checks live at the evaluators, and the second uses
`plansql.DanglingTableRefs`, which needs no outer scope — so it guards a site
that has LOST that scope, independently of whether the classifier is ever
repaired. The measured effect, against live PostgreSQL 17: an EXISTS inside
an aggregate ARGUMENT (0 for 4, NOT EXISTS 9 for 5), an EXISTS over a DERIVED
table at cross widths (0 for 3, NOT EXISTS 10 for 7), an EXISTS over a CTE (0
for 47, NOT EXISTS 50 for 3) and its SCALAR spelling (0 for 47 on all four
arms) all stop answering and start failing.

**This makes those queries LOUD, not right.** The producers are unchanged and
the model is still the one §1/§1a/§1b describe: the aggregate-argument compile
site never asks for the outer scope at all, `#679`'s re-run renders a DECIMAL
outer value as a QUOTED string, and the three outer-scope collectors read only
`NodeScan` so a CTE's scope — recorded on the subtree root as `CTEName` /
`CTERefAlias` — is invisible to them. General decorrelation (a dependent join)
is what removes the class; anything short of it narrows the silent set.

**One shape stays SILENT, and it is the guard's boundary rather than an
omission.** `DanglingTableRefs` asks whether a qualified reference names a
relation no FROM clause INSIDE the subquery provides, and it is blind to an
outer table correlated BY ITS TABLE NAME where the inner relation reads the
SAME table under an alias: in `FROM typemx WHERE EXISTS (SELECT 1 FROM typemx
sub WHERE sub.g = typemx.g)`, `typemx.g` is not dangling. That query answers
50 for PostgreSQL's 47 — every row — and the same query with the outer
relation aliased is right. No scope-free predicate can tell the two apart;
closing it is a producer repair. It is pinned as
`boundary_unaliased_base_table_correlation_stays_silent` in the arc-A census
and in the correlation census.

### 1d. A CTE reference is a SCOPE the correlation collectors can see

(Added 2026-09-03, #535.)

§1c named the producer gap and left it: "the three outer-scope collectors read
only `NodeScan` so a CTE's scope — recorded on the subtree root as `CTEName` /
`CTERefAlias` — is invisible to them". This is that repair, and it is one
sentence of model: **a CTE reference is a named scope exactly as a derived
table's alias is; the two record it in different PLACES, and every reader has
to know both.**

A derived table's alias is stamped onto every scan below it
(`setSubtreeAlias`), so `Node.ScopeNames` answers for that spelling. A CTE's
name is NOT stamped, and deliberately — stamping it would make two relations
comma-joined inside the body share one identity for predicate attribution
(#281's q18 spelling) — so it sits on the SUBTREE ROOT.
`physical.subtreeNamesRelation` has read both since #653; the four correlation
collectors read only the scans. The four are
`logical.collectTableNames`, `logical.collectScanInfo`,
`physical.collectTableAliases` and `physical.collectOuterColumns`, and each now
also reads `CTEName` / `CTERefAlias` off the node it is standing on, plus the
CTE subtree's PUBLISHED column names for the unqualified spelling — `did`, the
name the CTE's own Project invents, which no scan below emits.

Measured against live PostgreSQL 17 over the type-matrix fixture, before →
after, on all four arms:

| shape | before | after | PG |
|---|---|---|---|
| `EXISTS` over a CTE | loud (0 before v0.18.16) | 47 | 47 |
| `NOT EXISTS` over a CTE | loud (50 before) | 3 | 3 |
| its SCALAR spelling | loud (0 before) | 47 | 47 |
| correlated `IN` over a CTE | `ColColFilter: could not resolve kernel for k 0 did` | 47 | 47 |
| under a reference alias (`FROM u AS z`) | loud | 47 | 47 |
| correlated on a BARE name (`= did`) | loud | 47 | 47 |

The IN spelling is the one worth naming separately: it was not merely
un-decorrelated. The rewrite FIRED, and the correlation term — whose `u.`
qualifier named nothing either collector knew — was classified as an
INNER-only condition and stripped to `k = did`, a comparison the build side
has no `did` for. One missing scope name, three different failures.

Both DAG arms now EXECUTE these shapes (`CorrelatedLocalRoutes` delta 0)
rather than routing them to the coordinator-local pipeline, which is asserted
beside the rows in `coordinator.TestArcD5CorrelationMatchesPostgres`.

The boundary is unchanged, and stating it precisely matters because the
obvious statement is wrong: it is not "there is no CTE". **The boundary is
that the OUTER reference is UNALIASED.** An outer relation named bare — a
table by its own name, or a CTE by its own name — correlated against an inner
relation reading that same name under an alias gives `u.did` a scope the
INNER `u` answers to as well, and no collector can tell the two apart. Both
spellings are pinned, with the alias as the control that separates them:
`boundary_cte_on_both_sides_outer_unaliased_stays_silent` (silent 0 for
PostgreSQL's 47) against `control_cte_on_both_sides_outer_aliased` (47 on
every arm, one alias later), and
`boundary_unaliased_base_table_correlation_stays_silent` (silent 50 for 47).
Closing it is a classifier repair, §1c's subject, not a scope one.

### 1e. The re-run's outer values are TYPED, and a type with no literal is a refusal

(Added 2026-09-03, #679.)

The re-run is the fallback for a correlated subquery this engine cannot
express as a join, and it substitutes the outer row's values into the
subquery's WHERE as literal TEXT. What that text MEANS is decided by the
literal's own spelling — so a value whose Go box has lost its wadjet type gets
RE-TYPED by whatever the box happens to look like.

`batch.Vector.GetValue` is a boxing boundary, not a display one: a DECIMAL
comes back as its rendered text, a DATE as a formatted string, a TIMESTAMP as
a bare int64 of epoch milliseconds, BYTES as a `[]byte`, and the six
network-native types and UUID as their canonical text. The renderer read the
box, and its `default:` arm wrapped anything unrecognized in quotes. `a.w_d2 =
b.k` against a BIGINT inner therefore became `'2.00' = b.k` and raised 22P02
for a query PostgreSQL answers with 3 rows.

**The rule: every outer value is rendered as a literal this engine's own
parser reads back as the SAME value at the SAME type, and a type that has no
such literal is a REFUSAL rather than a guess.**

A CAST where the bare spelling would re-type the value (DECIMAL at the
column's own scale, DATE, TIMESTAMP, REAL, DOUBLE PRECISION), a bare numeric
where the type already is the literal's (the integer family, PORT, PROTOCOL,
DURATION), a quoted string where the comparison kernels resolve a string
operand against the column's declared type (the network types, UUID, STRING).
The CASTs are load-bearing and not decoration: a bare numeric literal is
float8 in this dialect (ADR-0024's literal-typing rule), so a REAL rendered
without one is compared as a double — measured, `c_f32 = 0.14285715` matches
0 rows where the column's own value matches 1.

Two families have no literal at all, and both FAIL the query with 0A000
(`expr.UnrenderableOuterValueError`) rather than being rendered as something
else:

- **ROW, MAP and VECTOR.** There is no literal for these containers in this
  dialect. (An ARRAY was in this family until arc CW round 5, 2026-09-25: it
  is now spelled as its typed array literal, `CAST('{1,2}' AS BIGINT[])` —
  `expr.ArrayValueLiteral`, the spelling a DAG scalar subquery's array
  already used — so a correlated re-run over an array column answers
  PostgreSQL's value; the #679 census cells `array_outer_value_rerun_*` gate
  it.)
- **A BYTES value that is not valid UTF-8, or that holds a NUL.** The only
  bytea spelling the parser accepts is a quoted string, and those bytes do not
  survive it — a NUL cannot travel through the wire's text format at all
  (#570). They would come back as DIFFERENT bytes, which is a wrong answer
  rather than a failure.

NaN and the infinities join them, for the reason §2 already gives about the
IN-set materialization: this dialect has no numeric literal for either.

The gate is `expr.TestOuterLiteralRendersEveryTypeAsItsOwnType` (all 22 types
plus the refusals) and the #679 family of the correlation census, which runs
all 18 flat types as the OUTER value of a correlated EXISTS over a derived
table — the decline that keeps the shape on the re-run — against PostgreSQL's
own answers over the same rows, on four arms. The counts differ per type (29,
30, 38, 58, 60), so no single wrong answer passes them all.

This does not make the shape distributed: a derived-table inner is still a
decline, so both DAG arms route it to the coordinator-local pipeline
(`CorrelatedLocalRoutes` 1, asserted). `innerRelationsAreScannable` declines it
because the decorrelations build their inner plan with
`NewScan(info.Tables[0].Name, …)` and a derived table has no table name to
scan. Decorrelating THROUGH one is a producer repair — build the inner plan
from the derived table's own SQL and let `repairDecorrelatedSpelling` model
what that subtree emits — and it is not attempted here.

**What the fallback costs, measured — and the first reading of this was
wrong.** A re-run scans the inner relation again for every outer row: `2N+1`
object-store reads of the inner file for `N` outer rows, against a flat 3 for
the base-table spelling that DOES decorrelate. That is LINEAR, not
superlinear, and the tracker's `used` is FLAT across the re-runs — the scan
charge is released each time and nothing leaks. The 295 forced-reservation
warnings that took the round-0 census past a thirty-minute timeout were 295
RE-RUNS, not 295 leaks.

Two seconds of each re-run is a SECOND and independent defect, in
`memory.ReserveOrForce`: it spends the caller's full relief wait on a
reservation of `n` bytes against a budget SMALLER than `n` (measured:
`bytes=933732`, `budget=524288`). Nothing can admit that reservation — relief
cannot free negative memory — so the wait buys the `ForceReserve` that was
inevitable on entry. It costs any query whose scan touches a file larger than
its budget, with no subquery involved; the re-run only multiplies it by the
outer row count. Both are pinned in
`coordinator.TestEveryCorrelatedInnerIsReadOnce` and
`coordinator.TestCorrelatedRerunPaysTheFullReserveWaitPerOuterRow`, and the
memory half belongs to
ADR-0006's territory, not to the correlation model.

### 1f. A correlated NOT IN is not an anti join

(Added 2026-09-03, #538 / #578.)

`x NOT IN (SELECT y FROM t WHERE <corr>)` is three-valued: TRUE only when x
differs from every y in ITS OWN correlation group, FALSE when it equals one,
and UNKNOWN — so WHERE drops the row — when x is NULL and the group is
non-empty, or when the group holds a NULL y that x did not otherwise match.

An anti join answers the TWO-valued question "did nothing match", which is its
NOT EXISTS twin. Measured against live PostgreSQL 17 over the multikey
fixture, three shapes answered 13 for 9, 6 and 9, on all four arms and in
silence — and 13 is exactly what the corresponding NOT EXISTS answers, which
is the diagnosis rather than a coincidence.

`Node.NullAwareAnti` cannot express the correlated form, and #507's comment
has said so since it shipped: the flag reads ONE fact off the WHOLE build side
("did any row have a NULL key") and empties the output when it is true, so
setting it here would drop every row the moment ANY group held a NULL, and
#539 makes such a join replicate its build rather than shuffle. The fact this
predicate needs is per correlation GROUP.

**The rule now: a CORRELATED NOT IN is not lowered to a join at all.** It
stays a subquery predicate, where `expr.CorrelatedInSubquery.EvalBoolNull`
carries the exact rule per outer row. The uncorrelated form is unchanged and
keeps #507's null-aware anti join; so does a correlated `IN`, which needs no
third value.

**Where the uncorrelated form's rule LIVES was the other half, and it is
settled here** (2026-09-04, #539). `walkStages` forces a `NullAwareAnti` join
onto the broadcast path past the size decision, counts it in
`physical.NullAwareAntiForcedBroadcasts` and logs the build's estimated bytes.
That was one place remembering one rule, carried by the stage TYPE: a
`StageHashJoin`'s build slot asked for `RequiredClusteredOn` whatever the join
MEANT, so every later pass that could re-type, fuse or split such a stage had
to remember not to (`fuse_stage_chains.go` does, in two functions), and nothing
read the counter. So the requirement is now a property of the JOIN —
`RequiredChildDistribution` returns `RequiredBroadcast` for a null-aware anti
join's build — and `assertNullAwareAntiBuildsAreReplicated` asserts on the
FINAL stage list that every such join reads a build every task sees whole,
refusing `0A000` and routing local rather than dispatching a plan whose tasks
would each answer a different question.

The refusal is `0A000` and the coordinator ROUTES it
(`runNullAwareAntiLocal`, counter `NullAwareAntiLocalRoutes`), because the
coordinator-local pipeline's single hash join holds the whole build by
construction. That sentence was written here before it was true: the error was
a bare `errors.New` with no coder and the coordinator had no arm for it, so a
plan that tripped the invariant would have reached the client with no class.
The gate takes the two production hunks away one at a time — with only the
forcing off the distribution property still splices a replicate exchange and
nothing is refused; with both off the build really is partitioned and the
invariant refuses and routes.

What #539 asks for beyond that — a lowering that does not need a replicated
build, so a large `NOT IN` can shuffle — is the two-join identity above, and
it is still blocked on the same thing: `physical.PlanContext.BuildSemiAntiFilter` reads a
semi/anti residual as TEXT.

That evaluator had the same empty-set defect the operator guards against, and
it is fixed here: `x NOT IN ()` is TRUE for EVERY row INCLUDING a NULL-keyed
one, because both halves of the three-valued reading are about a COMPARISON
and over an empty set there is nothing to compare. Reading the probe's NULL
before the set answered UNKNOWN and dropped the row — 36 of 40 where
PostgreSQL says 40, and 0 of 3 on the CTE spelling whose survivors ARE the
NULL-keyed rows. The empty set now decides first, which is
`exec.HashJoin`'s own `buildRows > 0` guard stated at the evaluator.

**What was built, measured and NOT shipped**, because it does not work in this
tree and the reason is worth recording. The identity

	x NOT IN (SELECT y FROM t WHERE corr)
	  ≡  NOT EXISTS (SELECT 1 FROM t WHERE corr AND y = x)
	     AND NOT EXISTS (SELECT 1 FROM t WHERE corr AND (y IS NULL OR x IS NULL))

lowers to an ordinary equi-key anti join beside a second one whose residual is
`(y IS NULL OR x IS NULL)`. Both hash-partition like any other join, so
neither needs #539's replicated build — it would keep the join AND the rule.
It fails at the semi/anti residual: `physical.PlanContext.BuildSemiAntiFilter` reads the
filter as TEXT (split on `" and "`, then find one of six comparison
operators), so an OR and an IS NULL compile to NOTHING and are dropped in
SILENCE, and `physical.extractFilterBuildColumns` narrows the stored build by
the same text split and would delete the very column the residual reads.
Measured with the two-join form in place: 0 rows for PostgreSQL's 9.

That is the same class of defect as #562 — a decorrelation's rendering
defeating a text splitter — one layer down. **Lifting this decline needs the
semi/anti residual to be a real expression compiler and its build-column
extractor to be AST-based**, and a filter neither can compile must be a
refusal rather than a silent drop. Until then the decline is the honest
lowering, and its cost is a route to the coordinator-local pipeline on both
DAG arms, asserted as `CorrelatedLocalRoutes` 1 beside the rows in the
correlation census rather than described here.

### 1g. An aggregate's derived ARGUMENT is a scope, like every other expression

(Added 2026-09-03, #734.)

§1c named this one too: "the aggregate-argument compile site never asks for
the outer scope at all". It is one call. `physical.buildAggregate` materializes
a derived aggregate argument into a synthetic `__agg_expr_N` column and
compiled it with `expr.CompileWithRunner` — a runner and nothing else — while
the SELECT-list projection site three thousand lines away compiles the SAME
kind of expression with `CompileWithScopeResolver` and the child's outer
tables and columns.

With no outer scope a correlated subquery is not RECOGNIZED as correlated: it
becomes the uncorrelated evaluator, which runs its text ONCE against no outer
row and memoizes a query-wide constant. `SUM(CASE WHEN EXISTS (SELECT 1 FROM
decpair y WHERE y.id = x.id * 2) THEN 1 ELSE 0 END)` read constant FALSE and
answered 0 for PostgreSQL's 4, in silence, until v0.18.16 made the dangling
re-run loud. The IDENTICAL expression one level down — the same CASE in a
derived table's SELECT list, summed above it — has always answered 4.

The site now asks, and every spelling of the position answers PostgreSQL's
value on all four arms: EXISTS, NOT EXISTS, IN, a scalar subquery, inside
SUM / COUNT / MAX, grouped and ungrouped.

**The residual is the ROUTE, and it is pinned as a PAIR rather than
described.** An aggregate ARGUMENT is not a decorrelation site at all — the
three decorrelation passes walk `NodeFilter` and nothing else — so even a
plain COLUMN-keyed correlation stays a per-row subquery there and both DAG
arms route the plan to the coordinator-local pipeline, while the SAME
correlation in a WHERE becomes a semi join both arms execute. The census
carries the two side by side (`control_same_correlation_in_a_where_
decorrelates` at 0 routes, `residual_same_correlation_in_an_aggregate_
argument_routes` at 1), so the day the second reaches 0 the pin fails and the
residual is closed.

Closing it is the `__sub_N` shape: lift the subquery into a marker LEFT join
below the aggregate, publish the marker under a hidden slot, and let the
argument read the column. It needs one thing this tree does not have — a
decorrelation that can key on an EXPRESSION (`y.id = x.id * 2`), which
`extractCorrelatedRefs` declines today, for the WHERE spelling as much as for
this one.

### 1h. A LATERAL runs per OUTER ROW, and an empty input still answers

(Added 2026-09-03, #767 part 1.)

PostgreSQL evaluates a LATERAL subquery once per outer row. An UNGROUPED
aggregate over an empty input still yields exactly one row, so an outer row
the lateral matches nothing for SURVIVES — `COUNT` reading 0 and every other
aggregate NULL.

`buildLateralSubquery` decorrelates by promoting the correlated equality into
the join condition and injecting the correlated inner column into the
subquery's GROUP BY. That turns "one row per outer row" into "one row per
GROUP THAT EXISTS", and the difference is the whole defect: an INNER join
dropped the unmatched outer row (2 for PostgreSQL's 3, in silence) and the
LEFT spelling kept it with `COUNT = NULL`, which is a different wrong answer
to the same question.

Restoring it needs the ORDER right, and the order is PostgreSQL's: the
lateral produces its row, THEN the join's ON tests the pair, and the join's
KIND decides what happens to a pair the ON rejects. A repair that forces a
LEFT join and defaults unconditionally has thrown the ON away, and that is
not a smaller fix — it turned six PostgreSQL-correct answers wrong, `ON s.n
> 5` answering three rows for PostgreSQL's none and printing 0 for counts of
2. So `logical.lateralEmptyInputPlan` reads the subquery and the join AS
WRITTEN, before the key injection, and decides between three cases — under a
fourth condition, stated after them, that overrides all three:

- **No written `ON`, or `ON true`** — the condition rejects nothing, so making
  the join LEFT on the correlation and defaulting the COUNT outputs IS the
  semantics, for the INNER and the LEFT spelling alike. (`lateralPadOnly`)
- **A written `ON` on an INNER join** — the padded row must still be TESTED.
  An inner join's ON and a WHERE are the same filter, so the join goes LEFT on
  the CORRELATION alone, giving every outer row its lateral row, and the ON
  moves into the enclosing WHERE where the same default substitution reaches
  it. `ON s.n = 0` then KEEPS the unmatched row, which is PostgreSQL's answer
  and the one the decorrelation alone cannot reach.
  (`lateralPadThenFilter`)
- **A written `ON` on an OUTER join** — a pair the ON rejects must be kept
  with the lateral side NULL, which needs the lateral's columns nulled per
  column rather than filtered: a CASE per output over a schema this pass does
  not have. **NOT REPAIRED.** The join is left exactly as written, which for
  every ON an unmatched outer row would FAIL is already PostgreSQL's answer;
  the one shape it still gets wrong is an ON the DEFAULT row would PASS —
  `LEFT JOIN LATERAL … ON s.n = 0` — and that is pinned in the census with
  PostgreSQL's answer beside it. (`lateralNoRepair`)

**A fourth condition cuts across all three and is checked first: a RIGHT or
FULL join LATER in the FROM clause declines the repair entirely.** Both halves
of it — the `COALESCE` and the moved `ON` — are rewrites of the ENCLOSING
query, so they see the whole FROM clause's result; what they are entitled to
speak about is the LATERAL's own output. A join that null-extends is exactly
what separates those two relations: it MANUFACTURES rows in which `s.n` is
NULL, and neither rewrite can tell one of those from a row the lateral
produced. Measured before the condition was added: the moved `ON s.n > 1`
DELETED the manufactured row (2 rows for PostgreSQL's 3), and `ON true`
printed `n = 0` in it where PostgreSQL prints NULL — both of them right at
fd679ae9, which makes this the same right-to-wrong class as the forced-LEFT
repair above, found by asking where else a rewrite's SCOPE and its WARRANT
come apart.

**What the decline costs is exactly the DEFAULT ROW, and nothing else.** That
is a measured bound, not a hope, and it takes three pinned spellings to state.
Where the later join drops the unmatched outer row anyway, the decline is free
and PostgreSQL agrees. Where it does not, the empty-input row is missing or
wrong:

| the later join | PostgreSQL | declined | what is lost |
|---|---|---|---|
| `RIGHT … ON c2.id = o.id AND c2.id < 3` | 3 rows | 3 rows | nothing |
| `FULL … ON c2.id = o.id AND c2.id < 3` | 4 rows | 3 rows | the default ROW |
| `RIGHT … ON c2.id = o.id` (plain) | `Carol\|0\|Carol` | `NULL\|NULL\|Carol` | the default's VALUES |
| `FULL … ON c2.id = o.id` (plain) | `Carol\|0\|Carol` | `NULL\|NULL\|Carol` | the default's VALUES |
| `ON s.n > 1 RIGHT … ON c2.id = o.id` | `NULL\|NULL\|Carol` | `NULL\|NULL\|Carol` | nothing |

The last row is the one that bounds the rest: with an `ON` the default row
would FAIL, PostgreSQL null-extends the pair exactly as the un-repaired plan
does, so no spelling of this shape diverges except through the default. All
five are census cells. Restoring the two that lose it needs the default
applied at the lateral's OWN output, before the later join sees it — a
plan-level change rather than a SelectInfo rewrite.

A LEFT join after the lateral cannot null-extend what is to its left, so it is
not affected and its control says so.

The default itself is `COALESCE(…, 0)` around references to the lateral's
COUNT outputs. NULL is already right for every other aggregate — `SUM` of
nothing IS NULL in PostgreSQL — so COUNT is the only family that needs one.

**A default that reaches only some positions is a wrong answer in the
others**, so within the tree it walks, the rewrite is complete rather than a
list of the places anyone thought of. It covers the enclosing SELECT list,
WHERE, HAVING and ORDER BY, and inside those, every `plansql` node that can
CONTAIN a column reference: ColRef, ParenNode, NotNode, UnaryOp, AndNode,
OrNode, BinaryOp, CmpExpr, IsExpr, LikeExpr, BetweenExpr, InExpr, AnyAllExpr,
CastNode, FuncCallNode, CaseNode, ArrayLitNode, TupleNode and WindowFuncNode —
plus an aggregate's ARGUMENT (`AggArgExpr`, `AggArgs`, `AggArg`), which is a
field of the select column rather than a node under it and was missed for
exactly that reason. A missing arm is SILENT — the walker's default returns
the node unwalked, so `WHERE s.n IN (0, 2)` dropped the unmatched outer row
for PostgreSQL's three while `BETWEEN` and `IS` beside it were right.

**Where it stops is inside a subquery, and the reason is not the one first
given here.** `SubqueryNode` and `ExistsNode` are not walked because they hold
SQL TEXT rather than a tree. This section used to add "and a lateral output is
not in their scope", which is FALSE and PostgreSQL says so: a subquery in the
enclosing query can name the lateral's output, PostgreSQL resolves it, and it
applies the empty-input default there like anywhere else.

```sql
SELECT o.customer, (SELECT COUNT(*) FROM lat_item i WHERE i.amount > s.n * 40)
  FROM lat_ord o JOIN LATERAL (SELECT COUNT(*) AS n FROM lat_item
                                WHERE order_id = o.id) s ON true
-- PostgreSQL 17  Alice 2, Bob 2, Carol 4
-- this engine    Alice 2, Bob 2, Carol 0      (all four arms, silently)
```

The outer row's `s.n` is substituted into the subquery's TEXT per row by the
re-run (§1e), and on the padded row it substitutes the LEFT join's NULL rather
than 0, so `amount > NULL` matches nothing. The `EXISTS` spelling of the same
reference drops the row outright — three rows for PostgreSQL, two here.

Both are pinned (`boundary_scalar_subquery_reads_the_pad_not_the_default`,
`boundary_exists_reads_the_pad_and_drops_the_row`) and neither is a
regression; fd679ae9 answers the same. Closing them is not a bigger walk: the
reference lives in text, so reaching it means parsing the subquery, rewriting
its tree and rendering it back, and the value that needs defaulting is an
OUTER value the re-run substitutes — which is §1c's layer, not this rewrite's.
So the honest statement of the boundary is positional: **the default reaches
every position in the enclosing query's own expression trees, and no position
inside a subquery's text.**

A subquery the QUERY grouped is untouched, and is the control: `GROUP BY x`
over an empty input yields NO row in PostgreSQL either, so the unmatched outer
row is correctly NULL-padded there and not defaulted.

**`SELECT *` is the boundary**, and it is pinned rather than described:
`boundary_select_star_over_an_aggregated_lateral` in
`coordinator.TestArcD5CorrelationMatchesPostgres`. A star expands in a later
pass over the plan's own schema, so there is nothing in the SelectInfo to
rewrite and the padded COUNT reads NULL where PostgreSQL reads 0. A second
boundary is pinned beside it and is not a lateral defect at all:
`COALESCE(x, 0) + 1` over the default boxes float64 where PostgreSQL says
bigint, and it reproduces with no lateral in the query.

**The DAG carries the COUNT column and not its siblings, and that is a
SECOND, older defect this does not touch.** A BARE projection of a NULL-padded
lateral aggregate's output is not carried by the join stage: it either refuses
with `ErrUnreachableGatherOutput` and routes local (right answer, recorded
cost) or reaches the worker and fails with `column "s.total_amount" does not
exist in the input schema`. The COUNT columns escape it because the COALESCE
makes them COMPUTED projections. #767's own text records the DAG failure
separately; the census pins the mixed shape with that message and the day it
answers there the pin fails.

### 1i. What the correlation arc MEASURED and did not move

(Added 2026-09-03, #616 / #614 / #714.)

Three filings describe a tree that has changed under them. Each is pinned in
the correlation census with what it does NOW beside PostgreSQL's answer, so
the record is a fixture rather than a memory.

- **#616 — a correlated scalar subquery whose own FROM is a comma join.** It
  ANSWERS PostgreSQL's value on all four arms over every fixture tried here
  (46, 47, 21). What still fails is narrower than the filing and is not a
  correlation defect: with the SAME table on both sides of the inner comma
  join, under a MEMORY BUDGET, the query panics in
  `exec.HashJoinProbe.lookupBuild` — the dual-int-key path reads
  `h.buildBatches[0]` before walking the chain, and a SPILLED build has no
  batch 0. Every other arm answers 9. **And TPC-H Q2 in its official comma
  spelling still HANGS** over the committed SF0.01 fixture (measured: a 5
  minute test timeout with no rows), which is the deadlock the issue reports.
  Both belong to the join — the spill path and the shared scan cache — not to
  the correlation model.

  The hang's condition is **narrower than a comma join and wider than TPC-H
  Q2**, and the round-1 review's second repro is what says so: `a.k IN (SELECT
  b.k FROM t b WHERE b.k > (SELECT AVG(c.k) FROM t c))` hangs with no comma
  join and no correlation anywhere in it. The discriminator is a control that
  changes ONE thing — the scalar subquery's TABLE — and answers in
  milliseconds, as does each level of the nesting alone. So the trigger is a
  RE-ENTRANT read of a table from inside a build that the same table's scan is
  feeding: the build waits on the scan, the scan's slot is held for the build,
  and `source init` never returns. Pinned with a three-second deadline and its
  control in `coordinator.TestScalarSubqueryOverTheSameTableAsAnEnclosingBuildHangs`.

- **#614 — a derived table in a subquery's FROM referencing the enclosing
  query.** MEASURED, because the question was open: it is LEGAL WITHOUT
  LATERAL and PostgreSQL 17 ANSWERS it (40 rows over the multikey fixture).
  LATERAL governs references to same-level FROM siblings; a reference to an
  OUTER-QUERY column from inside a sub-SELECT's derived table needs none.

  **The REFUSAL stands and its CLASS is corrected** (2026-09-04, arc F4). It
  was 42P01 `missing FROM-clause entry for table "a"` on all four arms — a
  message that asserts the SQL is invalid, which it is not, and which a user
  cannot act on. It is now 0A000 naming the two workarounds (lift the
  correlated predicate above the derived table, or write it as a LATERAL join),
  and `colScope.outerDiag` — the enclosing query levels, carried for DIAGNOSIS
  and never for resolution — is what tells the two apart. The SIBLING spelling
  (`FROM o a, (SELECT … WHERE n = a.n) d`) keeps 42P01, which is PostgreSQL's
  own disposition for it.

  **Opening the binder's scope instead was tried and WITHDRAWN, and the
  measurement is the reason this stays a refusal.** With the enclosing scope
  merged into the derived body's, the shape plans — and the reference binds
  INSIDE the body, because the logical builder plans that body as its own query
  block and the correlation analysis never sees its terms. Measured over the
  type matrix: `EXISTS (SELECT 1 FROM (SELECT id, g FROM t b WHERE b.g = a.g)
  d)` answered 5000 of 5000 rows where 4616 match, and the scalar-subquery
  spelling answered one constant for every outer row. A loud refusal became a
  silent wrong answer on every arm. Supporting the shape is a dependent join —
  what §1c calls the thing that removes the class — and it is that change, not
  a scope widening.

  Gated at `coordinator.TestArcF4DerivedTableCorrelationIsRefusedNotMisread`:
  the two legal spellings refused 0A000 on four arms, the sibling spelling
  refused 42P01, and the shape that DOES work — the correlated predicate
  written ABOVE the derived table — answering PostgreSQL's rows beside them.

- **#714 — an aggregate argument containing a scalar subquery.** The headline
  ("refused on the stage DAG") is gone: it ANSWERS on all four arms, the DAG
  routing the plan local for its SELECT-list subquery (#659's route), and the
  VALUE is PostgreSQL's. What diverges is the TYPE: `SUM(a + (SELECT 1))` over
  a DECIMAL column comes back float8 where PostgreSQL says numeric, while the
  same SUM without the subquery stays exact. That is a numeric-typing residual
  (ADR-0024's rung), not a correlation one, and the census pins all three
  boxes side by side.

### 1j. The build side is the subquery's OWN FROM clause, as a plan

(Added 2026-09-04, #852 / #616. This is the answer §1a and §1b declined to
give, and it SUPERSEDES both declines — read those two sections as the record
of why the decline was right at the time, not as current behaviour.)

Every one of the three decorrelations assembled its build side out of
`NewScan(info.Tables[0].Name, …)` plus one `Scan` per explicit `JOIN`. That is
a model of a FROM clause with three holes in it:

- a **derived table** has no name a Scan can hold (§1a),
- a **CTE reference** has the same exposure spelled as a bare identifier (§1b),
- a **comma list** past its first entry was dropped outright, and the
  equalities that would have joined it stayed in the subquery's WHERE, where
  `innerOnlyPredicate` declines a condition naming two inner relations.

§1a and §1b closed the first two by DECLINING them, which is right and slow:
the subquery stays a per-row predicate and the re-run reads the whole inner
relation once for every outer row. Measured with an object-store read counter
over an 8- and a 16-row outer, all three spellings of one question:

| subquery `FROM` | reads, 8 outer rows | 16 outer rows |
|---|---|---|
| base table | 3 | 3 |
| derived table | 17 | 33 | 
| CTE reference | 17 | 33 |

`2N+1` against a flat 3, with both DAG arms routing the plan to the
coordinator-local pipeline. The answers were all right, which is why no
answer-comparing gate could see it (#852).

**The build side is now the subquery's own plan.** `logical.buildFromClause`
is the BUILDER's own FROM assembly, extracted so the decorrelations call
exactly what a top-level query calls — so a derived table, a CTE reference, a
comma list and an explicit JOIN plan there the way they plan anywhere else.
`innerRelationsAreScannable` retires; what is left of it is
`innerRelationsAreBuildable`, whose decline list is one entry long: a
RECURSIVE CTE, which is a tagged scan the physical planner resolves by
fixed-point iteration from a cache a semi-join build side is not prepared
through. That decline is also what keeps the materialized-IN route's own
recursive refusal reachable (§1b, `physical/in_subquery_set.go`).

Two things had to follow, and they are the whole of §1's delicacy:

**Names.** A derived table's or a CTE's subtree ROOT is a Project, and the
enclosing query calls its columns by the SCOPE it gave that arm — `d.k`, not
`d5_inner.k`. `emittedColumns` now reads that scope off the node it is
standing on (`DerivedAlias`, `CTERefAlias`, `CTEName` — a CTE records it on the
root and a derived table on the root AND the scans, §1d), and re-owns every
column the root emits to it. Without that, `spellInner` reports "unresolved"
for `d.k`, the rewrite keeps its pre-reorder guess, and that is exactly the
silent wrong answer §1 exists to remove.

**A comma inner's equalities are JOIN CONDITIONS, not filters.** Built as
written they are condition-less cross joins with the equalities left in the
subquery's WHERE. `liftWhereEquiPredsIntoJoins` is the pass that already fixes
that shape one level up, so `decorrelatedInnerPlan` runs it HERE, on the
freshly built subtree, before the inner-only conditions are classified — which
is the ORDER this decision has always implied and #616 asked for. Two
consequences are load-bearing:

- The lift attributes an UNQUALIFIED column to its relation from the scan's
  own column list, so the subtree is ANNOTATED first. It is annotated ONLY on
  this path and only when something needs lifting: annotating every inner
  subtree would hand `reorderJoins` statistics it did not have before, which
  moves the join order of plans that have nothing to do with a comma inner
  (TPC-H Q2's explicit-JOIN spelling was the one that showed it).
- Whatever the lift cannot attribute is DECLINED, not left above the join. A
  qualified residual there names a column the join emits bare, which
  `expr.ResolveColumnRef` resolves by stripping the qualifier — onto whichever
  relation the reorderer put on the probe. `everyLiftedPredicateLanded` is the
  check, so the boundary is asserted rather than assumed.

**A latent defect one layer down became reachable and is fixed with it.**
`extractRightJoinKeys` (§3's narrowing) decided which side of each equality is
the build key by walking `collectSubtreeColumns` — every column the subtree
READS anywhere. That differs from what its ROOT EMITS exactly where a Project
renames: over `(SELECT c_bool AS k FROM typemx GROUP BY c_bool) b`, the read
set holds `c_bool` and the emitted set holds `k`, so `c_bool = k` attributed
the BUILD key to `c_bool` and projected a column the build root does not have
— `column "c_bool" does not exist in the input schema`, at build time, on
every arm. Side membership now reads `emittedColumns`, with the read set as
the fallback for an un-annotated subtree (a decline at worst).

**The boundary, and it is drawn wider than the shape that was caught.** A
derived table or a CTE reference JOINED to another relation is DECLINED. The
build side would then carry TWO renamings — the join's own, and the derived
arm's Project, whose published name is one no scan below it produces — and
while the model above tracks both, the stage DAG's carried-column derivation
does not. It answers a DIFFERENT number rather than failing:

```sql
SELECT COUNT(*) FROM nation a WHERE a.n_nationkey IN (
  SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk FROM nation c) s
  JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)
-- PostgreSQL 17 and the single-process arm: 3.  Stage DAG: 10.
```

The two-path invariance oracle is what caught it, on the first run after the
build side started planning derived tables. The spelling that puts the derived
arm on the PROBE agrees today, and that is the reason BOTH decline rather than
only the one measured: which arm goes where is `reorderJoins`' decision from
row counts, so a cut drawn there would move under the fixture. Closing it is
`dagplan/join_carried_columns.go`, not this rewrite.

**The second boundary, and it is §1's own rule one level down.** A derived
table or a CTE reference that COMPUTES a column it publishes is DECLINED.
`innerSemiJoinKey` already refuses a computed select item as a semi-join key
(#516) — the key would name nothing the build side emits — and a derived table
hides the computation from it, because from the subquery's side
`SELECT b.m FROM (SELECT n + 1 AS m FROM t) b` is a plain column reference.

```sql
SELECT COUNT(*) FROM mk_outer a WHERE a.n IN (
  SELECT b.m FROM (SELECT n + 1 AS m FROM mk_inner) b)
-- PostgreSQL 17 and the single-process arm: 32.  Stage DAG: 0.
```

The single-process arm evaluates `n + 1`; the stage carries `m` as if it were a
scan column of `mk_inner`, finds none, and the semi join builds EMPTY. The same
body with `n AS m` — a RENAME rather than a computation — answers 40 on both
arms, which is what says the trigger is the EXPRESSION and not the published
name. `coordinator.TestMultiKeyCorrelatedTwoPath/derived_in_computed` is the
entry that caught it. The decline is on ANY computed published column rather
than only the one the key names, because the three call sites spell their key
three different ways and none has resolved it when the build side is assembled.

**What it costs, measured rather than assumed.** A re-run builds no hash
table; the join that replaces it does. Under the correlation census's 512 KiB
arm three comma-inner cells that answered before now REFUSE past the budget —
`build_rows=15` against `used=498058` on the 40-row fixture, so what fills the
budget is the plan's other operators rather than the build — and the same
queries answer at 1 MiB. That is ADR-0006's designed answer to a plan whose
floor exceeds its budget, not a wrong one, and #823 names the part that cannot
be evicted (a grace eviction frees build columns, not index entries). All
three are pinned with `wantErrLikeSpilled` so the day the floor drops they
fail. A fourth cell — the correlated SCALAR over a derived table, which adds an
Aggregate and a LEFT JOIN — sits ON the floor rather than above or below it
and both answered and refused across census runs, so its budgeted arm is
dropped with that measurement recorded instead of a coin flip pinned.

**What it buys, per shape:** every derived-table, CTE-reference and
comma-joined correlated inner in the census moves from ONE
`CorrelatedLocalRoutes` to zero — both DAG arms execute them — and the inner
relation is read once instead of once per outer row. The answers are
unchanged, which is the point: only the counter and the read count can see it.

### 1k. An unqualified name binds the INNER relation's schema, whatever kind of relation it is

(Added 2026-09-07, #955.)

§1d gave the correlation COLLECTORS a CTE's scope so an outer reference
qualified by a CTE name could be recognized. This is the other direction, and
it is where the silent answers were: **which scope an UNQUALIFIED name binds
to.**

SQL scopes innermost-first. An unqualified column inside a subquery binds to
the subquery's own FROM when that FROM has a column of the name; only a name NO
inner relation carries is a reference to the enclosing query. The classifier
implemented exactly that rule (`walkForOuterRefs`'s bare-name arm, since #334)
and could not apply it, because it asked a CATALOG about a NAME:
`collectInnerColumns` called `resolve(t.Name)` per FROM entry, and
`Planner.subqueryInnerColumns` was `catalog.GetTable`. A CTE reference is not
in the catalog. A derived table's "name" is its own SQL text. Both resolved to
nothing, so every unqualified name they supply fell through to the outer scope.

The consequence is not a missed optimization. A subquery classified correlated
has the outer row's value SUBSTITUTED into its WHERE, so

```sql
WITH c AS (SELECT id, c_i64 AS v FROM typemx)
SELECT (SELECT MAX(v) FROM c WHERE id < 4000) AS mx FROM decpair WHERE id < 2
```

became `WHERE 1 < 4000` — constant TRUE, the predicate gone — and answered
`4999014997` for PostgreSQL 17's `3999011997` on all four arms, in silence. And
it is re-run once per outer row: over 5000 outer rows at a 512 KiB budget the
same shape did not finish in ten minutes. One misclassification, a wrong number
and a hang.

**The rule now: a FROM item is asked what IT publishes, and a resolver answers
the COMPLETE column list of a relation or nothing at all.** A derived table
answers from its own parsed body (the memoized parse, ADR-0032); a CTE
reference answers from the WITH items in scope — its explicit column list where
it has one, else the names its body publishes, which is `registerCTE`'s rule and
PostgreSQL's; a set operation publishes its LEFT arm's names; a column-alias
list renames the leading outputs positionally and HIDES what it replaces. The
block-namespace rule itself moved to `plansql.BlockOutputColumns` so the binder
and the classifier read one definition of it.

**A STAR is two rules, and reading them as one over-claims.** A bare `*` stands
for every FROM item in FROM order; a QUALIFIED `alias.*` stands for THAT source
alone. `SELECT dim.* FROM dim JOIN t ON …` publishes dim's columns and none of
t's, and a classifier that published both sides made an OUTER reference to a
name only the other side carries look INNER — this section's own defect with
the sign flipped, silently wrong on all four arms in the CTE spelling and loud
in the derived-table one. A qualifier that names no FROM item this layer can
resolve makes the whole block unknown rather than a superset. And a star
publishes IN THE POSITION IT IS WRITTEN, because a column-alias list is overlaid
POSITIONALLY over this list: `SELECT a, t.*, b` has to publish the star's
columns between `a` and `b`.

Complete-or-nil is load-bearing, not tidiness. A PARTIAL list would let a
column-alias list be overlaid on the wrong positions, and — worse — would read
as *"this relation does not have that name"* for every column missing from it,
which is the direction that turns an inner reference into an outer one. A star
over a source this layer cannot resolve therefore makes the whole block
unknown, and unknown falls back to the pre-existing identifier comparison: the
classifier never claims a relation it cannot name has a column.

**The expansion is bounded by the SCOPE, never by a count.** `CTEColumns`
resolves item i's body at scope i, so `scope` strictly decreases, an item's own
name is never in its own scope, and a `WITH RECURSIVE` body terminates as
unknown for that reason rather than because a counter ran out. A numeric depth
bound is not a safety net here: unknown is not a refusal, it falls back to the
outer scope, so a bound TRUNCATES a long chain of star-bodied items into exactly
the wrong number this section is about. Nine chained items answered 5000 for
PostgreSQL's 4000 while such a bound stood.

**The classifier decides, and so does the SUBSTITUTION.** Saying a name is inner
is only half of it: the per-row re-run rewrites a bare name to the outer row's
literal, and it took its list of names from "collides with an outer column"
rather than from the classifier. One QUALIFIED outer reference was therefore
enough to substitute the inner column beside it — `(SELECT COUNT(*) FROM c WHERE
id < d.id)` answered 0 for every outer row where PostgreSQL counts c's rows
below it, over a base table as well as a CTE. `OuterRef.Bare` records which
spelling the classifier resolved, `dedup` ORs it across the spellings of one
name, and `expr.buildUnqualOuterCols` reads it. Without that, this section's
rule holds only for a subquery with NO outer reference at all, which is not the
rule.

Measured against live PostgreSQL 17 over the type-matrix fixture, before →
after, on all four arms (`CorrelatedLocalRoutes` delta in brackets):

| shape | before | after | PG |
|---|---|---|---|
| the filing shape, inner FROM a CTE reference | 4999014997 [1] | 3999011997 [0] | 3999011997 |
| nested two deep | 4999014997 [1] | 3997011991 [0] | 3997011991 |
| the CTE column is an explicit alias | 5000 [1] | 4000 [0] | 4000 |
| inner FROM a DERIVED table | 4999014997 [1] | 3999011997 [0] | 3999011997 |
| inner FROM a SET-OPERATION arm | 4999014997 [1] | 3999011997 [0] | 3999011997 |
| the derived table's SELECT list is `*` | 5000 [1] | 4000 [0] | 4000 |
| the same name in both scopes | 5000 [1] | 10 [0] | 10 |
| the CTE reference under a JOIN | 4616 [1] | 10 [0] | 10 |
| the WHERE producer over 5000 outer rows | 3871, **spilled arm hangs** [1] | 3870 [0] | 3870 |
| a QUALIFIED star over a join (CTE and derived spellings) | 10 silently / loud [1] | 4616 [1] | 4616 |
| a nine- and a twelve-link chain of star-bodied CTEs | 5000 [1] | 4000 [0] | 4000 |
| an inner BARE name beside an outer QUALIFIED one, over a CTE and over a base table | 0 per outer row [1] | 1,2,3 [1] | 1,2,3 |
| `EXISTS` under `OR` / `NOT` / `CASE`, uncorrelated | loud on both DAG arms | right [0] | right |
| ctl qualified / aliased / base table / top level | right | right | right |
| a bare star over a join, and the qualified star naming the side that HAS the name | 4616 [1] | 10 [0] | 10 |
| ctl genuinely correlated (4 spellings) | right [1] | right [1] | right |

The three controls are the boundary. Their VALUES were right before this change
too; what separates "we stopped mis-correlating" from "we stopped correlating"
is that their counter still moves.

**What this does NOT close.** §1d's two pins are about a QUALIFIED outer
reference whose qualifier the INNER relation also answers to
(`boundary_cte_on_both_sides_outer_unaliased_stays_silent`,
`boundary_unaliased_base_table_correlation_stays_silent`). No schema decides
those — both scopes carry the identifier — and they stay pinned.

Nor did it close the shape a COLUMN-ALIAS LIST on a CTE reaches — **CLOSED
2026-09-07 by arc K1 (#958), the way this paragraph said it had to be.**
PostgreSQL's list renames the LEADING columns and the rest keep their names;
this engine treated it as the whole namespace, in the binder (`registerCTE`
stored `cte.Columns` outright) and in the correlation classifier
(`CTEColumns` returned it outright), so `WITH c(kk) AS (SELECT id, s FROM t)`
published `kk` alone and a subquery over such a CTE read `s` as the enclosing
query's and answered the enclosing row's value. Repairing it at the classifier
alone would have been a bandaid — the classifier would call the reference inner
while the binder still refused `s` as unknown, turning a wrong number into a
refusal for a legal query — so the rule is now in ONE place,
`plansql.OverlayColumnAliases`, and every reader takes it: the classifier, the
binder and the derived-table path that already had it right. ADR-0012's
divergence for a list over a `SELECT *` body closes with it, for every star the
expansion can count. The pin is deleted as the proof and its cell kept as a
value in `coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation`.

The interaction of the list with the scope rule IS closed, and two controls say
so: `(SELECT * FROM t) x(idd)` and `WITH c(kk) AS (SELECT * FROM t)` both HIDE
the name they rename, so a subquery naming it is reading the enclosing query —
PostgreSQL's answer, and this engine's.

**What the repair UNMASKED.** Three shapes were wrong on every arm because the
subquery was mis-correlated and its predicate dropped; with the scope right the
single-process arms answer PostgreSQL and the DAG arms reach lowering gaps the
wrong answer had been hiding. Each was pinned with the sentence it fails by, and two are now CLOSED: TWO
qualified stars in one SELECT list (the logical builder emitted a projection
column literally named `dim.*` — closed by arc J1's qualified-star expansion,
#962, verified on four arms at `bb8635a4`), and a QUALIFIED star over a JOIN
inside a derived table that is then FILTERED (closed by arc K1, #963: a
qualified star ALONE built no projection at all, so nothing carried the star
for the expansion to rewrite and the derived block published the join). Two
remain pinned: a RECURSIVE CTE named inside a subquery (§1b: it has no stage
lowering, and the DAG meets that as an unbuildable stage rather than as §1c's
routed refusal), and a UNION of two `SELECT *` arms as the subquery's FROM (the
union stage's column pruning drops a column its own arms declare) — and the
BARE-star twin of #963, which needs the ordered model of a join's emitted
columns ADR-0012's #810 entry names. wrong → right on two arms and loud on two,
or wrong → loud on four, is within doctrine; each pin fails the day its gap
closes.

### 1l. A FROM-less scalar subquery IS its SELECT expression, in the block that supplies the row — and a per-row re-run substitutes into every clause it rebuilds

(Added 2026-09-12, #1044. Rewritten the same day after round 2 moved the
decision and completed the substitution, and amended after rounds 3 and 4
narrowed the two refusals to the shapes that actually need them.)

§1c settled what a subquery this engine cannot RUN answers: it fails the query.
This section is about the two shapes that reach the per-row re-run from
somewhere other than a WHERE clause, and about where the decision belongs.

`(SELECT u.x)` produces one row whose one column is `u.x` evaluated in the
ENCLOSING scope — PostgreSQL plans it as a Result node under the SubLink with
the outer reference as a parameter. This engine ran the block as a STATEMENT,
where `u` names no relation the block provides; `expr.ResolveColumnRef`
stripped the qualifier, found no bare `x` either, and every row read the EMPTY
BOX under a text declaration. The filing's own shape,

```sql
SELECT SUM(a.v) AS a, SUM(b.v) AS b
FROM (SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM users) u) a
CROSS JOIN (SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM users) u) b
```

answered `""` and `""` under OID 701 where PostgreSQL 17.11 answers 18 (bigint,
OID 20) and 1026 (numeric, OID 1700). The unresolved name does not have to be a
derived table's output alias to reach it — `SELECT (SELECT u.id) FROM users u`
answered the empty box too — so the rule is stated about the MISSING FROM
CLAUSE and not about derived tables.

**THE REWRITE IS A SCOPE DECISION, SO IT IS MADE WHERE THE SCOPE IS KNOWN.**
The first form of it ran at the PARSER, at the site that builds a
`SubqueryNode`, and a parser standing on `(SELECT u.id)` cannot see which block
will supply `u`. Written there it fired inside a subquery that HAS a FROM
clause too — and such a block's text is REBUILT by the per-row re-run, so a
bare `u.id` left in its SELECT list survived into the rebuilt statement, where
the qualifier strip bound it to the INNER relation's own `id`. Nine shapes that
§1c's dangling guard had refused by name started answering one constant per
outer row instead. **A rewrite whose correctness depends on a scope must not be
written where the scope is absent**; the pass runs after the block is parsed,
with the block's own FROM list in hand (`plansql.unfoldFromlessScalars`), and
rewrites only where the reference RESOLVES THERE:

- a QUALIFIED reference whose qualifier is one of the block's own FROM
  identifiers (the alias where there is one, else the name);
- an UNQUALIFIED reference, when the block has a FROM item at all — SQL scopes
  innermost-first and a FROM-less subquery has no scope of its own;
- a subquery with no column reference at all (`(SELECT 1)`).

Running after the parse is also what keeps the PUBLISHED NAME right.
`SelectColumn.PublishedName` is stamped on the item AS WRITTEN, and PostgreSQL
names a scalar subquery's column after the subquery's own target list, ALIAS
INCLUDED: `SELECT (SELECT 1 AS zzz) FROM u` publishes `zzz`, `(SELECT u.name AS
nm)` publishes `nm`. The parser-time form returned the inner expression and
dropped the alias, so those became `?column?` and `name` — a name a BI client
binds a result set to (#732's territory), on the wire in both result formats.

**AND THE RE-RUN SUBSTITUTES INTO EVERY CLAUSE IT REBUILDS FROM AN AST.** The
other half of the same fact: `plansql.RebuildSQL` re-emitted every clause but
the WHERE as the text the parser recorded, so an outer reference in a SELECT
list or a HAVING survived the rebuild whether or not this rewrite put it there.
`SELECT (SELECT u.x FROM users y WHERE y.id = 1) FROM (SELECT id AS x FROM
users) u` — the spelling #1044 is TITLED for — answered NULL, NULL, NULL for
PostgreSQL's 1, 2, 3, on all five arms, before and after the rewrite alike.
`plansql.RebuildSQLForRerun` now rewrites the SELECT list, the WHERE and the
HAVING from their own trees, each item keeping its recorded text when the
rewrite does not change it.

GROUP BY, ORDER BY and a JOIN's ON condition are substituted too — SIX clauses
in all, and every one of them is written from its own tree. **THE ORDINAL TRAP
IS A RENDERING PROBLEM, AND THE RENDERING IS OURS TO CHOOSE.** `ORDER BY u.id`
with the outer row's 1 in it would render `ORDER BY 1`, which both engines read
as the FIRST SELECT ITEM, so `plansql.ClauseTermText` writes that value as
`CAST(1 AS BIGINT)` instead: measured statement for statement on PostgreSQL
17.11 and on this engine, a cast is a constant expression on both and a
select-list position on neither, where `(1)` is a position on both and is
therefore no repair. A rendering that is already an expression (`x.id * (1 -
2)`), a quoted string or a rendered DECIMAL is written as it is.

Two narrower dispositions were tried first and are recorded because each was
wider than the trap: refusing on the PRESENCE of an outer reference in those
clauses took four shapes main answers exactly as PostgreSQL does (round-3
review, P4), and refusing on the RENDERING took three more (round-4 review,
P2 — `(SELECT COUNT(*) FROM x GROUP BY u.id, x.id ORDER BY x.id LIMIT 1)` is
1, 1, 1 on PostgreSQL and at main). `expr.UnsubstitutedOuterRefError` (0A000)
survives as the POST-CONDITION of the rendering: it asks its question of the
text the rebuild writes, reports nothing today, and refuses loudly rather than
silently reading a position if a rendering ever becomes bare again.

**The refusal is only as wide as the walk that FINDS the reference**, and for
one round it was narrower than this paragraph said. `findCorrelatedRefs` read
the body's WHERE, HAVING and SELECT list and nothing else, so a subquery whose
ONLY outer reference sat in an ORDER BY term was never called correlated, never
reached the re-run, and answered the qualifier strip's constant — `(SELECT
x.visits FROM c2users x ORDER BY x.id * (u.id - 2) LIMIT 1)` was 100, 100, 100
for PostgreSQL 17.11's 200, 100, 100 (round-2 review, P1). `walkBlockForOuterRefs`
now reads every clause of the block that can carry a column reference — the
WHERE, the HAVING, the QUALIFY, the SELECT list, the GROUP BY terms, the
non-positional ORDER BY terms and each JOIN's ON condition — and every arm of
a set operation. The ON condition was the last one missing, and a reference
there was planned uncorrelated on all five arms (round-3 review, P1); LIMIT
and OFFSET carry no reference this parser will accept (`OFFSET u.id - 1` is
*expected number after OFFSET*), which is why the walk's silence about those
two clauses costs nothing today. Seeing the
reference is what lets it be substituted where the rebuild can and refused
where it cannot; neither is possible for a reference nobody looks for.

**A REWRITE MUST NOT PUT A BARE NUMERIC LITERAL IN AN ORDER BY TERM.** The same
ordinal trap caught the FROM-less rewrite itself. It ran after
`resolvePositionalRefs`, so a term written `(SELECT 1)` became the bare `1`
with nothing left to resolve it, and the planner refused it 42P10 as a position
naming no item — on ten shapes main answers exactly as PostgreSQL does, because
PostgreSQL reads only an integer literal WRITTEN IN THE CLAUSE as an ordinal,
never one a subquery evaluates to (round-2 review, B1). `ORDER BY (SELECT 1)`
is the generated-SQL idiom for a sort a query does not care about. The rewrite
declines any ORDER BY **or GROUP BY** term whose replacement would render as a
bare numeric literal; a constant sort or a constant grouping is what the
subquery is either way, so declining costs the shape nothing, and `SELECT
DISTINCT … ORDER BY (SELECT 1)` keeps PostgreSQL's own message rather than the
ordinal one. The decline was written in one of the two loops for a round, and
the other one turned `GROUP BY (SELECT 1)` into the ordinal `1`: seven
statements PostgreSQL and main both raise 42803 on answered a fabricated row
(`visits, n` = `NULL, 3` under OID 25) on all five arms instead (round-3
review, B2). One decline, both clauses.

**AN AGGREGATE BELONGS TO THE LEVEL OF THE DEEPEST VARIABLE IN ITS ARGUMENTS,
and this engine does not implement levels**, so a subquery holding an aggregate
whose argument names ONLY the enclosing query is refused
(`expr.OuterLevelAggregateError`, 0A000). PostgreSQL 17.11, measured: `SELECT
MAX((SELECT u.id)) FROM users u` is the ENCLOSING query's aggregate and answers
one row, 3; `SELECT id, (SELECT MAX(u.id) FROM x) FROM users u` is 42803,
because promoted to the outer query it leaves `id` ungrouped; `(SELECT
SUM(x.visits + u.id) FROM x)` names an inner variable too and is the inner
block's, answering 345, 348, 351; `(SELECT MAX(1) FROM x)` has no variable and
is the block's own, 1, 1, 1. Substituting the outer value and computing at the
inner level would answer a number PostgreSQL does not give.

**The boundary, as a table.** Every syntactic position a FROM-less scalar
subquery can occupy relative to the scope that owns its references, and what
each one does. The cell numbers are
`coordinator.TestArcC2ASubqueryReadsTheRowItIsCorrelatedOn`'s.

| the block enclosing the subquery | scope owning its references | disposition | cells |
|---|---|---|---|
| the statement's own SELECT list, WHERE, HAVING, GROUP BY or ORDER BY — over a base table, a derived table, a CTE, a set-operation body or a column-alias list | that block | REWRITTEN: value, declared type and published name are the expression's | 03–21, 27 |
| a derived table's or a CTE's body | that body's own FROM | REWRITTEN — such a body's text is never rebuilt | 01, 02 |
| an aggregate ARGUMENT of such a block | that block | REWRITTEN | 07 |
| a correlated subquery's SELECT list | the enclosing query | kept; the re-run substitutes the SELECT list | 45, 48, 49, 50 |
| a correlated subquery's WHERE | the enclosing query | kept; the re-run substitutes the WHERE | 51 |
| a correlated subquery's HAVING | the enclosing query | kept; the re-run substitutes the HAVING | 47 |
| a correlated IN set's SELECT list | the enclosing query | kept; substituted | 46, 69 |
| a correlated subquery's ORDER BY or GROUP BY, where the SUBSTITUTED term is an expression | the enclosing query | kept; substituted and answered | 52, 52a, 82, 83, 110 |
| a correlated subquery's ORDER BY or GROUP BY, where the SUBSTITUTED term would render as a BARE NUMERIC LITERAL | the enclosing query | kept; the value is rendered as a typed CAST (`CAST(1 AS BIGINT)`), a constant on both engines and a position on neither, and answered — nothing is refused for this reason any more | 52b, 53, 111, 112 |
| a correlated subquery's ORDER BY with NO LIMIT or OFFSET | the enclosing query | kept as written — a sort with no slice cannot change the answer | 109 |
| a JOIN's ON condition inside a correlated subquery | the enclosing query | kept; walked and substituted | 108 |
| a correlated subquery whose BODY is a SET OPERATION | the enclosing query | REFUSED 0A000 — `RebuildSQL` renders one select and has no arm for a union | 84, 85 |
| a SELECT item holding an AGGREGATE beside a nested subquery that NAMES THE ENCLOSING QUERY | the enclosing query | REFUSED 0A000 — the item has no type until the outer row is known | 86, 87, 88 |
| a SELECT item holding an AGGREGATE beside an UNCORRELATED nested subquery | the nested block itself | answers, as main answers it — the item's DECLARATION is still the FLOAT64 default where PostgreSQL says `numeric` (#1018, pinned on the wire) | 94–98 |
| an ORDER BY term of the ENCLOSING statement, `ORDER BY (SELECT 1)` | nothing — a constant sort | kept: only a literal WRITTEN in the clause is an ordinal | 71–80a |
| a GROUP BY term of the ENCLOSING statement, `GROUP BY (SELECT 1)` | nothing — a constant grouping | kept: PostgreSQL's 42803 on the ungrouped column, and its answer where the list is all aggregates | 101–107 |
| a LATERAL body | the enclosing query | kept; REFUSED 0A000 | 54 |
| an aggregate argument naming ONLY the enclosing query | the enclosing query, by PostgreSQL's level rule | REFUSED 0A000 | 60, 61 |
| a window call's argument or OVER terms | the enclosing query | REFUSED 0A000 (§1m) | 30–40 |
| a clause the rewrite DECLINES (ORDER BY, LIMIT, a non-constant WHERE) on the FROM-less body itself | the enclosing query | kept; answered through the re-run | 65–67 |
| a star with no relation | — | REFUSED 42601, PostgreSQL's own | 68 |

Measured against live PostgreSQL 17.11 over the three-row `c2users` fixture,
before → after, on all five arms:

| shape | at `bf99c56c` | at the tip | PG |
|---|---|---|---|
| the filing shape, derived and CTE spellings | `"",""` OID 701 | 18, 1026 OID 20/1700 | 18, 1026 |
| `(SELECT u.x)` over a derived alias, a CTE, a column-alias list, a set-operation body, a base-table alias | empty box | 1,2,3 | 1,2,3 |
| the same in WHERE / HAVING / ORDER BY / GROUP BY, and aggregated | 0 rows / 0 rows / unsorted / NULL / empty | PostgreSQL's | — |
| `(SELECT 1 AS zzz)`, `(SELECT u.name AS nm)` — the NAME | `zzz`, `nm` | `zzz`, `nm` | `zzz`, `nm` |
| the issue's titled shape WITH a FROM clause, over a derived table / CTE / base-table alias | NULL,NULL,NULL | 1,2,3 | 1,2,3 |
| an outer reference in a correlated subquery's HAVING | NULL | 342 | 342 |
| a FROM-less subquery nested in a correlated subquery's SELECT list / IN set / HAVING / expression, two and three deep, over a CTE | 0A000 | PostgreSQL's | — |
| the same in an ORDER BY term, a GROUP BY term, a LATERAL body | 0A000 | substituted / 0A000 on the ordinal rendering / 0A000 | answers |
| an aggregate beside an UNCORRELATED nested subquery, 7 spellings | answers, OID 701 | answers, OID 701 | answers, `numeric` |
| `GROUP BY (SELECT 1)`, 7 spellings | 42803 | 42803 | 42803 |
| an aggregate whose argument names only the enclosing query | 0A000 / `#277 schemaless batch` | 0A000 | 42803 |
| a declined clause on a FROM-less body (ORDER BY, LIMIT 1, a true WHERE) | NULL | 1,2,3 / 1,2,3 / NULL,2,3 | same |
| `(SELECT *)` with no relation | NULL | 42601 | 42601 |
| `(SELECT u.id WHERE 1=0)`, `LIMIT 0`, `OFFSET 1`; two columns | NULL; 42601 | NULL; 42601 | NULL; 42601 |

All three DAG arms EXECUTE the rewritten shapes as stages — the routing
counters are zero beside the rows — where every one of them routed to the
coordinator-local pipeline before.

**What this does NOT close.** Two positions, each pinned as a cell that fails
the day it closes:

- an AGGREGATE ARGUMENT whose subquery HAS a FROM clause (`SUM((SELECT u.x FROM
  y WHERE y.id=1))` over a derived table, cell 59) answers NULL. That compile
  site resolves its outer scope from the SCAN ALIASES below it
  (`physical.collectTableAliases`), which do not carry a derived table's alias,
  so the subquery is planned uncorrelated — §1c's named gap, identical at
  `bf99c56c`. The same subquery one position out, in the SELECT list, is cell
  55 and answers.
- a LATERAL body that PROJECTS an outer column rather than joining on it (cell
  70) answers the first row's value, and on the three DAG arms publishes the
  item under the inner expression's name. §1h's territory.

### 1m. A correlated subquery this engine cannot REBUILD is refused, and a WINDOW CALL is a position an outer reference can sit in

(Added 2026-09-12, #1045.)

The re-run of a correlated subquery substitutes the outer row's values into the
subquery's WHERE and REBUILDS the statement around it (`plansql.RebuildSQL`),
re-emitting every other clause as the text the parser recorded. A window call's
recorded text is `<func>(<args>) OVER (...)` — `WindowFuncNode.String()`
collapses the OVER clause deliberately, which is why
`plansql.ReplaceWindowFuncs` matches window nodes by POINTER and not by text —
so the rebuilt statement does not parse. That was already live and loud:
`(SELECT SUM(x.id) OVER () FROM users x WHERE x.id = u.id)`, correlated by its
WHERE and answered by PostgreSQL, died with `expected ')' after OVER clause`
from a runner re-reading a statement nobody wrote.

**#1045 is the SILENT half of the same fact.** `walkForOuterRefs` had no case
for a window node, so the whole call was a leaf and an outer reference inside
it was invisible — to the CLASSIFIER, which planned `(SELECT 1+SUM(u.id) OVER
() FROM users x WHERE x.id=1)` UNCORRELATED and ran it once, and to
`DanglingTableRefs`, which walks the same function with `anyOuter` set and is
what makes the three `window_*_correlated` shapes LOUD. That shared walk is the
entire difference between this shape and its siblings. With the case missing,
the qualifier strip rebound `u.id` to the inner relation's own `id` and every
outer row got one constant: 2, 2, 2 for PostgreSQL 17.11's 2, 3, 4.

The walk now descends into all four expression positions a window call has —
the ARGUMENTS, `PARTITION BY`, `ORDER BY` and the frame OFFSETS — and so does
the pruning collector `OuterColumnCandidates`, because a column only a window
reads still has to be projected by the outer query. The frame offsets cannot
carry a reference through this parser today (a non-literal frame bound is a
parse error) and are walked anyway, with the claim attempted against a
hand-built AST rather than left as untested code on the default path.

**The disposition is 0A000, PostgreSQL's feature_not_supported.**
`plansql.HoldsWindowCall` is what all three correlated constructs — a scalar
subquery, an `IN` set and an `EXISTS` — ask before building an evaluator that
would rebuild the text, and the refusal names the reference and the mechanism.
It is raised at COMPILE time, once per query, because it is a property of the
plan and not of a row.

**Its reach is exactly what that function reads**, and stating it loosely
overstates the engine. `HoldsWindowCall` asks the block's own window columns
(`collectWindowSpecs`), its SELECT items, its `HAVING`, its `QUALIFY` and its
set-operation arms. It does NOT read an `ORDER BY` term, and it does not
descend into a nested block — so a window in a correlated body's `ORDER BY`
(`EXISTS (SELECT 1 FROM x WHERE x.id=u.id ORDER BY ROW_NUMBER() OVER ())`), a
window inside a derived table in that body, and a window in a decorrelated
`EXISTS` are not refused, and answer PostgreSQL's rows. Those three clauses are
re-emitted as the text the parser recorded rather than rendered from a window
node, which is why they survive the rebuild.

Measured against live PostgreSQL 17.11 over the three-row `c2users` fixture,
before → after, on all five arms:

| shape | before | after | PG |
|---|---|---|---|
| `(SELECT 1+SUM(u.id) OVER () FROM x WHERE x.id=1)` | 2,2,2 silent | 0A000 | 2,3,4 |
| the window call alone; a BIGINT outer column; two outer columns in one argument | one constant, silent | 0A000 | 1,2,3 / 101,43,201 / 101,44,203 |
| the same subquery in a WHERE clause; over a CTE | 0 rows / empty box, silent | 0A000 | 2,3 / 2,3,4 |
| `PARTITION BY u.id`, `ORDER BY u.id`, `COUNT(*) OVER (PARTITION BY u.id)` over a ONE-row inner | right | 0A000 | right |
| the same two over a TWO-row inner | 1,1,1 silent | 0A000 | 3,3,3 / 2,2,2 |
| a correlated subquery holding an UNCORRELATED window | `expected ')' after OVER clause` | 0A000 | answers |
| an uncorrelated window in a subquery; a bare name the inner relation supplies; `PARTITION BY` the inner relation; a window over the query itself | right | right | right |

A FOURTH shape moves right → loud and belongs in the census beside them: a
correlated subquery with a `QUALIFY` that happens to be a no-op (`WHERE
x.id=u.id QUALIFY ROW_NUMBER() OVER () = 1`) answered 1, 2, 3 at `bf99c56c` and
is refused at the tip. Its rightness was the same kind of accident —
`RebuildSQL` drops a `QUALIFY` clause outright (this section's own residual
list), and dropping it changes nothing when the WHERE already leaves one row.
With a `QUALIFY` that MATTERS the engine was loud at `bf99c56c` too ("more than
one row returned by a subquery used as an expression"), so the pair is
loud → loud on the shape the clause decides.

The one-row/two-row pair is the discriminator, and it is why the refusal is not
a right answer traded for a loud one. `SUM(x.id) OVER (PARTITION BY u.id)` over
a ONE-row inner relation is right whatever the engine partitions by; the same
shape over two rows answered 1, 1, 1 for PostgreSQL's 3, 3, 3. Both are cells
of the census, and the right one moves to loud because its rightness was the
fixture's and not the engine's.

**What this does NOT close.** The SELECT list, the WHERE and the HAVING are
substituted now (§1l); `GROUP BY` and `ORDER BY` are not, because a substituted
term there renders as a bare literal and reads as a select-list POSITION, and
`RebuildSQL` drops a `QUALIFY` clause outright. All three are refused rather
than run, and since round 3 the classifier reads those clauses, so the refusal
reaches a subquery whose ONLY outer reference is in one of them. The WINDOW case stays refused for a different reason and a harder
one: there is no faithful rendering of an `OVER` clause to rebuild, and
`WindowFuncNode.String()` emitting `OVER (...)` — three literal dots — is a
defect of its own worth closing before anything here can.

(Arc L1, 2026-09-14.) Fifteen cells of
`coordinator.TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm` hold that
refusal on five arms — the scalar subquery in three join positions × the
window's argument, a bare argument, `PARTITION BY`, `ORDER BY` and a two-row
inner — plus the `IN` spelling and the three cells that die in the PARSER
instead, where the window sits in the body's own `ORDER BY` and
`HoldsWindowCall` does not read. Both doors are the same rendering, recorded
together so the day it is faithful they move together.

### 1n. A LATERAL body with NO FROM clause is a projection over the outer row

(Added 2026-09-12, #1033.)

§1h settles what a LATERAL means when it HAS a FROM clause: it runs per outer
row, the correlation in its WHERE is promoted to a join key, and an empty input
still answers. A body with NO FROM clause is a different relation and the
correlation machinery cannot see it at all: there are no equalities in a WHERE
to promote, so an outer reference in the body's SELECT LIST was promoted
nowhere. `BuildFromSelect` built `Project(Dual, [u.id AS v])`, the Dual carries
no `u`, and every projected value came back NULL under the STRING default an
unresolvable reference falls to — `SELECT l.v FROM users u, LATERAL (SELECT
u.id AS v) l` answered three NULLs with OID 25 where PostgreSQL 17.11 answers
1, 2, 3 as bigint, and `COUNT(*)` was right throughout. The rows were produced;
only the values were lost.

**THE POSITION.** Such a body yields exactly one row per outer row whose
columns are functions of that row, so the relation it ranges over IS the outer
row and

    left CROSS JOIN LATERAL (SELECT e(u) AS v)   ==   π(u.*, e(u) AS v)(left)

There is no dependent join, no correlation key and nothing to decorrelate. The
lowering records the body's items on the join node
(`logical.Node.LateralDualItems`), the single-process builder computes them
above the OUTER stream through the engine's own compiled expressions
(`exec.LateralOuterProject`, resolved from the first batch the way
`exec.LateralEmptyDefault` already is), and the body's WHERE and the written ON
move into the ENCLOSING query's WHERE — which for an inner join is exactly
where they already applied, and is the same move §1h's `lateralPadThenFilter`
makes.

**THE JOIN NODE AND THE DUAL STAY IN THE TREE**, for two reasons that are one
decision. The declaration walks ask a node what its CHILD publishes, and the
child of such a body's Project is the Dual — so the Dual records the OUTER
subtree (`logical.Node.LateralOuterScope`) and `inputColDecls` answers with the
outer row's columns, which is what types `u.id` in the body's SELECT list at
every walk at once. And a plan containing a Dual is handed to the coordinator's
in-process pipeline (`physical.ErrTableLessSelectDistributed`, #806), so all
five execution arms answer through this one lowering rather than needing the
distributed single-row source that refusal exists for. The DAG cells assert
`TableLessLocalRoutes` beside their rows.

**THE CLASSIFIER ASKS TWO QUESTIONS, NOT ONE** (amended 2026-09-12, round-2
review B1). A table-less body that names NO column is not correlated at all:
nothing about it depends on the outer row, the ordinary build already produced
`Project(Dual, …)`, and the cross join with its one row is exactly PostgreSQL's
answer. Classifying by the FROM clause alone took twelve such shapes —
`(SELECT 7 AS v LIMIT 1)`, `(SELECT DISTINCT 7 AS v)`, `(SELECT COUNT(*) AS c)`,
`(SELECT ROW_NUMBER() OVER () AS v)`, `LEFT JOIN … ON false`, and the rest —
from PostgreSQL's own rows to a refusal, 60 cells across five arms. So:

    body reads the outer row?    disposition
      no                         the BASE path, every clause class
      yes, and a projection      LOWERED
      yes, and not a projection  REFUSED 0A000 naming the class

In a table-less body every column reference IS an outer reference, which makes
the question exact rather than a heuristic: there is no relation of its own for
a name to resolve to. Three exclusions, each with a cell:

* **ONE WALK, not three.** `walkExprNodes` (lateral_scope_walk.go) is what the
  body's outer-read test, the star-list read test and the window refusal all
  ask. They used to be three tests — two `plansql.RewriteExpr` walks and a
  per-item `SelectColumn.IsWindow` flag — and `RewriteExpr` enters neither an
  AGGREGATE call nor a WINDOW call, so each lost a different piece: a read
  inside `SUM(l.w) OVER ()` or `HAVING MAX(l.w) > 2` was invisible to the
  star-list test (round-4 review, B2), and `(SUM(u.id) OVER ()) + 1` was not a
  window body to the flag (B3). The disagreement between them is what four
  rounds of oscillation between a too-wide and a too-narrow refusal were made
  of;
* a SUBQUERY is opaque — its references are its own FROM's;
* the body's OWN output names are not outer columns (this parser resolves
  `ORDER BY 1` to the item's alias);
* **every term is RESOLVED, not read as text.** `plansql.WindowSpec` carries a
  window's PARTITION BY / ORDER BY terms as strings, and counting any non-empty
  one as a column read made `OVER (ORDER BY 1)` and `OVER (PARTITION BY 1)` —
  integer literals — "reads the outer row"; a window body is not a projection,
  so the shape was refused where the base answered PostgreSQL's rows (round-2
  review, B1). A false positive here is not a lost optimization: the predicate
  is what ARMS the refusal.

And THE BODY'S OWN SORT TERM IS NOT ASKED AT ALL — a different question from
the enclosing query's ORDER BY over a column-alias list, which IS a read (see
the star-list paragraph below). A table-less body yields at most one row,
so its ORDER BY is the identity whatever it names; it is dropped rather than
refused, and `(SELECT 7 AS v ORDER BY u.id)` is the base's answer and
PostgreSQL's. The whole enumeration — every clause that can hold a term × what
the term is made of — is `TestC1EBThePredicateInputTable`. The table is enumerated once, in
`internal/coordinator/arc_c1_body_class_two_path_test.go`, one cell per class
per side on five arms.

**THE BOUNDARY IS A REFUSAL, NOT AN APPROXIMATION.** A CORRELATED body that is
not a projection over the outer row — an aggregate, a GROUP BY, a HAVING, a
window function, DISTINCT, a LIMIT/OFFSET, a set operation, a WITH
clause, a star, a subquery in an item — and an OUTER join over one that would
have to PAD (a non-trivial ON, or a body WHERE) are refused `0A000` naming the
class. Each of THOSE answered plausible NULLs before; loud beats plausible.

**THE ITEM IS TYPED AGAINST WHAT THE OUTER SIDE PUBLISHES** (amended
2026-09-12, round-2 review P1). `inputColDecls` walks to the scan annotation and
STOPS at a Project, so an outer side that is a derived table, a CTE, a sort or
an aggregate answered nothing and every item took the STRING default — the
right VALUES under OID 25, which is the half of #1033 the issue was filed for,
one position over. The declaration now merges the EMITTED walk, which types a
Project through the same `declaredProjectionDecl` the output schema uses.

**THE FROM ITEM'S COLUMN-ALIAS LIST RENAMES THE ITEMS POSITIONALLY** (round-2
review P2), on BOTH lateral lowerings and before either reads the list, because
the decorrelating one injects correlation keys into it. Without that,
`LATERAL (SELECT u.id AS v) l(w)` renamed a column nothing carried and `l.w`
answered NULL — the headline shape under a second spelling. A list longer than
the body is PostgreSQL's 42P10.

**A list over a body carrying a STAR is REFUSED 0A000 WHEN — and only when —
the enclosing query READS a name the list introduces** (amended 2026-09-12,
round-2 review B2; narrowed by round-3 review B2 and stated here in round 5,
where the review found this paragraph still unconditional). PostgreSQL applies a
SHORT list to the first k columns of the star's expansion and leaves the rest
under their own names, so a query that never mentions a renamed name is
unaffected by the rename, and refusing on the PRESENCE of the list was ten cells
right → refused.

WHAT COUNTS AS A READ is the one walk's answer, not a second scan: a reference
inside an AGGREGATE (`HAVING MAX(l.w) > 2`), inside a WINDOW call (its argument,
PARTITION BY or ORDER BY), in a WHERE, a GROUP BY, a QUALIFY or a written ON,
one in a LATER FROM item's body, one in the enclosing block's ORDER BY, and —
through a star in the enclosing block — one block up.

EACH POSITION IS KEYED ON WHAT THE REFERENCE RESOLVES TO, never on the name
alone and never on how it is QUALIFIED (amended 2026-09-12, round-5 review B1;
corrected 2026-09-12, round-6 review B1/B2/B3 — the first correction swung to
qualification, which is a different wrong answer). The three positions, as the
code asks them:

* A STAR republishes THIS lateral's names only when it is unqualified or
  qualified with this lateral's alias, so `SELECT u.*` over the outer relation
  is not a read of `l`'s list;
* A SIBLING FROM ITEM's reference counts when it is QUALIFIED with this
  lateral's alias, or BARE in a sibling that is itself TABLE-LESS and publishes
  no such name of its own — a bare name there can resolve to nothing else. A
  sibling that publishes its own `w` is reading its own; a sibling with a FROM
  clause may be reading that. Requiring qualification dropped
  `…, LATERAL (SELECT w + 100 AS z) m` and answered four NULLs for PostgreSQL's
  101..104, and `CASE WHEN w > 2 …` answered `0,0,0,0` for `0,0,1,1` — a wrong
  VALUE, not a NULL. That sibling's OWN sort term is NOT asked: a one-row body's
  sort is the identity whatever it names;
* AN UNQUALIFIED SORT TERM THAT STANDS ALONE in the enclosing block binds to the
  SELECT list's OUTPUT columns first, as PostgreSQL binds it, so
  `SELECT u.total AS w … l(w) ORDER BY w DESC` names that output column and never
  the list. Refusing it was five shapes right → refused in BOTH sort directions
  over three outer columns. INSIDE AN EXPRESSION the same name is an INPUT
  column — PostgreSQL: "an output column name has to stand alone, that is, it
  cannot be used in an expression" — so `ORDER BY w + 0`, `w * -1` and
  `CASE WHEN w > 2 …` name the alias list and ARE reads. Applying the skip per
  NODE rather than per TERM made those sorts bind nothing and answer the wrong
  order where the previous tip refused; the fixture that separates the two
  bindings is `SELECT -u.total AS w`, whose two candidate orders differ, and it
  is a gate row (round-7 review, B1). A PARENTHESISED bare name PostgreSQL still
  binds to the output column, and this layer CAN see the `ParenNode`: the
  refusal is forced by what skipping it would PRODUCE, not by what the AST
  shows. Peel the parentheses and take the skip, and the sort binds nothing —
  `SELECT u.total AS w … ORDER BY (w) DESC` then answers `150,150,200,200` for
  PostgreSQL's `200,200,150,150` (measured, round-8 review P2). That is round-5
  B2 again, and loud beats it.

  THE COST IS A MIXED SORT LIST, recorded rather than wished away: a list whose
  terms are `CASE WHEN w > 100 THEN 0 ELSE 1 END, w DESC` names the alias list
  in one term and the output column in the other, so it is refused where
  PostgreSQL answers `200,200,150,150` — as is its neighbour whose second term
  is a qualified `l.w`. Answering either needs the rename the star's width makes
  impossible. Both are gate rows carrying PostgreSQL's value, so the day the
  rename becomes possible the cells fail and are rewritten to it.

Keyed on the name alone, the star and sibling positions refused twenty cells
that PostgreSQL, main and the previous tip all answer; keyed on qualification,
the sibling position dropped three more. Every row of the seam — each position
crossed with what a reference there can resolve to — is one cell of
`TestC1FTheReadTestSeam` (`internal/coordinator/arc_c1_read_seam_two_path_test.go`),
on five arms, so a new position is a row rather than a round.

A SORT TERM THAT NAMES THE LIST IS A READ (amended 2026-09-12, round-5 review
B2, reversing this section's round-5 sentence). The reasoning for excluding it — a sort term
decides the order and never the values — holds only for a sort term that BINDS.
This one does not bind: the rename is dropped on the lowered path, so
`… l(w) ORDER BY l.w DESC` sorted on nothing and answered `1,1,2,2` for
PostgreSQL's `2,2,1,1`, and a permuted list `l(order_id, id, amount, product)
ORDER BY l.product` sorted by a different column of the same rows. The ASCENDING
spelling agreed with PostgreSQL by coincidence — an unbound sort term leaves the
scan's order, which over this fixture is ascending — and that coincidence is
what made the exclusion look measured for a round. The rename cannot be applied
for a sort term for the same reason it cannot be applied in the SELECT list (the
paragraph below), so the disposition is the refusal wherever the term names the
list — and a spelling that would bind nothing is refused rather than answered.

The earlier sentence said such a list was left to
`RefuseUnappliedColumnAliasLists` — and nothing on either lateral path called
`deferColumnAliasesOverStar`, so that refusal could never be reached and the
list was DROPPED: `LATERAL (SELECT * FROM i WHERE i.order_id = u.id) l(w)`
answered four NULLs for PostgreSQL's 1,2,3,4, which is the exact failure the
deferral mechanism exists to prevent. A LATERAL is refused rather than deferred,
unlike a CTE's and a derived table's list, because the decorrelation JOINS on
the column its correlated predicate names: a positional rename over an uncounted
star could take that column and leave the join keying on a name nothing carries
— zero rows, in silence. Wrong → loud, and the sentence now describes the code.

**NOT SETTLED:** a zero-row outer side and a lateral inside a scalar subquery's
own block still declare STRING with the right values (round-2 review, P2). Both
carry a fail-on-agree cell in
`internal/coordinator/arc_c1_scope_two_path_test.go`; the second was added in
round 4 after the review found the claim named one pin and only one existed.

**NOT SETTLED, and recorded rather than repaired here:** an accumulating
aggregate OVER such a column declares float8 where PostgreSQL declares numeric
(`SELECT SUM(l.v) …` is 6, correctly, under OID 701). The same shape over a
plain derived table diverges identically with no LATERAL in it —
`aggInputColumnType` stops at a JOIN because `emittedColTypes` has no arm for
one — so it is the "a declaration has to RIDE through every materialization"
gap ADR-0024 and #1018 name, one level up, and it is pinned in
`internal/coordinator/arc_c1_scope_two_path_test.go` with the derived-table
twin beside it as the proof.

### 1o. A recursive CTE is materialized where its BLOCK is planned

(Added 2026-09-12, #1047.)

§1b settles what a recursive CTE reference IS: a tagged `NewScan(cteName)` the
builder deliberately does not expand, served by the physical planner from
`cteCache`. What it did not say is WHERE that cache is filled, and the answer
was "from `root.CTEs`, once, at the statement root". A recursive CTE declared
in a NESTED block — a derived table, another CTE's body, a LATERAL, a
set-operation arm — is in no such list, so the lookup missed and the tagged
scan fell through to a scan of A RELATION THAT DOES NOT EXIST, which answers
ZERO ROWS instead of failing. That is #1041's "a missing cache entry reads as
an empty CTE" door, one level up, and it is silent.

**THE POSITION.** The DEFINITION rides on the REFERENCE
(`logical.Node.RecursiveCTE`, a pointer into the parser's own memoized tree for
ADR-0032's reason), and the physical planner materializes a cache miss from it
where the block is planned. Three consequences are part of the position:

* **The CTE's own name is in scope for its own iteration.** The fixed-point
  iteration re-plans the recursive term as a whole statement, which resolves
  names against `Planner.ctes`; at the root that list already held the
  definition, and inside a nested block it held the enclosing scope's — so the
  self-reference resolved as a TABLE, read nothing, and the iteration stopped
  after the anchor. The definition is APPENDED to the enclosing scope, not
  substituted for it, because the body may name an enclosing CTE too.
* **The materialization is keyed by the DEFINITION, not by the name.** Two
  sibling blocks may each declare `WITH RECURSIVE r` and they are two
  relations. `cteCache` is statement-wide and keyed by name — which the
  iteration needs, because the self-reference resolves by name — so the name
  binding is borrowed for the materialization and handed back, and the result
  is kept under the definition's identity (`Planner.nestedCTECache`).
* **A reference that cannot be served is a REFUSAL.** The name belongs to the
  CTE and to nothing else in scope, so reading it as a table is reading a
  relation that does not exist. `materializeRecursiveCTE` keeps ONE contract —
  either a cache entry or an error — and the reference surfaces that error;
  every path that used to return silently now names itself. The six refusal
  cells of §1o-a's gate take exactly that route, which is what makes this a
  reachable position rather than a claim about a branch nothing reaches
  (round-2 review, P3).

### 1o-a. The FORM is decided before the body is planned

(Added 2026-09-12, round-2 review B3.)

§1o's materialization PLANS the body, and the body's self-reference is a tagged
scan whose cache lookup misses until the first iteration seeds the work table.
`splitRecursiveUnion` recognises only `UNION ALL`, so a recursive CTE written
with plain `UNION` — PostgreSQL's cycle-safe spelling, and standard SQL — fell
to the columnar materialization, which planned the body, whose self-reference
re-materialized THE SAME DEFINITION from inside its own materialization. At the
statement ROOT, reachable by any client, the query never returned and took 25 GB
of RSS in 45 seconds. §1o made a bounded wrong answer unbounded, which is a
different defect in kind.

**The position has two halves and needs both.**

* **A definition under materialization carries an IN-PROGRESS marker**
  (`Planner.cteInProgress`, keyed by NAME because the name is what a
  self-reference resolves by). A self-reference that reaches the planner from
  inside its own materialization is served by the work table where the
  iteration has seeded one, and REFUSED otherwise — PostgreSQL's 42P19,
  "recursive reference to query %q must not appear within its non-recursive
  term". Re-entry is impossible by construction rather than by depth counting.
* **The FORM is decided from the PARSED SET-OPERATION TREE before anything is
  planned**, and the tree is left-associative exactly as PostgreSQL's is: the
  TOP node's Right is the last arm and its Left is every earlier arm together.
  So the questions are asked in PostgreSQL's own order — does any arm but the
  last name the CTE (42P19, "must not appear within its non-recursive term"),
  does the last arm name it at all (if not, the body is not recursive), and is
  the TOP operator `UNION ALL` (if it is a plain `UNION`, 0A000: PostgreSQL
  answers it by removing duplicates at every step, which is a feature gap and
  not a syntax error, ADR-0012). Asking the ALL-ness BEFORE the arm position
  gave a first-arm self-reference 0A000 where PostgreSQL raises 42P19 (round-2
  review, P1).

  Deciding it from the body's TEXT instead — a split at the FIRST top-level
  `UNION ALL` — put an arm that names the CTE and an arm that does not into one
  "recursive term", and the iteration re-ran the constant arm every round: 1002
  rows, one then 1001 NULLs, where PostgreSQL answers five (round-2 review,
  B3). The text split that feeds the iteration now takes the LAST top-level
  `UNION ALL` and is VERIFIED against the parse — the two halves are re-parsed
  and must name the CTE exactly as the tree said, or the body is refused.
  A set-operation anchor publishes its LEFT arm's names, which is what
  `inferCTESchema` reads for a multi-arm non-recursive term.

Two consequences fall out of asking the self-reference question at all. A
`WITH RECURSIVE` whose body does NOT name itself is not recursive — PostgreSQL
answers `WITH RECURSIVE r AS (SELECT 1 UNION SELECT 2)` — and keeps the ordinary
materialization. And a `UNION ALL` whose second arm names no CTE is not a
recursive TERM: the iteration re-ran it until `maxRecursiveIterations` and
answered 1001 rows where PostgreSQL answers 2.

**NOT SETTLED:** the DAG still cannot run any recursive CTE, at the root or
nested (#1042, #960) — the tagged scan becomes a stage with no scan files and no
dependencies and the dispatcher fails it, loudly. The census pins that on the
three distributed arms with the ROOT shape beside them as the proof it is not
this position's. (A `WITH` written inside a scalar or `IN` subquery was a second
NOT-SETTLED here, `42601` at the parser; §1p closed it.)

### 1o-b. A recursive CTE answers its whole closure or fails

(Added 2026-09-22, arc RC: #1246, #1041, #1074, #1193; the recursive half of
#1013.)

§1o settled WHERE a recursive CTE is materialized; this settles what the
materialization IS. The loop it replaced boxed every row into a map, re-derived
the CTE's schema from the Go values of the seed's first row, dropped any error
the recursive term raised, and stopped after 1000 iterations — and in each of
those four places it returned the rows it had as if they were the answer.
`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5000)`
answered 1001 rows; a five-year date series added 1 to a string; a term that
divided by zero on its third step answered the two steps before it.

**THE POSITION: the fixed point, or an error.** There is no silent cap. The
recursive term is iterated until it produces no rows, which is PostgreSQL's
rule, and the CTE holds the whole closure. Three bounds end a recursion that has
no fixed point, and each is an ERROR that replaces the answer:

* a **cancelled** statement, checked between iterations (statement_timeout and
  CancelRequest are 57014 at the door);
* **one iteration** producing more than the memory budget (53200). The working
  table — the last iteration's rows, which the self-reference reads — is held
  in memory, so a recursion whose rows grow at every step meets the budget in
  a few dozen iterations;
* **1,000,000 iterations** (54000, program_limit_exceeded). The closure spills
  past the budget like any materialized result, so a recursion whose rows do
  NOT grow — `SELECT n+1 FROM r` with no WHERE — is bounded by nothing else and
  would run until the disk filled.

The mechanism was chosen by measurement, against the two alternatives the
position allowed. A BUDGET-ONLY bound (the closure charged and not spilled)
cannot end a one-row-per-iteration recursion on an engine whose budget is most
of the machine, and at 512 KiB it refuses a legitimate 100 000-row series;
PostgreSQL answers both of those or runs them until a limit the operator set. An
ITERATION bound alone lets a doubling recursion take the process before it is
reached. So both, each where it is the honest one. The iteration costs were
measured on the tip: 20–45 µs for a term over the working table alone, ~440 µs
for a term that joins a catalog table every step — so the limit is reached in
20–45 s for the first shape and minutes for the second, and 100 000 iterations
answer in about 3 s. The number sits two orders of magnitude above any date
series or hierarchy an application writes (a minute-per-row year is 525 600).
PostgreSQL measured once for the record: the no-WHERE recursion is 53400 under
a 256 MB `temp_file_limit` after 4.9 s.

**THE SEED DECIDES THE TYPES.** Each arm is planned and run as its own
pipeline, columnar, and the CTE's schema is the seed's — its batches' when a
row arrives, its plan's declaration when none does. The reference is typed from
the materialization (`Planner.stampRecursiveReference`), so an expression over
it is declared from the real column types rather than falling to the untyped
rule (`n + 1` over an integer was declared float8 and the old loop truncated it
back). The recursive term's values are restated under the seed's types by
PostgreSQL's own UNION resolution with the seed first — 42804 exactly when that
resolution does not come back to the seed's type. The rule is the MEASURED
table, 14 seed spellings × 16 term spellings on 17.11
(`wadjet.TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType`, each cell
carrying PostgreSQL's answer): an integer or bigint seed accepts either integer
width (range-checked, 22003 — this engine declares `n + 1` bigint, a recorded
superset); an unconstrained numeric seed accepts integers, numerics and a
numeric literal; a CONSTRAINED numeric(p,s) seed only its own typmod; real
accepts integers, numeric and a numeric literal; double precision also real;
timestamp accepts date; and every seed accepts an UNKNOWN literal — a bare NULL,
or a quoted string read by the seed type's input function (22P02 / 22007).

An unconstrained numeric is one scale per VALUE in PostgreSQL and one per
COLUMN here, and the seed decided it: `SELECT 1::numeric UNION ALL SELECT n +
0.5 …` is DECIMAL(38,0) after the seed. A term value finer than the column
restarts the fixed point with the column declared at the scale that value
needs; a value exact at the column's scale is stored there (`1.0 * 0.5` is 0.50
by its declared scale and 0.5 by its value), so a product does not widen the
column without end. Scales only grow and stop at 38.

Round 1 of this arc checked the term against a two-entry list (integer↔integer,
integer/real→double) and refused shapes PostgreSQL and the base both answer —
an integer term under a numeric seed, a bare NULL term (review B1). The table
replaced the list; 0 of its 224 cells answered correctly at the base and
differently at round 2's tip.

**THE FORM IS PostgreSQL's.** §1o-a decided the set-operation form; the
recursive term's own shape is decided before anything runs, with PostgreSQL's
42P19 and sentence: no aggregate in a query block whose own FROM names the
reference (the term, or a derived table at any depth under it — an aggregate
over a derived table that reads the reference is answered, as PostgreSQL
answers it), no self-reference inside a subquery expression, on the nullable
side of an outer join, or more than once. Each of those, iterated, reaches no
fixed point or the wrong one. (Round 1 checked the aggregate at the term's own
level only, and `SELECT n FROM (SELECT max(n)+1 AS n FROM r …) q` answered —
review B2.)

**THE NAMES ARE THE SEED's.** The binder closes a recursive CTE's scope over
the seed's published names overlaid by the column list, so a name only the
recursive term spells is 42703 (#1074), and validates the body again under the
closed scope. The parser marks an item recursive only when its body names
itself, so a plain sibling in a `WITH RECURSIVE` list is an ordinary CTE with
the ordinary published names (#1193). A reference carries the alias it was
written under, so two references to one CTE in a join are two relations.

**A recursive CTE reference is not a policed relation.** The policy layer
read the tagged scan's NAME as a table and default-denied it, so under ABAC
every recursive CTE was 42501 (`permission denied for table "r"`) on every
door. The reference is the closure of its arms, and the arms are planned
through the subquery path that applies the context's policies and lookup —
access, masks and row filters — to every relation they read; so the reference
is skipped by `logical.PolicedScanTables`, the new-scan pass and the
plan-order check, and a relation the identity may not read is still refused
where an arm names it. Gated on every door by
`server.TestArcRCARecursiveCTEOverAPolicedRelationNeverPublishesAPolicedValue`
(24 of 63 pairs answer, all masked) and
`server.TestArcRCARecursiveCTEArmReadingAnUnreadableRelationIsRefused`.

**Per-iteration resources are per-iteration.** A join's build reservation and
an IN-set's charge are released when the iteration that built them ends; the
plan's Cleanup ran once, at statement end, so a term that joined held every
iteration's build until then.

**NOT SETTLED:** PostgreSQL's lazy evaluation — `… SELECT n FROM r LIMIT 5`
over a recursion with no stop answers five rows there and is 54000 here — and
a forward reference inside a `WITH RECURSIVE` list (42P01 here). Both are
refusals, listed in docs/postgres-differences.md. The DAG still cannot run a
recursive CTE (#1042).

### 1p. A decorrelated join's key has TWO references, and a block declares its own scope

§1's whole subject is the BUILD side: because the rewrite builds `Scan →
[Join …] → [Filter] → [Aggregate]` and never a Project, the build side carries
the source names of the relations it reads, and which of them a join emits bare
is `reorderJoins`' decision long after the rewrite runs. §1j, #526 and #527
settled that by RECORDING the reference and letting
`repairDecorrelatedSpelling` spell it once the order is final.

The premise under that machinery was written down and it was wrong by half:

> a probe-side term the rewrite already spelled correctly (it names the OUTER
> query's columns, which no inner reordering can move)

The outer query's columns do not move, but **the name the outer PLAN emits for
them does**, and for exactly the reason the build side's does: the enclosing
query is a join as often as the subquery is, and a join emits one arm's `id`
bare and the other arm's as `o.id`. Spelling the probe key by dropping the
qualifier therefore bound whichever arm the estimator put on the probe:

```
SELECT … FROM lat_ord o JOIN lat_item i ON i.order_id = o.id
  WHERE o.id IN (SELECT id FROM lat_ord)                      PG 4 → 3
  WHERE o.id IN (SELECT order_id FROM lat_item)               PG 4 → 2
  WHERE EXISTS (SELECT 1 FROM lat_ord z WHERE z.id = o.id)    PG 4 → 3
  WHERE o.id = (SELECT MAX(z.id) FROM lat_ord z
                WHERE z.id = o.id)                            PG 4 → 1
```

silently, on all five arms, with the INNER-side spelling and the no-join
spelling both right — which is what localises the loss to the probe rather than
to the build (#1098). **A decorrelated join's key is two references, and each is
spelled against the side that emits it.** `InnerKeyRef` is `KeyRef` because that
is what it always was; the repair resolves the probe side against
`emittedColumns(Children[0])` exactly as it resolves the build side against
`Children[1]`.

The same fact has a second site. A correlated subquery that is NOT decorrelated
re-runs per outer row, and `readOuterValues` reads the outer value out of the
enclosing query's BATCH — which is that same join output. It looked up the bare
name first, so `o.id NOT IN (…)` over a join read `lat_item`'s id. The
qualified spelling is tried first now; an unqualified reference, and a qualified
one the stream does not carry under its qualifier, still read the bare name.

**A BLOCK DECLARES ITS OWN SCOPE.** Three readers treated a subquery body's own
`WITH` as though it were not there:

- `decorrelatedInnerPlan` built the body's FROM with the ENCLOSING items alone,
  so a CTE the body declares was indistinguishable from a base table and the
  build side became a Scan of that name (#1067).
- `rebuildSQLFull` started at `SELECT ` and dropped `info.CTEs`, so a
  correlated body's per-row re-run named a FROM item nothing declared.
- the parser accepted a subquery that begins with `SELECT` and not one that
  begins with `WITH`, at `IN`, at `ANY`/`ALL` and at a parenthesised scalar
  subquery — `EXISTS` accepted it only because it captures the parenthesised
  text without looking.

The first two are the same wrong answer with two faces: where nothing answers
to the name the body read an EMPTY relation and a correlated `EXISTS` dropped
every outer row; where something did, it read the BASE TABLE, and `EXISTS (WITH
lat_item AS (SELECT id FROM lat_ord WHERE id = 1) SELECT 1 FROM lat_item WHERE
lat_item.id = o.id)` answered 3 rows for PostgreSQL 17.11's 1. The rule is the
one every other block is built with: `scopeCTEs(enclosing, own)`, and a rebuild
re-emits the clause it re-parses — `RECURSIVE` as a property of the clause,
which is PostgreSQL's spelling.

**AND A NESTED BLOCK IS A BLOCK.** §1l made the walk over a subquery's OWN
clauses complete, including its set-operation arms. One level down it was not:
`walkNestedForOuterRefs` read the WHERE, the HAVING and the SELECT list, so a
nested set operation's arms, a JOIN's `ON`, the GROUP BY, the ORDER BY and the
QUALIFY were invisible. A correlated scalar subquery holding

```sql
x.id IN (SELECT k FROM c2t2 WHERE k = u.id UNION ALL SELECT k FROM c2t2 WHERE k = 99)
```

was planned UNCORRELATED: `u.id` bound nothing, the IN set came out empty, and
every outer row answered NULL for PostgreSQL's `100, 42, NULL` (#1072). The same
blindness sat in the LOGICAL classifier: `nodeTableRefs` had no case for a
subquery, so such a condition reported neither side — built into the build side
by the IN rewrite, and DROPPED outright by the EXISTS rewrite, whose classifier
keeps only what reports `hasInner`.

One walk, one rule: a nested block is walked by `walkBlockForOuterRefs`, and
`nodeTableRefs` covers every node kind the correlation walk covers, asking the
correlation walk itself about a nested block rather than re-reading the same
trees.

Seen, the shape is REFUSED and not answered — the per-row rebuild renders one
select per block and has no arm for `info.Union` — and the refusal says THAT,
rather than borrowing the sentence about a bare numeric literal in a GROUP BY.
**A refusal that states one mechanism for every cause is a refusal that sends
the next reader to the wrong place.**

**A RECURSIVE CTE REFERENCE PUBLISHES A COLUMN LIST.** §1b and §1o settled what
a recursive CTE reference IS — a tagged scan the physical planner resolves from
its own cache — and left it saying nothing about its columns: no catalog answers
to its name, so the annotator left `ScanColumns` empty. A join keyed on such a
column could not tell which side owned which key
(`physical.assignJoinKeySides`), the executor resolved both to −1, and a key
that resolves to nothing hashes as a constant, so every probe row matched every
build row:

```
WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v < 3)
SELECT u.id, r.v FROM lat_ord u JOIN r ON r.v = u.id     PG 3 rows → 9
```

The IN spelling over the same CTE was right, and writing the CTE FIRST in the
FROM was right too — the reorderer then put it on the probe, where a bare name
resolves by accident. Both are what localise the loss to the KEY and not to the
CTE's rows (#1066). The list is the block's own published namespace, ADR-0026
§9 applied to a recursive item: a set operation publishes its LEFT arm's names,
and an explicit column list renames them positionally.

**AND THE PUBLISHED LIST IS ARITY-CHECKED, AND READ BY THE STAR.** Two consumers
of that list were found by the round-2 review and are part of this section, not
a follow-up:

- `physical.registerCTE` splits on `cte.Recursive` and the recursive branch
  returned straight out of `validateBlock`, so a COLUMN-ALIAS LIST over a
  recursive item was never counted. `OverlayColumnAliases` renames the leading
  columns positionally and says nothing about a list that is too long, so
  `WITH RECURSIVE r(w,x,y) AS (…two-column body…)` published a third column
  nothing produces and the statement answered ONE ROW OF NULL where the CTE has
  three rows — PostgreSQL 17.11 raises `42P10`, and the plain-CTE and
  derived-table spellings already raised that exact sentence. **A published list
  is counted against its alias list wherever it is built**, which is one check
  in one place per builder and not a rule per spelling.
- `logical.relationOutputColumns` treats a node that NAMES itself as a block and
  asks `blockOutputProjection` for its list. A recursive CTE reference is a named
  block with NO projection under it, so `SELECT r.*` was refused — *"a `r.*`
  expands only from a relation whose column list is known"* — where the BARE star
  over the same relation answered. The headline is the answer: the reference
  publishes a list, and the qualified star is one more consumer. The list still
  comes from `publishedScanColumns`, which asks the security barrier first, so
  the one-path rule (`TestOnlyOnePathReadsAScanColumnListForAStar`) is unweakened.
  That arm is the NAMED-BLOCK arm, so the same repair reaches every block whose
  body is itself a `SELECT *` — `WITH c AS (SELECT * FROM t) SELECT c.*`, its
  joined spelling and the derived-table twin were refused before it and answer
  PostgreSQL's rows after it, gated as cells of the R1 table and, over a POLICED
  relation on all nine doors, by
  `server.TestArcR1AStarOverAStarBodiedBlockNeverPublishesAPolicedValue`.

**A CORRELATED FROM ITEM IS NOT A SCOPE QUESTION.** The body's own `WITH` is in
scope for the body — that is what this section settles — but an outer reference
written INSIDE a WITH item's own body is something else: a relation in the FROM
clause whose body reads the ENCLOSING row, which is `LATERAL` semantics, and
which this engine plans as its own query block. `EXISTS (WITH n AS (SELECT z.id
FROM t z WHERE z.id = o.id) SELECT 1 FROM n)` is refused on all five arms, and so
is the derived-table spelling of the same shape, which is the measurement that
says which boundary it is. The two spellings do not say the SAME thing:
`physical.refuseOuterLevelReference` names the mechanism and its two workarounds
for the derived table, while the WITH item falls out as the generic `42P01` —
the CTE body is validated with no enclosing diagnostic to classify the reference
against. Recorded, with `docs/sql-reference.md` narrowed to the measurement.

**NOT SETTLED, with the mechanism.** A correlated subquery whose body IS a set
operation is still refused (`0A000`), and so is one holding a set operation one
level down. Closing it takes two hunks, not one: a set-operation arm in
`RebuildSQLForRerun`, AND the arms in `collectOuterCandidatesBlock`, which is
where the outer columns a re-run will read are added to the ENCLOSING query's
projection. That second one is deliberately omitted today, because widening the
projection over a POLICED relation is a plan the ABAC order invariant no longer
trips on (`server.TestPolicyMaskingIsPlanTimeOnEveryDoor`) — so the two must
land together, with that gate as the arbiter. Until then the shape is loud.

`coordinator.TestArcR1ACorrelatedBodyAnswersPostgresRowSetOnEveryArm` is the
gate: 473 cells of {operator} × {where the outer column sits} × {what the body
holds} on five arms, plus the alias-list arity matrix, the star over a recursive
CTE and over a star-bodied block, and the correlated FROM item, every want live
PostgreSQL 17.11, with those
boundaries and the DAG's recursive-CTE gap (#960) pinned by the sentence each
refusal says.

### 1q. A LATERAL's BOUND travels with the correlation key, its ALIAS is what the join qualifies it by, and everything else it reads is loud

(Added 2026-09-14, #1019, #1111, arc L1.)

§1h settled what a LATERAL means for an EMPTY input. This is the next layer,
and it is three facts about one lowering — `buildLateralSubquery` promotes the
body's correlated WHERE equalities into the join condition, and everything
that layer does NOT carry has to be either carried or refused.

**THE BOUND IS STILL NOT PER OUTER ROW, AND THE REPAIR WAS WRITTEN AND TAKEN
OUT.** PostgreSQL evaluates the body once per outer row, so `ORDER BY … LIMIT
n` bounds each evaluation; the decorrelation makes the body ONE relation joined
once and the bound applied to the whole of it, so the top-N-per-group idiom
answers ONE row for PostgreSQL's two, silently, on every arm (#1019).

*(2026-09-23, arc LT: superseded by §1s — the bound IS per outer row now, on
every arm, through the rewrite below with its three faults closed at their
seams; the paragraphs that follow are the record of why it was withdrawn.)*

The repair §1h named — the bound travelling with the correlation key as a
per-key top-N, `ROW_NUMBER() OVER (PARTITION BY <the inner column the
correlation keys on> ORDER BY <the body's own ORDER BY>)` and a `QUALIFY` over
it — was built, and an adversarial review measured three faults in it that are
all one fault:

- **The minted window's partition does not bind the body's own column on the
  DAG.** Over a SELF-correlated body (`FROM e7emp b JOIN LATERAL (… FROM e7emp
  c WHERE c.dept = b.dept …)`) the three DAG arms put every row in its own
  partition, so the bound kept every row and the join paired each outer row
  with itself — measured over an UNPOLICED correlation column, which is what
  says it is a name-binding fault and not a policy one. Over a POLICED column
  it is worse than wrong: `e7bal`'s masked `bal` has singleton equivalence
  classes under its STORED values, so the answer an analyst reads is arithmetic
  on the column the policy hides, on four of the nine doors (embedded/dag,
  embedded/dag-shuffled, pgwire/dag, http/dag), where all nine gave the mask's
  answer before. That is the class #859 round 2 named, reached through a window
  the PLANNER mints rather than one the user wrote.
- **The decline list was false.** `lateralBoundPerOuterRow` returned false the
  moment one correlated part named no inner column, so a body with BOTH
  `i.order_id = o.id` and a non-equality outer comparison kept the whole-
  relation bound — silently, on all five arms, for a shape none of the five
  declines names.
- **The minted `__win_N` was allocated per BLOCK, not per STATEMENT.** Two
  bounded laterals, or a bounded lateral under a user's own `QUALIFY`, both
  took `__win_0`: the single-process arms failed with a reserved slot name and
  the DAG arms answered the user's clause against the LATERAL's row number.

The first is the deciding one, and it is not this section's to close: it is the
window-key seam ADR-0026 §8j records — which arm owns a window key — answered
today by three mechanisms that disagree. A rewrite that MINTS a window inherits
every one of them, and a planner-minted window over a policed relation turns a
wrong answer into a disclosure. So #1019 stays open, the shape keeps the
disposition #1079 gave it (the row count PINNED, PostgreSQL's answer recorded
beside it, and only the QUALIFIED star declining), and the repair waits on
§8j. `LIMIT ALL`, which an earlier version of this section listed as a shape
the rewrite declines, is not parseable at all (`expected number after LIMIT`).

**ONE CONSEQUENCE OF THAT WORK IS KEPT**, because it is independent of the
rewrite and closes a refusal on its own: `EnsureDistribution` runs to a FIXED
POINT. It reads each child's distribution as it stands and the re-resolve came
after the whole pass, so a window partitioned on a SUBSET of the group keys
below it read its aggregate as Singleton while that aggregate's own exchange
was being spliced in, and `AssertExchangeConsistency` then REFUSED the plan on
all three DAG arms — for `SELECT …, ROW_NUMBER() OVER (PARTITION BY k) … GROUP
BY g, k`, with no lateral in the query at all.

**THE ALIAS IS WHAT THE JOIN QUALIFIES THE ARM BY.** A derived table stamps
its alias on its subtree root and `joinArmAlias` reads it (§1j's neighbour,
#751/#773); the LATERAL path never did, so the lateral was an arm with no
name. `joinOutputSchemaWithMapping` DROPS a colliding build column it cannot
qualify, so a body publishing a name the ENCLOSING relation also carries lost
its own column and the reference bound the outer row's:

```
SELECT t.dx FROM setopdecja a,
  LATERAL (WITH c AS (SELECT dx FROM setopdecjb) SELECT SUM(dx) AS dx FROM c) t
-- PostgreSQL 17.11  51.0000 x4      this engine  12.75 x4, the outer row's dx
```

Only the subtree ROOT is stamped: `setSubtreeAlias` would make the body's own
relations answer to the lateral's name, and the body resolves its own
references against the names it wrote (#1111).

**AND EVERYTHING ELSE THE BODY READS IS LOUD.** The decorrelation carries the
outer row into the body's WHERE equalities and no further, so an outer
reference in the body's SELECT list, `GROUP BY`, `HAVING`, `ORDER BY` or
`QUALIFY` resolved against the INNER relation — its column of that name, or
nothing. Measured, before → after, against live PostgreSQL 17.11, PER ARM
(single / spilled512k first, the three DAG arms second):

| the body's clause | PostgreSQL | before, single/spilled | before, DAG ×3 | after |
|---|---|---|---|---|
| `SELECT o.total + i.amount AS m` | 200, 250, 275, 325 | NULL ×4 | NULL ×4 | 0A000 |
| `SELECT o.id AS m` | 1, 1, 2, 2 | 1, 2, 3, 4 (`i.id`) | **RIGHT** | 0A000 |
| `GROUP BY o.id` | 150, 200 | 100, 50, 125, 75 | same | 0A000 |
| `HAVING SUM(i.amount) > o.total` | no rows | 3 rows of NULL | same | 0A000 |
| `SELECT CASE WHEN o.id > 1 …` | 0, 0, 1, 1 | 0, 1, 1, 1 | same | 0A000 |
| `ORDER BY i.amount * o.total LIMIT 1` | 50, 75 | wrong | wrong | 0A000 |

**Seven (cell, arm) results move RIGHT → LOUD** — the `SELECT o.id AS m` row's
three DAG arms, `LIFTED/leftArm`'s three, and `OUTERREF/whereInequality`'s
`dag-shuffled` — measured at `c34cdbcb`. It is taken deliberately: a refusal is
a property of the PLAN and has no per-arm spelling, and an answer that is right
on three arms and wrong on two is a two-path split this engine does not ship
either. The first version of this table stated the single-process behaviour as
if it were every arm's, which it is not; that is corrected here.

A FROM-less body is untouched — §1n lowers it as a projection over the outer
row — and so is its `ORDER BY`, which over a one-row body is the IDENTITY. And
a reference whose qualifier the BODY'S OWN `FROM` item or `WITH` item shadows
is not an outer reference at all: SQL scoping resolves it to the inner item, so
`buildLateralSubquery` subtracts those names before the WHERE split. Without
that subtraction `FROM lat_ord x, LATERAL (SELECT SUM(x.amount) … FROM lat_item
x …)` was refused for reading an outer row it never touches.

A correlated predicate that is NOT an equality is the same layer from the
other side, and it ANSWERS. It is lifted out of the body and evaluated at or
above the JOIN, over the body's OUTPUT rows, so every inner column it names has
to be there. Where the body publishes the column under its own name it always
was; where the body RENAMES it, or does not select it at all, the single-process
projection had dropped it and the predicate named nothing — `rows=0`, or a
LEFT-padded `NULL` per outer row, silently.

**The DAG answered these by MECHANISM, and that is what the repair follows.**
An earlier version of this section refused the shape uniformly and said closing
it needed "#1028's DAG-identity layer plus a lifted predicate evaluated AT the
join". The round-2 review refuted the first half by measurement: over 40 outer
rows and 5 000 inner ones, with the correlation column published under NO name
at all, the three DAG arms land on PostgreSQL's 177 500 rows — because the
DAG's stage plan reads the column off a stream that carries the SCAN's own
names and never consults the body's published list for this predicate.

So the body publishes the columns the predicate names, UNDER THEIR OWN NAMES,
and the predicate is not respelled. That last clause is the measured part: a
first attempt materialized them into hidden `__key_N` slots and respelled the
predicate to those, which made the two single-process arms right and took the
three DAG arms from PostgreSQL's nine rows to three NULL-padded ones — the
minted name is one the DAG's evaluation point does not carry. What the
single-process path lacked was not a NAME but the COLUMN.

**A MATERIALIZED COLUMN IS A PUBLISHED COLUMN**, and that is the rule the
repair is bounded by. The qualified star reads the body's own list, so
`Node.StarLiftedRefCols` keeps it out of `s.*`; everything else that reads a
PUBLISHED list — a bare `SELECT *`, which publishes the join's stream on all
nine doors and in `RowDescription`; a `DISTINCT` or `GROUP BY` key, computed
over the projection the injection widened; a reference to a name the body's own
alias or the ENCLOSING relation already carries — sees a column the query did
not write. ADR-0026 §3c's answer is a hidden slot dropped by position, and that
is not available here: the single-process path evaluates the predicate ABOVE
the join whenever an equality beside it keys the join, and a dropped slot
answers zero rows there (measured), while a MINTED NAME is one the DAG's
evaluation point does not carry (measured, round 3).

So the column is materialized ONLY where publishing it changes nothing else,
and three shapes decline — each returning to the disposition it had at
`c34cdbcb`, never to a new wrong answer: a body carrying `DISTINCT`; a body
whose own output alias already publishes the name, or an enclosing relation
that does; and an enclosing query that writes a star over this join. A GROUPED
body needs no decline — it aggregates, so the refusal above fires first.
Round 4's seven cells hold them, with the base measurement beside each.

An AGGREGATED body is still refused, for a reason the projection cannot answer:
publishing `i.amount` beside `SUM(i.amount)` needs it in the `GROUP BY`, which
changes what the aggregate computes. PostgreSQL evaluates the body per outer
row; this engine does not, for that shape.

**THE REFUSAL COMES BEFORE EVERY DECLINE, AND THAT ORDER IS ASSERTED RATHER
THAN COMMENTED.** The refusal and the declines look alike — neither
materializes anything — but they are not interchangeable: a decline on an
aggregated body DROPS the predicate, so every outer row is given the whole
relation's aggregate, or a NULL. Round 4 put the refusal in front of the
`DISTINCT` / contested arm and left the enclosing-star test as an early return
above the loop, and one statement then had two dispositions decided by the
ENCLOSING SELECT list: written with a named list it was `0A000`, written
`SELECT *` it answered `NULL | NULL | NULL` for PostgreSQL's `350 | 350 |
NULL` — silently, on all five arms and all nine doors, through a derived star,
a top-level star, a qualified star and a CTE star alike. Every decline is
therefore a FLAG folded into one arm below the refusal, and two properties hold
it there: `logical.TestNoLiftedRefDeclineSitsBeforeTheAggregatedRefusal` reads
the order off the function's own source, so a decline added in the wrong place
fails even when no cell reaches it, and
`logical.TestTheAggregatedRefusalPrecedesEveryLiftedRefDecline` crosses every
trigger with an aggregated body and with a non-aggregated control. The `R5/*`
cells gate the values on five arms.

**The OUTER-REFERENCE refusal above is the opposite verdict on the opposite
evidence, and the pair is the point.** `SELECT o.id AS m` was right on the three
DAG arms too — and it is an ACCIDENT: the qualified `o.id` degrades to the bare
`id` and the join happens to emit the OUTER arm's `id` bare. `o.id + 0` is the
inner relation's, `o.total` is NULL (or loud on the DAG, where the column does
not exist at that point at all), and a second outer column in the same list is
NULL. Three cells record that (`R3/outerRef*`), so the uniform refusal there is
not the same decision made twice.

**NOT SETTLED, with the mechanism.** A LEFT JOIN LATERAL over an UNCORRELATED
body has no join keys and is refused by three different layers in three
different sentences (`could not extract join keys from`, the DAG's SELECT-list
check, and the join operator's own `LeftKeys and RightKeys required`);
PostgreSQL answers the cross product with no padding, and the INNER and comma
spellings of the same body answer here too. `ORDER BY <ordinal>` over a
`SELECT *` whose FROM is a join — which a lateral always is once lowered —
has no position to count, and reproduces with an ordinary join in place of the
lateral. Both are cells of
`coordinator.TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm`, which
is this section's gate: 196 cells of {LATERAL, scalar subquery, EXISTS, IN} ×
{where the outer reference sits} × {INNER, LEFT, comma, a join below, a star
over} on five arms, every want live PostgreSQL 17.11.

### 1r. A decorrelated subquery's outer references are a SET over the WHOLE body

(Added 2026-09-20, #1232, #1104, arc DC.)

Every section above is about what a correlated reference MEANS once the
rewrite has found it. This one is about FINDING it. All three decorrelations
— IN/NOT IN, EXISTS/NOT EXISTS and the scalar comparison — classified
`info.WhereExpr` and nothing else, and a body is larger than its WHERE. A
JOIN's `ON`, the HAVING, the GROUP BY, the SELECT list, the ORDER BY and the
QUALIFY can each name the enclosing row, and PostgreSQL evaluates the whole
body once per outer row, so a reference in any of them decides the answer.

**THE RULE.** The outer references are collected over the whole body, and
each one is either CARRIED into the join the rewrite builds — as a key when
it is an equality with an inner column, as a residual over the (outer, inner)
row otherwise, as a filter on the OUTER side when it names outer columns
alone — or the rewrite DECLINES and the subquery runs per outer row, which the
rerun executes (see below for the one family that needed a fix to do so).
Never dropped, and never stripped.

**A JOIN'S ON WAS INVISIBLE.** The rewrite built the body's FROM from
`info.Joins` verbatim, so an outer reference written there went into a join
condition whose scope does not contain the enclosing relation:

```sql
SELECT o.id FROM lat_ord o
WHERE o.id IN (SELECT b.order_id FROM lat_item b
               JOIN lat_item c ON c.id = b.id AND o.total > b.amount)
-- PostgreSQL 17.11: 1 ; 2        was: 0 rows
```

silently, on all five arms, with three discriminators right beside it — the
same correlation in a plain join, the same correlation moved to the body's
WHERE, and the EXISTS spelling (#1232, localised to the PLAN and not the
binder by arc RS's review). An INNER join's ON conjunct IS a WHERE conjunct,
so a conjunct of one that names the enclosing query is LIFTED into the
classification the rewrite already runs, and becomes a key, a residual or an
outer-side filter exactly as the same text written in the WHERE does. An
OUTER join's ON is not a WHERE conjunct — its conjunct decides which rows
PAIR while the preserved side keeps its row NULL-extended either way — so it
declines, and so does a reference in the HAVING, the GROUP BY, the SELECT
list, the ORDER BY or the QUALIFY, none of which this rewrite has a place for.

**A CONDITION THAT NAMES ONLY THE OUTER ROW IS NOT AN INNER FILTER.** The IN
rewrite added it to the build side's filter with its QUALIFIER STRIPPED — so
`o.total > 100` became `total > 100` read against the subquery's own relation,
which answers the subquery's rows where that relation happens to have the
column and fails loudly where it does not — and the EXISTS rewrite dropped it
outright under a comment saying the shape "shouldn't happen" (#1104). The
scalar rewrite's name-only test could not tell `o.id = o.grp` from a
correlation and built a key on an inner column the body's relation does not
have.

PostgreSQL applies such a condition per outer row, so it gates WHICH outer
rows can match at all. In a WHERE, where only TRUE passes, that is a
conjunction:

```
WHERE o.id IN (SELECT z.id FROM t z WHERE P(o))
    ≡ WHERE P(o) AND o.id IN (SELECT z.id FROM t z)
WHERE EXISTS (SELECT 1 FROM t z WHERE Q(z) AND P(o))
    ≡ WHERE P(o) AND EXISTS (SELECT 1 FROM t z WHERE Q(z))
```

— when `P(o)` is false the body is empty, IN over an empty set is FALSE and
EXISTS is FALSE, which is what the conjunction says, NULL included. The
NEGATED spellings are NOT that conjunction and there is nothing to hoist them
into: an outer row for which `P` is false passes `NOT IN` and `NOT EXISTS`,
because the body it would have to contradict is empty. So `NOT IN`, `<> ALL`
and `NOT EXISTS` decline, and the condition stays where the query wrote it.

**AN UNQUALIFIED NAME IS DECIDED BY THE BODY'S OWN NAMESPACE.** PostgreSQL
binds a name innermost-first: an unqualified name the body's relations do not
publish is the enclosing row's. The enclosing column map alone cannot decide
that — TPC-H Q02's official spelling writes `p_partkey = ps_partkey` inside the
body with BOTH names in the map, because the enclosing query reads those
relations too — so the rewrite asks the catalog what the body's own FROM
publishes (`bodyOuterColumns`, through the annotator, on a throwaway Scan per
relation and never on the plan being built: annotating the real inner subtree
hands the join reorderer statistics it did not have, which is what once moved
Q02's join order). A name the body cannot supply is read as outer in every
clause — lifted from an inner join's `ON`, hoisted from the `WHERE`, and
blocking in the `HAVING`, the `GROUP BY`, the SELECT list, a bounded `ORDER BY`
and the `QUALIFY`. A name the body does publish stays the body's, whatever the
enclosing query also has: with the namespace known, the classifier reads the
enclosing column map RESTRICTED to the names the body cannot supply, so
`EXISTS (SELECT 1 FROM dc_side c WHERE id = c.j)` is `c.id = c.j` and not
`o.id = c.j` (round 3; round 2 applied the rule in the outward direction only
and answered 3 rows for PostgreSQL's 5). When the namespace cannot be named completely — a
table function in the body's FROM, whose columns are its call's or its input's
and are not re-read to answer this — an unqualified enclosing name in a clause
the rewrite cannot classify declines to the per-row rerun; in the `WHERE` it
keeps the build-side disposition and fails loudly by name.

Before this, the unqualified spelling was invisible outside the `WHERE`: in a
body `JOIN`'s `ON` it became a join key no relation publishes (zero rows), in
a `HAVING` the aggregate rewrite dropped it (every row), in the SELECT list it
became a key the build side lacks (zero rows) — silent, on every arm (arc DC
round 1, B1).

**TWO KEYS, ONE BUILD.** A correlated IN whose IN key and correlation key are
two integer pairs (`semi ON j = id AND j = id`) is a two-integer-key join, and
when the reorderer builds the enclosing side (RIGHT SEMI) the probe arm that
marks matched build rows had no two-integer case and marked nothing — the
single-process arms answered EMPTY for a self-join body. Round 2's hoist took
shapes that used to be refused onto that path; the executor arm is fixed
(`markKeyMatchedLocked` now IS `markKeyMatched`) and the review's cells are
gated on five arms and nine doors.

**A DECLINE IS RIGHT BECAUSE THE RERUN CAN EXECUTE IT.** The per-row rerun
substitutes the outer values, so `o.total > 100` in a body `RIGHT JOIN`'s `ON`
becomes `100 > 100`. When the body's `WHERE` rejects that join's padding,
`pushFilterThroughJoin` demotes it to inner (#335) after
`liftInnerJoinOnResiduals` (#336) has run, and `routeOuterJoinOnResiduals`
(#358) skips a join that is no longer outer — so a non-key conjunct sat in an
inner join's `ON` with nothing to place it, and the physical planner refused
the statement. The demotion now lifts it itself, as the inner join it has
become; the same fix answers the plain `RIGHT JOIN … ON c.j = b.k AND 100 >
100 WHERE b.tag = 10`, which refused before any subquery was involved.

**THE BOUNDARY, AS MEASURED (round 4).** Three closures and the cells that
are still open, rather than a claim over all of them:

- The rerun now scopes the body's OWN `WITH` items over its FROM
  (`blockScopeResolver`): `o.id IN (WITH d AS (SELECT k, amt AS total FROM
  dc_in) SELECT b.k FROM d b JOIN dc_side c ON c.j = b.k AND total > 100 WHERE
  b.k = o.id)` answers PostgreSQL's `1 | 2`, where it used to substitute the
  enclosing value.
- A computed output column of an ENCLOSING derived table (`total * 1 AS t2`)
  is an outer name in both enclosing column maps (the logical decorrelations'
  and the physical rerun's), as a CTE's already was.
- A correlated non-equality whose two sides render with the same bare name
  is guarded WHERE THE COLLAPSE HAPPENS (round 5): the stage DAG re-spells a
  residual leaf by asking which arm moves the name, so a renaming arm on
  either side — through any number of pass-through layers — could move both
  leaves onto one stage column (`total < total` became `x.amt < x.amt`). Round
  4 predicted the renaming shapes in the planner and missed a spelling twice;
  `dagplan.residualWithStageSpellings` now refuses any residual whose two leaves
  land on one stage column (`ErrResidualSidesMergedDistributed`) and the
  coordinator runs the plan single-process. The planner keeps one decline, for
  the column-alias list over a catalog table, whose decorrelated build cannot
  be planned single-process at all.
- A correlated subquery whose own WITH item shadows an enclosing one is
  refused, 0A000, by name: the rerun plans the body with the enclosing item
  first and the CTE cache is keyed by name (docs/internals/nested-with-scope-precedence.md),
  so it read the enclosing relation — zero rows where PostgreSQL answers.
- Still open, filed: a name supplied by a table function's alias is read as
  outer; a `HAVING` or `LIMIT` inside an `EXISTS` body is ignored; a `LATERAL`
  item in the body answers no rows; a column-alias list over a catalog table
  and a `USING`-merged column refuse; the case concession of ADR-0012 §5
  applies to mixed-case names.

**WHAT MOVES AND WHAT DOES NOT.** The optimized logical plans of all 22
TPC-H queries, under the catalog annotator, are byte-identical to the ones at
`16b924d1` — the six that decorrelate (Q02, Q04, Q17, Q20, Q21, Q22) included —
which is the measurement that says the namespace rule reads Q02's unqualified
keys as the body's.

`coordinator.TestArcDCADecorrelatedBodyKeepsEveryOuterReferenceOnEveryArm` is
the gate: 776 cells of {IN, NOT IN, = ANY, <> ALL, EXISTS, NOT EXISTS, a
scalar subquery in WHERE, a scalar subquery in the SELECT list} × {the
forty-nine places an outer reference can sit} × {an enclosing query that is
one relation and one that is a join whose arms share a column name} on five
arms, every want live PostgreSQL 17.11, with two boundaries pinned by the
sentence each refusal says. 221 of its cells fail at `6b9c7acf`, and 221 at
`0c0d33b6`. `coordinator.TestArcDCAnUnqualifiedOuterReferenceBindsWhereTheBodyCannotSupplyIt`
(121 statements, every position spelled unqualified) and
`coordinator.TestArcDCAnOuterJoinsOnBesideACorrelatedWhere` (27, an outer
join's `ON` beside a `WHERE` that rejects its padding, with and without a
subquery) are the round-2 halves.
`server.TestArcDCARelocatedBodyConditionReadsTheMaskOnEveryDoor` is the other
half: this rewrite moves a predicate ACROSS a relation boundary, and over a
policed relation the answer is the ROW SET rather than a cell, so the mask's
reading is asserted on all nine doors.

### 1s. A correlated body is decorrelated only where it is KEY-PARTITIONABLE, and evaluated per outer row otherwise

(Added 2026-09-23, #1019, #1238, #1274, #1131, #1130, arc LT. Supersedes
§1q's "THE BOUND IS STILL NOT PER OUTER ROW" and §2's LATERAL-bound mark.)

Every decorrelation in this record replaces "the body evaluated once per outer
row, with that row's values bound" by ONE evaluation of a body over the whole
inner relation, joined to the outer rows. The two agree exactly when the
body's result restricted to one outer key equals the body evaluated for that
key:

```
σ_{K = v}( Body′(R) )  ==  Body(R, outer = v)        for every outer value v
```

The implemented sufficient condition (amended 2026-09-24) is (1) every
correlated predicate is an EQUALITY between an inner-only expression and a
bare outer column — the correlation restricts the inner keys K — and (2) every operator between that
restriction and the body's output commutes with `σ_{K=v}`: filters and
projections always, a PIPELINE BREAKER only when it is partitioned by K (an
aggregate grouped on K, a DISTINCT keyed on K, a window partitioned on K, a
bound that is a PER-K bound). **That is the rule — key-partitionability of the
body's plan — and it is a property of the plan, not a list of spellings.**
§1h's GROUP BY injection, the DISTINCT body's key and `refuseDecorrelatedWindow` (now `lateralWindowsPerOuterRow`)
were already instances of making a breaker key-partitionable; the bound was
the breaker nobody had made so, and the shapes with no K had no disposition
but a plausible row set.

**THE BOUND TRAVELS WITH THE KEY.** A correlated LATERAL whose correlated
predicates are all equalities on inner columns carries its `ORDER BY … LIMIT n
OFFSET m` as `ROW_NUMBER() OVER (PARTITION BY K ORDER BY <the body's own ORDER
BY>)` and a QUALIFY `rn > m AND rn <= m+n` in place of the bound
(`lateralBoundPerOuterRow`). Over an aggregated body the window sits above the
aggregate and partitions on the name the aggregate publishes the key under; an
ordinal in the body's ORDER BY counts the list the query wrote, past the
injected slot. This is the rewrite §1q withdrew, and its three measured faults
are each closed at their own seam: the DAG partition binds since ADR-0026
§8j's corollary 1 (measured on the three DAG arms over two-relation AND
self-correlated bodies, every equality-keyed cell agreeing); the decline list
is the rule itself; the `__win_N` collision was a base defect of user-written
QUALIFY in two blocks under one join, closed in the physical slot rename.
The disclosure arc L1 measured on four doors is a different, PRE-EXISTING
fault of the distributed path, not of the minted window: a WINDOW over a
policed scan whose output feeds a JOIN answers the stored column's pairing
on every DAG door — for a user-written window inside a lateral body, a user
`QUALIFY` derived table joined on the masked column, and an uncorrelated
windowed body alike, with the security projection present on both scans of
the stage plan. Until it is localised (filed `distributed`, priority high)
`dagplan.CheckPolicedWindowUnderJoin` refuses every such stage plan and the
coordinator runs it on the single-process pipeline, which answers the mask
on all nine doors (`server.TestArcLTAPerOuterRowBodyReadsThePublishedValueOnEveryDoor`).
Cost: top-3-per-group over 100 000 outer rows × 10 inner each is 168 ms single
and 1.08 s at a 512 KiB budget.

**A BODY THAT IS NOT KEY-PARTITIONABLE IS EVALUATED PER OUTER ROW WHERE A
RUNNER EXISTS, AND REFUSED WHERE NONE DOES.** EXISTS / NOT EXISTS decline the
semi-join rewrite for a body it does not reproduce — a bound, an OFFSET, a
GROUP BY, a HAVING, an ungrouped aggregate (one row even over an empty input,
so `EXISTS (SELECT MAX(v) …)` is true for every outer row), a QUALIFY, a set
operation — after stripping what existence is invariant under (`LIMIT n`, n ≥
1, with no OFFSET); the compiled-predicate rerun answers, at 10.9 ms per outer
row over a 1 000 000-row inner relation (linear; recorded). `EXISTS (… LIMIT
0)` answered rows and `EXISTS (… GROUP BY … HAVING …)` ignored its HAVING on
all five arms before (#1238, #1274). IN / NOT IN and the scalar rewrite
already declined a bound; a QUALIFY or a set operation in their body is
declined by the shared build side since the round-2 review. The rerun's own
boundaries stand: a correlated body holding a window function, or a set
operation, is refused by the rerun's rebuild (§1m) on every consumer. A LATERAL has no per-row runner: a bound with a
correlated predicate that is not an equality on an inner column (`i.v >
o.total`, or `i.k = o.k AND i.v > o.total`), a DISTINCT or set-operation body
under a bound, a bound this planner cannot read as an integer, a DISTINCT body
whose lifted predicate names a column it does not publish (#1131), a lifted
column the body's own alias list or the ENCLOSING relation also publishes
(#1130 — decided on the annotated plan, `RefuseContestedLiftedRefs`, so a base
outer relation is seen), and a lifted predicate under an enclosing star are
REFUSED, 0A000, one sentence each, on every arm. Each answered a plausible
wrong row set at `51addfb6` (54 cells of the seam table), and one of §1q's
declines agreed with PostgreSQL by coincidence of the L1 fixture
(`aliasCollides`: `i.id < 150` and `i.amount < 150` select the same rows).

**AN UNGROUPED AGGREGATE WITH A HAVING** is §1h's pad one clause further: the
HAVING is folded over the default row (COUNT 0, NULL otherwise). FALSE or NULL
removes the pad, which is right on the INNER and the LEFT spelling; TRUE is
refused, because an outer row with no group (PostgreSQL's default row) and one
whose group failed the HAVING are the same unmatched key after the join.

**WHAT MOVED, MEASURED.** The seam table — `{IN, NOT IN, EXISTS, NOT EXISTS,
scalar, JOIN / LEFT JOIN / comma LATERAL} × {17 body shapes} × {equality,
inequality, mixed, shared name, none}`, 680 cells against live PostgreSQL
17.11 — had 87 wrong and 61 refused cells at base; at the tip 0 wrong and 183
refused, on five arms (`coordinator.TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm`).
On arc L1's table 28 cells went wrong → right, 9 wrong → loud, 2 loud → right
(the qualified star over a bounded body), and one right → loud
(`R4/aliasCollides`, the coincidence above). N1's, O2's, L1's and the QUALIFY
table's #1019 pins are deleted. Twelve (cell, arm) results moved right → loud
on the three DAG arms, for four lifted-predicate cells the single arms
answered wrong: a refusal is a property of the plan (§1q's rule). One cell
moved loud → wrong on the three DAG arms: `R2/collideWinBound`'s OUTER window
over the bounded lateral now takes the disposition its unbounded twin has
always had — the LATERAL-producer residue of ADR-0026 §8j, `distributed`,
pinned per arm, and the reason it refused before was incidental to the
whole-relation bound's stage shape. All 22 TPC-H plans are byte-identical to
`51addfb6` modulo Q19's brand-list order.

**WHAT THE ROUND-2 REVIEW MEASURED, AND WHAT MOVED (2026-09-24).** The rule
as first implemented was both too loose and too narrow, and each finding is
closed at its seam: the key is read off the PARSED predicate — `<inner
expression over the body's relations> = <bare outer column>` — so a side
mixing inner and outer references (`i.k = o.k + i.id - 3`) or an outer
EXPRESSION (`o.k + 0`, a join key this engine does not bind: zero rows with
or without a bound, filed) refuses instead of partitioning on a guess, while
`i.k + 0` and `i.k % 2` partition; a bound over a body carrying its OWN
QUALIFY refuses (the rank would number the rows before that clause removed
any); `LIMIT 0` is left to the body (empty per row and per relation alike);
`LIMIT n OFFSET m` saturates rather than wraps; a DISTINCT over exactly the
key drops a `LIMIT n` (one row per key at most); an ungrouped aggregate whose
one row the bound removes (`LIMIT 0`, any OFFSET) gets no §1h pad, so the
INNER spelling answers nothing; an ORDER BY naming a SELECT alias reads the
alias's expression; and the IN / scalar build side declines a QUALIFY or a
set operation exactly as EXISTS does (`decorrelatedInnerPlan`), so a dropped
`QUALIFY` no longer answers rows — the rerun refuses a window loudly. Where
the three DAG arms were RIGHT and the two single arms wrong, the refusal is
the single-process pipeline's alone: a lifted column the enclosing relation
contests is refused by `physical.Plan` and, on the DAG, only for an OUTER
join (whose residual padded every row there); a lifted predicate under a bare
enclosing star DECLINES again and `RefuseDeclinedLiftedRefs` refuses it on
the single path only. A window ABOVE a lateral join is refused on the DAG
(`dagplan.refuseWindowOverDependentJoin`) and routed single-process, which
answers PostgreSQL's rows — the loud → wrong move of `R2/collideWinBound` is
withdrawn, and its unbounded twin is right for the first time.

**AN OUTER EXPRESSION IS A KEY (2026-09-24, arc JP, #1302).** The rule above
says "an expression over the outer row alone", and the code said "a bare outer
column": `i.order_id = o.id - 0` answered ZERO rows unbounded and was refused
bounded. The unbounded zero was a placement fault, not a key question: the
lowering minted the key slot `__key_0` and DROPPED it at the join
(`Node.HiddenJoinCols`), while the equality — no hash key, because its outer
side is not a column — was lifted into a filter ABOVE the join, where the slot
read NULL. Now one reader decides both paths (`lateralCorrelatedEquality`: an
inner-only side and an outer-only side, either way round, parentheses
looked through): where the outer side is a bare column the join keys on the
pair and drops the slot, as before; where it is an expression the slot is
EMITTED and hidden from a qualified star only (`Node.StarLiftedRefCols`, the
disposition a lifted predicate's slots already have) and the equality is
evaluated over the join's output — exactly what an ordinary `JOIN … ON
i.k = o.k - 0` does, a cross product filtered, so one predicate shape has one
answer in both spellings. The bound partitions by the INNER side, which is
right for any outer-only expression: the inner rows one outer row may match
are those whose key equals ONE value. A BARE enclosing star is refused, 0A000
(ADR-0012 §5): a star over a LATERAL is not expanded and would publish the
emitted slot. Projecting the outer expression into a slot of its own so the
pair becomes a hash key was considered and not taken: it needs a pass-through
projection over an arbitrary outer subtree on both paths, for a shape the
plain join answers the same way today — a performance question, recorded.
Gate: `coordinator.TestArcJPALateralOuterExpressionKeyAnswersOnEveryArm`, 231
cells — {`o.k`, `o.k - 0`, `o.k + 1`, CAST, `abs`, `coalesce`, two outer
columns} × {inner side left, outer side left, an inner expression} × {JOIN /
LEFT / comma LATERAL, ORDER BY LIMIT 1 and 2, COUNT(*), SUM, a bare star, a
qualified star, EXISTS, EXISTS LIMIT 1, NOT EXISTS, IN, scalar} on five arms:
54 wrong and 42 refused on the single arm at base, 0 wrong and the 12 bare
stars refused at the tip.

**Round 2 (2026-09-24): the key is an identity, never a name.** Every body in
that gate aliased its columns; with the body publishing a name the outer
relation also has (`SELECT DISTINCT i.k … = o.k - 0`, `SELECT i.id … LIMIT 2`)
the key travelled under the body's name and the lifted equality or the
enclosing `s.id` read the OUTER column — 12 rows for 3 on every arm, and every
colliding name on the stage DAG. A lifted key is now always minted into its
own slot, a DISTINCT body keeps the lateral's name, and the DAG resolves the
lateral's unaliased items and aggregate-published slot to what its stream
carries (ADR-0026 §8l). A grouped body keyed on an outer expression, which
answered zero or every row on the DAG at base and at round 1, answers too.
Gate: `coordinator.TestArcJPBLateralBodyNamesNeverBindTheOuterRelationOnEveryArm`.

**Round 3 (2026-09-24): the DAG carries a LATERAL by a PROPERTY, and routes
the rest.** The round-2 closure review found the round-2 DAG rule wrong through
new spellings — a body naming its relation by its TABLE or CTE name (28 rows
for 9), `SELECT DISTINCT *` (`s.id` read `o.id`), a `SELECT *` body with a bare
key, a derived table or CTE inside the body, a third relation joined on `s.k`
— the same defect as round 1's colliding unaliased column: the stage DAG does
not run a decorrelated body's Project, the join stage reads the body's SCAN
stream, and every reference above it is re-spelled onto that stream, binding
by bare name where a qualifier is lost. Two rounds taught the re-spell one
spelling each; round 3 asks the plan instead
(`dagplan.refuseCollidingLateral`, `ErrLateralIdentityDistributed`). The DAG
carries a correlated LATERAL only when (1) no non-minted name its arm carries
ACROSS its join — what its SELECT list, aggregate or bare scan publishes, and
the columns those items are computed from (a column the body only filters on
stays in the body's own stage) — is carried by another relation of the query
(the subtrees hanging off the path from the root to the arm; a slot the
planner mints in a RESERVED family such as `__key_0` is unique by
construction — round 4: only those families, `plansql.ReservedSlotFamily`,
not every `__` name), and (2) the join does not null-extend a grouped arm (a LEFT
lateral over a DISTINCT or GROUP BY body wrote pad and aggregate files of
different widths, ADR-0010, or padded every row NULL through a table-named
body, measured over arms that share no name). Every other correlated LATERAL
runs on the coordinator-local single-process pipeline
(`Coordinator.runLateralIdentityLocal`, counted and logged; the async door
runs it as one pipeline task). The property is asked of every LATERAL arm,
correlated or not: an uncorrelated body aliasing an aggregate `total` beside
`lat_ord.total` read the outer column on the DAG too (arc L1's
`UNCORRLAT/collidingName` pin, deleted). Right-and-single beats
wrong-and-distributed.
Measured over the closure corpus (1143 statements) and 634 carried cells
(`lt_o` × `jp_q`, no shared name): the dag, dag-shuffled and fast-path arms
differ from single-process on no LATERAL cell; 710 of the corpus route and 123
of the carried cells route (clause 2), the other 511 run as stages and answer
PostgreSQL's rows.

The single-process half, at the seams the review named: the outer side of a
key binds through ONE path — the outer names include a CTE reference's name
(its alias alone when it has one) and a derived table's, a qualifier is read
from the parse (a delimited `"O"` included), and the equality's two sides come
from `lateralCorrelatedEquality` rather than the text's first `=` (which sat
inside `CASE WHEN o.k = 1 …`) — so `c.k - 0` no longer reaches the scan as a
string; an ungrouped aggregate's empty-input default rewrites the lateral's
OWN column (qualified by its alias, matched exact-first, named as the built
body publishes it: `max(i.id) AS id` beside `o.id`, and an unaliased
`count(*)` answering 0); and a non-key correlated conjunct (`i.id <> o.id`) is
spelled through the lateral's alias, `s.id`, which reads the body's column
over the join's output — so arc LT's "the two columns cannot be told apart"
refusal stands only for a lateral with no alias. A filter whose value side
names a column never falls back to the text path's literal comparison: a
LATERAL nested in another that names the outermost relation is refused,
not compared as the string `o.k`.
Gate: `coordinator.TestArcJP3CorrelatedLateralRoutesOrAnswersOnEveryArm`
(every cell's routing decision recorded).

**Round 4 (2026-09-25): a routed LATERAL is right only when single-process is
right, and every net keys on the predicate's SHAPE.** The round-3 closure
review found the property passable and the routing dishonest in three places,
each one mechanism: (1) the guard dropped every `__`-prefixed name as minted,
so a LATERAL over user names `__id` / `__k` crossed exactly the names the
property is about and ran as stages with the round-2 wrong rows; it now skips
only the planner's reserved families and compares names under
`strings.EqualFold`'s folding, the identity the re-spell's resolvers use (a
body's `"ſ"` beside an outer `s`). (2) Three routed cells answered the
single-process pipeline's wrong rows on every arm — routing made the arms
agree, not right. The single arm is fixed at its seams: the lateral's alias
names its arm when the body is a derived table's star (ADR-0026 §8l round 4),
and (3) the body's WHERE splits into conjuncts on the AST (the text split cut
`BETWEEN o.total AND o.total + 20` in two; a part that parses and is not a
correlated equality has no key, so `LIKE CASE WHEN o.k = 1 …` no longer mints
`1 THEN …`). The raw-text filter path is netted by shape: it reads only a
bare column against constants, and every other predicate that reaches it —
a column in a value position, NOT BETWEEN / NOT IN, `<>`, AND / OR / NOT, CASE,
IS DISTINCT FROM, a function — is refused, never compared as text or dropped
(this supersedes round 3's "a filter whose value side names a column"). A
bare `SELECT *` over an expression-keyed LATERAL is no longer refused: it
expands to the FROM arms' own lists (the lateral's read as `s.*` reads it,
without the key slot), which is PostgreSQL's star — round 1's refusal refused
cells base answered right; a star that cannot be expanded (a lateral list
naming one column twice) is refused after expansion, on both paths. For the
same reason a lifted non-equality predicate under a bare star is no longer
declined: the expanded star hides its materialized column as `s.*` always
did. A constant predicate that reaches the filter as text (`WHERE 1=1` in a
body over no table) is compiled rather than read as a column. A correlated predicate of a LATERAL body that reaches the enclosing
relation through a subquery whose FROM holds a LATERAL join is refused, 0A000:
such a subquery loses its correlation on every path (filed), and b2070cbb had
turned base's accidental refusal into every-pair answers.
Gate: `coordinator.TestArcJP4RoutedLateralIsRightOnlyWhenSingleIsRight` (643
cells incl. a 208-cell predicate-shape census; a routed cell's single-process
answer asserted against PostgreSQL's) and
`physical.TestTheTextPathReadsOnlyAColumnAgainstConstants`. Routing is the
distributed disposition, not the destination: evaluating such a lateral as
stages is #1323 (a planner-phase follow-up); until then a routed cell is
right exactly when the single-process pipeline is.

**Round 5 (2026-09-25): a correlated body is evaluated for ONE outer row in
every clause the lowering moves.** Round 4 split the body's WHERE on the AST
but rebuilt its LOCAL terms as text with no AST, and the filter re-split that
text at every AND — `q.qv BETWEEN 10 AND 30` became `q.qv between 10` and a
constant `30` (zero rows single-process, a parse error on the DAG). The split
now keeps each local term as its parsed node from the split to the filter;
no text round-trip remains on the path, and a WHERE the parser cannot read
is refused, never cut. Two shapes the text path had refused by accident are
refused by their property, 0A000: a local term holding a CORRELATED subquery
whose FROM holds a LATERAL join (it loses its correlation, filed), and a term
naming a relation that is neither the body's nor to its left — a LATERAL
nested in another that names the outermost relation (42000 until now). A
Filter pushed below a Project hands the subtree's name (its derived alias,
the lateral marker) to the new root, round 4's alias rule at the rewrite
that moves the root. A WINDOW is the other clause: a window anywhere in the
body — a bare or nested SELECT item, QUALIFY, HAVING, ORDER BY — is
partitioned by every correlation key it does not already carry (the per-key
partition arc LT's bound mints), which is exactly the rows one outer row
sees when every correlated part is `<inner expression> = <outer
expression>`; a non-key correlated part (evaluated above the join, after the
window numbered rows it removes) or an ungrouped-aggregate body (whose
no-match row is the join's pad) is refused. `QUALIFY row_number() OVER (…) =
1` answered zero rows on every arm. On the DAG a LEFT lateral over an arm
that publishes a window's output routes single-process like a grouped arm
(its pad file was a column narrower). Gate:
`coordinator.TestArcJP5LateralBodyIsRightPerOuterRow` (1096 cells: local
predicate shapes × key forms × body kinds × join kinds, window positions ×
key forms × join kinds × bounds, the property refusals).

**THE STRUCTURAL CLOSURE OF THE REFUSED SHAPES IS A DEPENDENT JOIN** — the
body re-run per outer row with the outer values substituted, the way the
scalar rerun does, emitting the joined rows — recorded as a filing candidate
with its mechanism (a logical node the reorderer treats as a barrier, a
physical operator over the subquery runner, a DAG refusal routed local, the
nine-door masking gate). It is what closes #1131 and #1130 in full and the
as-of idiom (`ev.host = o.host AND ev.ts < o.ts ORDER BY ev.ts DESC LIMIT 1`),
which no partitioned bound can express. `docs/internals/lateral-per-outer-row-bound.md`
is the design record with the alternatives that lost.

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

### 2b. An uncorrelated EXISTS is a query-wide constant, and the coordinator evaluates it

(Added 2026-09-07, #955's second half.)

§2 is the same sentence for `IN`: a subquery predicate the join could not
express reaches the stage DAG, the worker has no `SubqueryRunner`, and the
filter used to ship verbatim and fail (#524). `resolveSubqueryAST` gained an
arm for a scalar `SubqueryNode` (executed or deferred to a producer stage) and
one for `InExpr` (materialized as a SET). `ExistsNode` fell through `default:`
and every task failed with *"EXISTS subquery requires a SubqueryRunner"* —
`#524`'s family with the EXISTS arm never written.

It was reachable at base for a base table, for `NOT EXISTS`, for a QUALIFIED
CTE reference and beside another predicate — none of them through the scope
classifier — and §1k made one more shape reach it, because an `EXISTS` that
stops being mis-correlated stops being refused-and-routed as well.

**An UNCORRELATED `EXISTS` reads no outer row, so it is TRUE or FALSE for every
row of every task: a query-wide constant.** It is evaluated once at plan time
and the predicate becomes that boolean, which is what the single-process path's
memoizing evaluator already computes. A subquery that is not self-contained is
NOT evaluated — `plansql.DanglingTableRefs` guards it exactly as the
`SubqueryNode` arm above is guarded, and the coordinator answers on its local
pipeline with `CorrelatedLocalRoutes` moving (§1c). The correlated control is
an INEQUALITY correlation, the spelling that does not decorrelate into a semi
join and therefore reaches this site.

**A subquery is a leaf of a PREDICATE, so the walk has to be the predicate's.**
`resolveSubqueryAST` had arms for a comparison, an arithmetic operator and a
parenthesis, and none for `AND`, `OR`, `NOT` or `CASE`. `AND` looked handled
because the filter is split into conjuncts before this walk; `OR` and `NOT`
cannot be split, so `… WHERE d.id < 2 OR EXISTS (…)` shipped verbatim and every
task still failed while this section claimed the shape worked. Walking the
boolean tree is what makes the rule true of a PREDICATE rather than of one shape
of predicate; a `HAVING` and a `JOIN … ON` reach the same walk and are cells of
the gate.

**In a boolean position the walk hoists ONLY the EXISTS leaves, and that bound
is semantic rather than cautious.** A boolean connective SHORT-CIRCUITS, and
hoisting is UNCONDITIONAL evaluation: it turns a subquery the query may never
reach into one the query always runs, so every way that subquery can fail
becomes the query's answer. Measured on PostgreSQL 17 over the same five-row
subquery:

```sql
… WHERE d.id < 100 OR d.id > (SELECT id FROM t WHERE id < 5)   -- 9 rows
… WHERE d.id < 0   OR d.id > (SELECT id FROM t WHERE id < 5)   -- 21000
```

The first answers because the left arm is true for every row and the right one
is never needed; the second raises because it IS needed. **The semantics
settled here are PostgreSQL's, and they are LAZY**: the subquery in a
short-circuitable arm is evaluated only where the arm is reached, so a
cardinality violation in an arm that is never reached is not this query's
answer. This engine's single-process path agrees with PostgreSQL on both,
because it evaluates per row and lazily rather than as an InitPlan — the two
disagree about WHEN the subquery runs, and agree about every observable of it.
Hoisting made the first 21000 as well, which is a query PostgreSQL answers,
refused.

An `EXISTS` is the leaf where hoisting is sound: it reads no outer row, it is a
BOOLEAN rather than a value, and it cannot raise the cardinality violation that
is the failure at issue. A SCALAR or `IN` leaf in a short-circuitable position
therefore keeps whatever disposition its path had — right on the single-process
arms, a loud task failure on the DAG, pinned per arm beside PostgreSQL's answer
in `coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation`. Answering it
there needs the DAG to evaluate a subquery lazily per row, which is a lowering
and not a scope repair. The one place a hoisted EXISTS's failure IS the query's
answer is an authorization refusal, and that is ADR-0034's rule rather than an
exception to this one.

**A refusal keeps its own sentence.** The evaluation asks the shared access
lookup for every relation the subquery's plan reads, and discarding its error
turned `permission denied for table "…"` into a task failure — the sentence the
scalar and IN siblings hand back at the same site. A 42501 is not a routing
refusal but the query's answer on every path, so it is PARKED the way a
cardinality violation is (ADR-0034 item 6).

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

#### 2a. A SELECT-LIST scalar subquery takes the same producer stage

(Added 2026-09-04, #659.)

§2 gave the stage DAG a way to execute an IN-subquery it could not join. This
is the same question one clause over, and the answer is the machinery that was
already there rather than a second one.

A scalar subquery in a PREDICATE has had a distributed lowering since Q11:
`resolveFilterSubqueries` replaces it with a `:scalar_N` placeholder,
`emitScalarProducerStages` emits a producer stage, `Stage.ScalarDependencies`
records the edge and the coordinator substitutes the producer's single row into
the filter text before dispatch. The same subquery in the SELECT LIST had none.
`attachScanSelectProjections` attached it verbatim, the worker's compiler has
no `SubqueryRunner`, and every task failed — so the planner refused the plan
and the coordinator ran the WHOLE query single-process (#659). Right, and the
route fired identically over a CTE, a base table and a derived table: the gap
was the POSITION, not the relation.

**A SELECT-list item's subquery is deferred to a producer stage on the same
terms a predicate's is**, and the alternative is the one this project already
rejected: executing the subquery at plan time and splicing a literal is what
`scalarDeferAll` moved AWAY from, because a value accumulated on the
coordinator's single-process pipeline and one accumulated across stages differ
in float order — the Q15 SF0.1 zero-row root cause.

Two things a predicate does not need:

- **The spec keeps the ITEM's name.** `ProjectExprSpec.Name` is what
  `extractOutputRenames` maps to the user's alias, and that pass reads the
  logical projection, which the lowering does not touch. Only `Expr` carries
  the placeholder and only `Expr` is substituted.
- **The spec is typed the way the SINGLE PATH types the item, and deliberately
  NOT from the producer.** The producer's plan knows the value's type and the
  projection could declare it — measured, that makes `SELECT (SELECT
  MAX(c_i64) FROM t)` a bigint on the DAG, which is PostgreSQL's answer. The
  single-process pipeline answers a TEXT box for the same query
  (`expr.ScalarSubquery` declares nothing, so the item falls to the
  projection's string fallback), and a lowering that made the two paths
  disagree about a column's TYPE would trade a cost for a divergence. The box
  defect is on both paths, is older than this section, and is filed. The one
  thing the spec must NOT do is claim a type it does not know: an undeclared
  spec that sets `TypeKnown` takes TypeID zero, which is BOOLEAN, and the
  first cut of this answered `true` for that query.

**FOUR declines, each with a mechanism.** An item whose subquery survives
`resolveSubqueryAST` — a `CASE` arm, a function argument, anywhere the walk's
`default:` returns the node unwalked. A CORRELATED subquery, which needs a
re-run per outer row and routes on the correlated counter. A subquery sharing a
CTE BODY with the outer query, which is a CYCLE rather than a preference:
`ctePlannedTerminal` hands the producer the stage the outer walk already
emitted, the stage carrying the SELECT list is that body's consumer, so the
carrier would await a producer that depends on the carrier. Measured:
`WITH c AS (…) SELECT id, (SELECT MAX(v) FROM c) FROM c` HUNG on the
coordinator's stage barrier until a twenty-minute test deadline.
`attachProjectionScalarDependencies` walks the dependency closure and declines.

And the fourth is **§1e's rule at the other end of the same round trip**. A
producer's value reaches the worker as TEXT and is re-parsed there, exactly as
a correlated re-run's outer values do, so a type whose literal does not carry
its own scale or width is read back as something else: `AVG(a)` over a
`DECIMAL(p,2)` emits scale 6, substitutes `7.570000` — the right digits — and
comes back `7.57`, because the enclosing projection is declared from the
subquery's SOURCE COLUMN rather than from its aggregate. The integer family,
BOOL and STRING have no second parameter and lower; everything else declines
and routes, where the value is never rendered at all. Lifting it needs a
producer that declares its own (p,s) — or its own width, or its own instant —
to the projection that reads it, which is a stage-model change rather than a
rewrite. `coordinator.TestNumericArc2ShapesMatchPostgres` found it on the run
after the lowering first landed.

**"Everything else declines" is a claim about EVERY PATH that renders a value,
and the first cut of it was false.** `resolveSubqueryAST` has two exits and
only one is a producer: a subquery that is not provably one row is EXECUTED on
the coordinator at plan time and its value spliced in as a literal, which never
meets the check above. Measured — `(SELECT b FROM t ORDER BY id LIMIT 1)` over
a `DECIMAL(18,4)` reached a worker as `1` where PostgreSQL answers `12.7500`,
and a TIMESTAMP, a FLOAT64 and a DURATION came back int64-boxed where the
single path answers text. `WADJET_SCALAR_DEFER=0` is the same door for EVERY
shape, because with the switch off nothing defers at all.

So the rule is counted rather than inspected: **as many producers as the item
had subqueries, or the item is not lowered.** "No subquery left in the tree" is
true both when every one became a producer and when one was replaced by a
literal, and those are not the same disposition.

A fifth decline stands beside it, for a hard failure rather than a wrong value:
a producer that REUSES a CTE body somebody already planned emits a `cte-alias`
phantom, and `flattenCTEAliases` runs inside `generateStages`, before this
lowering exists. The phantom reached dispatch and the stage failed three times
with `empty Operators on task … (StageType="cte-alias")` and no SQLSTATE — two
SELECT-list subqueries over one CTE, and a SELECT-list subquery beside a WHERE
one. Re-running the alias flattening after every producer is emitted would
renumber stages the surrounding passes have already bound references into, so
the shape keeps what it had before the lowering: refused, routed, right.

`WADJET_SCALAR_DEFER` is a registered `optswitch` toggle (`scalar-defer`)
rather than a bare env read, because it decides which ENGINE answers a query.
A CTE-referencing subquery defers whatever it says — eager evaluation over the
cteCache float-drifts against the outer query's distributed aggregate, which is
Q15's zero-row root cause — so the two-path cells state which of them lower
both ways.

The refusal moves from before stage generation to after the attach pass,
because that pass is what decides whether an item is lowered; it is asked
before the assert battery so a declined item keeps earning its OWN typed
refusal rather than the unreachable-gather-output one — the same engine, a
different recorded cost.

Twelve cells in `coordinator.TestArcD5CorrelationMatchesPostgres` are the
gate, and the three `wantScalarProjRoutes: 1` pins #659 shipped with are
deleted, which is the proof. One divergence is recorded rather than fixed and
is not this section's: `(SELECT MAX(bigint)) + 1` boxes float64 where
PostgreSQL says bigint, on every arm and at this arc's base — a scalar
subquery declares no type to the const-arith fold, which is ADR-0024's rung
(#714's third box).

## 4. What was rejected

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

### 5. A scalar subquery is at most ONE row, and the second row is 21000

(Added 2026-09-04, arc E6 round 1.)

Every site in the engine that reduced a scalar subquery's result to a value
took `rows[0]` and said nothing about the rest. There were four of them: the
two expression evaluators (`expr.ScalarSubquery`, `expr.CorrelatedScalarSubquery`),
the planner's plan-time substitution (`resolveSubqueryAST`), and the producer
emission fallback beside it.

**The rule, which is PostgreSQL's:**

| rows | value |
|---|---|
| 0 | SQL NULL. An absent row is not an error, and every comparison against it is UNKNOWN. |
| 1 | that row's value |
| more | SQLSTATE **21000**, `more than one row returned by a subquery used as an expression`. Never the first row. |

`expr.ScalarSubqueryValue` is the single function that decides it and every
site calls it. The MULTI-COLUMN case is deliberately not decided there:
PostgreSQL refuses it at analysis time with 42601 and this engine does not,
which is a separate gap.

**Why it is a rule and not a preference.** The row `rows[0]` names is whichever
the runner or the producer emitted first — a partitioning and scheduling fact,
not a property of the query — so the same statement answered differently on
different paths. And this engine had just given the DML doors a subquery
runner (ADR-0031's amendment), which took that gap through a WRITE door:
`DELETE FROM t WHERE n < (SELECT n FROM src)` over a two-row `src` DELETED
EVERY ROW, on all three doors, where PostgreSQL raises and deletes nothing.
"Loud beats plausible" is exactly this case.

**One consequence in the distributed planner is worth stating, because it
moves a performance lever.** A scalar subquery is DEFERRED to a producer stage
only when it yields one row BY CONSTRUCTION — an ungrouped aggregate, possibly
wrapped (`SUM(…) * 0.0001`), which is what Q11 and Q22 are. Anything else is
executed at plan time, where the whole result is in hand and the rule can be
applied. It cannot be applied at the coordinator's end instead: a producer's
rows are neither one per task nor one per file, and a SINGLE-row producer can
surface in more than one file, so a count taken there is unsound in both
directions. The lever the deferral exists for is untouched.

**How much a construct READS is part of the rule** (added round 2). `> 1` is
the whole cardinality test, so the second row decides it and the read stops
there (`plansql.AppendRowLimit`, `LIMIT 2`); `EXISTS` asks whether there is A
row and reads one. This is not only an efficiency: the DML doors bound what a
subquery may return (`WADJET_IN_SET_MAX`, ADR-0031's amendment), and a bound
applied to every construct alike refused `DELETE … WHERE EXISTS (SELECT 1 FROM
big)` where PostgreSQL answers, and reported a multi-row scalar subquery as
**54000** — a resource complaint — where the rule above says **21000**, a
statement about the data. A bound belongs to the construct that wants what it
bounds: `IN` wants a SET and keeps it (`expr.WithSetRowBound`, on the node and
not in the runner, because a runner sees SQL text and cannot tell which
construct asked). The append is declined where a trailing `LIMIT` would not be
an append — the subquery already carries `LIMIT`/`OFFSET`, or is a `UNION` —
because bounding a READ must never change a QUERY.

### 5a. An ALIAS hides its table name — §1c's boundary, closed

§1c recorded a shape it could not close and said what closing it needed: *"No
scope-free predicate can tell the two apart; closing it is a producer
repair… a classifier repair, §1c's subject, not a scope one."* §1d restated
the same boundary from the other side: *"the boundary is that the OUTER
reference is UNALIASED."*

It was a classifier repair, and it is one line of SQL's own rule.
`plansql.collectInnerTables` registered a subquery's FROM items under BOTH
their table name and their alias, so inside `SELECT 1 FROM typemx sub`, the
reference `typemx.g` was read as naming the INNER relation. The subquery was
therefore not correlated at all, ran once, and answered a constant TRUE — 50
rows for PostgreSQL's 47, in silence. An alias HIDES the table name: only the
alias is registered now, which is the same rule `checkDMLColumns` has enforced
one level up since #686 (`DELETE FROM pr AS a WHERE pr.id = 1` is 42P01).

Three pins became controls with it, and that is the repair's proof:
`boundary_unaliased_base_table_correlation_stays_silent` (50 → 47, in two
censuses) and `boundary_cte_on_both_sides_outer_unaliased_stays_silent` (0 →
47), all now answering PostgreSQL's value. Both DAG arms went from LOUD
("EXISTS subquery requires a SubqueryRunner") to routed-and-right, which the
counters assert beside the rows. The DML door reached the same boundary as a
42P01 naming a table that plainly exists, and it now answers PostgreSQL's
`DELETE 2`.

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
  twin — #507's remainder, closed by §1f) and #584 (an unqualified outer
  conjunct pushed onto a decorrelated EXISTS's subquery scan)
- §1j: #852 (the derived / CTE / comma inner the build side now plans), #616
- The producer half, §1d–§1i: #535 (the CTE scope), #679 (the typed re-run),
  #538 / #578 / #539 (correlated `NOT IN`), #734 (the aggregate argument),
  #767 (LATERAL over an empty input), #809 / #601 (an aggregate in a
  subquery's own WHERE), and #616 / #614 / #714 (measured, not moved).
  `internal/coordinator/arc_d5_correlation_two_path_test.go` is their census.
- §1q: #1111 (the lateral's alias as a join arm's qualifier), the lateral
  scope refusals arc L1 measured, and #1019's repair written, measured and
  withdrawn.
- §1p: #1098 (the probe key, the per-row batch read), #1067 (a block's own
  WITH, in the parse / the build / the rebuild), #1072 (the nested walk and the
  refusal that names its own mechanism), #1066 (a recursive CTE reference's
  published column list), #960 (its DAG stage, open).
  `internal/coordinator/arc_r1_decorrelation_row_sets_test.go` is their gate.
- `internal/planner/logical/inner_key_spelling.go`,
  `internal/planner/logical/semi_anti_dedup.go`,
  `internal/planner/physical/in_subquery_set.go`,
  `internal/oracle/multikey` (the fixture and corpus, answers pinned to live
  PostgreSQL 17),
  `docs/internals/native-dag-execution.md` §Correlated subqueries
