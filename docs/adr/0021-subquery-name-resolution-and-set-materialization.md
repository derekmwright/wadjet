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
(§1i).

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

The §1c boundary is unchanged: an outer table correlated by its own TABLE NAME
against an inner relation reading the same table under an alias has no CTE and
no scope to record, so it stays silent and stays pinned.

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

- **ARRAY, ROW, MAP and VECTOR.** There is no container literal in this
  dialect.
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
(`CorrelatedLocalRoutes` 1, asserted). Decorrelating THROUGH a derived-table
inner is a producer repair and is not attempted here.

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
It fails at the semi/anti residual: `physical.BuildSemiAntiFilter` reads the
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

Two things restore it, both decided in the builder from the subquery AS
WRITTEN (`logical.lateralEmptyInput`), before the key injection:

- the join is a LEFT join whatever the query wrote, because the lateral side
  produces a row for every outer row and only the decorrelation made that
  conditional;
- references to the lateral's COUNT outputs are wrapped in `COALESCE(…, 0)` in
  the enclosing SELECT list, WHERE, HAVING and ORDER BY. NULL is already right
  for every other aggregate — `SUM` of nothing IS NULL in PostgreSQL — so
  COUNT is the only family that needs one.

A subquery the QUERY grouped is untouched, and is the control: `GROUP BY x`
over an empty input yields NO row in PostgreSQL either, so the unmatched outer
row is correctly NULL-padded there and not defaulted.

**`SELECT *` is the boundary**, pinned rather than described: a star expands
in a later pass over the plan's own schema, so there is nothing in the
SelectInfo to rewrite and the padded COUNT reads NULL where PostgreSQL reads
0.

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

- **#614 — a derived table in a subquery's FROM referencing the enclosing
  query.** MEASURED, because the question was open: it is LEGAL WITHOUT
  LATERAL and PostgreSQL 17 ANSWERS it (40 rows over the multikey fixture).
  LATERAL governs references to same-level FROM siblings; a reference to an
  OUTER-QUERY column from inside a sub-SELECT's derived table needs none. This
  engine refuses it on all four arms with 42P01 `missing FROM-clause entry for
  table "a"` — a message that asserts the SQL is invalid, which it is not. The
  refusal comes from the PHYSICAL column-scope validator, whose scope for a
  derived table inside a subquery does not merge the enclosing query's
  aliases. Loud, not wrong; supporting the shape is a dependent join and is
  what §1c calls the thing that removes the class.

- **#714 — an aggregate argument containing a scalar subquery.** The headline
  ("refused on the stage DAG") is gone: it ANSWERS on all four arms, the DAG
  routing the plan local for its SELECT-list subquery (#659's route), and the
  VALUE is PostgreSQL's. What diverges is the TYPE: `SUM(a + (SELECT 1))` over
  a DECIMAL column comes back float8 where PostgreSQL says numeric, while the
  same SUM without the subquery stays exact. That is a numeric-typing residual
  (ADR-0024's rung), not a correlation one, and the census pins all three
  boxes side by side.

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
  twin — #507's remainder, closed by §1f) and #584 (an unqualified outer
  conjunct pushed onto a decorrelated EXISTS's subquery scan)
- The producer half, §1d–§1i: #535 (the CTE scope), #679 (the typed re-run),
  #538 / #578 / #539 (correlated `NOT IN`), #734 (the aggregate argument),
  #767 (LATERAL over an empty input), #809 / #601 (an aggregate in a
  subquery's own WHERE), and #616 / #614 / #714 (measured, not moved).
  `internal/coordinator/arc_d5_correlation_two_path_test.go` is their census.
- `internal/planner/logical/inner_key_spelling.go`,
  `internal/planner/logical/semi_anti_dedup.go`,
  `internal/planner/physical/in_subquery_set.go`,
  `internal/oracle/multikey` (the fixture and corpus, answers pinned to live
  PostgreSQL 17),
  `docs/internals/native-dag-execution.md` §Correlated subqueries
