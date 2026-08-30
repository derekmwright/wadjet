# ADR-0025: A stage never carries a predicate or a projection its fragment will not run

Status: Accepted (2026-08-29, #656)

## Context

`walkStages` lowers a logical Filter by appending its predicate text to
the stage it has just emitted, and a logical Project by emitting nothing
at all — the DAG's long-standing convention, because the gather's
`OutputRenames` recovers the SELECT list and every consumer resolves a
derived alias back to its source column (docs/internals §Derived-table
aliases: eight resolvers, one convention).

Both shortcuts are unchecked. Nothing asserted that the stage underneath
would RUN what it was handed, and for five producers it did not:

- `buildSortFragment` was the one fragment builder with no `OpFilter`
  slot, so a WHERE above an `ORDER BY` was never evaluated;
- `collapseRedundantFinalMergeSort` and `flattenCTEAliases` DELETE the
  stage a predicate had just been attached to;
- a deduped `cte-alias`'s target is SHARED with the other reference of
  the same CTE, so it must not be filtered on that reference's behalf;
- an aggregate's output carries a computed group key under the TEXT of
  its GROUP BY expression, which the re-spelled predicate reads as
  arithmetic over a column the output does not have;
- `attachScanSelectProjections` accepted only a scan or a join, so a
  SELECT list above a window was never computed.

Every one of these answered the query WITHOUT the predicate or the
projection. Silently — no operator ever sees a name it cannot resolve,
so there is no loud failure to notice. Seven shapes were catalogued
(#656 a–g) before the mechanism was named, and each had been filed
separately.

## Decision

**A stage carries `FilterExprs` or `ProjectExprs` only if the fragment
built for it evaluates them, and the planner proves that rather than
assuming it.**

Three parts:

1. **The planner mirrors the fragment builders.**
   `stageRunsFilterExprs` / `stageAppliesProjection`
   (`planner/physical/filter_carrier.go`) answer, per stage type,
   whether the coordinator emits an `OpFilter` / `OpProject` for the
   field. They are read off `execute_stage_dag.go` and are deliberately
   separate from `projectableProducer`, which answers a different
   question — whether a computed sort key can be materialized INSIDE a
   fragment, below its ordering.

2. **When nothing can carry it, emit a stage that can.**
   `StageProject` is a Singleton one-dependency stage whose fragment is
   `[OpShuffleSource, OpProject?, OpFilter?, sink]`, with the filter
   ABOVE the projection. `filterCarrierIndex` inserts one exactly when
   the last emitted stage would not run the predicate. The alternative —
   refusing the shape at plan time and routing it to the local engine —
   is reserved for constructs the DAG has no lowering for at all
   (correlated subqueries, unstageable DISTINCT, unmaterializable IN
   sets, and now SELECT-list subqueries, #659); a filter is not one of
   those.

3. **A mismatch is a plan-time refusal, not a wrong answer.**
   `ValidateNativeDAGShape` rejects any plan that populates either field
   on a stage that ignores it. This is the #349 precedent: fail where
   the shape is visible. All FIVE checks below run on every distributed
   plan, not only in tests — they cost microseconds on a plan of tens of
   stages, and a check that runs only over a corpus closes the class for
   the corpus and leaves it open everywhere else. A 155-shape sweep found
   twelve live flags outside the corpus the day they were promoted,
   three of them silent wrong answers.

   The reachability check refuses at PLAN time rather than at dispatch,
   so the coordinator can route the query to its local engine and ANSWER
   it (`ErrUnreachableGatherOutput`), the way it already does for a
   correlated subquery and a SELECT-list subquery. A shape whose SELECT
   list the DAG cannot compute gets the right answer instead of the
   producer's raw columns.

4. **A SHARED producer carries nothing consumer-specific.** A CTE
   referenced more than once is planned ONCE — that dedup is what keeps
   Q15's two chains from drifting — so its terminal stage belongs to
   every reference equally. A WHERE above one reference is not a
   property of the CTE, in EITHER direction: the first reference walked
   emits the body's real producer, so its WHERE landed on the stage every
   other reference reads. `filterCarrierIndex` refuses such a stage as a
   carrier (the reference gets a `StageProject` of its own), and
   `assertNoConsumerScopedFilterOnSharedStage` then refuses ANY stage
   with more than one consumer that carries a filter or a projection —
   derived from the PLAN, not from a marker, because a guard that trusts
   `Stage.ConsumerScoped` passes any plan built another way. The one
   exception is a scan's own pushed-down predicate, which is part of the
   relation every consumer reads; there the marker still selects, because
   only stage emission knows whether the filter was attached inside the
   CTE body or above a reference.

Four things a per-stage check cannot see run alongside it, on every
plan, and are gated over TPC-H plus the shape corpus
(`TestStageDAGCarriesEveryFilterAndProjection`) and over the CROSS of
every producer class with every consumer shape
(`TestStageShapePlacementSweep` — the breadth gate, whose assertion is
that every shape ends in an ANSWER: planned, or refused and routed
local):

- **conservation** — every predicate and every projection output stage
  emission attached is still readable off some stage after every
  rewriting pass (a pass that DELETES a carrier);
- **name resolvability** — every expression on a carrier resolves against
  that carrier's INPUT SCHEMA, not merely against its stage type. The two
  differ exactly where the class is silent: `expr.ColRef.Eval` answers nil
  for a name it cannot resolve, so the predicate is UNKNOWN on every row.
  Join stages are excluded and named as excluded, because their input is
  the qualified union of two sides and asserting over it produces false
  refusals;
- **output reachability** — every `OutputRename.From` on the gather is a
  column some stage really emits. This is the only one that sees a
  projection that was NEVER ATTACHED: nothing was deleted and nothing is
  misplaced, the SELECT list simply became nobody's job, and the client
  gets the producer's raw columns;
- **sort-key resolvability** — the same name question asked of the one
  field the schema check does not cover. A sort key is not silent (`sort:
  key column "d" does not exist in the input schema` is a loud dispatch
  failure), but it is a loud failure for a query PostgreSQL ANSWERS, and it
  is reached by exactly this route: the projection that would have computed
  the key was never attached. `SELECT COUNT(*) FROM (SELECT k * 2 AS d FROM
  … ORDER BY d) x` is the shape — the outer aggregate needs no columns, so
  the projection is pruned and the sort keys on a name nothing emits.
  Refusing at plan time routes it local, which answers it. Materializing
  the key on the DAG instead is the open residual (#716); the refusal is
  what makes the shape ANSWER in the meantime. The locally-routed class as
  a whole is #717, and a union over two identical sorted producers is
  refused by a DIFFERENT check that does not route local (#715).

## Alternatives rejected

- **Keep attaching to `len(*stages)-1` and fix each shape.** Seven
  issues had already been filed this way. The placement is the defect;
  the shapes are how it surfaces.
- **Give every logical Project its own stage.** Correct and much
  simpler, and it would add an S3 round-trip per Project to every TPC-H
  query. The projection fuses into the producing fragment wherever that
  fragment can evaluate it (sort, window, limit, aggregate all gained an
  `OpProject` slot); a separate stage is the exception.
- **Rename an aggregate's outputs to the SELECT list's aliases.** Tried,
  and it broke Q15 and two derived-alias joins: every DAG resolver maps
  an alias BACK to the source column, so emitting the alias instead
  makes the join key name a column that is no longer there. The
  projection therefore carries a name only where the stage has none a
  consumer can use — a computed group key, or an expression over the
  aggregate's outputs.
- **Fuse the outer SELECT list into every producer.** A producer that
  COLLAPSES its input — an aggregate, a union, a fused scan-aggregate —
  has an output that is a NEW column set, so the SELECT list can neither
  fuse into it nor be evaluated below it. Those get a `StageProject`
  inserted directly above them, positioned so a sort between the two can
  key on what it computes. When the sort above needs BOTH a column the
  projection drops and one only the projection provides, neither side
  works and the pass declines: the reachability refusal then routes the
  query local, which answers it.
- **Let any projection-capable stage carry the outer SELECT list.**
  Tried, and it broke `SELECT DISTINCT COALESCE(x, 0)`: the list is
  written against the producer's OUTPUT, which for a scan, join, window,
  sort, limit or project IS its input's columns, and for an AGGREGATE is
  the group keys and aggregate outputs. Re-evaluating `COALESCE(x, 0)`
  over a stream that no longer carries `x` answers nothing. The
  aggregate's route is `absorbAggregateOutputProjection`, which spells
  against the output names.

## The marker decides ownership, in both directions

A shared producer may carry a filter or a projection perfectly safely, when it
belongs to the RELATION every consumer reads rather than to one of them: a
scan's own pushed-down predicate, and the aggregate-output projection
`absorbAggregateOutputProjection` puts on a CTE body whose group key is
computed.

A rule that refused any Filter or Project on a stage with two consumers looked
structural and was wrong. `WITH a AS (SELECT g+1 AS gk, COUNT(*) AS n FROM t
GROUP BY g+1) SELECT gk FROM a UNION ALL SELECT gk FROM a` has no filter
anywhere and PostgreSQL answers 16 rows; the rule refused the query outright,
and refused the self-join and the filter-on-each-reference spellings with it.

Ownership is knowable only where the attachment happens. Stage emission
distinguishes a predicate attached INSIDE a CTE body from one attached ABOVE a
reference and records the second as `ConsumerScoped`; `filterCarrierIndex`
already REFUSES to attach a consumer's filter to a stage it knows is shared,
giving that consumer its own `StageProject` instead. The assert's remaining job
is the case emission could not see — a stage that was single-consumer when the
filter landed and acquired a second consumer afterwards — and that is exactly
when the marker is set. Deriving ownership structurally instead means asking
whether every consumer's LOGICAL ancestry carries the same predicate, and the
pass is handed only `[]Stage`.

### The marker is set at one site, and that is a checked fact

`ConsumerScoped` is written in exactly one place — `filterCarrierIndex`, when a
consumer's predicate lands on a single-reference CTE body's terminal. The
assert trusts it, which is sound for the route the marker covers and says
NOTHING about any other route to a second consumer: shared-subplan dedup, a
join fusion that folds two stages into one, an exchange dedup. A stage that
gained its second consumer that way would carry no marker and be waved through.

No SQL reaching that state is known. `TestSharedProducerAttachmentsAreProducer-
Owned` is what keeps it a checked fact rather than a belief: it walks every
plan the shape sweep emits and asserts that a shared stage carrying a Filter or
a Project is one of the two shapes the ownership rule permits — a scan's own
pushed-down predicate, or a projection over the stage's OWN outputs — or that
the plan was refused. It also fails if the corpus stops producing a shared
stage at all, so it cannot pass vacuously. A future dedup route trips it
without anyone having to remember the rule.

## A computed group key is a column NAME above its aggregate

An aggregate emits its group key under the TEXT of the GROUP BY expression, so
above that stage `g + 1` is the name of ONE column and not arithmetic.
Rebuilding it as arithmetic reads `g`, which the aggregate's output does not
carry, and answers NULL for every row.

`absorbAggregateOutputProjection` already knew this and requotes such a term as
a delimited identifier. Three other sites did not, and each answered NULL
silently once the shapes above stopped being refused: the `StageProject`
inserted above a collapsing producer, a union ARM's projection, and — still
open, on both execution paths — a HAVING predicate (#720).

`respellSpecsOverProducerOutput` is the shared repair, and
`respellUnionArmProjections` runs it over union arms LATE in `PlanDistributed`,
after `flattenCTEAliases`: a deduped CTE reference names a `cte-alias` phantom
until that pass repoints it, so an arm respelled at emission time sees no
producer at all — which is why the SECOND arm of a twice-referenced CTE stayed
NULL when the repair ran beside the arm construction.

The same asymmetry governs the CHECK. `unresolvableColumnRefs` walks an
expression for column references, and a walk that descends into `g + 1 > 2`
sees a reference to `g` that the aggregate genuinely does not emit. Reading
that as unresolvable refused a HAVING the fragment computes correctly, so the
walk stops at any subterm whose TEXT is an emitted column name.

## An inserted stage is a stage, and ordering is read off the direct dependency

`StageProject` is the escape hatch for a producer that cannot carry the SELECT
list. It is also a stage, and putting one anywhere costs whatever its position
carried.

The coordinator asks its gather what its DEPENDENCY is and whether that stage
is ordered; the worker's merge asks the same one level down. A producer with a
FUSED ordering — a join or an aggregate whose sort was folded into it — answers
yes. A `project` between the two answers no unless it is told otherwise, and
whether it MAY be told rests on two conditions, both of which have to hold:

- the ordering's keys survive the projection under their own names, because a
  stream nothing can name the ordering OF is not an ordered stream. The
  self-join renames both keys (`a.s_suppkey` to `lo`, `b.s_suppkey` to `hi`)
  and fails this;
- the producer emits a SINGLE ordered stream. The inserted fragment
  CONCATENATES its inputs, and the concatenation of two ordered files is not
  ordered — declaring the keys anyway would be a lie in the other direction. A
  union (`Tasks = len(arms)`) and a probe-split join fail this.

When both hold the ordering rides onto the inserted stage and the consumer
still finds one. When either fails the insertion is refused and the pass falls
back to declining, which leaves the gather renaming from the producer's own
column names — the shape that already worked.

Keeping the two conditions rather than banning the insertion outright is what
keeps an ordered AGGREGATE on the DAG: it is one stream, its key survives, and
it is the producer the insertion exists for.

The symptom this cost is worth naming, because it is the one a multiset gate
cannot see: `SELECT a.s_suppkey AS lo, b.s_suppkey AS hi FROM supplier a JOIN
supplier b ON … ORDER BY lo, hi` came back as the CORRECT nine rows in the
WRONG sequence. Every value gate in this ADR's corpus passed it. The DuckDB
comparison caught it because that gate compares an ORDERED digest when the
query has a top-level ORDER BY, and the two-path corpus now asserts this shape
as a sequence for the same reason.

## A declaration is read as truth, so it may not disagree with the bytes

The respell above made a union arm COPY a column where it used to compute one,
and that exposed a second asymmetry in the same place. `reconcileSetOpArmTypes`
makes the arms agree by their DECLARED types, and it can only type an arm it
can resolve; above an aggregate the walk read the computed key `g + 1` as
arithmetic, could not resolve `g`, and fell to the float rule. One untyped arm
sent the whole set operation to FLOAT64 and CAST the other arm — and once the
aggregate arm stopped answering NULL, the two arms agreed on paper and wrote
different bytes, because the worker DirectCopies a bare reference and never
reads the declaration. The sort above read an INT64 key as float, got an empty
typed slice and indexed it: a recovered PANIC, on a query PostgreSQL answers.

The repair is the same rule one layer down — a computed group key is a column
NAME, so its type is the key's declared type and not the float fallback — and
it is applied wherever the question is asked: `emittedColTypes` types a derived
key from `derivedGroupKeyTypes` rather than a bare map lookup, `setOpArmDecls`
tries the whole TERM before reading it as arithmetic, and
`emitMergeAggregateTree` carries `GroupByTypes` onto the stages it emits, which
it had been dropping.

The backstop is `assertUnionArmsAgreeOnTypes`, and it checks the half arm
agreement cannot see: a spec that is a bare REFERENCE is copied off the
producer's stream with its declaration unread, so the declaration must match
what the producer says it emits. Two arms declaring the same wrong thing is
still wrong. It refuses with the sentinel, so the coordinator answers the query
locally instead of panicking — verified by disabling the type repair and
watching the same SQL answer correctly through the refusal.

## A carrier is never handed what it cannot evaluate

The attach pass now asks, before choosing a carrier, whether that carrier's
input can resolve every spec — the same question `assertCarrierSchemaResolves`
asks of the finished plan. Building a plan its own validator rejects reached
the client as a hard error on `SELECT k, s FROM (SELECT id AS k, SUM(c) OVER ()
+ 1 AS s FROM t) x WHERE k > 0`, whose no-rename spelling the gather's own
`OutputRename` already computes correctly.

## A name inside an EXPRESSION is a name (2026-08-30, #702, #694, #672)

The convention above — every stream carries source column names and each
consumer resolves the alias back through the logical plan — has been read as a
rule about NAMES. A consumer resolves the name it is handed, and the resolvers
are catalogued per consumer (docs/internals §Derived-table aliases). Three
faces of one gap show that the unit is not the name the consumer is handed but
every name the consumer will EVALUATE.

**An aggregate's argument.** `resolveAggInputName` resolves an argument that IS
an alias. An argument that is an EXPRESSION over one — `SUM(CASE WHEN s = 'x'
THEN twice ELSE 0 END)` over `(SELECT s, id * 2 AS twice FROM t)`, which is
TPC-H Q08's shape — travelled to the worker as TEXT and was compiled against
the scan's columns, where `twice` names nothing. The aggregate summed the
CASE's ELSE branch: 0 where PostgreSQL answers 2, silently, on every type. Where
the derived alias SHADOWS a base column the worker read the BASE column and
answered a plausible DIFFERENT number, which is the arm no eyeball catches
(#702).

**A window's argument** was the same gap one operator over, and #672 closed it
by materializing the argument into `__winkey_N` — the rule this section
generalizes rather than a separate one.

**A window's OUTPUT name** is the mirror image: not a name that resolves to
nothing, but a name that resolves to the WRONG THING. `exec.Window` APPENDS its
result, so a bare window writing under the user's ALIAS gave the projection two
columns of that name and it took the input one. `SELECT id, SUM(a) OVER () AS s`
came back with the table's `s` on BOTH paths (#694).

The rule, in both directions:

**A value a stage computes lives in a RESERVED slot, and consumers reference
the slot. A name a stage will evaluate is resolved through the producer before
the stage carries it — every name in it, not the outermost one.**

The first statement of this said "a slot no input can be spelled like", and
that was FALSE as written. `__win_N` is an ordinary identifier and the SQL
grammar can produce any string as a delimited one, so a user can spell it:

```sql
SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM t) x
-- PostgreSQL 52.99 on every row; wadjet answered t.b, on every execution path
```

That is #694 re-created under the slot's own name, and it is not the window's
doing — `s AS __winkey_0` made a window over an expression answer NULL before
#694 existed, and every other slot family has the same shape. There is no
unspellable name to retreat to, so the namespace is RESERVED instead: a query
that spells one of the eighteen families is refused, 42601, naming the family
(`planner/physical/reserved_slots.go`, checked where a name ENTERS the query's
namespace — a base table's columns, a derived table's or CTE's outputs, a
SELECT alias; a bare reference to a slot needs no check of its own, because no
source provides it and it is already 42703).

Refusing a query PostgreSQL answers is a real cost and is taken deliberately.
The alternative is not answering it either — it is answering it WRONGLY, which
is what the engine did. The refusal names the collision so a user with such a
column knows to alias it.

`__win_N` and `__winkey_N` are that slot for the window's output and its
argument; `respellAggInputExpr` is that resolution for the aggregate's
argument, and it applies `resolveAggInputName` per reference — a rename becomes
its source column, a computed alias becomes its defining expression
parenthesized.

Two things this needed that the per-consumer patches did not:

- **One walk, and it says what it did not understand.** Three respell sites had
  grown their own AST walk, each covering the node kinds its own defect
  happened to need — `respellDerivedAliasRefs` handled arithmetic, a paren, a
  cast and a function call, so `SUM(v * 2) OVER ()` over a derived alias was
  respelled and `SUM(CASE … v … END)` was not. A walk that does not descend
  into a node kind is not a no-op; it leaves the references inside it naming
  nothing. `rewriteColRefs` covers every kind and returns a `complete` flag for
  the ones it deliberately will not rewrite (a subquery, an EXISTS, a window
  call, anything added since), so a caller cannot read silence as coverage.

- **The backstop refuses rather than answers.** `assertAggregateInputsResolve`
  extends the name-resolvability check to `AggSpec.InputExpr`, over the two
  stage shapes that run a pre-projection: a standalone aggregate, whose input
  is its dependency's stream, and a fused scan-aggregate, whose input is the
  scan's read set. Final and merge aggregates are excluded — their InputExpr is
  already materialized upstream — as are join- and union-fed inputs, the same
  exclusion the checks above name. Removing the respell and re-running the
  gate turns Q08's shape from `0.000000` into `stage scan-0 (scan) aggregates
  sum(case when s = '1.50' then volume else 0 end) and its input carries no
  [volume]`, which is the difference this ADR exists for.

  It earned its place immediately: it found TWO residuals of the fix that
  introduced it, both of which had been silent 0s. A ROW field path whose
  CONTAINER is the rename (`rw.b` over `SELECT c_row AS rw`) resolves neither
  as a whole nor as a field, so only the qualifier is a name to resolve; and a
  COMPUTED alias over a COMPUTED alias needs a FIXPOINT, because
  `resolveAggInputName` stops at the first computed alias it meets and
  substituting once leaves `twice * 3` naming `twice`.

**And the rule has a boundary, which is the same one the sort keys have.**
Below a JOIN the derived table's SELECT list really IS materialized —
`attachScanSelectProjections` puts an alias-naming `OpProject` on the arm's
fragment — so `x.v` is a column of the join's output and the SOURCE spelling is
the one that is not. Respelling there took a CORRECT 25.50 to 0.00 on
`SUM(CASE WHEN x.s = '1.50' THEN x.v ELSE 0 END)` over
`(SELECT s, a * 2 AS v FROM t) x JOIN t y`, because a self-join qualifies both
sides' `a` and the bare name resolves to neither.

So the question is never "does this name exist below" but "is this name
MATERIALIZED here" — which is what `resolveDerivedAliasSortKeys` decides for a
sort key and what `aggInputRespellable` now decides for an aggregate argument.

**And it is answered POSITIVELY, per producer, not by enumerating the
materializing kinds.** That enumeration was tried and was wrong twice, once per
kind nobody had thought of: after the JOIN came the DISTINCT, which
`rewriteDistinctAsGroupBy` lowers to an aggregate whose OUTPUT is the
projection's names, so `SUM(CASE WHEN v > 0 THEN v ELSE 0 END)` over
`(SELECT DISTINCT a * 2 AS v FROM t)` went from a LOUD failure to a silent 0.
The rule is now the other way round — respell only where the walk reaches a
SCAN through Project and Filter alone, which is exactly where `walkStages`
provably emits no stage for the Project — and every other producer keeps
today's behaviour. Controls hold the boundary per kind (join, distinct, sort +
limit, window, aggregate); they are controls rather than assertions of the fix,
because all of them were right before this work and have to stay right.

**What the backstop can and cannot see.** `assertAggregateInputsResolve` is a
NAME check, so it catches a reference that resolves to NOTHING and never one
that resolves to the WRONG THING. With the respell removed, the shadowing arm
(`SUM(CASE WHEN s = '9' THEN a ELSE 0 END)` over `SELECT b AS a`) answers
2.0000 instead of 10.0000 and the check is silent, because the base table
really does have an `a`. Its reach is every feeding stage kind
`stageEmittedColumns` can enumerate — scan, sort, limit, window, project,
exchange, aggregate, union — with JOIN excluded and named as excluded; the
aggregate kinds were missing at first, which is exactly how the DISTINCT-fed
shape was waved through. Its refusal wraps `ErrUnreachableGatherOutput`, so the
coordinator routes the query to its local engine and ANSWERS it rather than
failing the client.

**A stage's Columns list is a FILTER, not a promise.** A join's `Stage.Columns`
is an OutputFilter and an exchange's is a payload manifest: both NARROW what
arrives and neither can invent a column. Reading them as "emitted" made
`assertGatherOutputIsReachable` believe in a name nothing computes — a window
inside a derived table under a join reached the gather as
`OutputRename{__win_0 -> w}` while the shuffle preserved `w` and the window
emitted `__win_0`, and the client got `[id, y.id]` for a query that asked for
`[id, w]`. Three repairs, each at the layer that was lying: the pruner stops
pushing a window's OUTPUT name down past the window that computes it; the join
shuffle's payload is `resolveJoinNeededColumns`, the same resolved spelling the
join stage's own Columns have carried since #385; and the reachability check
walks PAST the movers to the stage that really produces columns
(`providedColumns`) before believing a name.

## Consequences

- `StageProject` is Singleton with `Tasks: 1`, so it SERIALIZES its
  input: one task reads every partition. That is correct for a per-row
  operator and cheap for the shapes it is emitted for (a filter above a
  CTE reference, above an aggregate's SELECT list), all of which sit
  above an already-collapsed producer. It would not be acceptable above
  a wide partitioned scan, and the planner never puts it there — but a
  future pass that does must give the stage a partitioned variant first.

- The class is closed structurally: a new stage type, a new fragment
  builder, or a pass that deletes a stage now fails one of the two
  gates rather than silently dropping a WHERE.
- `Stage.FilterAliases` makes a predicate carry BOTH spellings — the
  query's own and the one re-spelled into source columns — because which
  is evaluable depends on a pass (`attachScanSelectProjections`) that
  runs later. `resolveFilterAliasSpelling` picks, mirroring what
  `resolveDerivedAliasSortKeys` already does for a sort key.
- TPC-H stage shapes are unchanged: `TestTPCH_EnsureDistribution_Snapshot`
  needed no update, because no TPC-H query puts a WHERE above a CTE's
  ORDER BY or a SELECT list above a window.
