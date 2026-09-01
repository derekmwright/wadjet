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
asks whether ANY producing stage in the plan computes the name before believing
it (`dropUnbackedJoinColumns`), with movers and joins excluded from the
producing set because their column lists are the thing under suspicion.

That last one is deliberately the WEAKER question. Intersecting with the join's
own inputs was tried first and refused TPC-H Q02 plus ten other tests: a
producer's emitted set is modelled per stage type, and a subtree the model
narrows differently makes a real column look absent — a false refusal breaks a
working query, which is the trade this check exists to avoid making in the
other direction. The price of the weaker question is that a name another ARM
produces can back a phantom qualified to this one (#742), and scoping it to the
join's own dependency subtree per side is that issue's job, with Q02 as its
first regression test.

## A hidden slot is a RESERVED name, and reading is not minting (2026-08-30, #694)

The section above gives a value a stage computes "a slot no input can be
spelled like". `__win_N` is an ordinary identifier and the SQL grammar produces
any string as a delimited one, so a user can spell it, and the first repair for
that was **wrong in a way worth recording in full**: it refused a table's
STORED columns at READ time.

A table carrying `__winkey_1` — written by any binary that predates this, or by
the Go API, which had no such check — became unreadable by every query:

    oldtab(id, __winkey_1, __win_0, plain)   -- 4 rows
    SELECT *                     -- 42601
    SELECT id, plain             -- 42601
    SELECT COUNT(*)              -- 42601
    SELECT id FROM oldtab AS t2  -- 42601

while `CREATE TABLE`, `wadjet.CreateTable`, an Ingester and `INSERT` all still
SUCCEEDED. So the trap closed behind the user: the engine accepted the data,
then refused to show it, and the refusal advised aliasing a column that could
not be selected. `DROP TABLE` was the only exit. It also inverts ADR-0018's
direction — a table written by an older binary must stay readable — and the
site had ZERO tests, which is how it got that far.

**Reading is not minting.** The column already exists; nothing is being
created; there is no ambiguity to resolve, because the planner has not put
anything there yet. The reservation binds at the moment a NAME IS CREATED and
at no other.

### The three rules

1. **A stored column is never refused at read.** No hook in the binder's
   `resolveSource`, and none anywhere else on the read path. A `SELECT` naming
   the column bare is a read too: `SELECT __win_0 FROM oldtab` is admitted, and
   the check is on an explicit `AS <alias>` rather than on the block's output
   names, because `blockOutputs` cannot tell a passed-through column from a
   minted one.

2. **The reservation binds where a user MINTS a name**, at SQLSTATE **42939**
   (`reserved_name`) — not 42601 (`syntax_error`), because the query is not
   malformed:

   - an explicit `AS <alias>` in a SELECT list;
   - an explicit CTE column list (a CTE body's own aliases are covered by the
     body's own validation);
   - the three DOORS: `wadjet.CreateTable`, `CREATE TABLE`, and
     `NewIngester` — whose refusal is deferred to the first `Ingest` or
     `FlushAll`, since the constructor returns no error.

   Each door has its own test (`wadjet.TestReservedSlotNamespaceDoors`),
   because the read hook that had none is what produced the trap.

3. **The planner's slot moves, not the user's column.**
   `renameCollidingSlots` (`planner/physical/slot_collision.go`) runs after
   `AnnotateScanColumns` — the pass that puts a table's real column list on the
   Scan node, and therefore the first point at which a collision is visible —
   and renumbers a window slot past any stored name. The user's column keeps
   its name and its values, because it is the one they can see and address:

       SELECT id, __win_0, SUM(id) OVER () AS w FROM oldtab
       -- __win_0 = 100,200,300,400 (stored)   w = 10 (the window)

   The rename is keyed on `logical.Projection.SlotSource`, a field the builder
   sets for a window column. Renaming by NAME moved BOTH projections — the
   user's and the planner's — and handed the stored column the window's value.
   The two are otherwise indistinguishable: same Project, same spelling, and
   only provenance separates them, which is the same lesson `__win_N` exists
   for one level up.

### The reservation is a PREFIX rule, and that is wider than what SlotName mints

A name is reserved when it BEGINS with a family's prefix, not when it matches
`__<family>_<digits>`. So `__win_`, `__win_x`, `__win_00` and `__win_1x` are all
refused at a mint site, though `SlotName` can never produce them.

That is deliberate and it is the rule, not an approximation of a narrower one:
the families are a namespace, and reserving a namespace means reserving its
shape rather than the finite set of names one constructor happens to emit
today. Five families are minted with a DISCRIMINATOR rather than a bare index
(`__precomp_agg_<n>`, `__subsume_f<n>`, `__row_loc`, `__rowcount_only__`,
`__default__`), and a narrower rule would have to enumerate each of those
spellings and be re-narrowed every time a family grows one. The prefix rule
covers them without knowing about them.

The cost is that the refusal is wider than strictly necessary, which is the
right side to err on: a name inside a reserved namespace is refused at CREATION
only, and rule 1 guarantees a table that already holds one stays readable.

Longest match wins, so the message names the most specific family: `__agg_expr_`
reports as `__agg_expr_` and not as `__agg_`, which it also begins with.

### The PostgreSQL divergence, stated

PostgreSQL has no reserved column namespace and answers every query in rule 2.
Wadjet refuses them. This is a deliberate divergence from ADR-0012's "PostgreSQL
decides semantics", taken because the alternative is not answering them either
— it is answering them WRONGLY, which is what the engine did before the
namespace existed. The refusal names the family it collided with so a user with
such a column knows to alias it, and rule 1 guarantees that data already in such
a table is never stranded by the choice.

The divergence is bounded to CREATION. Nothing a user can already read stops
being readable, and nothing about the shape of the reservation grows without a
row being added to `reservedSlotPrefixes`.

### No kill switch, and why

A kill switch is for an OPTIMIZATION whose only risk is a wrong row set
(#287's convention: `WADJET_<NAME>=0`, and the invariance oracle then runs the
corpus with it off). This is not one. Turning it off does not restore a slower
correct answer; it restores the silent wrong answer the reservation exists to
prevent — `SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM
t) x` answering `t.b` on every path. A switch here would be a second behaviour
to test and a second thing for a user to be wrong about, with nothing on the
other side of the trade.

Rule 1 is what makes that acceptable. A reservation with no escape hatch is
only safe because it cannot strand data: the escape hatch it would otherwise
need is exactly "let me read my table", and that is unconditional.

### Why the slot API lives in `internal/planner/sql`

The reservation table and the name constructor are in the PARSER package, not
in `physical` where they started, and the import direction is the whole reason.

Slots are minted in three packages: the logical builder (`__win_N`,
`__having_N`, `__agg_N`), the logical optimizer (`__scalar_N`, `__tl_N`), and
the physical planner (`__winkey_N`, `__sortkey_N`, `__gb_expr_N`,
`__agg_expr_N`). `internal/planner/logical` cannot import
`internal/planner/physical` — the dependency runs the other way — so a table
living in `physical` was unreachable from more than half the sites that mint
into it, and every one of those sites built its names with a local
`fmt.Sprintf`.

A table half the minting sites cannot reach is a table that drifts, and it had:
`SlotCovarState` read `"__covar_stat"` while the reservation and
`worker/var_fold.go` used `"__covar_state"`. It was latent only because nothing
called the constructor. `plansql` is imported by logical and physical alike, so
that is where the mechanism belongs; `planner/physical/reserved_slots.go`
remains as aliases for the code already written against it.

### A slot is ALLOCATED, not named

`SlotName(family, n)` renders the nth name of a family. It does not allocate
one, and the gap between those two things produced the same defect twice within
a day, written by two authors who never saw each other's code:

- the window renamer, moving a slot past a stored `__win_0`, took the first
  name not in the STORED set — `__win_1`, which the query's SECOND window
  already held. Both wrote it and the projection handed window #2 window #1's
  value;
- the group-key minting, materializing two computed GROUP BY keys, skipped
  names in scope but not slots issued to earlier keys of the same aggregate.
  Two keys landed in one column and twelve groups collapsed to three.

Both are the same omission — a search that excludes the names already in scope
but not the names it has itself just handed out — and both are silent. Two
independent authors reaching for the same wrong shape is a statement about the
API, not about the authors: it offered a namer where the callers needed an
allocator, so each of them wrote their own search and each got it wrong the
same way.

`plansql.SlotAllocator` is that allocator, and it is now the only way a slot is
obtained. It is created for ONE SCOPE, seeded with the names in that scope
(a table's stored columns, an input schema, the SELECT list's outputs), and
`Next(family)` hands out one fresh name at a time, excluding BOTH the seeded
names and every slot it has already issued. Its cursor is per family, so
allocating a group key does not renumber a window. It terminates by
construction — the cursor advances monotonically and the loop is bounded — and
when a family is exhausted it returns `ok == false`, which callers must treat as
a reason to leave the plan as it was rather than to reuse a name.

Which scope that is, is not the allocator's choice and was wrong for `__win_N`
until #747 — see "A scope is the QUERY, not the block" below.

The gate proves both halves and the second one by MUTATION: a stub allocator
that records its seeds but not its issues is driven through the same contract,
and the test asserts that it repeats a name. Without that, the contract test
could pass against an implementation whose issued-set was dead code.

### The coverage gate runs in both directions

`internal/planner/sql/reserved_slots_test.go` asserts the table in BOTH
directions, the way the ANALYZE coverage gate does — every family constant
mints a name the reservation claims, and every reservation is one some family
or a named suffix-minted slot produces. A one-directional test would not have
seen the covar drift, because the PREFIX was reserved and it was the CONSTANT
that was wrong. It also refuses to pass vacuously.

### A scope is the QUERY, not the block (2026-08-30, #747)

The section above says a slot allocator "is created for a query SCOPE". The
allocator that hands out `__win_N` is created in
`logical.BuildFromSelectWithCTEs`, which RECURSES per SELECT BLOCK — so the
scope was the block, every derived table started its counter at zero, and two
sibling subqueries minted the same `__win_0`:

```sql
SELECT p.w AS pw, q.w AS qw
  FROM (SELECT id, SUM(b) OVER () AS w FROM t) p
  JOIN (SELECT id, SUM(a) OVER () AS w FROM t) q ON p.id = q.id
-- PostgreSQL pw=49.2400 qw=52.99; both DAG arms answered 49.2400 TWICE
```

Both arms carried a column of that one name into the join and one window's
value was published under both output columns. At THREE siblings the last
arm's slot won and every path answered it, single-process included. The
collapse is provenance, not spelling: it happened just as completely when the
two blocks published `w` and `w2`.

The ADR asserted the opposite in writing — "the blocks' slots are already
distinct, the allocator is per query" — and #747 was filed against that
sentence. It is method 10 of the correctness protocol in its purest form: the
design named an impossibility, no fixture attempted it, and the class was
invisible to every gate.

**`renameCollidingSlots` is where the scope becomes the query.** It already
had the machinery — an allocator seeded with the plan's slots, and a walk
whose multi-child node is a rename BOUNDARY — and only the trigger was too
narrow: it fired solely when a TABLE stored a reserved-family column
(`hot`), which is the rarest shape in the family and the only one the #694
work had in mind. It now also fires when two window nodes mint one name, and
renumbers every occurrence after the first. The first block keeps its slot, so
a query with no collision is untouched and TPC-H's stage snapshot does not
move.

Two things this needed:

- **A window WRAPPED in an expression has no `SlotSource`.** The builder's
  nested-window rewrite leaves only a `ColRef` inside `ASTExpr`, so the
  provenance field the #694 renamer keys on is empty and the rename could not
  reach it. Rewriting a bare reference is sound exactly when the old name is
  stored NOWHERE in the query — then no source provides it and the planner's
  window is its only writer. When it IS stored the reference may be the user's
  own column, and moving it is the #694 defect this pass exists to avoid.
- **The pass runs on both entry paths and twice per query.** It is idempotent
  by construction: the second run sees distinct slots and renames nothing.

### A join carries what it will EVALUATE, and what its gather will READ (#700, #726)

Making the slots distinct immediately exposed the other half. A join stage's
`Columns` is an OutputFilter and its input exchanges' `Columns` are payload
manifests; both are built from the join node's `NeededColumns` at stage
EMISSION — before `attachScanSelectProjections` decides that this join is
where the outer SELECT list is computed, and before `resolveFilterAliasSpelling`
decides how a WHERE above it is spelled. A name only those late passes
introduce is absent from every list the payload is built from, so the shuffle
drops the column the fragment is about to read: `column "__win_0" does not
exist in the input schema`, on a query PostgreSQL answers.

That is the same gap #700 and #726 were filed for, with the loud face instead
of the silent one — there the exchange carried the CTE's ALIAS while the
predicate had been re-spelled to the base column, so the filter was UNKNOWN on
every row and the query answered zero rows on the shuffled arm alone. The two
passes below, plus the #694-round-2 payload repair that had already landed,
close the ONE-JOIN face of both issues. They do not close the two-join face:
both were reopened against this paragraph, and "A subtree publishes what it
MINTS" below is what actually closes them.

**`ensureJoinCarriesEvaluatedColumns`** unions the column references in a join
stage's own `FilterExprs` and `ProjectExprs` back into that stage's
OutputFilter and into its input exchanges' manifests, after the late passes
have settled what it evaluates. **`ensureJoinCarriesGatherOutputs`** does the
same for the one consumer that reads a join's output without evaluating
anything: the gather, whose `OutputRename.From` named a column the
OutputFilter had narrowed away.

Neither can invent a column. A payload naming something an arm does not have
is ignored — the manifest is applied per side and both sides already receive
the union of the two — and a name NO stage produces is removed again by
`dropUnbackedJoinColumns` before `assertGatherOutputIsReachable` reads the
set, so a genuinely unreachable SELECT list is still refused and still routed
local. Widening here can only rescue a name something really computes. A stage
carrying neither field is untouched, and an already-empty list stays empty,
because for both kinds of list empty means "carry everything".

**What is NOT closed, and is filed rather than described as fixed.** Two
residuals belong to the join-output resolution and one to each of two other
mechanisms. A qualified reference satisfied by another arm's identically-named
column is still wrong on BOTH stage-DAG arms (#742 — the single-process path
answers PostgreSQL's value since the pruner repair below). A sibling nested
inside a sibling, and the CTE spelling of two sibling window blocks, are wrong
on the single path alone (#751, #753). Where two join arms publish the SAME
output alias the single path renders one of them at the other arm's DECIMAL
scale — right digits, wrong typmod, and it is the duplicate ALIAS that
triggers it and not the DECIMAL (#754). A three-way join over derived arms
fails loudly on the shuffled lowering (#755). A stored slot column beside a
WRAPPED window answers the stored value on the single path and fails loudly on
both DAG arms (#750), and that one is a deliberate decline: moving the slot
there would move the USER's column instead. Each has a pinned fixture whose
failure is that fix's proof.

### A subtree publishes what it MINTS, not what its scans store (2026-08-30, #700, #726)

The section above closed #700 and #726 on a nineteen-shape sweep that stopped
at ONE join, and both were reopened: with a SECOND join the same filter is
dropped again, unchanged.

Column pruning partitions a join's needed columns between its two sides by
asking which side can supply each name, and `collectSubtreeColumns` answered
that from the subtree's SCAN columns alone. A name a Project MINTS is supplied
by no scan, so it was in NEITHER available set, went into neither `probeNeeds`
nor `buildNeeds`, and disappeared — and a join's `NeededColumns` IS its
OutputFilter, so the INNER join stopped emitting the column the filter above
the OUTER join was about to read:

```sql
WITH c AS (SELECT id, a AS v FROM t)
SELECT COUNT(*) FROM c JOIN t x ON c.id = x.id JOIN t y ON c.id = y.id
WHERE c.v > 1
-- PostgreSQL 5
-- single         ERROR  filter column "c.v" does not exist in the input schema
-- DAG broadcast  5
-- DAG shuffled   0      silently
```

Three arms, three symptoms, one cause — and the broadcast arm being RIGHT is
what kept it out of the default gate for so long.

**One join hid it, and so did the derived-table spelling.** With a single join
the partitioned join is the one the Project feeds directly, so the alias never
has to survive a second partition. And `pushdownPredicates` swaps a filter
below a Project and substitutes the alias away — except above a CTE's Project,
which is a materialization fence and declines the swap. So the shape needs a
CTE *and* a second join, which is exactly the pair neither issue's original
repro nor its first fix carried.

The repair answers the question the caller is really asking: a subtree
publishes its Projects' output names, its aggregate's group keys and outputs,
and its windows' output slots, as well as its scans' columns. Adding a minted
name can only make a subtree claim MORE, so it can only push down a need that
used to be dropped and never withhold one. A name pushed to a side that cannot
supply it is already tolerated — dropped again at the scan by
`sanitizeScanNeeds`, and deleted at the window that mints it by
`pushColumnNeeds`' `NodeWindow` arm.

**It closed three other filings' single-process halves**, which is the evidence
that it is the mechanism and not a patch: #753's join-of-joins shapes (a
derived computed column answering NULL or the last arm's value, with no window
anywhere and a distinct alias per arm) and #742's qualified reference across
two arms both answer PostgreSQL's value on the single path now. #742's two
stage-DAG arms remain wrong and remain pinned.

### The executor resolves the direction its own checker assumes

A second, smaller disagreement surfaced in the same sweep, on the spelling
where the CTE is the BUILD side of the first join. `QualifyAllBuildCols`
renames every build column for a self-join, so the stream publishes `t.a` and
nothing called `a`, while the re-spelled filter names `a`.

`ResolveColumnRef` tried the exact spelling, then — for a QUALIFIED reference
— the bare name, then the qualifier as a ROW container. A BARE reference that
matched nothing returned immediately, so the one remaining direction was never
tried. `physical.columnResolves` has matched a reference against a qualified
column by its bare part since #656, so the planner's check believed in a
resolution the evaluator did not implement; that gap is the defect, and
closing it is not a special case but the removal of a disagreement. It is a
last resort and unambiguous only — two columns whose bare names collide
decline, keeping the loud failure rather than guessing an arm.

**What the gates carry.** `TestCTEFilterAboveAJoinChainThreeArms` is chain
LENGTH x the CTE's POSITION x CTE versus derived x the filter's spelling x
join kind, on three arms against PostgreSQL; the one-join sweep beside it is
now the control that says the chain is the trigger. Reverting the pruner fails
eleven of its subtests and reverting the resolver fails three, so neither is
gated by the other. `postgresJoinArmCases` asks the same questions of a live
server.

Every shape in it publishes a plain RENAME, which left the whole COMPUTED half
of the class open — see "A MINTED name is not a RENAMED one" below.

### A MINTED name is not a RENAMED one, and the corpus has to attempt both (2026-08-31, #700, #726)

The section above closed #700/#726 on a chain sweep of twenty shapes. Every
one of them published a plain RENAME (`c_i64 AS v`), and a CTE publishing an
EXPRESSION was still dropped. `grep '\* 2'` over the gate returned nothing:
the claim "a subtree publishes what it MINTS" was written down and no fixture
attempted the minting. That is METHOD 10 with the claim and the hole in the
same file, and it is the third time in this arc the same shape has repeated.

The two travel differently and that is the whole point. A rename resolves back
to a source column through every DAG resolver, so it survives anywhere. A
computed output has to be MATERIALIZED by some fragment or it exists nowhere,
and two mechanisms were dropping it:

**The chained join's own filter.** `fuseStageChains` moves a downstream join's
`FilterExprs` onto the `ChainedJoinSpec`, and `ensureJoinCarriesEvaluated-
Columns` read only the stage's own field — so the re-spelled predicate
`(a * 2) > 1` was invisible to the pass whose whole job is carrying its
columns. Reading the chained specs is necessary and not sufficient: the column
must survive EVERY narrowing stage between the filter and its producer, and
with the CTE on the build side the FIRST join had already dropped it before the
second one's filter ran. The refs are now pushed down the dependency graph
through joins and exchanges — the two kinds whose `Columns` is a filter or a
payload manifest and can only narrow — stopping at producers, whose list is a
read set and must not be widened.

**A decline with nowhere to land.** `absorbAggregateOutputProjection` refuses a
computed output over a DECIMAL aggregate on purpose (ADR-0024 item 2: `AggSpec`
has an OutputType but no (p,s), and a wrong DECIMAL declaration is worse than
no projection). The decline is right. What was wrong is that nothing then
computes the column and the query answers WITHOUT the predicate — silently,
and only when a JOIN is present, because `assertCarrierSchemaResolves` models
an aggregate stage's input and excludes JOIN stages by design. The join-free
spelling of the same query already refused and was routed local.

`assertJoinFiltersAreBacked` asks the WEAKER question on a join stage's filter
— does ANY producing stage in the plan compute this name — which is the
question `dropUnbackedJoinColumns` already asks and which is known not to
refuse Q02. It refuses with `ErrUnreachableGatherOutput`, so the coordinator
answers the query locally, matching what its join-free twin already did. It
cannot see a name that resolves to the WRONG column, and that limit is real:
#762 is exactly such a shape and is pinned rather than caught.

**Controls are what identified the trigger.** The same computed-over-aggregate
shape over a FLOAT or a BIGINT aggregate was never declined and was always
correct on every arm; a plain rename of the aggregate output was always
correct. The TYPE is the trigger and the join is only what hid it — which is
also why the fix belongs at the refusal and not at the column pruner, where
the round-2 review first pointed. The pruner already published computed
aliases: `projectionPublishedName` keys on the ALIAS, so `a * 2 AS dv`
publishes `dv`, and the stage dump shows `dv` in the inner join's needed set.

**What the gate carries now.** `TestCTEComputedColumnAboveAJoinChainThreeArms`
crosses arithmetic over a column, over an aggregate and over a window, with
CASE, CAST, COALESCE, DECIMAL arithmetic and Project-over-Project, against 2-
and 3-join chains, the qualified and bare filter spellings, the derived twin,
and the projecting and HAVING forms — on three arms, against values taken from
a live PostgreSQL 17 rather than computed by hand (three of the first
expectations were wrong and the server corrected them). Reverting the two
fixes fails 24 of its subtests.

Two residuals are pinned there rather than described as fixed: a computed
output over an aggregate with the CTE on the BUILD side of a chain still
answers zero on both DAG arms, because the two shuffle sides share one payload
manifest and the name resolves to the wrong side (#762, verified pre-existing);
and the DERIVED spelling of the same shape is refused LOUDLY, because
`assertCarrierSchemaResolves` runs at dispatch and its error does not wrap the
routing sentinel, so nothing routes it local (#763).

### Carrying a column is not free, and "the snapshot is green" does not mean the plan is (2026-08-31, #700, #726)

The section above fixed the chain by pushing a referenced column down every
narrowing stage between producer and consumer. That is correct and it is also
a SECOND CARRY: on a query that was already right, the consumer already had a
path to the value, and the extra entry is bytes on the wire on every row of
every task. Six TPC-H queries paid it — 21 stage lines, including `n1.n_name`
and `n2.n_name` added to three Q07 exchange MANIFESTS, two STRING columns
crossing the network twice more.

**The claim that it did not was made, and no gate could have caught it.**
`TestTPCH_EnsureDistribution_Snapshot` records a stage's ID, TYPE and
DISTRIBUTION; it records nothing about COLUMNS. "22/22 green, so the plans are
identical" was a conclusion the evidence did not support, and the honest
reading is that a widening of every shuffle manifest in the suite was
invisible to the entire tree.

`TestTPCHStageDumpGolden` is the gate that closes it: id, type, scan-file
count, sorted Columns, sorted Dependencies and per-stage operator counts for
all 22 queries, against a committed golden, with a refusal recorded as a line
of its own so a query that starts or stops routing local also shows. Bytes on
the wire is a co-equal metric with wall time (CLAUDE.md), and until this test
existed the planner had no gate on it at all.

**Five narrowings, each answering a specific line of that diff.** None of them
is a threshold; each is a thing the pass was getting wrong:

- **A `Columns` list is not evidence of production.** It is a filter or a
  manifest and can never invent a column — the rule this ADR already states
  for `dropUnbackedJoinColumns` — so the subtree-supply test reads only what a
  stage COMPUTES. Reading `Columns` made `s_nationkey` look available under a
  lineitem-only branch of Q05, because the two sides of one shuffle share a
  single manifest.
- **A join CONDITION is not payload.** It is resolved by the join's key
  machinery from both sides and never read off the narrowed probe stream.
- **`BuildFilterExprs` filters the BUILD input**, before that hash table is
  built, so its columns come from the build dependency. That was Q21's
  `__subsume_f0`.
- **A chained link's residual filter reads both sides**, and only the
  PROBE-side columns can be dropped by this stage's OutputFilter. Which side a
  name comes from is decided by the build dep's declared MANIFEST and not by
  its table — a self-join makes those disagree completely, since the chained
  build of `c JOIN t x JOIN t y` is the same relation as the probe. An empty
  manifest is the one case that really does mean "everything", and only then
  is the subtree the right question; that is how Q07 reaches its nation names
  through a replicated build.
- **The push-down only enters a subtree that can supply the name.**

**What stays unconditional is what was load-bearing.** A join stage's own
residual filter and projection, and a chained link's probe-side filter, are
evaluated against a view this same `Columns` list narrows, so they belong in
it whatever the input carries. `join-4 FILTER=[(a * 2) > 1]` over `COLS=[dv id
…]` with no `a` is the broadcast half of the shape, on a probe that plainly
has one.

An input-reachability gate — "push only what the input cannot already supply"
— was the obvious narrowing and is NOT in the code. With the five above in
place it changed no TPC-H line, and it cost the three-sibling window shape its
`__win_1`, because a join's own filter is evaluated after its own narrowing
rather than on its input. Recorded because it is the first thing the next
reader will reach for.

**What is still open here.** The FILTER forms of the chain shapes are correct
on three arms; the PROJECTING forms of two of them hard-fail on the shuffled
lowering at dispatch (#766), which is #763's routing class rather than a wrong
answer — whatever carries the column for the predicate is not carrying it for
the SELECT list.

### A name is resolved through the RELATION it names, and a value is materialized where it is computable (2026-08-31, #742, #753, #755, #762, #763, #766)

Six filings, one family: the join's output stream carries SOURCE column names
and every consumer above it resolves back through the plan (§Derived-table
aliases). Where that resolution has a hole, the DAG either fails loudly on a
query PostgreSQL answers, or — when ANOTHER arm happens to publish the same
name — resolves to it and answers silently. An 811-shape sweep over the
`decpair` fixture (join kind × CTE vs derived × arm position × chain length
1–3 × consumer × body kind × broadcast/shuffle, every value taken from a live
PostgreSQL 17) found 430 divergent (shape, arm) rows on `376b2cac`.

That sweep's harness is not in the tree, and its residual figure was restated
from memory each round — 52, then 48 — while the corpus behind the number did
not move with it. So the figure this ADR carries is the one measured LAST, on
a corpus that IS on disk and reproducible: 273 shapes on three arms (819
arm-rows), being the review corpora of every round plus the families each fix
added, 243 of them over `decpair` with a live PostgreSQL 17 answer. Against
`376b2cac`: **0 regressions, 261 arm-rows fixed, 61 still divergent** — 45 of
those byte-identical to `376b2cac` and 16 loud on both trees under different
messages — and exactly ONE arm-row goes from a loud refusal to a wrong answer,
which is the GROUP-BY residual named at the end of this section. The other 30
shapes (90 arm-rows) read `typemx_nested`, which the `--locale=C` PostgreSQL
fixture cannot hold, so they are compared base→tip instead: 43 cells changed
and every one of them base-wrong or base-loud → tip-right, against the field
values the join-free spelling of the same query returns.

**The census is the gate this arc did not have, and it earned that twice.** It
is not a summary of the fixes; it is the only thing that measures the one way
this work could leave a deployment WORSE than `376b2cac` — a refusal turning
into a wrong answer, on the shuffled lowering the chain rewiring made runnable.
Round 3 reported that number as 1 without measuring it and it was 18. Round 4
fixed those two mechanisms, re-measured 1, and the number was 1 for the corpus
it was measured on: the next review's corpus put a WINDOW between the SELECT
list and the join and found ten more. Both times the finding came from
widening the corpus, not from a gate — so the standing obligation is that a
round which claims this number states the corpus it measured, and a round that
adds a node kind between a SELECT list and a join adds it to that corpus.

**Five sites, each a specific thing a pass was getting wrong.**

- **A deleted stage keeps its name in a FOURTH place.** `fuseScanShuffle`
  absorbs an `exchange-repartition` into the scan that feeds it and rewires
  `Dependencies`, `LeftDepStage` and `RightDepStage`. A chained or fused join
  names its build side in `ChainedJoinSpec.BuildDepStage`, which dispatch looks
  up in the `inputs` map by ID and which the rewire did not touch — `stage
  join-4 chained join 0: build dep "exchange-repartition-7" output not found`,
  at dispatch, on a query the broadcast lowering answers (#755). Only a scan
  with a projection or a pushed filter is absorbed, which is why the shape
  needs a DERIVED arm: a plain base-table arm's scan is pass-through and never
  fuses. `elideCoPartitionedExchanges` deletes stages the same way and had the
  same hole; it is repaired beside it rather than left as the next filing.

  A UNION ARM names its producer in `UnionArm.DepStage`, which is the same kind
  of second reference, and `ValidateNativeDAGShape` asserts that it equals the
  corresponding `Dependencies` entry — so a pass that rewires one and not the
  other builds a plan its own validator rejects. Both passes rewire it now.

  **No SQL shape reaches either loop, and one of them cannot.** A corpus of
  eight union shapes — unions of joins, of grouped aggregates, of derived
  arms, UNION and UNION ALL, with and without a join above — reaches neither,
  verified by panicking inside both loops and watching every shape pass. For
  `fuseScanShuffle` that is provable rather than incidental: a union stage
  lists its arms' producers in `Dependencies` (the invariant above), so it is
  one of the exchange's consumers, and condition 4 admits only `hash_join`,
  `sort_merge_join` and `final_aggregate` — the fusion always declines. For
  `elideCoPartitionedExchanges`, which has no consumer-type condition, the
  loop is reachable and no SQL shape was found that reaches it.

  So the fixtures are WHITE-BOX and drive the passes directly:
  `physical.TestFuseScanShuffleDeclinesAUnionArmsExchange` pins the decline and
  its reason, so a future widening of condition 4 that admits a union fails
  there rather than relying on a rewire nothing ever ran; and
  `physical.TestElideCoPartitionedExchangeRewiresAUnionArm` constructs the
  reachable shape and asserts the arm moved with its dependency — removing the
  rewiring fails it. An SQL test that attempts nothing is worse than no test,
  and the first version of this fixture was one.

- **A QUALIFIED reference resolves against a column under a DIFFERENT
  qualifier.** `QualifyAllBuildCols` renames EVERY build column to the build's
  TABLE alias, so a CTE or derived table on the build side of a self-join
  publishes `decpair.dv` while every consumer above the join spells it `c.dv`.
  `exec.columnIndexFallback` and `expr.ResolveColumnRef` tried the exact
  spelling, then the bare one, and returned — never reaching the suffix scan
  that the BARE spelling of the same reference already used. Each consumer then
  failed its own way: a filter was UNKNOWN on every row (`WHERE c.dv > 1` above
  a two-join chain answered 0 where PostgreSQL answers 6, silently, #762), a
  projection and a sort key failed loudly, an aggregate refused its input.
  `physical.columnResolves` has compared a reference against a qualified column
  by its bare part since #656, so the planner's checker believed in a
  resolution the evaluator did not implement; closing it removes a
  disagreement rather than adding a special case. It stays a LAST RESORT and
  UNAMBIGUOUS ONLY — two arms that both spell `.w` decline and keep the loud
  failure instead of guessing an arm.

- **A derived arm's COMPUTED column has to be materialized wherever it is
  computable, not only on a scan.** `absorbComputedSubqueryProjection` handled
  exactly `Project → Filter* → Scan`. Above a WINDOW the value is computed over
  the window's output SLOT (`__win_0 + 0`), which no scan has, and the window
  stage emits the slot and nothing named by the user's alias — so the name was
  free for the other arm of the join to satisfy, which is #742's silent face
  (`x.w` answered `z`'s `a * 3`) and, with the arms' aliases spelled
  differently, its loud one (`column "p.w1" does not exist`). A window stage's
  `OpProject` runs ABOVE the operator (#656 shape g), so the alias is
  computable exactly there. A rename-only Project BELOW the computing one is
  now walked through and substituted away, which is `A/nestedproj`'s whole
  class.

  Two things this needed, and both were wrong in the first cut:

  - **The declaration is read against the schema the expression NOW names.**
    A respelled reference reads the SOURCE column, which the rename's own
    output schema does not declare, so the type fell to the float rule and the
    fragment tried to store a DECIMAL's rendering into a float vector. Same
    repair and same helpers as `attachScanSelectProjections`' #387 branch, with
    the FILTER nodes between the Projects stripped — neither emits a stage and
    the substitution walks through both.
  - **A window SLOT has no catalog column to read, and leaving the type
    unknown is not the safe side.** A projection whose type the plan does not
    state answers NULL to the AGGREGATE above it (`SUM(c.dv)` came back NULL
    and its HAVING admitted no row) even where the same column PROJECTS
    correctly. `windowSpecOutputType` is the stage's own answer for the slot,
    DECIMAL (p,s) included, so the slot is declared here exactly as the window
    stage declares it.

  And it declines rather than guesses: an expression naming something the
  target's stream does not carry, a subtree with two window stages, a stage
  that already carries a projection — all keep today's behaviour.

- **A scan's SHIPPED set is a narrowing too.** `pruneScanOutputColumns` sets
  `OutputColumns` from what a consumer DECLARED it wanted, and it runs before
  `attachScanSelectProjections` resolves the outer SELECT list back to a
  SOURCE column — so a column the scan reads for its own pushed filter and does
  not ship is exactly what the projection above the join then asks for
  (`column "a" does not exist in the input schema`, #766). `widenNarrowing-
  StagesBelow` now puts such a name back into what the scan SHIPS. The READ set
  is untouched, because widening THAT would change what is scanned; only a name
  the scan already reads is restored. `subtreeProducedColumns` reads a scan's
  read set for the same reason — a scan's `Columns` is the one column list that
  IS evidence of production — without which the pass declines at the join above
  and then widens a scan nothing downstream carries.

- **A qualified reference resolves through the arm that owns the qualifier.**
  `resolveRenameSource`'s join case recursed into both arms and took the first
  substitution, and `attachScanSelectProjections`' qualified→bare fallback
  dropped the qualifier entirely. With two arms publishing `w` that is the
  OTHER arm's column: `SELECT p.w, q.w FROM (…SUM(a) OVER () AS w) p JOIN t y …
  JOIN (…a * 3 AS w) q` projected p's window slot under BOTH names, on every
  execution path. `subtreeNamesRelation` — the scope test `derivedScopeBareName`
  already uses, which reads a derived table's stamped scan alias and a CTE's
  subtree-root name — picks the arm, and only when exactly ONE arm answers to
  the qualifier. Neither or both keep the old walk, which resolves nothing new.

**A refusal is the answer where a value is not.** `assertCarrierSchemaResolves`
now wraps `ErrUnreachableGatherOutput` like every other check in
`carrier_assert.go`, and `ExecuteSQL` asks `ValidateNativeDAGShape` once
directly after planning — where routing is still possible — so its refusals
reach the coordinator's local engine instead of the client (#763). The dead
end #763 records is real and is why the wrap alone was not enough: the check
runs at DISPATCH and the routing block reads the PLANNING error. Only the
SENTINEL routes; a validation error that is not one keeps today's path and
still surfaces from `executeStageDAG`, which runs the same check.

`assertJoinFiltersAreBacked` asks its weak question of a join's PROJECTION as
well as its filter, which is what makes the two lowerings of #766's first shape
AGREE: the broadcast plan was already refused (its gather still renamed from
the alias, so `assertGatherOutputIsReachable` saw it) and answered locally,
while the shuffled plan attached the projection to the join instead, satisfied
that check, and failed inside the fragment.

**The plan-identity claim, and the gate that can now support it.**
`TestTPCHStageDumpGolden` recorded `Stage.Columns` and nothing else, so the
field this work widens — `Stage.OutputColumns` — was invisible to it, as were
a chained link's own `Columns` and the build dependency a rewiring pass can
leave dangling. It now records all four. Regenerating the golden on `376b2cac`
and on this tip produces BYTE-IDENTICAL files for all 22 queries: no TPC-H
stage gains a column, a shipped column, a chained-link column or a different
build dep, and none gains a projection (the `ops=` counter already carried
that). The claim was true before and the citation did not support it; now it
does.

**What the gates carry.** `coordinator.TestDerivedArmsAboveAJoinChainThree-
Arms` is #755's four shapes, #753's join-of-joins shapes and #766's projecting
shapes with their COUNT twins as controls, on three arms against live
PostgreSQL 17 values; `TestSiblingWindowSubqueriesUnderAJoinKeepTheirOwnValues`
gains #742's shape, its MIRROR (`z.w` asked for while `x.w` is present, which
is what says the repair is scoped and not merely reordered), both arms
projected at once, and the single-arm control; the two chain gates lose the
`shuffledRefuses` (#755), `refusesLoudly` (#763) and `pinDAG` (#762) pins and
assert PostgreSQL's values instead; `postgresJoinArmCases` asks the same
questions of a live server on the single-process arm.
`TestBothArmsPublishOneAliasProjectBothThreeArms` gains the CTE-arm-filter
family (the qualified and bare predicate spellings × the CTE first/middle/last
× a computed and a plain-rename body × chains of 3 and 4 × the build-arm-only
projection × a LIMIT wrapper, with the derived-table spelling and a
no-collision `WITH k` beside them as the controls that bound it); and
`TestACTEsQualifiedColumnKeepsItsOwnArmThreeArms` is the sibling-binding cell
with each column projected alone and a `distinct-names` control.

**The mutation count, re-derived rather than restated.** Every source file the
arc touches — thirteen of them, `internal/coordinator/coordinator.go`,
`internal/engine/exec/{aggregate,join}.go`, `internal/engine/expr/expr.go`,
`internal/planner/logical/optimizer.go` and eight under
`internal/planner/physical/` — reverted to its `376b2cac` content with this
tip's TESTS in place: **70 leaf subtests fail, across ten coordinator tests
plus `physical.TestElideCoPartitionedExchangeRewiresAUnionArm`** (which fails
at its top level and has no subtests). Zero fail on the tip. Counted with
`grep -c '^    --- FAIL'` on `go test -v`; per test:
`BothArmsPublishOneAlias…` 15, `CTEChainPositionCarriesItsFilter…` 13,
`DerivedArmsAboveAJoinChain…` 8, `ACTEsQualifiedColumnKeepsItsOwnArm…` 8,
`SiblingWindowSubqueriesUnderAJoin…` 6, `RowFieldPathSurvivesAJoin…` 6,
`QualifierCollidingWithAColumnNameResolves` 5,
`CTEComputedColumnAboveAJoinChain…` 5, `FilterQualifiedToOneJoinArmTwoPath` 2,
`CTEFilterAboveAJoinChain…` 2. `TestTPCHStageDumpGolden` PASSES in that tree,
which is the byte-identical claim above checked from the other direction.
(Earlier figures in this ADR — 22, 37, 42, 46, 47 — were each true of the gate
set of their own round and were restated as if they were the current one; 47
was also short by two, the review's re-derivation being 49 across nine because
the `OrAcrossArms` pin entries belong in the count. Each fix ROUND adds gates,
so the number is only meaningful with the tree and the gate set it was measured
on, both of which this paragraph names.)

**What a same-alias fixture must not also be about.** Every same-alias pair in
these gates publishes at DECIMAL scale 2 on purpose. A cross-SCALE pair
(`SUM(b) OVER ()` beside `a * 3`) renders one arm at the OTHER arm's typmod on
the single-process path — right digits, wrong scale — and that is #754, which
would otherwise fail these entries for a reason that has nothing to do with
join-output resolution. One entry carries #766's literal cross-scale spelling
with the single arm PINNED at the wrong rendering, so #754's fix is what
deletes it.

### A chained LINK'S list narrows the joined stream, not the probe's (2026-08-31, #755 round 2, #762)

The rewiring above makes a chain over derived arms RUNNABLE where it used to
be refused at dispatch, and what those plans then ran was #762's silent zero.
That has to be said plainly, because the first draft of this section said the
shape was "pre-existing" and answered zero on `376b2cac`: it does not. On
`376b2cac` the SHUFFLED arm REFUSES it (`chained join 0: build dep
"exchange-repartition-7" output not found`) and the client gets an error. The
rewiring turned a loud refusal into a silent wrong answer, and the repair
below is what makes it an ANSWER.

    WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
    SELECT COUNT(*) FROM decpair t JOIN decpair u ON t.id = u.id
    JOIN c ON c.id = t.id WHERE c.dv > 1
    -- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG shuffled 0

`probeSideChainRefs` classifies a chained link's residual-filter references and
drops the ones the link's own BUILD input supplies. That is right for
`Stage.Columns` — the primary probe's OutputFilter, applied before the build
enters — and wrong for `ChainedJoinSpec.Columns`, which narrows the JOINED
stream that the link's `OpFilter` then reads. The fragment runs

    primary probe (OutputFilter = Stage.Columns)
      → link 0 (OutputFilter = ChainedJoins[0].Columns) → OpFilter(link 0)
      → link 1 → … → PostFilter(Stage.FilterExprs)

so everything evaluated AT OR AFTER link k has to survive `ChainedJoins[k].
Columns`, wherever its value came from. Each link now takes its own filter's
refs, every later link's, and the stage's own PostFilter and projection.

**Three spellings of one query disagreed, and the two that were right say what
the trigger is.** A RENAME body is correct because the pruner resolves `dv`
back to `a` and the link's list — the absorbed join's `NeededColumns` — already
carried it; a COMPUTED body publishes `dv` and the re-spelled predicate reads
`a`, which nothing put there. The DERIVED spelling is correct because the
predicate is pushed INTO the arm's scan, which a CTE's Project declines as a
materialization fence. So the cell is CTE × computed × any position but first ×
shuffled, and `TestCTEChainPositionCarriesItsFilterThreeArms` is that cell
crossed with 2/3/4-join chains, the qualified and bare predicate spellings, the
three numeric bodies of the fixture, and a probe table — `IS NULL`,
`IS NOT NULL`, a range, a disjunct with a base-table term, `MIN` beside
`COUNT`, a projection, `GROUP BY` — because a COUNT alone cannot tell a
DROPPED predicate from one that is UNKNOWN, and those seven do.

### A ROW FIELD PATH is not a column, in the three lists that narrow (2026-08-31)

ADR-0022 says `c_row.b` is not a column reference: the QUALIFIER is the column
and the expression compiler resolves the field out of the ROW. Three narrowing
lists spelled it as if it were a column and dropped the container with it —
the logical pruner's join partition (neither side publishes the dotted name, so
the need went to NEITHER side and no scan read `c_row` at all), the join
stage's `Columns` on the DAG, and the single-process probe's `OutputFilter`.

    SELECT x.id, c_row.b FROM typemx_nested x JOIN typemx_nested y ON x.id = y.id
    -- the true field values are 0, 11, NULL, NULL, 44
    -- 376b2cac: single all-NULL, both DAG arms `column "c_row.b" does not
    --   exist in the input schema`

The same query with NO join is correct on `376b2cac`, because nothing narrows
there — which is what says the lists are the site. Each is repaired where it
narrows: the pruner pushes the CONTAINER to whichever side publishes it, the
join's evaluated-column pass expands a dotted reference the plan does not
PRODUCE into its qualifier (asked of producers only — an exchange manifest
listing `c_row.b` is not evidence that anything computes it), and the
single-process probe's OutputFilter carries the qualifier of every dotted
entry, which is free because an OutputFilter can only narrow.

`expr.ResolveColumnRef` gets the matching half: a container is looked up with
the same qualified fallback the scalar branches use, so `c_row.b` finds
`x.c_row` after a join has qualified it, and a reference whose qualifier names
a ROW column resolves as a FIELD PATH or as NOTHING — never as some other
column that happens to share the field's name. That guard is what the new
last-resort suffix scan needed: without it a field path whose container had
been pruned could bind to a scalar, which is ADR-0022's violation exactly.

**The guard is on the ROW-ness, not on the lookup succeeding, and getting that
backwards cost a wrong answer.** Written as "the qualifier resolved to
something, so refuse", it was reached only where the qualifier named a column
that is NOT a ROW — the ROW arm returns above it — so every qualified
reference whose qualifier collided with an ordinary scalar column name was
refused instead:

    WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
    SELECT COUNT(*) FROM decpair t
    JOIN (SELECT id, b AS c FROM decpair) z ON z.id = t.id
    JOIN c ON c.id = t.id WHERE c.dv > 1
    -- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG shuffled 0

`rowColumnNamed` is the whole of the refusal: it finds a ROW spelled
`<name>` or `<qualifier>.<name>`, which is the only case a field path must not
fall through to the suffix scan. `TestQualifierCollidingWithAColumnName-
Resolves` carries the shape with the non-colliding arm and the bare predicate
spelling as its controls.

**And two ROW columns spelled alike do NOT decline — they pick the exactly-
spelled one.** An earlier statement of this said otherwise. `x JOIN x` puts a
`c_row` on both sides; the resolver's exact-name lookup wins before the
qualified fallback is reached, so `c_row.b` reads ONE of the containers,
deterministically. PostgreSQL rejects that query outright (no FROM-clause entry
for `c_row`), so this is superset territory and the pick is recorded as a
fixture rather than described as a refusal. Only when NEITHER container is
spelled bare does the ambiguity actually decline.

**Which one it picks was written down without a fixture that could tell.** An
earlier cut of this ADR said "the probe side", and the fixture for the claim
was byte-identical SQL to the self-join entry beside it — the same table on
both sides, the same values in both containers, so it passed whichever way the
resolver went. That is the third can't-fail fixture this arc produced and the
second in this section. The discriminating shape SHIFTS the second arm's rows,
so the two containers differ at every row:

    SELECT x.id AS xid, c_row.b AS fb FROM typemx_nested x
    JOIN (SELECT id + 1 AS id, c_row FROM typemx_nested) y ON x.id = y.id
    WHERE x.id < 5 ORDER BY x.id
    -- x's own c_row.b at ids 1-4 is 11, NULL, NULL, 44
    -- answered:                      0,  11,   NULL, NULL   — the DERIVED arm's

It answers the DERIVED arm's container, and it answers the SAME with the FROM
order swapped. `SELECT *` over either spelling shows why: both produce one
stream carrying `c_row` (the derived arm's, left BARE) and `x.c_row` (the base
table's, qualified by the join), so what the exact-name lookup takes is the
container nothing qualified — not "the probe side", which is a property the
fixture could not have observed and which the planner's own side assignment
does not preserve. Both orders are fixtures now, with the join-free reading of
the same ids beside them as the control that tells the two containers apart.

Wadjet answers these queries where PostgreSQL rejects them (`c_row` is not a
FROM-clause entry). That is the deliberate superset ADR-0012 records, and it is
gated as such: `TestRowFieldPathSurvivesAJoinThreeArms` asserts the field's own
values, with the join-free spelling beside it as the control the join must
agree with.

### A join's OutputFilter needs BOTH directions of the same fallback (2026-08-31, #742)

The third list with only one half of the qualified/bare asymmetry is the
join's own OutputFilter. `joinOutputSchemaWithMapping` kept a QUALIFIED stream
column when the filter named it bare — that is Q07's `n2.n_name` under a
filter asking for `n_name` — and had no mirror. A derived arm that is the
PROBE ships its column BARE, because nothing qualified it, while the consumer
above spells it the way the query wrote it:

    SELECT x.id, x.w, y.w FROM (SELECT id, a AS w FROM decpair) x
    JOIN (SELECT id, b * 100 AS w FROM decpair) y ON x.id = y.id
    JOIN decpair u ON x.id = u.id WHERE x.w > 1
    -- PostgreSQL 5 rows · single 5 · DAG broadcast 5
    -- DAG shuffled  ERROR  column "y.w" does not exist in the input schema

The filter matched neither spelling, the column was dropped, and the
projection above failed at dispatch. On `376b2cac` the same query answered x's
`w` under BOTH names — #742's capture — so the carrier work turned a silent
capture into a loud failure and this turns it into the answer. Keeping a
column the filter did not name exactly costs bytes and never an answer, which
is why the mirror is unconditional; the stage-dump golden is unchanged,
because this is the RUNTIME application of a list the plan already had.

It also closed a divergence pinned SEPARATELY, which is the evidence that the
missing mirror was the mechanism and not a patch: `TestFilterQualifiedToOne-
JoinArmTwoPath`'s `AliasCollidesWithBuildColumn` rows, and the
`TestBuildSideRefWithCollidingProbeAliasIsASeparateDefect` test written to
isolate them, pinned `d.k > 3 OR c.g > 100` answering 0 on the DAG whenever the
PROBE arm published an alias equal to the build column's name. Attributed by
reverting each candidate on its own: the mirror alone makes that shape answer,
and the resolver's ROW-guard repair alone does not. Both pins are deleted and
the isolating test with them.

### A scope walk descends through the wrappers a join wears (2026-08-31, #742 round 4)

`attachScanSelectProjections` resolves a qualified SELECT-list item inside the
subtree its qualifier names — that is `resolveRenameSourceInScope`, and it is
what stopped `y.w` resolving through x's Project. `relationScopeSubtree` finds
that subtree by descending the JOIN tree, and it descended NOTHING else: the
first node that was not a two-arm join ended the walk, because a Project there
is the scope's own SELECT list and must not be walked past. A FILTER is not,
and a filter is exactly what a CTE arm puts there:

    WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
    SELECT x.id AS xid, x.w AS xw, y.w AS yw
    FROM (SELECT id, a AS w FROM decpair) x
    JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
    JOIN c ON c.id = x.id WHERE c.dv > 1 ORDER BY x.id
    -- PostgreSQL 5 rows of 12.75 | 1275.00
    -- 376b2cac  shuffled REFUSED at dispatch (chained join build dep)
    -- round 3   shuffled answered 12.75 | 12.75 — x's w under BOTH names

The DERIVED spelling of the identical query is right, and that pair is the
whole diagnosis: a CTE's Project is a materialization fence, so the predicate
cannot be pushed into the arm and stays as a `Filter` between the outer Project
and the join, where the derived spelling pushes it into the arm's own scan and
leaves the join directly below. With the Filter there the walk returned the
WHOLE join subtree as `y`'s "scope" and the caller's bare lookup took the first
arm that answered — the OTHER arm's column, which is #742 restated one node up.

`Filter`, `Sort`, `Limit` and `Distinct` leave both the arm set and the output
schema alone, so the walk descends through them; `Project`, `Aggregate`,
`Window` and the set operations do not, and it still stops at those. In the
plan the fix shows as `join-4 PROJ=[… {a yw}]` becoming `PROJ=[… {y.w yw}]`,
which is the derived spelling's plan exactly.

### A WINDOW is scope-preserving, and its ARGUMENT is not scope-free (2026-08-31, #742 round 4)

The walk above lists the nodes it may descend through, and the list was one
short. `resolveRenameSource` — the other walk in the same file, over the same
tree, answering the other half of the same question — descends through every
single-child node it has no case for, and that includes a Window. So the two
disagreed about what a scope is for exactly one node kind, and a window in the
SELECT list is what puts that node between the outer Project and the join:

    SELECT x.id, x.w, y.w, SUM(y.w) OVER () AS s
    FROM (SELECT id, a AS w FROM decpair) x
    JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
    -- PostgreSQL  yw = 1275.00 · both DAG arms answered yw = 12.75

A Window APPENDS its output columns to its child's schema: it renames nothing
and every relation below it keeps the name the enclosing query calls it, which
is the whole of the test. An AGGREGATE fails that test — its output schema is
its own GROUP BY keys and aggregate outputs, so a bare name resolved below it
answers from a schema the stream does not carry — and it stays a stop, in both
walks. A set operation is never a candidate: two arms, and the output naming is
re-rooted onto the first.

The agreement is asserted now instead of argued.
`physical.TestScopePreservingWrapperMatchesTheRenameWalk` runs the two walks
over EVERY logical node type, with a completeness check that fails when the
logical package gains one — because this omission was invisible to every gate
in the tree and was found by a review widening its corpus, and the same
omission is available to the next node kind. Removing `NodeWindow` from the
list fails it; and its own first cut was wrong in the way METHOD 10 predicts —
it built a one-child Join and a one-child Union, which no plan contains, and
reported a disagreement about four node types that does not exist.

**The window's ARGUMENT is a second site, and the whitelist does not reach it.**
`cleanExpr` strips a window argument's table qualifier unconditionally, so
`SUM(y.w) OVER ()` reaches the operator as `SUM(w)` and binds whichever arm's
copy the stream spells BARE. That is right almost everywhere and a coin toss
where two arms of a join publish one alias — and the two execution paths land
on opposite sides of it, because they name a join's duplicate columns
differently:

    -- single-process join output: `w` (probe x's) and `y.w`
    -- DAG join output:            `w` (build y's) and `a`  (x's source column)
    SUM(y.w) OVER ()   single 52.99 (Σ x.w)      · DAG correct
    SUM(x.w) OVER ()   single correct            · DAG 5300.00 (Σ y.w)

So the qualifier is kept exactly where it is load-bearing —
`windowArgKeepsItsQualifier`: the qualifier names a relation of the window's
input AND more than one arm publishes the bare name — and is dropped as before
everywhere else, which leaves every existing plan untouched. Keeping it is
enough on the single-process path and for the DAG's build arm, because
`exec.Window` already runs `columnIndexFallback` over `InputCol`. It is not
enough for the DAG's PROBE arm, whose column the streams carry under its SOURCE
name: `walkStages` re-spells it, and `derivedAliasSourceColumn` stops at a Join
so it answered nothing for a qualified argument. `windowArgSourceInScope`
scopes it first — the same composition `resolveRenameSourceInScope` performs for
a projection — and the argument reaches the worker as `a`.

`coordinator.TestAWindowBetweenTheSelectListAndItsJoinThreeArms` is both sites
on three arms: the window over each arm's column in turn and both projected
beside it, MIN/MAX (the value-function branch reads the argument separately),
`PARTITION BY` on the colliding alias, `ROW_NUMBER` with no argument at all, the
CTE-arm and base-self-join spellings — the self-join SHIFTED by one id, because
an unshifted one gives both arms the same number and passes whichever way the
argument binds — and three controls: the window removed, the arms publishing
different aliases, and the window in an OUTER query over a subquery that does
the join. It also carries the aggregate-side fixture METHOD 10 asks for beside
the "Aggregate is deliberately not in the list" claim: two
aggregate-between-the-SELECT-list-and-the-join shapes, which today are correct
or LOUD on every arm and never silently wrong, pinned so that the day one of
them starts answering, it has to answer PostgreSQL's values.

**The two sites ship in ONE commit, and the per-site reverts are the substitute
for two.** They cannot be separated in a green tree: the projection entries
assert the window's own value, so a fixture that gates one site alone has to
stop asserting the other's column, and a weaker fixture is the wrong price for
a tidier history. What a split commit would have bought — each mechanism
attributed to its own gate rows — is bought instead by reverting each site on
its own: the whitelist entry alone fails 8 leaf subtests (7 of the gate plus
the walk-agreement test's `Window` case) and the argument scoping alone fails 9,
including the base-self-join and MIN/MAX entries the whitelist does not touch.
No control fails under either. A future reader asking "which of these two did
what" should run those two reverts, not read the diff.

### A join arm answers to the name the QUERY calls it (2026-08-31, #742 round 4, #753)

`findScanAlias` walks to the scan under a join arm and returns its alias — the
name a base table and a DERIVED table both answer to, because
`BuildFromTable`'s `setSubtreeAlias` stamps a derived alias onto every scan
below it. A CTE reference records its name on the SUBTREE ROOT instead
(`Node.CTEName`, `Node.CTERefAlias`), deliberately, so two relations comma-
joined inside a CTE body keep separate identities. Reading only the scan
returned the CTE's underlying TABLE, and the join then qualified that arm's
duplicate columns with a name no reference in the query is written against:

    WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
    SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv
    FROM (SELECT id, b - 100 AS dv FROM decpair) p
    JOIN decpair x ON p.id = x.id JOIN c ON c.id = p.id
    JOIN decpair y ON c.id = y.id ORDER BY x.id
    -- PostgreSQL 17  cdv 25.50, pdv -87.2500 — two different columns
    -- 376b2cac  single and DAG: cdv == pdv; shuffled REFUSED at dispatch
    -- round 3   shuffled answered cdv == pdv too

The stream carried `dv` (p's, bare) and `decpair.dv` (c's, renamed by
`QualifyAllBuildCols`). `c.dv` matched neither exactly, fell through to
`ResolveColumnRef`'s qualifier strip, and bound the SIBLING arm's bare column.
No runtime rule can decide that one: `p.dv` reaches the same bare column by the
same route and is CORRECT, so the two are indistinguishable from the batch
schema alone. The information is the plan's, and `joinArmAlias` is where the
plan says it — the arm is named `c`, the exact match wins, and `p.dv` keeps the
bare-strip path it already took.

It closes the first residual this section used to list. Two sibling CTEs
publishing one alias collapsed to the first arm's value on the SINGLE-process
path (#753, pinned in `coordinator.TestSiblingWindowSubqueriesUnderAJoinKeep-
TheirOwnValues` and in the PostgreSQL oracle's `JoinArmSiblingWindowsInCTEs`)
for exactly this reason: `q.w` matched neither p's bare `w` nor the
`decpair.w` the join had renamed q's to. Both pins are deleted, and the oracle
pin's own failure — *"Wadjet now agrees with PostgreSQL, so this known
divergence is FIXED"* — is what says so rather than a claim here.

The alias reaches four places (`Stage.BuildTableAlias`, `HashJoin.BuildTable-
Alias` on the single-process and the semi/anti-swapped paths, the sort-merge
join's, and the compile-check of an outer join's ON residual), and all four
take it from `joinArmAlias` so the two paths cannot disagree about what an arm
is called. `TestTPCHStageDumpGolden` is byte-identical: no TPC-H stage's shape
moves, and Q15 — the one TPC-H query with a CTE, joined on its build side —
answers as it did.

**What is NOT closed, measured rather than assumed.** Nine residuals survive
the sweep, and none of them is this mechanism. Each is stated with where it
reproduces, re-measured on this tip against `376b2cac` and a live PostgreSQL 17:

- **an arm that is ITSELF A JOIN is still captured, in both spellings and on
  both paths** — and the CTE half of it is the shape the arm-naming section
  above cites as the REASON a CTE stamps its name on the subtree root. Both
  halves of that citation are true and they do not meet. The two faces are one
  mechanism seen from opposite sides:

      -- the CTE spelling, wrong on both DAG arms, right on single
      WITH c AS (SELECT m.id AS id, m.a * 2 AS dv
                 FROM decpair m, decpair n WHERE m.id = n.id)
      SELECT p.id, p.dv, c.dv
      FROM (SELECT id, b - 100 AS dv FROM decpair) p JOIN c ON c.id = p.id
      -- PostgreSQL  c.dv 25.50 · p.dv -87.2500 · both DAG arms c.dv == p.dv

      -- the DERIVED spelling, wrong on SINGLE, right on both DAG arms (#773)
      SELECT t.id, t.w, m.w
      FROM (SELECT id, a AS w FROM decpair) t
      JOIN (SELECT g.id AS id, g.w AS w
            FROM (SELECT id, a * 100 AS w FROM decpair) g
            JOIN decpair h ON g.id = h.id) m ON t.id = m.id
      -- PostgreSQL and both DAG arms  m.w 1275.00 · single  m.w 12.75 (t's)

  A one-relation body is correct on all three arms in both spellings, and the
  comma-join and explicit-JOIN bodies fail identically, so it is the arm being
  a JOIN and nothing about the comma or the CTE keyword. Naming the arm `c`
  fixed the single path for the CTE spelling and is not sufficient where that
  arm lowers to stages of its own; the derived spelling is the mirror, and the
  distinct-alias control is clean in both. Base-identical, filed as #773;
- **the two window sites COMPOSING** — a window in the SELECT list above a join
  one of whose ARMS is itself a window (#772). Each site is fixed alone and the
  nesting is not:

      SELECT p.id, p.w, q.w, SUM(q.w) OVER () AS s
      FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) p
      JOIN (SELECT id, a * 100 AS w FROM decpair) q ON p.id = q.id
      -- PostgreSQL  p.w 52.99 · s 5299.00
      -- both DAG arms  p.w 5299.00 (q's window under p's name)
      -- the SUM(p.w) spelling  p.w and s both NULL on both DAG arms

  The identical query WITHOUT the outer window is correct on all three arms
  after this arc — it was wrong on both DAG arms before — and the distinct-alias
  spelling is clean, which is what places the residual at the composition and
  not at either site. Base-identical;
- a CTE that SHADOWS a base table's name cannot reference the table in its own
  body — `WITH decpair AS (SELECT … FROM decpair)` resolves the inner `FROM` to
  the CTE itself. Loud on every arm, pre-existing on `376b2cac`, and filed as
  #771;
- a SIBLING nested inside a sibling answers the OUTER sibling's window on the
  single-process path (#751). Both DAG arms are right, so the wrong side is the
  local engine's slot allocation and not the aliasing of scopes — which is why
  `joinArmAlias` above closed #753's CTE face and left this one. Pinned in
  `TestSiblingWindowSubqueriesUnderAJoinKeepTheirOwnValues`;
- two arms publishing one alias make the single-process path render one at the
  other's DECIMAL scale (#754). Right digits, wrong typmod: `x.w` at scale 2
  beside `y.w` at scale 4 prints `12.7500`. A DECIMAL/FLOAT pair under one
  alias does NOT refuse — an earlier statement of this said it did, measured
  against a shape that no longer reaches the refusal — it renders the FLOAT at
  the DECIMAL's scale on the single arm (`1.50` for `1.5`) and is right on both
  DAG arms. Pinned per entry in the two `…OneAlias…` gates;
- `GROUP BY` on a QUALIFIED derived or CTE alias collapses to one NULL group on
  both DAG arms. The scope is WIDER than an earlier statement of it: not only
  ONE join in the DERIVED spelling, but also the CTE spelling over a three-join
  chain, and identically on `376b2cac` on the arms where that shape runs. The
  BARE spelling is correct everywhere, so it is the qualified group KEY's own
  resolution. Pinned in `TestCTEChainPositionCarriesItsFilterThreeArms`, so the
  day it agrees the gate fails;
- `MIN`/`SUM` of a derived DECIMAL alias on the shuffled arm refuses the
  shuffle read — `column "m" is DECIMAL … but FLOAT64 in an earlier file of the
  same stage` — because a task whose partition is empty declares the aggregate
  from `AggSpec.OutputType`'s float fallback and a task with rows declares it
  from the vector. It reproduces on `376b2cac` for the CTE-FIRST and the
  ONE-JOIN spellings, which nothing here touches; the chain shapes newly REACH
  it because their filter stopped emptying every task. The gate carries the
  FLOAT body for that probe and names the DECIMAL one here;
- an OUTER join whose `COUNT(*)` reads `__rowcount_only__` above a derived arm
  with a computed column fails at the scan, on both DAG arms, identically on
  `376b2cac`. It belongs to the row-count sentinel rather than to name
  resolution;
- `DISTINCT` over a join whose two arms publish one alias drops the BUILD arm's
  column on both DAG arms — two rows with a NULL where PostgreSQL and the
  single-process path have five (#770). Identical on `376b2cac` and here; the
  UNION spelling of the same dedup takes a different path, which is where the
  localization starts.

**The loud→silent census on this tip is ONE arm-row, and it is the GROUP-BY
residual above.** That is the number this section has to carry, and it has been
wrong twice, both times because the corpus it was measured on was narrower than
the mechanism. Round 3 claimed 1 without measuring; the review measured 18 —
fifteen of the CTE-arm capture, two of the sibling-arm binding, and this one.
Round 4 fixed those two and re-measured 1; the next review put a WINDOW between
the SELECT list and the join and measured 11, ten of them that family. Both
mechanisms are fixed above and the corpus now carries the shapes that found
them, which is what the number is worth.

The remaining cell is `WITH c AS (…) SELECT c.dv, COUNT(*) … GROUP BY c.dv`
over a three-join chain, whose shuffled lowering `376b2cac` refused at dispatch
and which now collapses to the same one NULL group the broadcast lowering has
always answered. It is the qualified group KEY, it is pinned, and both DAG arms
now agree about it rather than one of them being loud by accident of a
different defect.

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
