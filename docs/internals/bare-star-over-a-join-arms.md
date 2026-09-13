# A bare star over a join is the FROM clause's arms

Source: internal/planner/logical/star_join_order.go — joinStarColumns,
ElideUnstatedJoinStar, ResolveStarJoinOrdinalSortKeys; the mint is in
builder.go's `BuildFromSelect` (#997, #1012, #993, arc O1, 2026-09-13)

PostgreSQL expands `SELECT *` over a join to every FROM arm's own output
list, left arm first, duplicate names kept BY POSITION and never qualified:

	SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id
	  →  id, order_id, product, amount, id, order_id, product, amount

Wadjet published the JOIN OPERATOR's stream instead. `exec.joinOutput
SchemaWithMapping` emits the probe side's columns, then the build side's with
every DUPLICATE name qualified by its owning alias — and which side probes is
a COST decision. `logical.reorderJoins` swaps a two-relation chain by
estimated rows and `costBasedJoinReorder` rebuilds a longer one, so the SAME
statement published a different relation as the data moved:

  - `SELECT * FROM t a JOIN t b ON a.id = b.id` published `…, b.id, …` with
    no predicate and `…, a.id, …` under `WHERE a.id < 100`, a predicate that
    changes no row (#997);
  - `SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id` published
    `i`'s four columns before `o`'s three (#1012);
  - through a derived block the shuffle arm qualified the outer relation's
    non-key columns where the broadcast arm published them bare, because
    `markCoPathingSelfJoinBuilds` reads each join's build dependency out of
    the ARM's stage DAG (#993).

A client keys on column LABELS — JDBC by label, DataGrip, Superset — so a
list that moves with an estimate is a different relation to them.

## The rule

The star is expanded into the FROM clause's arms at Optimize step 1, BEFORE
any pass that reorders a join, and each item is a QUALIFIED reference
published under the column's own name. The projection that carries them is
what permutes the executed stream back into the query's order wherever the
plan chose to probe. The join operator's qualification survives underneath as
a RESOLUTION spelling — ADR-0026 §2's pair of names, applied to a join's
output — and `expr.ResolveColumnRef` binds a qualified item to its own
relation's column whichever side built: exactly, when the join qualified that
side, and through the qualifier-stripping fallback when it did not.

`SELECT o.*, i.*` — the same list spelled by hand — already answered
PostgreSQL on all five arms before this arc. That is the evidence the
machinery below the expansion was right and only the bare star's list was
missing.

## Why a BUILD-SIDE MARK on the join node cannot state it

The filings proposed recording which side builds as a property of the join
node, leaving the children in written order. It does not complete.
`reorderJoins` only SWAPS the two children of a TWO-relation chain; for three
or more it calls `costBasedJoinReorder`, which REBUILDS the tree. The written
FROM order is then not a permutation of one node's children but a SHAPE that
no longer exists in the plan, so there is nothing left for a mark to be
relative to. The order has to be recorded where it is still written down,
which is the star's own expansion.

## Where the projection goes, and why it is ABOVE the ORDER BY

`isStarOnly` builds no Project for a bare star, because the star selects its
input unchanged — a claim that holds for a scan and fails for a join, whose
stream is the plan's order. `BuildFromSelect` therefore mints one, and mints
it ABOVE the ORDER BY and the LIMIT, which is where an OUTPUT permutation
belongs:

  - the Sort reads the FROM clause's own namespace, where `i.id` names one
    column, so every sort-key resolver below sees the tree it saw before this
    projection existed. Placed BELOW the sort instead, the projection's
    published list (`id`, …, `id`, …) is what a written term resolves
    against, and `derivedAliasSourceColumn` bound `i.id` to the FIRST `id` —
    o's — which ties every row on the broadcast arms and sorts them
    arbitrarily. Measured, on `dag` and `dag-morsel4`.
  - a LIMIT is applied to the same rows either way, and a projection above it
    computes fewer.

The one term that addresses the OUTPUT list rather than the input is a
POSITIONAL one, and `ResolveStarJoinOrdinalSortKeys` answers it from the
expanded list, rewriting the key to the item's SOURCE expression (`i.id`, not
the published `id`). `ORDER BY 4` over a star join was refused before this arc
(42P10, "a star over a join … is left unexpanded") for a statement PostgreSQL
answers.

## The mint is a HYPOTHESIS

The builder cannot ask what an arm publishes — no scan is annotated when it
runs — so it mints the projection on SHAPE alone, and `ExpandStarProjections`
either states the arms or does not. `ElideUnstatedJoinStar` takes an unstated
node back out, restoring the tree the builder would have built, including the
naming the enclosing query stamped on a block's root (DerivedAlias, CTEName,
CTERefAlias) and the WITH list. Without it a shape whose arms cannot be
enumerated would reach `refuseUnexpandedStarAnywhere` and be refused, where
it used to answer.

## What it declines, and why the boundary is exactly there

Each of these keeps the answer it had. A PARTIAL expansion is never returned:
the star covers every arm or none of them.

  - **An arm that publishes one name TWICE.** Every item is a qualified
    reference, and `s.id` over a block publishing two `id`s binds the first —
    so the second column would carry the first's VALUES. A wrong value is
    worse than a wrong name. Closing it needs a block's column addressed by
    POSITION.
  - **Two arms of one name.** Both would expand to the same qualified
    reference. PostgreSQL refuses the spelling outright.
  - **A LATERAL arm, or a join carrying a manufactured lateral's lowering.**
    Its projection carries the correlation slot the join is about to drop
    (ADR-0026 §3c), and the arms are not the relations the query wrote.
  - **A SEMI or ANTI join.** It publishes its probe alone; no star spells one.
  - **A table function arm**, which has no catalog annotation to publish from.
  - **An arm that is an aggregate, a set operation, or a sorted or limited
    block with no projection of its own.** Its output is its own, not the
    scan's, and this walk cannot state it.
  - **An Aggregate, Window or set operation between the star and the join.**
    The emitted columns are that operator's.

## Gates

| gate | what it holds |
|---|---|
| `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` | the seam: 47 shapes × five arms against PostgreSQL 17.11 |
| `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms` | the same rule on the wire, names AND type OIDs |
| `logical.TestABareStarOverAJoinExpandsToTheFromClausesArms` | the list itself, per shape |
| `logical.TestABareStarOverAJoinDeclinesWhatItCannotState` | the declines above |
| `logical.TestAnUnstatedStarProjectionIsTakenBackOut` | the hypothesis, and the naming that travels with it |
| `logical.TestAPositionalSortKeyOverAStarJoinBindsItsItemsSource` | the ordinal, in the input's spelling |
