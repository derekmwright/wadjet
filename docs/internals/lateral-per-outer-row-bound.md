# A correlated body is evaluated per outer row (arc LT, ADR-0021 §1s)

The design record behind `logical.lateralBoundPerOuterRow`,
`logical.existsBodyIsReproduced`, the refusals in `publishLiftedRefs` and
`logical.RefuseContestedLiftedRefs`, and the physical slot-rename arm for a
QUALIFY filter. It states the RULE the code keys on, what it decorrelates,
what it evaluates per row, what it refuses, and what it measured.

## The question

PostgreSQL evaluates a correlated subquery body — an `EXISTS`, an `IN`, a
scalar subquery, a `LATERAL` item — once per outer row, with the outer row's
values bound. This engine replaces that, where it can, with ONE evaluation of
a body over the whole inner relation joined to the outer rows. The two agree
exactly when the body's result restricted to one outer row's key equals the
body evaluated with that row's values:

```
σ_{K = v}( Body′(R) )  ==  Body(R, outer = v)        for every outer value v
```

The implemented sufficient condition (amended 2026-09-24) is:

1. every correlated predicate is an EQUALITY between an inner-only expression
   and a bare outer column, restricting inner keys K; and
2. every operator in the body between that restriction and the body's output
   COMMUTES with `σ_{K=v}`. Filters and projections always do. A PIPELINE
   BREAKER does only when it is partitioned by K: an aggregate whose GROUP BY
   contains K, a DISTINCT whose key contains K, a window whose PARTITION BY
   contains K, and a bound (`ORDER BY … LIMIT / OFFSET`) that is a PER-K
   bound.

That property — **key-partitionability of the body's plan** — is what the
rule keys on. It is a property of the plan, not a list of spellings, and every
lowering this engine already had is an instance of MAKING a breaker
key-partitionable: the aggregated LATERAL injects K into the GROUP BY
(ADR-0021 §1h), a DISTINCT body carries K in its list, and the window refusal
(`refuseDecorrelatedWindow`) checks PARTITION BY for K.

## The decision

**The bound travels with the key.** A correlated LATERAL whose correlated
predicates are all equalities on inner columns K carries its `ORDER BY … LIMIT
n OFFSET m` as `ROW_NUMBER() OVER (PARTITION BY K ORDER BY <the body's own
ORDER BY>)` and a QUALIFY `rn > m AND rn <= m+n` in place of the bound. Over
an aggregated body the window sits above the aggregate and partitions on the
name the aggregate publishes the key under. An ORDINAL in the body's ORDER BY
counts the list the query wrote — the injection prepends the correlation slot,
so the position is read past it.

**A body that is not key-partitionable is evaluated per outer row where a
per-row runner exists, and refused where none does.** EXISTS / NOT EXISTS
decline the semi-join rewrite for a body it does not reproduce — a bound (a
`LIMIT n`, n ≥ 1, with no OFFSET is stripped first: existence is invariant
under it), an OFFSET, a GROUP BY, a HAVING, an ungrouped aggregate (which is
one row even over an empty input), a QUALIFY, a set operation — and the
compiled-predicate rerun executes the text as written. IN / NOT IN and the
scalar rewrite already did this. A LATERAL has no per-row runner, so a
bounded body with no equality key, a DISTINCT body under a bound, a DISTINCT
body whose lifted predicate names a column it does not publish, a lifted
column the body's own alias list or the enclosing relation also publishes, and
a lifted predicate under an enclosing star have the boundaries in ADR-0021
§1s. A contested column or bare-star decline refuses on the single-process
pipeline; the DAG retains its supported inner/comma cases and refuses a
contested column in an outer join.

**An ungrouped aggregate with a HAVING** is a special case of the pad rule
(§1h): the HAVING is folded over the default row (COUNT 0, NULL otherwise).
FALSE or NULL removes the pad — right on the INNER and the LEFT spelling. TRUE
is refused: an outer row with no group (PostgreSQL's default row) and one
whose group failed the HAVING (dropped, or NULL-padded on LEFT) are the same
unmatched key after the join and cannot be told apart.

## What it measured

Base `51addfb6`, the seam table `{IN, NOT IN, EXISTS, NOT EXISTS, scalar, JOIN
LATERAL, LEFT JOIN LATERAL, comma LATERAL} × {17 body shapes} × {equality,
inequality, mixed, shared name, none}` = 680 cells against live PostgreSQL
17.11 (`coordinator.TestArcLTACorrelatedBodyIsEvaluatedPerOuterRowOnEveryArm`):
87 wrong on the single arm, 61 refused. At the tip: 0 wrong, 183 refused
loudly, on five arms.

The withdrawn L1 rewrite, re-applied verbatim, closed exactly the 18
equality-keyed LATERAL cells and nothing else. Its three faults were each
closed at their own seam:

- the DAG partition binding — measured on the three DAG arms after arc WK's
  binding rule (ADR-0026 §8j corollary 1): every equality-keyed bound cell
  agrees with PostgreSQL on all five arms, and the nine-door masking gates
  (`server.TestArcL1ABoundedLateralReadsThePublishedValue`,
  `server.TestArcLTAPerOuterRowBodyReadsThePublishedValueOnEveryDoor`) hold
  the disclosure class;
- the false decline list — the rule itself: a bound with any correlated part
  that is not an equality on an inner column refuses;
- the `__win_N` collision — a base defect of user-written QUALIFY in two
  blocks under one join, closed in `physical.renameCollidingSlots` (the
  Filter arm of `applySlotRenameNodeOnly`).

Cost: top-3-per-group over 100 000 outer rows × 10 inner each (1 000 000 inner
rows) is 168 ms on the single arm and 1.08 s at a 512 KiB budget
(`wadjet.TestArcLTTopNPerGroupOver100kOuterRowsIsBounded`). The per-row rerun
is linear in the outer rows: 10.9 ms per outer row over that inner relation
(1k rows 11.3 s, 10k rows 109 s) — O(|outer| × |body|), the cost of every
decline to it.

## What lost

- A per-row rerun for LATERAL (a dependent join operator). The structural
  closure of every refused shape, and #1131 / #1130 in full; too structural
  for an arc whose priority-high item is the equality idiom, and its cost
  profile is the wrong route for the idiom even when it exists. Recorded as a
  filing candidate with its mechanism: a logical node the reorderer treats as
  a barrier, a physical operator over the subquery runner substituting the
  outer values the way the scalar rerun does, a DAG refusal routed local, the
  nine-door masking gate.
- A dedicated per-key top-N operator. Same semantics; a new breaker needs its
  own spill, DAG stage emission, distribution property and masking path,
  which the window already has.
- Pushing DISTINCT or the bound ABOVE the join with a minted outer row number
  — a second planner-minted window over the enclosing relation, and it still
  cannot express an inequality-correlated bound.
- Keeping the keyless shapes pinned (silent). Loud beats plausible.

## Round 2 (Codex review, 2026-09-24)

Seven findings, each closed at its seam: the key rule is checked on the
parsed predicate (`<inner expression> = <bare outer column>`; mixed sides and
outer expressions refuse, `i.k + 0` partitions); a bound over the body's own
QUALIFY refuses; `LIMIT 0` stays the body's; `LIMIT + OFFSET` saturates; a
DISTINCT over exactly the key drops `LIMIT n`; an ungrouped aggregate whose
row the bound removes gets no pad; ORDER BY resolves a SELECT alias; the
shared build side declines a QUALIFY / set operation for IN and the scalar
rewrite too. Where the DAG arms were right and the single arms wrong, the
refusal is the single path's alone (a contested lifted column on an INNER or
comma lateral; a bare-star decline), and a window above a lateral join is
routed single-process on the DAG. The per-row runner's cost is stated in the
SQL reference with the deadline that bounds it.

## Boundaries left

- A hash aggregate (DISTINCT, GROUP BY) or a window over one on a hash join's
  build side at a 512 KiB budget may refuse with `hash join build: memory
  budget exceeded … forced by "spill tracking"` — nondeterministically for
  the lateral spellings, deterministically for the user-written `GROUP BY …
  QUALIFY` block under a LEFT join, identically at base. The five-arm gate
  bounds those cells' spilled arm to {PostgreSQL's rows, that refusal}; the
  other four arms assert the rows. Filing candidate (exec/memory).
- A window above a lateral join runs on the single-process pipeline when
  requested through the DAG. `refuseWindowOverDependentJoin` handles the
  bounded and unbounded shapes; `WindowOverLateralLocalRoutes` measures the
  route (amended 2026-09-24, ADR-0021 §1s).
- Three self-joined arms of one table with per-arm ON filters answer a wrong
  pairing on the single-process path with no lateral in the query
  (`lat_ord o JOIN lat_item s ON s.order_id = o.id AND s.id IN (2,4) JOIN
  lat_item t ON … AND t.id IN (1,3) JOIN lat_item u ON … AND u.id IN (1,3)`:
  `1,2,3,3` for PostgreSQL's `1,2,1,1 | 2,4,3,3`). Found beside the QUALIFY
  slot fix; filing candidate (engine, join order).
