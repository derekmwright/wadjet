# ADR-0032: A query block is parsed once, and every layer reasons about the same tree

Status: Accepted (2026-09-04, #851)

## Context

A statement is a tree of QUERY BLOCKS: the outer SELECT, each derived table in
its FROM, each CTE body, each subquery in an expression. The parser hands the
planner ONE `plansql.SelectInfo` for the outermost block, and every nested
block reaches the planner as SQL **text** inside it — `TableRef.Name` holds
`(SELECT …)` verbatim, `CTEDef.SQL` holds the body.

Two layers read that text, and until this ADR each parsed it for itself:

| layer | what it does with the block |
|---|---|
| the BINDER, `physical.validateColumns` → `validateBlock` | resolves every column reference against the FROM sources, and DECIDES the questions a parser cannot because they need a schema |
| the LOGICAL BUILDER, `logical.BuildFromSelectWithCTEs` | plans the block |

The binder runs first and records its decisions by REWRITING the block's own
terms. For the top-level block that works, because the `*SelectInfo` it mutates
is the one the builder is about to plan. For a nested block it does not: the
binder parsed the text into a tree of its own, mutated that, and dropped it;
the builder parsed the same text again and planned the parser's unrevised
answer.

PostgreSQL's GROUP BY precedence is exactly such a decision. A bare name there
binds an INPUT COLUMN before a SELECT alias — `SELECT g*0 AS g, COUNT(*) FROM t
GROUP BY g` is six groups, not one — and the parser substitutes the alias's
expression unconditionally because it has no schema, recording the origin in
`SelectInfo.GroupByAliasOrigin` so the substitution can be undone.
`RevertGroupByAliasesShadowedByInput` undoes it at the layer that knows the
FROM sources (#739). Inside a derived table or a CTE the undo was discarded
with the binder's copy:

```sql
SELECT x.g FROM (SELECT g*0 AS g, COUNT(*) AS n FROM t WHERE id < 6 GROUP BY g) x
-- PostgreSQL 17: 6 rows   wadjet: 1, on all four arms, in silence (#851)
```

Nothing in either layer was wrong on its own. What was wrong is that they were
talking about two different trees.

## Decision

**A nested query block is parsed at most once, ON THE REFERENCE THAT NAMES IT,
and every layer that reasons about that block reasons about the same
`*SelectInfo`.**

`TableRef.SubSelect()` and `CTEDef.BodySelect()`
(`internal/planner/sql/sub_block.go`) parse the block's text on first use and
memoize the result — and the parse ERROR with it, so a caller that wants to
wrap the failure in its own message still can. The binder's `resolveSource`
and `registerCTE` take those; so do the logical builder's `resolveTableOrCTE`
and the physical planner's CTE materialization
(`materializeCTEColumnar` → `buildSubqueryPipelineFor`).

Two consequences follow from where the memo lives.

**It propagates to any depth without anything carrying a path.** The builder
plans the very `SelectInfo` the binder validated, and that object's own
`Tables` and `CTEs` carry their own memos — so a derived table inside a derived
table inside a CTE gets the same treatment with no bookkeeping. Recording the
DECISION instead (a list of names to revert, keyed by block) was the
alternative, and it stops at depth one: the builder re-parses the outer block
and mints fresh `TableRef`s for everything under it.

**A reference is held by POINTER.** A `TableRef` copied by value caches into
itself and the original never sees it, so the four call sites take
`&info.Tables[i]`, `&info.CTEs[i]` and `JoinInfo.RightTableRef` directly. That
is the one thing a reader has to know about the memo, and it is why
`resolveTableOrCTE` and `joinRightRef` changed signature rather than staying
value-typed.

## 2026-10-09: an expression subquery's body is parsed once too (ADR-0047 stage 3)

The statement's reader parses a parenthesised body once, to check its syntax
(`bodySyntax`), and that parse IS the body: it seeds the memo on the NODE that
names the subquery (`SubqueryNode.Select()`, `ExistsNode.Select()`,
`sub_block.go`), as `SubSelect` does on a derived table's reference, and the
subqueries nested in the body were seeded by the same parse. The node records
the WITH chain in scope where it is written (`CTEScope`, stamped by
`plansql.StampSubqueryScopes`). Every requester reads that one tree: the
binder (which validates and binds it with the enclosing query's scope, and
reads an ORDER BY term that holds a subquery as the item's own tree), the
aggregate-placement check, the outer-column candidates, the correlation
classifier's by-name fallback, the dangling-reference guard, the evaluators'
read bound, the integral-EXTRACT mark, the outer-level aggregate checks, the
decorrelators, the physical planner's runner, declaration, column count and
scope (a child planner over the node's chain, `physical.subqueryScopingIn`),
and the stage DAG's correlation refusal, dangling guard, eager runs and
producer stages. A FROM-less body unfolded into its item takes the memo's tree
(`takeBody`) rather than parsing the text again. A declaration plans the body
once — the answer column, the integer width and PostgreSQL's category come
from one plan, where three plans each re-declared the nested bodies and a
body at depth k was planned 3^k times.

Parsed privately, from TEXT that is not the body as written:

- a correlated re-run's per-row statement (the outer values substituted) and
  its bounded form, and the tree its rebuild renders from (the compile's
  `ParsedInfo`, which the per-row rewrite reads concurrently);
- a correlated body's declaration text, its outer references typed as casts
  (`expr.OuterTypedSubqueryNodeSQL`);
- an enclosing expression the plan carries as TEXT and re-parses, which
  re-parses the subqueries in it: on the stage DAG, an aggregate whose
  argument holds a subquery is carried by its input's spelling, and four
  readers re-parse it (`sum((SELECT max(id) FROM t))`: the body 5 times on the
  DAG arms, once on the embedded door) — the text-carried plan, stage 4.

Counted by `wadjet.TestArcCI3SubqueryBodyParsesOnce` through
`plansql.SetParseProbe`: an uncorrelated body is parsed once per statement
(7–16 times at a4054c18, 11–29 at 542b4f37); a correlated statement's parses
are at or under 542b4f37's; a nested body parses once at every depth, and the
statement's count grows linearly in depth (×3 per level at a4054c18). Planning
time over nesting depth is held by `wadjet.TestArcCI3SubqueryNestingPlanningBound`.
Answers are gated by `coordinator.TestArcCI3SubqueryBodyInItsScopeEveryArm`,
`wadjet.TestArcCI3EmbeddedSubqueryBodyInItsScope`,
`pgwire.TestArcCI3SubqueryScopeOnTheWire` and
`server.TestArcCI3SubqueryBodyNeverPublishesAPolicedValue`.

## What this does NOT change

- **The parser still substitutes unconditionally.** It has no schema; that is
  why the substitution is provisional and why `GroupByAliasOrigin` exists. This
  ADR is about the undo reaching the block it was decided for, not about moving
  the decision.
- **An entry point with no catalog behaves exactly as before.** The binder is
  reached through `ValidateColumns`, and a planner constructed without one
  never runs it — so the memo is filled by whichever layer parses first and no
  revert is applied, which is the pre-#739 answer and the answer that entry
  point already had.
- **The memo is per-statement and single-threaded.** A `SelectInfo` belongs to
  one planning pass; the coordinator's DAG plan and its local-pipeline fallback
  run in sequence over the same object, and the revert is idempotent.

## Consequences

- A pass that wants to reason about a nested block asks the reference for it
  rather than re-parsing the text. A new private parse re-opens exactly this
  gap, silently, and the symptom is a decision that works at the top level and
  not one block down — which is what makes it expensive to find.
- The parse itself is now paid ONCE per block instead of twice, which is a
  small win and not the reason for the change.
- Gated by `internal/coordinator/arc_e3_names_scopes_two_path_test.go`
  (`851/*`): the same GROUP BY collision bare, in a derived table, in a
  `SELECT *` over one, in a CTE and two derived tables deep, on four arms,
  with the ORDER BY and HAVING halves of PostgreSQL's precedence beside it —
  ORDER BY prefers the OUTPUT alias and HAVING binds the INPUT column, so a
  fix that moved GROUP BY's rule onto either of them is a new wrong answer
  rather than a green gate.

## Related

- ADR-0021 (subquery name resolution and set materialization), ADR-0026
  (a group key has one identity and one name), #739 (the precedence itself),
  #613 / #771 (the derived-table and CTE scope work this builds on).
