# Join filter production check

Source: internal/planner/physical/join_carried_columns.go — producedColumnsInPlan / assertJoinFiltersAreBacked, moved 2026-09-11 (#1026)

```go
// assertJoinFiltersAreBacked refuses a plan whose JOIN stage carries a
// predicate naming a column NOTHING in the plan computes.
//
// `assertCarrierSchemaResolves` deliberately excludes join stages: a join's
// input is the qualified union of two sides with per-column origin rules only
// the executor resolves, and asserting over it produces false refusals. That
// exclusion is right, and it is also why one silent zero survives every gate
// in this file — the identical query without the join REFUSES, loudly and
// correctly:
//
//	WITH c AS (SELECT id, SUM(a) * 2 AS dv FROM t GROUP BY id)
//	SELECT COUNT(*) FROM c WHERE c.dv > 1
//	-- native-DAG: stage final_aggregate-1 filters on "c.dv > 1" and its
//	--   input carries no [c.dv]; input: [__agg_0 id]   -> routed local, ANSWERS 5
//
//	… the same CTE with `JOIN t x ON c.id = x.id` added
//	-- PostgreSQL 5 · single 5 · both DAG arms 0, in silence
//
// The cause is upstream and is not this check's to repair: `SUM(a) * 2 AS dv`
// over a DECIMAL aggregate is DECLINED by absorbAggregateOutputProjection,
// because AggSpec carries an OutputType but no (p,s) and a wrong DECIMAL
// declaration is worse than no projection (ADR-0024 item 2). The decline is
// correct; what is not correct is that nothing then computes `dv` and the
// query answers WITHOUT the predicate. The same shape over a FLOAT or BIGINT
// aggregate is not declined and answers correctly on every arm, which is what
// says the type is the trigger and the join is only what hides it.
//
// So this asks the WEAKER question — does ANY producing stage in the plan
// compute this name — which is the one `dropUnbackedJoinColumns` already asks
// and which is known not to refuse TPC-H Q02. It cannot see a name that
// resolves to the WRONG column, only one that resolves to nothing, and that is
// exactly the class that answers zero in silence. Movers and joins are
// excluded from the producing set for the same reason they are there: their
// column lists are the thing under suspicion.
//
// The refusal wraps ErrUnreachableGatherOutput, so the coordinator routes the
// query to its local engine and ANSWERS it — the same disposition its
// join-free spelling already had.
// producedColumnsInPlan is every column name a PRODUCING stage of the plan
// computes. Movers and joins are excluded because their column lists are a
// payload manifest and an OutputFilter — neither can invent a column, and
// reading them as production is the mistake dropUnbackedJoinColumns exists to
// undo.
```
