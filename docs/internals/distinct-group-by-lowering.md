# Distinct group by lowering

Source: internal/planner/logical/distinct_rewrite.go — rewriteDistinctAsGroupBy, moved 2026-09-11 (#1026)

rewriteDistinctAsGroupBy rewrites every user SELECT DISTINCT into an
aggregate-free GROUP BY over the projection's expressions:

	Distinct → Project[e1 [AS a1], …] → child
	⇒ Project[e1 [AS a1], …] → Aggregate{GroupBy: [e1, …]} → child

This is the tree BuildFromSelect produces for the equivalent
`SELECT e1 … GROUP BY e1, …`, so DISTINCT rides the distributed aggregate
machinery (fused partial dedup at the scan → hash-partition exchange →
sharded final merge over disjoint key ranges) instead of the
coordinator-side dedup fallback, which funnels every pre-dedup row to a
single node — and which cannot project expression output at the gather.

It runs over the WHOLE tree, not just the root path, because the DAG has
no execution for NodeDistinct at all: walkStages passes it through as a
passthrough node and emits no stage (#163). Two compensations hid that —
this rewrite for the root Distinct, and the coordinator's post-gather
dedup keyed off MergeInfo.HasDistinct — and a DISTINCT inside a derived
table whose consumer is an aggregate fell between them, so
`SELECT COUNT(*) FROM (SELECT DISTINCT c FROM t) u` counted every raw row
on the DAG and the deduplicated rows single-process (#466). Rewriting the
Distinct wherever it sits gives it a stage wherever it sits.

Scope:
  - A Distinct marked BuildSideDedup is planner-inserted (semi/anti build
    dedup, decorrelated semijoin key source), carries no user-visible
    semantics, and has dedicated physical handling. Left alone.
  - A `SELECT DISTINCT *` without a Project takes rewriteStarDistinct,
    which reads group keys off the relation. An expanded join star has a
    Project and uses its qualified items as group keys (ADR-0026 §9).
  - Aggregate projections (SELECT DISTINCT a, SUM(b) …) and subquery
    expressions still fall through: neither has a group key. On the root
    path the coordinator dedup (MergeInfo.HasDistinct) answers them;
    anywhere else PlanDistributed refuses the query rather than dropping
    the DISTINCT silently (dagplan.refuseUnstageableDistinct) and the
    coordinator answers it on its local single-process pipeline
    (Coordinator.runDistinctLocal).
