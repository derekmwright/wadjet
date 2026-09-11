# Hidden sort key materialization

Source: internal/planner/physical/hidden_sort_key.go — resolveHiddenSortKeys, moved 2026-09-11 (#1026)

```go
// The synthetic ORDER BY key on the DAG, and the stage that has to emit it.
//
// `ORDER BY b` over `SELECT a` names a column the SELECT-list Project drops,
// so logical.resolveOrderBy MATERIALIZES the term as a hidden projection
// called __sortkey_N and points the Sort at it (see
// planner/logical/order_by_keys.go). On the single-process pipeline that
// Project is a real operator: it runs below the Sort, computes the column,
// and hiddenSortTrimOp drops it again before the rows reach the client.
//
// On the DAG an ordinary Project emits NO STAGE. The name therefore exists
// nowhere unless some pass writes it onto the fragment that produces the
// sort's input, and exactly one pass did: attachScanSelectProjections, which
// runs only for the OUTERMOST SELECT list — the one feeding the terminal
// gather. A sort anywhere else — an ORDER BY inside a derived table or a CTE,
// whose consumer is an aggregate or a join rather than the gather — got no
// such projection, and the task failed with
//
//	sort: key column "__sortkey_0" does not exist in the input schema
//
// while the single-process path answered the same query (#424). That is the
// loud half of the family #313/#316/#320 closed the silent half of: the same
// missing name, caught by the sort operator instead of matching nothing.
//
// resolveHiddenSortKeys settles every such key, and runs LAST — after
// attachScanSelectProjections — because the repair depends on what that pass
// did. Where it fired, the producing fragment already emits __sortkey_N and
// there is nothing to do; where it declined, this pass makes the producer
// carry the term:
//
//   - a plain column reference (`ORDER BY s_acctbal`) needs no computation.
//     The producer already ships that column under its own name — the DAG's
//     convention, which every other resolver compensates for — so the KEY is
//     renamed to it and no projection is added. Nothing downstream reads
//     __sortkey_N: the gather projects to the visible SELECT list, and a
//     consumer stage reads the producer's columns by their source names.
//
//   - a computed term (`ORDER BY LENGTH(s_name)`) has no source column, so it
//     is projected INTO the producing fragment under the hidden name — the
//     same materialize-at-source shape as absorbComputedSubqueryProjection
//     (#383) and the same Stage.ProjectExprs → OpProject machinery (#169).
//
// A shape it does not recognize is left exactly as it was, which keeps
// today's loud failure rather than inventing an order.
```
