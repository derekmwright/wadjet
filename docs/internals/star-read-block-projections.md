# Star read block projections

Source: internal/planner/physical/block_projection_stage.go — starReadBlockProjections, moved 2026-09-11 (#1026)

```go
// A DERIVED BLOCK A STAR READS IS A RELATION, AND SOME STAGE PUBLISHES IT
// (#984).
//
// A Project emits no stage (walkStages' `default:` arm). On the DAG a derived
// table's SELECT list is therefore not a relation of its own: the Aggregate or
// the Scan below it is what materializes, and every consumer above compensates
// per consumer — resolveShuffleKey, resolveAggInputName, resolveSortKeyColumn
// and the gather's OutputRenames each map the name the query wrote back to the
// name the stream carries.
//
// A STAR has no name to map. It reads the stream BY POSITION, so it publishes
// whatever the stage below the block emits:
//
//	SELECT * FROM lat_ord o
//	  JOIN (SELECT order_id, order_id AS oid FROM lat_item) s ON s.order_id = o.id
//	PostgreSQL        id, customer, total, order_id, oid
//	the stage's stream            …,       order_id        ← `oid` is not a column
//
// Four ways a block's projection leaves its stream behind, all four measured
// as silently wrong answers on both DAG arms at v0.18.60 and all four one
// question — is the projection, by position, the list the stage emits:
//
//   - a source column published TWICE (`order_id, order_id AS oid`): the
//     stream carries one of it;
//   - a RENAME (`order_id AS k`): the stream carries the source name, so the
//     client is handed a column it never asked for under a name the query
//     does not use;
//   - an ALIAS OVER AN AGGREGATE (`CAST(COUNT(*) AS VARCHAR) AS n`): the
//     aggregate stage emits its own `__agg_0` beside the computed `n`, and
//     the reserved slot reaches the client;
//   - a COMPUTED item (`amount * 2 AS d`): absorbComputedSubqueryProjection
//     is deliberately ADDITIVE, so the stream carries the computed column AND
//     the source it was computed from.
//
// The fix is the one the ADR names: the stage that materializes the block
// publishes the BLOCK'S PROJECTION — by position, under the block's own names
// — so the relation above the block is the relation the query wrote. Nothing
// predicts a name here: the projection becomes a real OpProject through
// Stage.ProjectExprs, exactly the machinery attachScanSelectProjections uses
// for the statement's own SELECT list, and the join operator's own naming rule
// then produces the star's columns from a relation that is already right.
//
// SCOPED TO A STAR, and the scope is the whole of why this is not a wider
// change. A named SELECT list over every one of these blocks answers
// PostgreSQL on all four arms today, because each consumer resolves its own
// column; materializing under those is churn with no defect to fix. The test
// is `projected` — a Project anywhere between the root and the block means the
// statement named its columns — and it is the same test
// refuseLateralProjection applies.
```
