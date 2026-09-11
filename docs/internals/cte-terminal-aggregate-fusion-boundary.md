# Cte terminal aggregate fusion boundary

Source: internal/planner/physical/stage_cost.go — fusesIntoACTETerminal / canFuseScanAggregate, moved 2026-09-11 (#1026)

```go
// canFuseScanAggregate returns true when child stages are all scans (or
// filter-pushed scans). This means partial aggregation can be fused directly
// into scan tasks, eliminating the separate aggregate stage and its S3 round-trip.
// fusesIntoACTETerminal reports whether fusing an aggregate into these child
// stages would REWRITE a stage that a CTE reference is (or may be) pointed at.
//
// The scan-aggregate fusion is the one optimization that changes what an
// ALREADY-EMITTED stage emits: it stamps FusedAggSpecs onto the scan and
// prunes its output columns, so the stage stops producing the CTE's rows and
// starts producing that consumer's partial aggregates. walkStages' CTE dedup
// then points every LATER reference at it — and the later reference's own
// aggregate reads a relation that no longer exists.
//
// #876 measured it as a hard failure on both DAG arms:
//
//	WITH c AS (SELECT id, c_i64 AS v FROM typemx)
//	SELECT COUNT(*) FROM typemx
//	WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c)
//	  hash aggregate: aggregate input "c_i64" is not a column of its input
//	  (input has: max(v))
//
// `MAX(v)` fused into the CTE body's scan; `MIN(v)`'s producer deduped to that
// same scan and asked it for `c_i64`.
//
// The reference COUNT cannot decide this. p.cteRefCounts is computed from the
// statement's own logical plan, and a CTE named only inside a scalar
// subquery's TEXT appears there ZERO times: each producer is planned by its
// own emitScalarProducerStagesTyped walk, sharing this cache, and the second
// walk has not happened when the first one fuses. What IS knowable at the
// fusion is that the stage was recorded as a CTE terminal — which is the
// engine's own claim that another reference may be pointed at it.
//
// Declining costs one scan -> aggregate materialization for an aggregate
// whose direct child is a CTE body's scan. It is the same rule
// assertNoConsumerScopedFilterOnSharedStage states for filters and
// projections (#656), applied to the third thing a consumer can attach to a
// producer it does not own.
```
