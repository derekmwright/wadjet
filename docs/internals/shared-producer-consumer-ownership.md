# Shared producer consumer ownership

Source: internal/planner/physical/shared_cte_producer.go — assertNoConsumerScopedFilterOnSharedStage (ConsumerScoped check), moved 2026-09-11 (#1026)

```go
		// The question is OWNERSHIP, not presence, and the marker is the
		// only thing that knows it — in BOTH directions.
		//
		// A shared stage may perfectly well carry a filter or a projection
		// that belongs to the RELATION every consumer reads: a scan's own
		// pushed-down predicate (`WITH c AS (SELECT … WHERE id < 100)`
		// referenced twice), and — the case that made this concrete — the
		// aggregate-output projection absorbAggregateOutputProjection puts
		// on a CTE body whose group key is computed. `WITH a AS (SELECT g+1
		// AS gk, COUNT(*) AS n FROM t GROUP BY g+1) SELECT gk FROM a UNION
		// ALL SELECT gk FROM a` has no filter anywhere and PostgreSQL
		// answers 16 rows; a rule that refused any projection on a shared
		// stage refused the query outright.
		//
		// What makes an attachment unsafe is that it belongs to ONE
		// consumer, and stage emission is where that is knowable: it
		// distinguishes a predicate attached INSIDE a CTE body from one
		// attached ABOVE a reference.
		//
		// filterCarrierIndex sets the marker and this walk is its LIVE
		// trigger: it attaches a consumer's filter to a CTE terminal that is
		// not yet known to be shared and marks it, and #876's own residual —
		// two scalar producers that each filter one reference of one CTE —
		// is refused here when the second producer is pointed at that stage
		// (TestArcH1TwoFilteredProducersOverOneCTEAreRefusedLoudly).
		//
		// #876 tried the stronger rule — never attach to any recorded CTE
		// terminal, give the consumer its own StageProject — and WITHDREW it:
		// the project that then carries an outer WHERE over a shared CTE
		// answered ZERO for PostgreSQL's 4838 on Q15's own shape. So the
		// marker stays, and so does this assert.
		//
		// Deriving ownership structurally instead would mean asking whether
		// every consumer's LOGICAL ancestry carries the same predicate, and
		// this pass is handed only []Stage.
```
