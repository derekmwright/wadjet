package coordinator

import (
	"os"
	"strconv"
	"time"
)

// gatherReceiveTimeout bounds the coordinator's wait for worker terminal
// gather messages. Matches shuffleStageTimeout so long-running final
// aggregation stages don't falsely time out.
const gatherReceiveTimeout = 10 * time.Minute

// eagerDispatchStageCompletionHook, when non-nil, runs synchronously right
// before a stage's `done` channel closes, with the coordinator and IDs
// needed to inspect that stage's own eager feed. Test-only seam: nil in
// production (zero cost, one nil check), so it changes no behavior there.
//
// Eager consumer dispatch (docs/design/eager-consumer-dispatch.md §3.3)
// races a producer's `done` (full completion) against its `f.dispatched`
// (task layout fixed) — real work, so `dispatched` almost always wins. At
// small scale (SF001, a handful of milliseconds of real work per task)
// under CPU contention, the consumer's own per-dependency loop can be
// scheduled late enough that BOTH channels are already closed by the time
// it checks: Go's select then picks between them at random, so the eager
// path can lose even though nothing was actually wrong. A test asserting
// engagement needs the race decided every time, not most times.
//
// The hook lets a test hold `done` open past that window — but only for
// stages that are actually eager-feed producers, identified by their own
// f.dispatched already being closed (runShuffleSide and the A3 compute
// path always call feed.dispatch before returning, whether or not any
// consumer ever registers as eager, so this reads true for exactly the
// stage types that race a consumer and false for everything else, e.g.
// scans and non-shuffle joins). Delaying only those, instead of every
// stage in the DAG uniformly, avoids also delaying the consumer's own walk
// through its OTHER (non-eager) dependencies, which would just push the
// race back by the same amount instead of resolving it.
// Package var so tests can pin it. See eager_dispatch_e2e_test.go.
var eagerDispatchStageCompletionHook func(c *Coordinator, queryID, stageID string)

// wshfShufflePrune gates the WSHF-input shuffle projection (fix 2 of the
// scan-output column leak, docs/benchmarks/ + memo 2026-07-28).
// Kill switch WADJET_WSHF_SHUFFLE_PRUNE=0.
var wshfShufflePrune = os.Getenv("WADJET_WSHF_SHUFFLE_PRUNE") != "0"

// replicateMaterializeMinFiles is the minimum upstream file count that
// triggers materialization in dispatchReplicateStage. Below this, the
// upstream is small enough that having every probe-split task read the
// raw files is fine; at or above, we consolidate so the per-task readers
// pay parquet decode cost once instead of N times.
//
// Declared as var (not const) so tests can lower it to force the
// materialization path on small fixtures.
var replicateMaterializeMinFiles = 2

// replicateMaterialize is dispatchReplicateStage's call to the consolidation
// step, indirected through a var for the same reason
// replicateMaterializeMinFiles is one: a test cannot otherwise reach the
// fallback, which on a healthy cluster needs S3 or a worker to be unhealthy.
// Nothing but a test replaces it.
var replicateMaterialize = (*Coordinator).materializeReplicate

// singleFileShardThresholdBytes is the minimum file size that triggers
// row-group sharding for a single-file scan. Below this threshold the
// scan runs as one task (whole file); above it the dispatcher fans out
// N row-group shards so the downstream broadcast-join chain can probe-
// split (which requires probe upstream to have ≥ 2 files).
//
// 64 MB is small enough to catch any moderately-compacted production
// file (SF10 partsupp = 691 MB single file fans out cleanly) and large
// enough that small dimension tables (region/nation/supplier) stay
// single-task and don't waste a worker slot on a few KB of data.
//
// Var rather than const so tests can lower it to force the sharded path
// on small fixtures.
var singleFileShardThresholdBytes int64 = 64 * 1024 * 1024

// aggregatePartialSplit reports whether a partial "aggregate" stage
// qualifies for round-robin fan-out, returning the dep ID and per-task
// input file groups when it does.
//
// The planner labels grouped partial aggregates DistRoundRobin when
// workerCount > 1 (OutputDistribution, StageAggregate case) so the
// property algebra forces a hash-shuffle ahead of the grouped final. The
// dispatcher's task-count switch, however, had no DistRoundRobin arm — the
// correctness-first default ran the partial as ONE task reading the entire
// upstream (e.g. a 24-partition join output) while the rest of the cluster
// idled. The 2026-07-19 arrival-waits evidence pass measured this as the
// exclusive serial leg on Q10 (12.9s partial + downstream effects) and
// Q13/Q02/Q03 at SF100. Partial aggregation is valid over any disjoint
// cover of its input, so the fix is the same shape as the scan-fused
// fan-out (dispatchScanAggregateStage): aggregate disjoint slices in
// parallel, let the existing downstream machinery (exchange-repartition
// from EnsureDistribution, or dispatchFinalAggregateFanout for Singleton
// finals) merge the partials.
//
// Slicing: an even split of the flattened upstream file list across at
// most workerCount tasks — the same shape as probe-split. The first cut
// of this fan-out used one task PER upstream partition (24-way at SF100)
// with no size gate; the 2026-07-19 SF100 validation run showed why the
// probe-split precedent caps at workerCount: small aggregates became
// swarms of ~2ms tasks whose per-task dispatch/result overhead (~0.5-1s)
// dwarfed the work, serialized through max_concurrent worker slots, and
// queued the NEXT query's scan tasks behind the swarm (+18% suite task
// count, steady pass +28% — slower than its own cold pass). Chunky
// tasks or no split.
//
// aggSplitMinBytes is the same "fanout only wins when each task performs
// non-trivial work" reasoning as finalAggregateFanoutCandidate's
// K > workerCount gate: below it, the single-task partial is already
// cheap and the split is pure scheduling overhead. Bytes are the
// worker-reported on-disk size of the upstream output; 0 (unknown,
// legacy worker) declines the split — degrade to the pre-split shape,
// never to a regression.
//
// Guards: SortKeys/Limit/FilterExprs never appear on a partial today
// (fuseSortIntoPredecessor folds into finals only; HAVING lands on the
// final) — the guard keeps the split sound if that ever changes, since a
// sort, limit, or post-filter applied per-slice would be wrong before the
// merge has seen all partials. Eager provisional inputs are excluded the
// same way probe-split/skew slicing is: their manifest-fed partition-range
// convention does not match custom file groups.
//
// WADJET_AGG_SPLIT_MIN_BYTES overrides the floor (small-scale harness runs
// force the split path everywhere with =1 so the correctness gate actually
// exercises it; SF1 upstreams are all under the production floor).
var aggSplitMinBytes = func() int64 {
	if v := os.Getenv("WADJET_AGG_SPLIT_MIN_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 64 << 20
}()
