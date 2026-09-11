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

// eagerDispatchStageCompletionHook runs synchronously immediately before done closes,
// with the coordinator/query/stage IDs to inspect that stage's own eager feed.
// Test-only package var, nil in production (one nil check); see eager_dispatch_e2e_test.go.
// Tests hold done open only for eager producers whose own f.dispatched is closed:
// runShuffleSide and A3 compute always dispatch before returning, even without consumers.
// Delaying every stage also delays non-eager dependencies and recreates the race.
// At small scale both channels can already be closed; select chooses randomly,
// so an engagement gate must force dispatched to win, not rely on timing.
// See docs/design/eager-consumer-dispatch.md §3.3.
// See docs/internals/eager-stage-completion-test-seam.md for the design.
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

// Partial aggregate fan-out must cover disjoint input slices; downstream
// exchange-repartition or final-aggregate fan-out merges the partials.
// Split the flattened upstream file list evenly into at most workerCount tasks.
// Below aggSplitMinBytes, or with unknown (zero) reported bytes, decline splitting.
// SortKeys, Limit and FilterExprs prohibit splitting: per-slice application
// would be wrong before all partials merge. Exclude eager provisional inputs,
// whose manifest partition ranges do not match custom file groups.
// WADJET_AGG_SPLIT_MIN_BYTES overrides the floor; harnesses use =1 to force it.
// See docs/internals/partial-aggregate-file-fanout.md for the design.
var aggSplitMinBytes = func() int64 {
	if v := os.Getenv("WADJET_AGG_SPLIT_MIN_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 64 << 20
}()
