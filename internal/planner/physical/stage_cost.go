// This file holds stage cost for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// QueryCost summarizes the estimated cost of a query across all scan stages.
type QueryCost struct {
	TotalBytes int64
	TotalRows  int64
	TotalFiles int
	HasFilter  bool
	HasLimit   bool
}

// EstimateCost computes the aggregate cost of a set of stages.
func EstimateCost(stages []Stage, node *logical.Node) QueryCost {
	var cost QueryCost
	for _, s := range stages {
		if s.Type == "scan" {
			cost.TotalBytes += s.EstimatedBytes
			cost.TotalRows += s.EstimatedRows
			cost.TotalFiles += len(s.ScanFiles)
		}
	}
	cost.HasFilter = hasFilterOrPartition(node)
	cost.HasLimit = hasLimit(node)
	return cost
}

// CanProbeSplit returns the scan alias and file list for probe-split pipeline
// routing. Probe-split distributes the dominant probe table's files across
// workers while each worker scans build tables in full. This enables parallel
// execution for join-heavy queries where compute is the bottleneck.
//
// Returns the probe scan alias, its file list, and true if probe-split is viable.
func CanProbeSplit(stages []Stage, workerCount int) (probeAlias string, probeFiles []string, ok bool) {
	if workerCount <= 1 {
		return "", nil, false
	}

	// Pick the largest scan as the probe-split candidate. No exclusions:
	// the physical planner uses RightSemiJoin/RightAntiJoin to swap the
	// local join's build/probe when the inner table is much larger than
	// the outer. This makes it safe to partition the inner (large) table
	// across workers — each worker builds the small outer as hash table
	// and probes with its partition of the large inner table.
	var bestAlias string
	var bestFiles []string
	var bestBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > bestBytes {
			bestAlias = s.ScanAlias
			bestFiles = s.ScanFiles
			bestBytes = s.EstimatedBytes
		}
	}

	// Need enough files to give each worker meaningful work. For large
	// datasets (> 1 GB), relax from 2 files/worker to 1 file/worker since
	// each file is substantial. At SF100, tables may have only 3-6 files
	// but each is multi-GB.
	minFiles := workerCount * 2
	if bestBytes > 1<<30 {
		minFiles = workerCount
	}
	if bestAlias == "" || len(bestFiles) < minFiles || bestBytes < ProbeSplitMinBytes {
		return "", nil, false
	}

	return bestAlias, bestFiles, true
}

// ShuffleCandidate describes a join in the plan whose build side is large
// enough to warrant the shuffle execution path instead of broadcast.
type ShuffleCandidate struct {
	JoinStageID string   // the join stage to be served by shuffled inputs
	BuildAlias  string   // which scan stage produces the build side
	ProbeAlias  string   // which scan stage produces the probe side (the largest scan)
	BuildKeys   []string // build-side join keys (the join's JoinRightKeys)
	ProbeKeys   []string // probe-side join keys (the join's JoinLeftKeys)
	JoinKeys    []string // canonical (build-side) join key names for partitioning
	BuildBytes  int64    // EstimatedBytes of the build scan (for logging)
}

// PickShuffleCandidate identifies the largest non-probe scan above
// thresholdBytes as the shuffle candidate — the table that would otherwise be
// broadcast-duplicated as the runtime build side — and returns the join stage
// that connects it to the probe.
//
// The approach deliberately does NOT read BuildTableAlias on the join stage
// because in probe-split mode the planner's logical build/probe assignment is
// inverted at runtime: the planner labels the largest scan as the build (e.g.
// "lineitem"), but probe-split partitions that scan across workers, making the
// second-largest scan (e.g. "orders") the actual broadcast hash table.
// Shuffling orders instead of broadcasting it is the correction.
//
// Algorithm:
//  1. probeAlias = largest scan (matches CanProbeSplit's heuristic).
//  2. candidate = largest non-probe scan above thresholdBytes.
//  3. Walk join stages to find one that directly references the candidate
//     scan (via LeftDepStage or RightDepStage) or via a FusedJoin entry
//     whose BuildTableAlias matches the candidate alias.
//  4. Extract build/probe keys from the matching join or fused-join entry.
//
// Phase 1: returns the single best candidate. Phase 2 (chained shuffles)
// will return all candidates.
func PickShuffleCandidate(stages []Stage, thresholdBytes int64) (ShuffleCandidate, bool) {
	// stage-id → stage lookup.
	byID := map[string]Stage{}
	for _, s := range stages {
		byID[s.ID] = s
	}

	// Step 1: identify probe alias (largest scan, mirrors CanProbeSplit).
	var probeAlias string
	var probeBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > probeBytes {
			probeAlias = s.ScanAlias
			probeBytes = s.EstimatedBytes
		}
	}

	// Step 2: find the largest non-probe scan above the threshold.
	var candidateScan Stage
	var candidateScanID string
	var candidateFound bool
	for _, s := range stages {
		if s.Type != "scan" || s.ScanAlias == probeAlias {
			continue
		}
		if s.EstimatedBytes <= thresholdBytes {
			continue
		}
		if !candidateFound || s.EstimatedBytes > candidateScan.EstimatedBytes {
			candidateScan = s
			candidateScanID = s.ID
			candidateFound = true
		}
	}
	if !candidateFound {
		return ShuffleCandidate{}, false
	}

	// candidateCols is the set of column names the candidate scan exposes in its
	// raw Parquet files. Used to validate that join keys are actually rooted in
	// the candidate scan and not in a fused-join output (e.g. Q10: orders is the
	// left dep of join-4, but join-4's left key c_nationkey comes from the fused
	// customer join, not from orders' Parquet files).
	// If Columns is empty (not annotated), validation is skipped and the original
	// dep-match logic applies unchanged.
	candidateCols := make(map[string]bool, len(candidateScan.Columns))
	for _, c := range candidateScan.Columns {
		candidateCols[c] = true
	}
	// keysInCols returns true iff all keys are present in cols.
	// Returns true when cols is empty (no annotation = skip validation).
	keysInCols := func(keys []string, cols map[string]bool) bool {
		if len(cols) == 0 {
			// No column annotation: cannot validate, assume valid.
			return true
		}
		if len(keys) == 0 {
			return false
		}
		for _, k := range keys {
			if !cols[k] {
				return false
			}
		}
		return true
	}

	// probeCols is the set of column names the probe scan exposes in its raw
	// Parquet files. Used to validate that probe-side keys are directly readable
	// from the probe table, not from an intermediate join output.
	probeCols := make(map[string]bool)
	for _, s := range stages {
		if s.Type == "scan" && s.ScanAlias == probeAlias {
			for _, c := range s.Columns {
				probeCols[c] = true
			}
			break
		}
	}

	// Step 3: walk join stages to find the one referencing the candidate.
	for _, j := range stages {
		if j.Type != "hash_join" && j.Type != "broadcast_join" {
			continue
		}

		// Direct left-dep match: candidate is on the left (probe) side of the join.
		// Guard: the join's left keys must actually live in the candidate scan's raw
		// Parquet columns. If they don't (e.g. Q10: join-4 has LeftDepStage=orders
		// but JoinLeftKeys=[c_nationkey] which originates from the fused customer
		// join, not from orders files), skip this match and let the column-anchored
		// fallback below find the correct join.
		if j.LeftDepStage == candidateScanID && keysInCols(j.JoinLeftKeys, candidateCols) {
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
				ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
				JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// Direct right-dep match: candidate is on the right (build) side of the join.
		// The left dep must be the probe scan directly (not an intermediate join)
		// to ensure JoinLeftKeys map to raw probe Parquet columns.
		if j.RightDepStage == candidateScanID {
			probeScanID := ""
			for _, s := range stages {
				if s.Type == "scan" && s.ScanAlias == probeAlias {
					probeScanID = s.ID
					break
				}
			}
			if j.LeftDepStage != probeScanID {
				continue
			}
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinRightKeys...),
				ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
				JoinKeys:    append([]string(nil), j.JoinRightKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// FusedJoin match: candidate is the build of a secondary join fused into
		// this join stage. This covers Q03's shape where orders is not the top-level
		// join's direct dep but is referenced as a fused build.
		for _, fj := range j.FusedJoins {
			if fj.BuildTableAlias == candidateScan.ScanAlias {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), fj.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), fj.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), fj.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// Column-anchored fallback: the candidate appears as a left dep somewhere in
	// the join chain, but the matching join's keys were not valid for the raw
	// candidate scan (e.g. Q10: orders appears as the left dep of join-4 but with
	// c_nationkey keys from a fused join output). Find any join where:
	//   - JoinLeftKeys are all present in the candidate scan's raw columns, AND
	//   - JoinRightKeys are all present in the probe scan's raw columns.
	// This anchors both sides to their actual Parquet files regardless of the
	// dep-chain topology.
	if len(candidateCols) > 0 && len(probeCols) > 0 {
		for _, j := range stages {
			if j.Type != "hash_join" && j.Type != "broadcast_join" {
				continue
			}
			if keysInCols(j.JoinLeftKeys, candidateCols) && keysInCols(j.JoinRightKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
					ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
					JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
			// Also check if the keys are swapped: candidate provides the right
			// keys and probe provides the left keys.
			if keysInCols(j.JoinRightKeys, candidateCols) && keysInCols(j.JoinLeftKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), j.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// No join stage references the candidate.
	return ShuffleCandidate{}, false
}

// LargeBuildScans returns scan stages that are build-side (not the probe alias)
// and whose estimated size exceeds the given threshold. These are candidates for
// the build-side broadcast cache: the coordinator pre-scans them once, caches the
// result in S3, and each worker loads the shared cache instead of independently
// scanning the large source table N times.
//
// The cache provides two wins for queries with selective build-side filters
// or wide build tables:
//
//  1. Avoids decoding the source parquet on every worker (parquet decode is
//     CPU-expensive; the cached WSHF format is essentially raw typed bytes
//     and reads in a fraction of the time).
//  2. Lets the planner overlap the slow source scan with the rest of the
//     query once instead of N times.
//
// We previously gated this on len(large) >= 2 ("only cache when multiple
// large builds would compound a worker's hash table footprint"), reasoning
// that single-large-build queries can fit one hash table in memory and the
// cache only adds spill+upload latency. SF100 deploy disproved that: Q07's
// historical 3m13s was caching orders, and skipping it pushed the same
// query past 19 minutes (workers stuck spilling/scanning parquet 3 times).
// The win from caching orders comes mostly from amortising parquet decode,
// not from memory deduplication.
func LargeBuildScans(stages []Stage, probeAlias string, thresholdBytes int64) []Stage {
	var large []Stage
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if s.ScanAlias == probeAlias {
			continue // skip probe table
		}
		if s.EstimatedBytes >= thresholdBytes {
			large = append(large, s)
		}
	}
	return large
}

// CountJoinStages returns the total number of joins in the stage list,
// including hash_join and broadcast_join stages plus fused joins that were
// absorbed into parent stages by fuseJoinStages().
func CountJoinStages(stages []Stage) int {
	n := 0
	for _, s := range stages {
		if s.Type == "hash_join" || s.Type == "broadcast_join" || s.Type == StageSortMergeJoin {
			n++
		}
		n += len(s.FusedJoins)
	}
	return n
}

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
func (p *Planner) fusesIntoACTETerminal(childStages []Stage) bool {
	if len(p.ctePlannedTerminal) == 0 {
		return false
	}
	terminals := make(map[string]bool, len(p.ctePlannedTerminal))
	for _, id := range p.ctePlannedTerminal {
		terminals[id] = true
	}
	for _, s := range childStages {
		if terminals[s.ID] {
			return true
		}
	}
	return false
}

func canFuseScanAggregate(childStages []Stage) bool {
	if len(childStages) == 0 {
		return false
	}
	for _, s := range childStages {
		if s.Type != "scan" {
			return false
		}
	}
	return true
}

func hasFilterOrPartition(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeFilter {
		return true
	}
	if n.Type == logical.NodeScan && (len(n.PartitionFilter) > 0 || len(n.ScanPredicates) > 0) {
		return true
	}
	for _, c := range n.Children {
		if hasFilterOrPartition(c) {
			return true
		}
	}
	return false
}

// needsLimitStage reports whether this NodeLimit needs a StageLimit of its
// own, given whether walkStages just handed its bound to a sort stage.
//
// Exactly three things can bound a stream on the DAG, and only one of them
// applies to any given LIMIT:
//
//   - The coordinator's post-gather pass (`mi.Limit`/`mi.Offset` in
//     ExecuteSQL). It reads `logical.ExtractMergeInfo`, which inspects the
//     PLAN ROOT and nothing else — so it reaches a top-level LIMIT and no
//     other.
//   - A sort stage's top-N. It needs an ORDER BY below the LIMIT, and it
//     truncates to limit+OFFSET rather than skipping, because the OFFSET is
//     the coordinator's job in the shape it was written for. So it covers a
//     sorted LIMIT with no OFFSET that NO LOWER LIMIT has already claimed,
//     and only that.
//   - This stage.
//
// "Disjoint" is a property of the OWNERSHIP RULE, not of the shapes: two
// LIMITs in one query can both want the same sort stage, and until #525 the
// outer one took it — overwriting the inner's bound and then suppressing its
// own stage because it had just found a sort. walkStages' backwards scan
// stops at a claimed sort for that reason, so `sorted` here means "a sort
// stage carries THIS limit", never "there is a sort somewhere below".
//
// A LIMIT the first two miss bounded NOTHING before #478: `SELECT COUNT(*)
// FROM (SELECT DISTINCT k FROM t LIMIT 2) u` counted every distinct k, and
// its plain and explicit-GROUP-BY twins did the same. Silent, deterministic,
// and dependent only on how much data sits behind the query.
//
// The root case is left exactly as it was rather than moved onto this stage:
// the coordinator's pass is correct there, and emitting a stage as well would
// apply the OFFSET twice.
func (p *Planner) needsLimitStage(node *logical.Node, sorted bool) bool {
	if node == p.limitStageRoot {
		return false // the coordinator's post-gather pass owns this one
	}
	if sorted && node.OffsetVal == 0 {
		return false // the sort stage's top-N is already the global bound
	}
	// An OFFSET alone still needs a stage: nothing below skips rows.
	return node.LimitVal != logical.NoLimit || node.OffsetVal > 0
}

func hasLimit(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeLimit {
		return true
	}
	for _, c := range n.Children {
		if hasLimit(c) {
			return true
		}
	}
	return false
}

// QueryLimitSQLState is the SQLSTATE a cost-guard rejection carries:
// PostgreSQL class 53 (insufficient resources), 53400
// configuration_limit_exceeded. The limit is a configured bound, not a
// property of the statement, so a client can tell "your administrator caps
// this" apart from a syntax or type error and stop retrying (#803).
const QueryLimitSQLState = "53400"

// enforceQueryLimits checks estimated query cost against the limits in force:
// the deployment's (Planner.QueryLimits, from `query_limits:` and its per-role
// overrides) narrowed by the calling identity's, which an ABAC `query_limit`
// obligation puts on the context. See identity_limits.go for why the two
// arrive by different routes and meet here.
func (p *Planner) enforceQueryLimits(ctx context.Context, stages []Stage, node *logical.Node) error {
	limits := tightestLimits(p.QueryLimits, IdentityQueryLimitsFromContext(ctx))
	if limits == nil {
		return nil
	}
	cost := EstimateCost(stages, node)

	if limits.MaxScanBytes > 0 && cost.TotalBytes > limits.MaxScanBytes {
		return sqlerr.New(QueryLimitSQLState, "query would scan %s (%d bytes) across %d files, exceeding limit of %s — add a WHERE clause or partition filter",
			formatBytes(cost.TotalBytes), cost.TotalBytes, cost.TotalFiles, formatBytes(limits.MaxScanBytes))
	}
	if limits.MaxScanRows > 0 && cost.TotalRows > limits.MaxScanRows {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d rows across %d files, exceeding limit of %d rows — add a WHERE clause or LIMIT",
			cost.TotalRows, cost.TotalFiles, limits.MaxScanRows)
	}
	if limits.MaxScanFiles > 0 && cost.TotalFiles > limits.MaxScanFiles {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d files, exceeding limit of %d — add a partition filter",
			cost.TotalFiles, limits.MaxScanFiles)
	}
	if limits.RequireFilterAboveBytes > 0 && cost.TotalBytes > limits.RequireFilterAboveBytes && !cost.HasFilter {
		return sqlerr.New(QueryLimitSQLState, "query scans %s without a WHERE clause (filter required above %s)",
			formatBytes(cost.TotalBytes), formatBytes(limits.RequireFilterAboveBytes))
	}
	if limits.RequireLimitAboveRows > 0 && cost.TotalRows > limits.RequireLimitAboveRows && !cost.HasLimit {
		return sqlerr.New(QueryLimitSQLState, "query scans %d rows without a LIMIT (limit required above %d rows)",
			cost.TotalRows, limits.RequireLimitAboveRows)
	}
	return nil
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1fTB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
