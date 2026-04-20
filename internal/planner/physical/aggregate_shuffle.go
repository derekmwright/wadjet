package physical

import (
	"fmt"
	"strings"
)

// PreComputedAggregateMeta travels on physical.Stage to tell task creation
// which cache files back a derived aggregate subtree the worker should
// substitute. Distinct from distributed.PreComputedAggregate (the wire
// type) — the coordinator converts between them when assembling tasks.
type PreComputedAggregateMeta struct {
	InputTable  string
	GroupByCols []string
	AggSpecs    []AggSpec
	CacheFiles  []string
}

// AggregateShuffleCandidate describes a join in the plan whose build side is a
// derived aggregate subplan (e.g. Q17's decorrelated scalar subquery aggregate
// over full lineitem). When the aggregate's input scan is large enough that
// broadcasting the whole subplan to every probe-split worker would cause
// memory pressure, the coordinator can dispatch a distributed partial-then-
// merge aggregate stage shuffled by the GROUP BY keys — mirroring the shape
// PickShuffleCandidate returns for base-table builds.
//
// Phase 1 detection: aggregate(GROUP BY K)(scan(T)) feeds a hash_join, and
// K ⊇ join keys (so the partitioning lines up with the join).
type AggregateShuffleCandidate struct {
	JoinStageID      string   // outer join whose build side is a derived aggregate
	AggregateStageID string   // aggregate stage directly feeding the join (through any shuffles)
	InputScanID      string   // base-table scan feeding the aggregate's input
	InputScanAlias   string   // scan alias (e.g. "lineitem:1" for Q17's inner scan)
	InputScanBytes   int64    // EstimatedBytes of the aggregate's input scan
	GroupByKeys      []string // the aggregate's GROUP BY columns (= partition keys)
	JoinBuildKeys    []string // the outer join's keys on this side (must be ⊆ GroupByKeys)
	JoinProbeKeys    []string // the outer join's keys on the probe side
}

// AggregateShuffleRejectReason explains why a join stage was not chosen as
// an aggregate-shuffle candidate. Used by PickAggregateShuffleCandidateDiag
// so callers can log exactly which gate fired for visibility on real
// workloads. The set of reasons is intentionally coarse (one per gate) —
// finer-grained diagnostics belong in the caller's log line.
type AggregateShuffleRejectReason int

const (
	AggShuffleRejectNone               AggregateShuffleRejectReason = iota
	AggShuffleRejectNoJoin                                          // no hash_join/broadcast_join stages at all
	AggShuffleRejectBuildNotAggregate                               // join's right dep chain doesn't terminate in an aggregate
	AggShuffleRejectAggNotScanRooted                                // aggregate isn't rooted in a single base scan
	AggShuffleRejectBelowThreshold                                  // input scan bytes ≤ threshold
	AggShuffleRejectScanHasFilters                                  // input scan has pushed predicates (Phase 2 scope)
	AggShuffleRejectKeysNotCovered                                  // aggregate GROUP BY keys don't cover join build keys
)

// AggregateShuffleDiag records the best observed rejection for telemetry.
// When a candidate IS found, Candidate is populated and Reason == None.
type AggregateShuffleDiag struct {
	Candidate AggregateShuffleCandidate
	Reason    AggregateShuffleRejectReason
	// ObservedScanBytes holds the input scan size of the closest-matching
	// rejected candidate (populated when we got past followToScan). Useful
	// for tuning the threshold on real data.
	ObservedScanBytes int64
	// JoinStageID / InputScanAlias of the closest-matching rejected join, when
	// available. Empty for NoJoin / BuildNotAggregate paths.
	JoinStageID    string
	InputScanAlias string
}

// PickAggregateShuffleCandidate scans stages for a join whose build side is a
// derived aggregate over a scan larger than thresholdBytes. Returns the first
// such candidate found. Phase 1: single candidate per query (matches
// PickShuffleCandidate's single-candidate contract).
//
// The function is conservative: it only returns a candidate when the
// aggregate's GROUP BY columns include the join's equi-keys for this side.
// If they don't, shuffling by GROUP BY keys would not align the aggregate
// output with the probe side's partitioning, and the join would be incorrect.
// In that case we return !found and let the caller fall back to the existing
// probe-split or broadcast path.
func PickAggregateShuffleCandidate(stages []Stage, thresholdBytes int64) (AggregateShuffleCandidate, bool) {
	diag := PickAggregateShuffleCandidateDiag(stages, thresholdBytes)
	return diag.Candidate, diag.Reason == AggShuffleRejectNone
}

// PickAggregateShuffleCandidateDiag is the diagnostic variant that also
// returns the reason for rejection (or Candidate + Reason=None for success).
// Use this when you want to log why detection declined — essential for
// threshold tuning on real production data.
func PickAggregateShuffleCandidateDiag(stages []Stage, thresholdBytes int64) AggregateShuffleDiag {
	byID := make(map[string]Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}

	// Track the "best" rejection across joins so the caller has something
	// informative to log. Priority order (most useful first):
	//   KeysNotCovered > ScanHasFilters > BelowThreshold > AggNotScanRooted > BuildNotAggregate > NoJoin
	// A later gate beats an earlier one because reaching it means the join
	// structure is closer to what we accept.
	best := AggregateShuffleDiag{Reason: AggShuffleRejectNoJoin}
	setBest := func(r AggregateShuffleRejectReason, d AggregateShuffleDiag) {
		if int(r) >= int(best.Reason) {
			best = d
			best.Reason = r
		}
	}

	for _, j := range stages {
		if j.Type != "hash_join" && j.Type != "broadcast_join" {
			continue
		}
		// A join exists — upgrade from NoJoin.
		if best.Reason == AggShuffleRejectNoJoin {
			best.Reason = AggShuffleRejectBuildNotAggregate
		}
		aggStage, ok := followToAggregate(byID, j.RightDepStage)
		if !ok {
			setBest(AggShuffleRejectBuildNotAggregate, AggregateShuffleDiag{JoinStageID: j.ID})
			continue
		}
		scan, ok := followToScan(byID, aggStage)
		if !ok {
			setBest(AggShuffleRejectAggNotScanRooted, AggregateShuffleDiag{JoinStageID: j.ID})
			continue
		}
		if scan.EstimatedBytes <= thresholdBytes {
			setBest(AggShuffleRejectBelowThreshold, AggregateShuffleDiag{
				JoinStageID:       j.ID,
				InputScanAlias:    scan.ScanAlias,
				ObservedScanBytes: scan.EstimatedBytes,
			})
			continue
		}
		if len(scan.FilterExprs) > 0 {
			setBest(AggShuffleRejectScanHasFilters, AggregateShuffleDiag{
				JoinStageID:       j.ID,
				InputScanAlias:    scan.ScanAlias,
				ObservedScanBytes: scan.EstimatedBytes,
			})
			continue
		}
		if !keysCovered(j.JoinRightKeys, aggStage.GroupByCols) {
			setBest(AggShuffleRejectKeysNotCovered, AggregateShuffleDiag{
				JoinStageID:       j.ID,
				InputScanAlias:    scan.ScanAlias,
				ObservedScanBytes: scan.EstimatedBytes,
			})
			continue
		}
		return AggregateShuffleDiag{
			Candidate: AggregateShuffleCandidate{
				JoinStageID:      j.ID,
				AggregateStageID: aggStage.ID,
				InputScanID:      scan.ID,
				InputScanAlias:   scan.ScanAlias,
				InputScanBytes:   scan.EstimatedBytes,
				GroupByKeys:      append([]string(nil), aggStage.GroupByCols...),
				JoinBuildKeys:    append([]string(nil), j.JoinRightKeys...),
				JoinProbeKeys:    append([]string(nil), j.JoinLeftKeys...),
			},
			Reason: AggShuffleRejectNone,
		}
	}
	return best
}

// String renders a reject reason as a short tag suitable for logs.
func (r AggregateShuffleRejectReason) String() string {
	switch r {
	case AggShuffleRejectNone:
		return "matched"
	case AggShuffleRejectNoJoin:
		return "no_join_stages"
	case AggShuffleRejectBuildNotAggregate:
		return "build_not_aggregate"
	case AggShuffleRejectAggNotScanRooted:
		return "aggregate_not_scan_rooted"
	case AggShuffleRejectBelowThreshold:
		return "below_threshold"
	case AggShuffleRejectScanHasFilters:
		return "scan_has_filters"
	case AggShuffleRejectKeysNotCovered:
		return "keys_not_covered"
	default:
		return "unknown"
	}
}

// followToAggregate walks the dependency chain from startID through transparent
// stages (shuffle, final_aggregate, merge_aggregate) looking for the aggregate
// stage that defines the GROUP BY keys. Returns the stage and true on success;
// false if the chain hits a non-aggregate stage or branches in a way that
// doesn't resolve to a single aggregate producer.
//
// For Q17 the chain is:
//   hash_join.RightDep → shuffle → final_aggregate (defines GROUP BY) → merge_aggregates → scan
// We follow until we hit the stage that carries GroupByCols; that's the
// aggregate's identity for detection purposes.
func followToAggregate(byID map[string]Stage, startID string) (Stage, bool) {
	seen := make(map[string]bool)
	current := startID
	for current != "" && !seen[current] {
		seen[current] = true
		s, ok := byID[current]
		if !ok {
			return Stage{}, false
		}
		// An aggregate stage with GroupByCols populated is our target.
		if (s.Type == "aggregate" || s.Type == "final_aggregate" || s.Type == "merge_aggregate") && len(s.GroupByCols) > 0 {
			return s, true
		}
		// Shuffle and grouped-merge stages are transparent — follow through.
		if s.Type == StageExchangeRepartition || s.Type == "final_aggregate" || s.Type == "merge_aggregate" {
			if len(s.Dependencies) == 0 {
				return Stage{}, false
			}
			current = s.Dependencies[0]
			continue
		}
		// Anything else (scan, hash_join, etc.) breaks the chain.
		return Stage{}, false
	}
	return Stage{}, false
}

// followToScan walks from an aggregate stage down to its root base-table scan.
// Returns the scan stage on success. Phase 1 requires a single-scan-rooted
// aggregate subplan; joins or multi-scan aggregates are out of scope.
func followToScan(byID map[string]Stage, agg Stage) (Stage, bool) {
	seen := make(map[string]bool)
	// Walk the first dependency transitively. All intermediate stages should
	// be aggregate/shuffle transparents; the root must be a single scan.
	type frame struct{ id string }
	stack := []frame{{id: agg.ID}}
	var root Stage
	found := false
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n.id] {
			continue
		}
		seen[n.id] = true
		s, ok := byID[n.id]
		if !ok {
			return Stage{}, false
		}
		if s.Type == "scan" {
			if found && s.ID != root.ID {
				// Multiple distinct scans reach this aggregate — not a simple
				// aggregate-on-scan pattern. Phase 1 rejects.
				return Stage{}, false
			}
			root = s
			found = true
			continue
		}
		// Follow all dependencies — the aggregate fan-in (many merge_aggregate
		// stages all feed from the same scan) is typical.
		for _, dep := range s.Dependencies {
			stack = append(stack, frame{id: dep})
		}
	}
	if !found {
		return Stage{}, false
	}
	return root, true
}

// BuildAggregateShuffleSQL reconstructs the SQL text for the derived-aggregate
// pre-compute task from a candidate + the physical stage graph. The
// coordinator dispatches this SQL as a normal pipeline task so the workers
// run it via the same execution path that handles any other GROUP BY query;
// the output rows are written to S3 and later streamed into probe tasks as
// a pre-computed build input.
//
// Phase 1 scope: single-scan-rooted aggregate with simple GROUP BY and
// supported aggregate functions. Scan filters push through unchanged. Any
// shape the reconstruction cannot represent causes a caller-level fallback
// to the existing in-pipeline execution — safety first, performance later.
func BuildAggregateShuffleSQL(cand AggregateShuffleCandidate, stages []Stage) (string, error) {
	byID := make(map[string]Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}
	agg, ok := byID[cand.AggregateStageID]
	if !ok {
		return "", fmt.Errorf("aggregate stage %q not found", cand.AggregateStageID)
	}
	scan, ok := byID[cand.InputScanID]
	if !ok {
		return "", fmt.Errorf("input scan %q not found", cand.InputScanID)
	}
	if scan.TableName == "" {
		return "", fmt.Errorf("input scan %q has no TableName", cand.InputScanID)
	}
	if len(agg.GroupByCols) == 0 {
		return "", fmt.Errorf("aggregate stage %q has no GroupByCols", cand.AggregateStageID)
	}
	if len(agg.AggSpecs) == 0 {
		return "", fmt.Errorf("aggregate stage %q has no AggSpecs", cand.AggregateStageID)
	}

	// Projection: GROUP BY columns first (so workers stream them unchanged),
	// then aggregate expressions. Preserve OutputCol names so downstream joins
	// referencing __scalar_N match the parent query's column identifiers.
	projs := make([]string, 0, len(agg.GroupByCols)+len(agg.AggSpecs))
	projs = append(projs, agg.GroupByCols...)
	for _, spec := range agg.AggSpecs {
		expr, err := formatAggExpr(spec)
		if err != nil {
			return "", fmt.Errorf("agg %s: %w", spec.OutputCol, err)
		}
		if spec.OutputCol != "" {
			projs = append(projs, fmt.Sprintf("%s AS %s", expr, spec.OutputCol))
		} else {
			projs = append(projs, expr)
		}
	}

	// FROM: the base table (scan.TableName). We intentionally do NOT preserve
	// the :N alias — the re-planned pre-compute SQL runs standalone, with no
	// sibling scans that would collide on alias.
	sql := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(projs, ", "),
		scan.TableName)

	// WHERE: any scan-pushed filter expressions from the inner scan. FilterExprs
	// are already SQL-text fragments produced by the planner's filter-pushdown
	// pass, so they concatenate with AND unchanged.
	if len(scan.FilterExprs) > 0 {
		sql += " WHERE " + strings.Join(scan.FilterExprs, " AND ")
	}

	sql += " GROUP BY " + strings.Join(agg.GroupByCols, ", ")
	return sql, nil
}

// formatAggExpr renders a single aggregate spec as SQL: "fn(input)" or
// "fn(*)" for COUNT(*). Only the aggregate functions that currently round-
// trip cleanly through the pre-compute SQL are supported; unsupported
// functions return an error so the caller falls back instead of silently
// producing a wrong plan.
func formatAggExpr(spec AggSpec) (string, error) {
	fn := strings.ToLower(spec.Func)
	switch fn {
	case "count":
		if spec.InputCol == "" || spec.InputCol == "*" {
			return "COUNT(*)", nil
		}
		return fmt.Sprintf("COUNT(%s)", spec.InputCol), nil
	case "sum", "avg", "min", "max":
		if spec.InputCol == "" {
			return "", fmt.Errorf("%s requires InputCol", fn)
		}
		return fmt.Sprintf("%s(%s)", strings.ToUpper(fn), spec.InputCol), nil
	default:
		return "", fmt.Errorf("unsupported aggregate function %q for shuffle pre-compute", spec.Func)
	}
}

// keysCovered returns true iff every key in required is present in available.
// Used to verify that aggregate GROUP BY keys cover the join's equi-keys so
// shuffling by GROUP BY keys also partitions by the join key.
func keysCovered(required, available []string) bool {
	if len(required) == 0 {
		return false
	}
	have := make(map[string]bool, len(available))
	for _, k := range available {
		have[k] = true
	}
	for _, k := range required {
		if !have[k] {
			return false
		}
	}
	return true
}
