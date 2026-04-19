package physical

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
	byID := make(map[string]Stage, len(stages))
	for _, s := range stages {
		byID[s.ID] = s
	}

	for _, j := range stages {
		if j.Type != "hash_join" && j.Type != "broadcast_join" {
			continue
		}
		// Follow the build-side dependency through transparent intermediate
		// stages (shuffle, final_aggregate merges) until we hit either an
		// aggregate producer or something that isn't an aggregate-on-scan.
		aggStage, ok := followToAggregate(byID, j.RightDepStage)
		if !ok {
			continue
		}
		// The aggregate must be rooted in a single scan (not a join subplan) —
		// Phase 1 scope.
		scan, ok := followToScan(byID, aggStage)
		if !ok {
			continue
		}
		if scan.EstimatedBytes <= thresholdBytes {
			continue
		}
		// Alignment check: the aggregate's GROUP BY keys must cover the
		// join's build-side keys so that partitioning by GROUP BY keys also
		// partitions by the join key.
		if !keysCovered(j.JoinRightKeys, aggStage.GroupByCols) {
			continue
		}
		return AggregateShuffleCandidate{
			JoinStageID:      j.ID,
			AggregateStageID: aggStage.ID,
			InputScanID:      scan.ID,
			InputScanAlias:   scan.ScanAlias,
			InputScanBytes:   scan.EstimatedBytes,
			GroupByKeys:      append([]string(nil), aggStage.GroupByCols...),
			JoinBuildKeys:    append([]string(nil), j.JoinRightKeys...),
			JoinProbeKeys:    append([]string(nil), j.JoinLeftKeys...),
		}, true
	}
	return AggregateShuffleCandidate{}, false
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
		if s.Type == "shuffle" || s.Type == "final_aggregate" || s.Type == "merge_aggregate" {
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
