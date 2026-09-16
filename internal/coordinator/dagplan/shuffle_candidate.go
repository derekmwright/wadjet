// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

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

// PickShuffleCandidate returns the largest non-probe scan above thresholdBytes
// and its connecting join. Probe is the largest scan, matching CanProbeSplit;
// do not use the join's logical BuildTableAlias, which probe-split can invert.
// Find direct LeftDepStage/RightDepStage or a matching fused-join build alias,
// and take that entry's build/probe keys. Return one best candidate only;
// chained-shuffle multi-candidate selection is not implemented.
// See docs/internals/shuffle-candidate-selection.md for the design.
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

// LargeBuildScans returns non-probe scan stages meeting the size threshold as
// build-side broadcast-cache candidates. Pre-scan/cache once in S3 so workers
// share typed WSHF rather than repeatedly decoding source parquet, and overlap
// the one source scan with other work. A SINGLE large build still qualifies:
// caching amortizes decode, not merely multi-build memory footprint.
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
