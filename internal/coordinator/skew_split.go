package coordinator

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Adaptive skew-aware task layout for shuffled hash joins
// (docs/design/skew-aware-shuffle.md). The shuffle stages report
// per-partition output bytes (StageOutput.PartitionBytes); at join dispatch
// the planner below detects task groups whose probe input exceeds an
// absolute byte threshold and splits them into k sub-tasks that divide the
// group's probe files and replicate its build files — the broadcast
// probe-split pattern applied per hot partition group. Flag-gated by
// Config.SkewSplit (--skew-split), default off.

var (
	// skewSplitTargetBytes is the per-sub-task probe input target: a hot
	// group splits into ceil(probeBytes / target) sub-tasks. Bytes are
	// on-disk uncompressed .wshf — the same unit per-task admission
	// estimates use. Var so tests can lower it to force splits on small
	// fixtures.
	skewSplitTargetBytes int64 = 256 << 20

	// skewSplitMinGroupBytes is the absolute floor below which a group is
	// never considered hot, regardless of how uneven the layout looks.
	skewSplitMinGroupBytes int64 = 256 << 20

	// skewSplitMinRatio is the minimum group-probe-bytes / mean-group-bytes
	// ratio for a group to count as hot. v1 shipped the absolute floor
	// alone ("what breaks under skew is one task's bytes vs its budget,
	// not the shape of the distribution") — the 2026-07-11 SF10 no-harm
	// A/B falsified that: TPC-H Q21's uniformly-heavy shuffle (~374 MB per
	// group, mean_ratio ≈ 1.0, nowhere near any memory budget) tripped the
	// floor on EVERY group, doubled the stage's task count for zero skew
	// relief, and regressed +21% wall. Requiring a real skew signal keeps
	// the mechanism to its name; the hot-key fixture's groups sit at
	// ratio 2.9 (local, 3 groups) to 21.7 (SF10) and still fire. Note the
	// mean includes the hot group itself, so with N groups the ratio is
	// bounded by N — at N=3 a group must carry 2/3 of all probe bytes.
	skewSplitMinRatio = 2.0

	// skewSplitMaxBuildBytes caps the build bytes a hot group may have and
	// still split, when the cluster hasn't reported worker pool budgets
	// (broadcastThresholdFromCluster returns 0). Replicating the build to
	// k sub-tasks multiplies its footprint cluster-wide; a build-side-hot
	// group must NOT split — the worker's grace partition-on-arrival
	// already bounds an oversized build inside a single task, whereas k
	// replicas of an over-budget build multiply the OOM.
	skewSplitMaxBuildBytes int64 = 200 << 20
)

// SkewSplitsPlanned counts hot-group split decisions (one per group per
// stage dispatch). The A/B mechanism marker: a wall-clock delta without
// this counter moving (and its "skew split planned" Info log) is window
// drift, not skew-split signal.
var SkewSplitsPlanned atomic.Int64

// skewTaskAssignment is one task of a skew-aware layout. group is the
// logical output slot — the task index of the UNSPLIT layout — so the
// dispatcher can re-group sub-task outputs by original partition range,
// keeping the stage's OutputPartitioned shape byte-compatible with the
// unsplit layout (downstream consumers already read multi-file slots).
type skewTaskAssignment struct {
	group    int
	inputs   map[string][]string
	estBytes int64 // per-task admission estimate; 0 = use the stage-wide default
}

// planSkewSplitTasks decides the skew-aware task layout for a shuffled
// hash-join stage. Returns nil (dispatcher keeps the standard layout) when
// the stage is ineligible or no group crosses the hot threshold; otherwise
// one assignment per task, in group order.
//
// Eligibility:
//   - hash_join with hash-partitioned distribution (broadcast_join has
//     probe-split; sort-merge-join needs aligned sorted runs — excluded v1)
//   - probe-side join semantics only (inner/left/semi/anti). right/full
//     emit unmatched BUILD rows, which a replicated build would duplicate
//     k times.
//   - both primary deps are OutputPartitioned with the same partition count
//     and reported PartitionBytes (nil vectors = legacy workers → off).
//
// A group is hot when it crosses the absolute floor (skewSplitMinGroupBytes)
// AND carries at least skewSplitMinRatio× the mean group's probe bytes —
// heavy-but-uniform stages (every group over the floor, ratio ≈ 1) keep the
// standard layout. A hot group splits into k = ceil(probeBytes/skewSplitTargetBytes)
// sub-tasks, capped by workerCount and by the group's probe file count
// (v1 splits at file granularity; a hot partition written by T shuffle
// tasks has up to T files). Each sub-task reads 1/k of the probe files and
// the group's FULL build files — a probe row for key k needs the complete
// build side for k, which replication preserves. Fused builds are already
// replicated to every task by the dispatcher (task.FusedJoins[i].BuildFiles)
// and need no handling here.
func (c *Coordinator) planSkewSplitTasks(
	stage physical.Stage,
	inputs map[string]StageOutput,
	numTasks, workerCount int,
) []skewTaskAssignment {
	if stage.Type != physical.StageHashJoin ||
		stage.Distribution.Kind != physical.DistHashPartitioned ||
		numTasks <= 0 || workerCount <= 1 {
		return nil
	}
	switch stage.JoinType {
	case "inner", "left", "semi", "anti":
	default:
		return nil
	}
	if len(stage.Dependencies) != 2+len(stage.FusedJoins) {
		return nil
	}
	probeDep := stage.LeftDepStage
	buildDep := stage.RightDepStage
	if probeDep == "" {
		probeDep = stage.Dependencies[0]
	}
	if buildDep == "" {
		buildDep = stage.Dependencies[1]
	}
	probeOut, probeOK := inputs[probeDep]
	buildOut, buildOK := inputs[buildDep]
	if !probeOK || !buildOK ||
		probeOut.Kind != OutputPartitioned || buildOut.Kind != OutputPartitioned ||
		probeOut.NumPartitions != buildOut.NumPartitions ||
		len(probeOut.PartitionBytes) != probeOut.NumPartitions ||
		len(buildOut.PartitionBytes) != buildOut.NumPartitions {
		return nil
	}

	// Build replication bound: prefer the cluster-derived broadcast
	// threshold (the same "does a duplicated build fit the smallest
	// worker's pool" question broadcast planning answers).
	buildBound := skewSplitMaxBuildBytes
	if c.workers != nil {
		if t := broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget()); t > 0 {
			buildBound = t
		}
	}

	// Mean group probe bytes, denominator of the skew-ratio trigger.
	var totalProbeBytes int64
	for _, b := range probeOut.PartitionBytes {
		totalProbeBytes += b
	}
	meanGroupBytes := totalProbeBytes / int64(numTasks)

	buildAlias := stage.BuildTableAlias
	if buildAlias == "" {
		buildAlias = "build"
	}
	probeAlias := "probe"
	if probeAlias == buildAlias {
		probeAlias = "probe_side"
	}

	assignments := make([]skewTaskAssignment, 0, numTasks)
	anySplit := false
	for w := 0; w < numTasks; w++ {
		start, end := partitionRangeForWorker(probeOut.NumPartitions, w, numTasks)
		var probeBytes, buildBytes int64
		var probeFiles, buildFiles []string
		for p := start; p < end; p++ {
			probeBytes += probeOut.PartitionBytes[p]
			buildBytes += buildOut.PartitionBytes[p]
			probeFiles = append(probeFiles, probeOut.Files[p]...)
			buildFiles = append(buildFiles, buildOut.Files[p]...)
		}

		k := 0
		if probeBytes > 0 {
			k = int((probeBytes + skewSplitTargetBytes - 1) / skewSplitTargetBytes)
		}
		if k > workerCount {
			k = workerCount
		}
		if k > len(probeFiles) {
			k = len(probeFiles)
		}
		ratio := float64(0)
		if meanGroupBytes > 0 {
			ratio = float64(probeBytes) / float64(meanGroupBytes)
		}
		hot := k > 1 && probeBytes > skewSplitMinGroupBytes && ratio >= skewSplitMinRatio

		if hot && buildBytes > buildBound {
			// Build-side-hot: do not split (see skewSplitMaxBuildBytes).
			// Logged so the no-split decision is visible in benchmark logs.
			c.logger.Info("skew split skipped: build side over replication bound",
				"stage_id", stage.ID, "group", w,
				"partitions_start", start, "partitions_end", end,
				"probe_bytes", probeBytes, "build_bytes", buildBytes,
				"build_bound", buildBound)
			hot = false
		}

		if !hot {
			assignments = append(assignments, skewTaskAssignment{
				group: w,
				inputs: map[string][]string{
					buildAlias: buildFiles,
					probeAlias: probeFiles,
				},
			})
			continue
		}

		anySplit = true
		SkewSplitsPlanned.Add(1)
		c.logger.Info("skew split planned",
			"stage_id", stage.ID, "group", w,
			"partitions_start", start, "partitions_end", end,
			"probe_bytes", probeBytes, "build_bytes", buildBytes,
			"split_factor", k, "probe_files", len(probeFiles),
			"mean_ratio", ratio)
		perSubEst := buildBytes + probeBytes/int64(k)
		for _, slice := range splitFilesEvenly(probeFiles, k) {
			assignments = append(assignments, skewTaskAssignment{
				group: w,
				inputs: map[string][]string{
					buildAlias: buildFiles,
					probeAlias: slice,
				},
				estBytes: perSubEst,
			})
		}
	}
	if !anySplit {
		return nil
	}
	return assignments
}
