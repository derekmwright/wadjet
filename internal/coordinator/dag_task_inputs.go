// This file holds stage task execution, input assignment, row bounds, and probe splitting.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// runStageTasks publishes the given tasks under stageQueryID, subscribes to
// the per-stage result subject, and waits for all completions. Returns one
// []string of result files per task in dispatch order, plus the worker-
// reported total output bytes. Used by dispatchFinalAggregateFanout for
// both phases; mirrors the inline dispatch+collect in
// dispatchScanAggregateStage / dispatchComputeStage.
//
// retryEnabled must be false when any task streams output mid-task
// (gather-fused terminal sinks) — a retried task would duplicate the
// streamed rows.
func (c *Coordinator) runStageTasks(
	ctx context.Context,
	stageQueryID, stageLabel string,
	tasks []distributed.Task,
	retryEnabled bool,
) ([][]string, int64, error) {
	if len(tasks) == 0 {
		return nil, 0, nil
	}
	trackerStages := map[string]*StageInfo{
		stageLabel: {StageID: stageLabel, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.RegisterInternal(stageQueryID, "", trackerStages, []string{stageLabel})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stageLabel, progress)()
	retrier := newTaskRetrier(tasks, retryEnabled, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("task retry publish failed",
				"stage_id", stageLabel, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stageLabel, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return nil, 0, fmt.Errorf("stage %s: subscribe: %w", stageLabel, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return nil, 0, fmt.Errorf("stage %s: publish: %w", stageLabel, err)
	}
	if err := awaitStageProgress(ctx, done, progress, stageLabel); err != nil {
		return nil, 0, err
	}

	if f, failed := retrier.FirstError(); failed {
		return nil, 0, stageTaskFailure(f, fmt.Errorf("stage %s: task %s failed after %d attempts: %s", stageLabel, f.TaskID, maxTaskAttempts, f.Message))
	}
	return retrier.Files(), retrier.TotalBytes(), nil
}

// estimateComputeTaskBytes estimates one compute task's input footprint
// from upstream output sizes, mirroring the slicing semantics of
// buildTaskInputsForStage / buildTaskInputsForBroadcastJoinSplitProbe:
// OutputPartitioned deps split across tasks; Replicated and SinglePart
// deps are read in full by every task; a probe-split's probe dep is
// manually sliced, so it splits too. Deps with unknown size (Bytes == 0,
// e.g. legacy workers) contribute nothing — the estimate degrades toward
// 0 = unknown rather than inventing numbers.
func estimateComputeTaskBytes(stage physical.Stage, inputs map[string]StageOutput, numTasks int, probeSplit bool) int64 {
	if numTasks <= 0 {
		numTasks = 1
	}
	probeDep := ""
	if probeSplit {
		probeDep = stage.LeftDepStage
		if probeDep == "" && len(stage.Dependencies) > 0 {
			probeDep = stage.Dependencies[0]
		}
	}
	var est int64
	for depID, out := range inputs {
		if out.Bytes <= 0 {
			continue
		}
		if depID == probeDep || out.Kind == OutputPartitioned {
			est += out.Bytes / int64(numTasks)
		} else {
			est += out.Bytes
		}
	}
	return est
}

// aggregateInputRowBound returns an EXACT upper bound on the number of rows
// one aggregate task will read: the sum of the upstream stage's reported
// per-partition row counts over the partition range bound to task w. Every
// aggregate emits at most one group per input row, so this bounds its group
// count too — which is what the worker's group-index layout decision needs
// (distributed.OpSpec.InputRowBound, exec/two_level_hash.go).
//
// 0 means UNKNOWN, and unknown must stay unknown: the layout rule pins an
// index flat when the bound is small, so a bound that reads low where the
// truth is high is the one error that costs. Every uncertain case therefore
// returns 0 —
//   - more than one dependency (the row counts do not compose),
//   - a dep that is not hash-partitioned, or one whose worker build reported
//     no PartitionRows,
//   - a partition vector that does not align 1:1 with NumPartitions.
//
// The caller is responsible for the fourth: it must not call this for a task
// whose inputs were assigned by probe-split / skew-split / round-robin
// file grouping rather than by partitionRangeForWorker.
func aggregateInputRowBound(stage physical.Stage, inputs map[string]StageOutput, w, numTasks int) int64 {
	if len(stage.Dependencies) != 1 {
		return 0
	}
	out, ok := inputs[stage.Dependencies[0]]
	if !ok || out.Kind != OutputPartitioned || out.NumPartitions <= 0 {
		return 0
	}
	if len(out.PartitionRows) != out.NumPartitions {
		return 0
	}
	start, end := partitionRangeForWorker(out.NumPartitions, w, numTasks)
	var rows int64
	for p := start; p < end; p++ {
		if out.PartitionRows[p] < 0 {
			return 0
		}
		rows += out.PartitionRows[p]
	}
	return rows
}

// aggregateRowBoundTotal sums aggregateInputRowBound(stage, inputs, w,
// numTasks) over every task w in [0, numTasks) — for the dispatch log line,
// not the per-task fragment build (canMigrateAggregate calls
// aggregateInputRowBound directly, once per task, with the same guard).
//
// ok is false whenever any of the four task-input slicings that do NOT
// assign a plain partitionRangeForWorker range — probe-affine (probeSets),
// broadcast-join probe-split (probeSplit), round-robin partial-agg fan-out
// (rrAggGroups), or skew-split (skewAssign) — apply, mirroring the guard at
// canMigrateAggregate's rowBound computation exactly: those slicings hand a
// task a file group aggregateInputRowBound cannot address, so a value
// computed as though it could would be meaningless. When ok is false the
// caller must not log total/tasksWithBound.
//
// The sum is taken over ALL tasks rather than sampled at w=0: a stage whose
// partition 0 happens to be empty (e.g. an empty hash bucket) must not read
// as "no bound" in the log when every other task has one.
func aggregateRowBoundTotal(stage physical.Stage, inputs map[string]StageOutput, numTasks int, probeSets [][]string, probeSplit bool, rrAggGroups [][]string, skewAssign []skewTaskAssignment) (total int64, tasksWithBound int, ok bool) {
	if probeSets != nil || probeSplit || rrAggGroups != nil || skewAssign != nil {
		return 0, 0, false
	}
	for w := 0; w < numTasks; w++ {
		if b := aggregateInputRowBound(stage, inputs, w, numTasks); b > 0 {
			total += b
			tasksWithBound++
		}
	}
	return total, tasksWithBound, true
}

// buildTaskInputsForStage maps a stage's Dependencies (upstream stage IDs)
// into Task.Inputs keyed by a per-stage alias convention:
//   - hash_join/broadcast_join: use stage.BuildTableAlias for the build
//     side (dep index 1) and "probe" for the other side (dep index 0).
//     With FusedJoins, additional dep entries are the fused builds — those
//     are NOT placed in task.Inputs because the worker reads
//     task.FusedJoins[i].BuildFiles directly (the dispatcher populates that
//     wire field by looking up each fused build's upstream output).
//   - aggregate/sort/etc: use the single dep's ID as alias.
func buildTaskInputsForStage(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, workerCount int) (map[string][]string, error) {
	inputs := make(map[string][]string)
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin, physical.StageSortMergeJoin:
		expectedDeps := 2 + len(stage.FusedJoins) + len(stage.ChainedJoins)
		if len(stage.Dependencies) != expectedDeps {
			return nil, fmt.Errorf("join stage %s expects %d deps (2 primary + %d fused + %d chained), got %d",
				stage.ID, expectedDeps, len(stage.FusedJoins), len(stage.ChainedJoins), len(stage.Dependencies))
		}
		probeDep := stage.LeftDepStage
		buildDep := stage.RightDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
		if buildDep == "" {
			buildDep = stage.Dependencies[1]
		}
		buildAlias, probeAlias := joinInputAliases(stage)
		inputs[buildAlias] = partitionFilesForWorker(upstreams[buildDep], workerIdx, workerCount)
		inputs[probeAlias] = partitionFilesForWorker(upstreams[probeDep], workerIdx, workerCount)
		// Fused-build deps are intentionally NOT added to inputs[]; their
		// files flow through task.FusedJoins[i].BuildFiles, populated by
		// dispatchComputeStage from the upstream stage outputs.
	case physical.StageUnion:
		// Task w IS arm w: it reads that arm's output WHOLE (not a
		// partition slice of it) and projects it onto the result columns.
		// The stage's concatenation is the union of what its tasks emit.
		if workerIdx < 0 || workerIdx >= len(stage.UnionArms) {
			return nil, fmt.Errorf("union stage %s: task %d has no arm (stage has %d)",
				stage.ID, workerIdx, len(stage.UnionArms))
		}
		dep := stage.UnionArmDep(workerIdx)
		inputs[dep] = flattenStageFiles(upstreams[dep])
	default:
		if len(stage.Dependencies) == 0 {
			return nil, fmt.Errorf("stage %s has no dependencies and no ScanFiles", stage.ID)
		}
		// Single-input stages (aggregate/sort): use dep ID as alias.
		depID := stage.Dependencies[0]
		inputs[depID] = partitionFilesForWorker(upstreams[depID], workerIdx, workerCount)
	}
	return inputs, nil
}

// joinInputAliases names a join fragment's two source aliases. Split out so
// the projection map (stageInputScanColumns) is keyed exactly the way
// buildTaskInputsForStage keys the file map — an alias that agreed by
// coincidence would silently drop the projection instead of failing.
func joinInputAliases(stage physical.Stage) (buildAlias, probeAlias string) {
	buildAlias = stage.BuildTableAlias
	if buildAlias == "" {
		buildAlias = "build"
	}
	probeAlias = "probe"
	if probeAlias == buildAlias {
		probeAlias = "probe_side"
	}
	return buildAlias, probeAlias
}

// stageInputScanColumns maps each source alias of a compute stage's
// fragment to the column projection its input should be read with — but
// only for inputs that are PASS-THROUGH LEAF SCANS, whose "files" are
// base-table parquet rather than a stage's own WSHF output.
//
// Without this the DAG reads those tables at FULL WIDTH while the
// single-process path projects, which is a correctness difference and not
// only a bytes one: scan.HasUnsupportedColumnarTypes tests the columns the
// read actually asks for, so one MAP or ARRAY column that the query never
// mentions drags the whole read into the row fallback (#410, #393). It is
// also the projection pushdown the DAG's base-table reads never had.
//
// Safe by construction on the worker side: a .wshf input ignores the hint
// (source_select.go), and cachedFileStreamSource applies it all-or-nothing
// — any name missing from the file schema reverts the read to full width.
func (c *Coordinator) stageInputScanColumns(ctx context.Context, stage physical.Stage, upstreams map[string]StageOutput) map[string][]string {
	out := make(map[string][]string)
	for alias, depID := range stageInputDeps(stage) {
		up, ok := upstreams[depID]
		if !ok || up.ScanTable == "" || len(up.ScanColumns) == 0 {
			continue
		}
		if p := c.prunedScanColumns(ctx, physical.Stage{
			TableName: up.ScanTable,
			Columns:   up.ScanColumns,
		}); len(p) > 0 {
			out[alias] = p
		}
	}
	return out
}

// stageInputScanSchemas is stageInputScanColumns' other half: for the same
// pass-through leaf-scan inputs, the catalog's declared TYPES for the columns
// those names refer to (#423).
//
// The projection says which columns to read; this says what they ARE. A
// parquet file cannot express nine of them, so a file written before the
// declared-schema footer key existed hands the consumer an INT64 where the
// catalog says IPv4 and the DAG answers 167772165 for 10.0.0.5. Empty for
// every WSHF input, which carries its own types.
func stageInputScanSchemas(stage physical.Stage, upstreams map[string]StageOutput) map[string][]distributed.ColumnSpec {
	out := make(map[string][]distributed.ColumnSpec)
	for alias, depID := range stageInputDeps(stage) {
		up, ok := upstreams[depID]
		if !ok || up.ScanTable == "" || len(up.ScanSchema) == 0 {
			continue
		}
		out[alias] = wireColumnSpecs(up.ScanSchema)
	}
	return out
}

// stageInputDeps maps each source alias of a compute stage's fragment to the
// dependency stage whose output that alias reads. Shared by every pass that
// has to say something per INPUT rather than per operator — what to read
// (stageInputScanColumns) and what it is (stageInputScanSchemas) — so the two
// cannot disagree about which alias is which dep.
func stageInputDeps(stage physical.Stage) map[string]string {
	out := make(map[string]string, 2)
	put := func(alias, depID string) {
		if alias == "" {
			return
		}
		out[alias] = depID
	}
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin, physical.StageSortMergeJoin:
		if len(stage.Dependencies) < 2 {
			return out
		}
		probeDep, buildDep := stage.LeftDepStage, stage.RightDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
		if buildDep == "" {
			buildDep = stage.Dependencies[1]
		}
		buildAlias, probeAlias := joinInputAliases(stage)
		put(buildAlias, buildDep)
		put(probeAlias, probeDep)
		// A fused or chained join's build is a dependency of this stage
		// too — buildTaskInputsForStage counts 2 + len(FusedJoins) +
		// len(ChainedJoins) — and buildJoinFragment gives each one an
		// OpBroadcastProbe keyed by its own BuildTableAlias. Leaving them
		// out here meant applySourceColumnTypes had no entry to find, so a
		// fused build over a PASS-THROUGH leaf scan read base-table parquet
		// with no declared types (#423/#503).
		for _, fj := range stage.FusedJoins {
			put(fj.BuildTableAlias, fj.BuildDepStage)
		}
		for _, cj := range stage.ChainedJoins {
			put(cj.BuildTableAlias, cj.BuildDepStage)
		}
	case physical.StageUnion:
		// Every arm, not just this task's: the alias IS the dep ID, so one
		// map serves all tasks.
		for i := range stage.UnionArms {
			put(stage.UnionArmDep(i), stage.UnionArmDep(i))
		}
	default:
		// The alias IS the dep ID on every non-join fragment, so every
		// dependency can be mapped, not just the first: a stage with a
		// stat-dep edge or a scalar-producer dep alongside its real input
		// used to drop everything past Dependencies[0].
		for _, dep := range stage.Dependencies {
			put(dep, dep)
		}
	}
	return out
}

// applySourceProjection stamps the per-alias projection onto a fragment's
// source operators. Applied after the fragment is built rather than inside
// each builder: the projection belongs to the alias, not to the operator
// chain that happens to read it, and every builder emits its source op the
// same way. A source that already carries an explicit projection (the leaf
// scan builders set theirs from stage.Columns) is left alone.
func applySourceProjection(ops []distributed.OpSpec, byAlias map[string][]string) {
	if len(byAlias) == 0 {
		return
	}
	for i := range ops {
		op := &ops[i]
		if op.Type != distributed.OpShuffleSource && op.Type != distributed.OpScan {
			continue
		}
		if len(op.Columns) > 0 {
			continue
		}
		if p, ok := byAlias[op.InputAlias]; ok {
			op.Columns = append([]string(nil), p...)
		}
	}
}

// applySourceColumnTypes stamps the per-alias DECLARED SCHEMA onto the same
// fragment's inputs — its source operators, and the build side of its join
// probes, which reads its own files under its own alias (#423).
//
// Placed here for the same reason as applySourceProjection: the declaration
// belongs to the alias, not to whichever operator chain reads it. An operator
// that already carries an explicit declaration (the leaf-scan builders set
// theirs from stage.ScanSchema) is left alone.
func applySourceColumnTypes(ops []distributed.OpSpec, byAlias map[string][]distributed.ColumnSpec) {
	if len(byAlias) == 0 {
		return
	}
	for i := range ops {
		op := &ops[i]
		switch op.Type {
		case distributed.OpScan, distributed.OpShuffleSource:
			if len(op.ColumnTypes) > 0 {
				continue
			}
			if s, ok := byAlias[op.InputAlias]; ok {
				op.ColumnTypes = s
			}
		case distributed.OpHashJoinProbe, distributed.OpBroadcastProbe, distributed.OpSortMergeJoin:
			if len(op.BuildColumnTypes) > 0 {
				continue
			}
			if s, ok := byAlias[op.BuildAlias]; ok {
				op.BuildColumnTypes = s
			}
		}
	}
}

// broadcastJoinProbeSplit reports whether a broadcast_join stage qualifies
// for probe-split fan-out. Returns the desired numTasks and ok=true when:
//   - the stage is a broadcast_join the planner left at numTasks=1 (DistSingleton)
//   - the cluster has spare workers (workerCount > 1)
//   - the probe upstream produced at least 2 files (whether OutputSinglePart
//     or OutputPartitioned — for a broadcast join, the probe-side parallelism
//     is just "split probe rows N ways," and that is correct regardless of
//     whether the upstream pre-partitioned by some hash key, because the
//     broadcast cache is replicated to every shard task anyway)
//
// Triggers only for DistSingleton broadcast_join; the hash-partitioned path
// is already parallelized by the planner via Exchange{Repartition}.
//
// History: this check used to require probeIn.Kind == OutputSinglePart
// strictly. That was overly conservative — at SF10 with compacted single-
// file dimension scans, scan-sharding (commit 47630b3) produced N partial
// outputs as OutputPartitioned, which the strict check rejected, leaving
// every broadcast_join in the chain single-tasked. Relaxed 2026-04-30 after
// observing the regression on the SF10 deploy of 47630b3 (Q02 22m vs 12m
// baseline) — sharding was firing but probe-split wasn't picking it up.
func broadcastJoinProbeSplit(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount, currentNumTasks int,
) (numTasks int, ok bool) {
	if stage.Type != physical.StageBroadcastJoin {
		return 0, false
	}
	if currentNumTasks != 1 {
		return 0, false
	}
	if workerCount <= 1 {
		return 0, false
	}
	// A fused-chain broadcast_join has 2 + N deps (probe + primary build +
	// N fused builds). Probe-split semantics are identical: split probe
	// files across shard tasks, replicate the broadcast caches (primary +
	// fused) to every shard. The shard count gate just needs to verify the
	// stage has the expected dep count.
	expectedDeps := 2 + len(stage.FusedJoins)
	if len(stage.Dependencies) != expectedDeps {
		return 0, false
	}
	probeDep := stage.LeftDepStage
	if probeDep == "" {
		probeDep = stage.Dependencies[0]
	}
	probeIn, present := inputs[probeDep]
	if !present {
		return 0, false
	}
	if probeIn.Kind != OutputSinglePart && probeIn.Kind != OutputPartitioned {
		return 0, false
	}
	probeFiles := flattenStageFiles(probeIn)
	if len(probeFiles) < 2 {
		return 0, false
	}
	n := workerCount
	if n > len(probeFiles) {
		n = len(probeFiles)
	}
	return n, true
}

func aggregatePartialSplit(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount, currentNumTasks int,
) (depID string, groups [][]string, ok bool) {
	if stage.Type != physical.StageAggregate {
		return "", nil, false
	}
	if stage.Distribution.Kind != physical.DistRoundRobin {
		return "", nil, false
	}
	if currentNumTasks != 1 || workerCount <= 1 {
		return "", nil, false
	}
	if len(stage.Dependencies) != 1 || len(stage.FusedJoins) != 0 {
		return "", nil, false
	}
	if len(stage.SortKeys) != 0 || stage.HasLimit || len(stage.FilterExprs) != 0 {
		return "", nil, false
	}
	depID = stage.Dependencies[0]
	in, present := inputs[depID]
	if !present || in.eager != nil {
		return "", nil, false
	}
	if in.Bytes < aggSplitMinBytes {
		return "", nil, false
	}
	files := flattenStageFiles(in)
	if len(files) < 2 {
		return "", nil, false
	}
	n := workerCount
	if n > len(files) {
		n = len(files)
	}
	groups = splitFilesEvenly(files, n)
	if len(groups) < 2 {
		return "", nil, false
	}
	return depID, groups, true
}

// buildTaskInputsForBroadcastJoinSplitProbe is the probe-split variant of
// buildTaskInputsForStage for broadcast_join. Each task receives:
//   - build alias → the full broadcast set (every task sees every build row)
//   - probe alias → its 1/numTasks slice of probe upstream files
//
// Caller must verify the stage has 2 deps and probe upstream is single-part
// with multiple files; this helper assumes the dispatcher already vetted
// eligibility.
func buildTaskInputsForBroadcastJoinSplitProbe(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, numTasks int) (map[string][]string, error) {
	probeDep := stage.LeftDepStage
	if probeDep == "" && len(stage.Dependencies) > 0 {
		probeDep = stage.Dependencies[0]
	}
	var probeFiles []string
	parts := splitFilesEvenly(flattenStageFiles(upstreams[probeDep]), numTasks)
	if workerIdx >= 0 && workerIdx < len(parts) {
		probeFiles = parts[workerIdx]
	}
	return probeSplitTaskInputs(stage, upstreams, probeFiles)
}

// probeSplitTaskInputs binds one probe-split task's inputs: the full
// broadcast build set plus an explicit probe slice (an even split, or one
// owner's group from probeSplitAffineSets). Any disjoint cover of the probe
// files is a correct split, so the slicer is the caller's choice.
func probeSplitTaskInputs(stage physical.Stage, upstreams map[string]StageOutput, probeFiles []string) (map[string][]string, error) {
	expectedDeps := 2 + len(stage.FusedJoins)
	if len(stage.Dependencies) != expectedDeps {
		return nil, fmt.Errorf("broadcast_join split-probe stage %s expects %d deps (2 primary + %d fused), got %d",
			stage.ID, expectedDeps, len(stage.FusedJoins), len(stage.Dependencies))
	}
	buildDep := stage.RightDepStage
	if buildDep == "" {
		buildDep = stage.Dependencies[1]
	}
	buildAlias := stage.BuildTableAlias
	if buildAlias == "" {
		buildAlias = "build"
	}
	probeAlias := "probe"
	if probeAlias == buildAlias {
		probeAlias = "probe_side"
	}
	out := make(map[string][]string, 2)
	out[buildAlias] = flattenStageFiles(upstreams[buildDep])
	if len(probeFiles) > 0 {
		out[probeAlias] = probeFiles
	}
	// Fused build deps don't go in inputs[]; their files travel via
	// task.FusedJoins[i].BuildFiles, populated by dispatchComputeStage.
	// Each shard task gets the full broadcast cache for every fused build,
	// just like the primary build.
	return out, nil
}
