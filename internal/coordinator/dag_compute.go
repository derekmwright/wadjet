// This file holds compute-stage dispatch and partitioned task construction.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// dispatchComputeStage handles non-leaf compute stages (hash_join,
// broadcast_join, aggregate, final_aggregate, sort, merge_sort) by
// emitting workerCount TaskTypeStage tasks. Each task's Inputs are
// sliced from upstream stage outputs via partitionFilesForWorker; each
// task writes unpartitioned .wshf output to a per-worker result key.
func (c *Coordinator) dispatchComputeStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
	fusion *gatherFusion,
	scalars scalarResolver,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1 // single-worker fallback for sparse test / bootstrap setups
	}
	// Singleton final_aggregate over a fanned-out upstream is a serialization
	// point — one worker re-aggregates every partial while the others idle.
	// Reshape into a parallel intermediate-merge phase + a 1-task final
	// merge when the preconditions hold (multi-worker, K>=2 upstream files,
	// no AVG). Falls back to the standard 1-task path otherwise.
	if _, _, ok := finalAggregateFanoutCandidate(stage, inputs, workerCount); ok {
		return c.dispatchFinalAggregateFanout(ctx, queryID, stage, inputs, workerCount, fusion, scalars)
	}
	// Deferred scalars on the single-task fallback: no earlier phase to
	// overlap with, so resolve before building the task — same wall
	// behavior as the pre-deferral upfront await.
	if scalars != nil {
		var rerr error
		stage, rerr = scalars(ctx)
		if rerr != nil {
			return StageOutput{}, fmt.Errorf("stage %s scalar substitution: %w", stage.ID, rerr)
		}
	}
	// Task count derivation:
	//   - Singleton output: exactly 1 task. The stage produces one logical
	//     result; running workerCount tasks on the same unpartitioned input
	//     duplicates the output N× (caught on SF10 Q01: 18 rows instead of
	//     6 with workerCount=3).
	//   - Hash-partitioned output: one task per output partition. The
	//     planner's distribution.Count is authoritative; fall back to
	//     workerCount if unset.
	//   - Broadcast output: 1 task producing a file every consumer reads.
	//   - Anything else: correctness-first default of 1 task.
	numTasks := 1
	switch stage.Distribution.Kind {
	case physical.DistSingleton:
		numTasks = 1
	case physical.DistHashPartitioned:
		numTasks = stage.Distribution.Count
		if numTasks <= 0 {
			numTasks = workerCount
		}
	case physical.DistBroadcast:
		numTasks = 1
	default:
		// Unknown distribution — fall back to 1 rather than workerCount.
		numTasks = 1
	}
	// A union stage's task count is its ARM count, not a distribution
	// property: task i reads arm i. Its RoundRobin label says only that the
	// outputs carry no key clustering.
	unionStage := stage.Type == physical.StageUnion
	if unionStage {
		if len(stage.UnionArms) < 2 {
			return StageOutput{}, fmt.Errorf("stage %s: union stage has %d arms, expected at least 2",
				stage.ID, len(stage.UnionArms))
		}
		numTasks = len(stage.UnionArms)
	}
	// Probe-split for broadcast_join: when the planner picks DistSingleton
	// (because probe upstream is a leaf scan with DistSingleton output), the
	// join would otherwise run as a single task on one worker — the entire
	// probe scan + hash probe is serialized. When workerCount > 1 and the
	// probe upstream is a multi-file OutputSinglePart, fan out to
	// min(workerCount, len(probeFiles)) tasks each scanning 1/N of probe
	// files. The build is broadcast (every task reads the same OutputSinglePart
	// Files[0]), so memory pressure stays bounded — the per-task hash table
	// is identical to the single-task case. Only the probe scan + probe
	// compute is parallelized.
	probeSplit := false
	// Ownership-aware probe slicing (scan_affinity.go probeSplitAffineSets):
	// when the probe is a pass-through leaf scan, group its base-table
	// files by rendezvous owner and pin each task to that owner's NVMe
	// cache, so no probe file has to cross the peer wire at task start.
	// nil sets = the plain even split below.
	var probeSets [][]string
	var probeOwners []string
	if n, ok := broadcastJoinProbeSplit(stage, inputs, workerCount, numTasks); ok {
		numTasks = n
		probeSplit = true
		probeDep := stage.LeftDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
		probeIn := inputs[probeDep]
		var bal *affineBalance
		probeSets, probeOwners, bal = probeSplitAffineSets(flattenStageFiles(probeIn), probeIn.ScanFileSizes, c.activeWorkerIDs())
		if probeSets != nil {
			numTasks = len(probeSets)
			c.logAffineBalance(stage.ID, bal)
		}
	}
	// Round-robin partial-aggregate fan-out (see aggregatePartialSplit).
	// Mutually exclusive with probe-split and skew-split by stage type.
	var rrAggGroups [][]string
	rrAggDep := ""
	if c.config.AggPartialSplit {
		if dep, groups, ok := aggregatePartialSplit(stage, inputs, workerCount, numTasks); ok {
			rrAggDep, rrAggGroups = dep, groups
			numTasks = len(groups)
		}
	}
	// Adaptive skew split (--skew-split, docs/design/skew-aware-shuffle.md):
	// a shuffled hash join whose deps report per-partition bytes may split
	// hot partition groups into k sub-tasks (probe files divided, build
	// files replicated). skewGroups keeps the UNSPLIT slot count so the
	// output below re-groups sub-task files by original partition range —
	// downstream partition-aligned consumers see the same layout shape
	// either way.
	skewGroups := numTasks
	var skewAssign []skewTaskAssignment
	// Partitioned chained builds (stage-chain fusion of a downstream
	// hash_join) bind build partition i to task i; a skew sub-task's index
	// no longer equals its partition index, which would mis-slice those
	// builds. Broadcast chained builds ride whole and stay skew-safe.
	partitionedChain := false
	for _, cj := range stage.ChainedJoins {
		if cj.Partitioned {
			partitionedChain = true
			break
		}
	}
	if c.config.SkewSplit && !probeSplit && !partitionedChain {
		if a := c.planSkewSplitTasks(stage, inputs, numTasks, workerCount); a != nil {
			skewAssign = a
			numTasks = len(a)
		}
	} else if c.config.SkewSplit && partitionedChain {
		c.logger.Info("skew split skipped: stage has partitioned chained joins",
			"stage_id", stage.ID, "chained", len(stage.ChainedJoins))
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	dispatchAttrs := []any{
		"stage_id", stage.ID, "stage_type", stage.Type,
		"deps", stage.Dependencies, "num_tasks", numTasks,
		"distribution_kind", stage.Distribution.Kind,
		"distribution_count", stage.Distribution.Count,
		"rr_agg_split", rrAggGroups != nil,
		"probe_split", probeSplit,
		"probe_affine", probeSets != nil,
		"skew_split", skewAssign != nil,
		"inputs_aliases", len(inputs),
	}
	// Group-index layout marker for aggregate stages: the exact upstream row
	// count these tasks will re-aggregate, summed across every task rather
	// than sampled at task 0 (aggregateRowBoundTotal), and only when the
	// same slicing exclusions the per-task rowBound computation at
	// canMigrateAggregate applies (probeSets/probeSplit/rrAggGroups/
	// skewAssign all hand a task a file group aggregateInputRowBound cannot
	// address). Together with the worker's `two_level_born_flat` counter
	// this makes "did the unbounded final aggregate take the flat layout" a
	// one-grep answer per run (the ADR-0014 lesson: a counter nobody can
	// read is a counter nobody reads).
	if stage.Type == "aggregate" || stage.Type == "final_aggregate" || stage.Type == "merge_aggregate" {
		if total, tasksWithBound, ok := aggregateRowBoundTotal(stage, inputs, numTasks, probeSets, probeSplit, rrAggGroups, skewAssign); ok && total > 0 {
			dispatchAttrs = append(dispatchAttrs, "agg_row_bound_total", total, "agg_row_bound_tasks", tasksWithBound)
		}
	}
	// Plan-side engagement marker for late-materialization A/B arms: join
	// stages dispatched with view-column output enabled are grep-able from
	// benchmark.log (worker-side runtime counters live in worker logs, which
	// benchmark teardown discards). Flag-off logs stay byte-identical.
	if c.config.LateMaterialization &&
		(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) {
		dispatchAttrs = append(dispatchAttrs, "late_mat", true)
	}
	// Stage-chain fusion engagement marker (grep-able from benchmark.log):
	// number of absorbed downstream joins running in this stage's fragment.
	if len(stage.ChainedJoins) > 0 {
		dispatchAttrs = append(dispatchAttrs, "chained_joins", len(stage.ChainedJoins))
	}
	c.logger.Info("dispatchComputeStage", dispatchAttrs...)

	// Observability for fused-chain probe-split: log the cluster-wide
	// broadcast-cache file count this stage will read across all shard
	// tasks. With N shards × (1 primary + M fused) caches, total S3 reads
	// scale linearly. Useful for spotting when amplification is dominating
	// wall time on bigger SF deploys.
	if len(stage.FusedJoins) > 0 || probeSplit {
		var primaryFiles, fusedFiles int
		if buildDep := stage.RightDepStage; buildDep != "" {
			primaryFiles = len(flattenStageFiles(inputs[buildDep]))
		}
		for _, fj := range stage.FusedJoins {
			fusedFiles += len(flattenStageFiles(inputs[fj.BuildDepStage]))
		}
		c.logger.Info("fused/probe-split broadcast cache load",
			"stage_id", stage.ID,
			"num_tasks", numTasks,
			"fused_count", len(stage.FusedJoins),
			"primary_cache_files", primaryFiles,
			"fused_cache_files_total", fusedFiles,
			"cluster_cache_reads_estimate", numTasks*(primaryFiles+fusedFiles))
	}

	// Build one task per output partition. Input slicing uses numTasks as
	// the divisor so each task reads its share of the upstream partitioned
	// input — for Singleton stages every task reads the full upstream; for
	// Hash-partitioned N-task stages each task reads its N-th partition.
	tasks := make([]distributed.Task, 0, numTasks)
	inputScanCols := c.stageInputScanColumns(ctx, stage, inputs)
	inputScanTypes := stageInputScanSchemas(stage, inputs)
	for w := 0; w < numTasks; w++ {
		var taskInputs map[string][]string
		var err error
		if probeSets != nil {
			taskInputs, err = probeSplitTaskInputs(stage, inputs, probeSets[w])
		} else if probeSplit {
			taskInputs, err = buildTaskInputsForBroadcastJoinSplitProbe(stage, inputs, w, numTasks)
		} else if rrAggGroups != nil {
			// Partial-aggregate fan-out: task w aggregates its disjoint
			// file group; alias follows buildTaskInputsForStage's
			// single-input convention (dep ID).
			taskInputs = map[string][]string{rrAggDep: rrAggGroups[w]}
		} else if skewAssign != nil {
			taskInputs = skewAssign[w].inputs
		} else {
			taskInputs, err = buildTaskInputsForStage(stage, inputs, w, numTasks)
		}
		if err != nil {
			return StageOutput{}, fmt.Errorf("stage %s worker %d: %w", stage.ID, w, err)
		}
		// Convert stage.AggSpecs → distributed.AggSpec, decomposing AVG
		// into (SUM, COUNT) pairs so the merge step sees only mergable
		// aggregates. The worker's avg-fold (executor_stage.go) reads
		// the synthetic columns and emits the original AVG output names.
		aggs := wireAggSpecs(stage.AggSpecs)
		aggs = c.decomposeOhlcvFor(decomposeCovar(decomposeVar(decomposeAvg(aggs))))
		// Convert stage.SortKeys → distributed.SortKeySpec.
		var sorts []distributed.SortKeySpec
		for _, s := range stage.SortKeys {
			sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc,
				NullsLast: distributed.NullsLastPtr(s.NullsLast), SlotPos: s.SlotPos})
		}
		// Translate planner-side FusedJoinSpec (carries BuildDepStage) into
		// wire-side FusedJoinSpec (carries BuildFiles) by looking up each
		// fused build's upstream stage output. This is what lets a fused
		// broadcast-join chain run as one pipelined task instead of N
		// separate stages with S3 round-trips between them.
		var wireFused []distributed.FusedJoinSpec
		for fi, fj := range stage.FusedJoins {
			buildOut, ok := inputs[fj.BuildDepStage]
			if !ok {
				return StageOutput{}, fmt.Errorf("stage %s fused join %d: build dep %q output not found",
					stage.ID, fi, fj.BuildDepStage)
			}
			wireFused = append(wireFused, distributed.FusedJoinSpec{
				JoinType:        fj.JoinType,
				JoinLeftKeys:    append([]string(nil), fj.JoinLeftKeys...),
				JoinRightKeys:   append([]string(nil), fj.JoinRightKeys...),
				JoinKeyTypes:    wireKeyTypes(fj.JoinKeyTypes),
				BuildFiles:      flattenStageFiles(buildOut),
				BuildTableAlias: fj.BuildTableAlias,
				BuildColOrigins: fj.BuildColOrigins,
				JoinFilter:      fj.JoinFilter,
				FilterExprs:     append([]string(nil), fj.FilterExprs...),
				BuildSchema:     wireColumnSpecs(fj.JoinBuildSchema),
			})
		}
		// Translate ChainedJoins (stage-chain fusion; docs/design/
		// stage-chain-fusion.md) into post-primary probe OpSpecs. A
		// Partitioned chained build is hash-partitioned 1:1 with this
		// stage's tasks — task w reads its own slice, same semantics the
		// absorbed stage's dispatch would have applied; broadcast builds
		// ride whole, shared per worker via the broadcast-join cache.
		// The absorbed stage's residual filters and output projection ride
		// the op chain so the fused output is byte-equivalent to what the
		// separate stage produced.
		var chainedOps []distributed.OpSpec
		for ci, cj := range stage.ChainedJoins {
			buildOut, ok := inputs[cj.BuildDepStage]
			if !ok {
				return StageOutput{}, fmt.Errorf("stage %s chained join %d: build dep %q output not found",
					stage.ID, ci, cj.BuildDepStage)
			}
			var chainBuildFiles []string
			opType := distributed.OpBroadcastProbe
			if cj.Partitioned {
				opType = distributed.OpHashJoinProbe
				chainBuildFiles = partitionFilesForWorker(buildOut, w, numTasks)
			} else {
				chainBuildFiles = flattenStageFiles(buildOut)
			}
			chainedOps = append(chainedOps, distributed.OpSpec{
				Type:                opType,
				JoinType:            cj.JoinType,
				LeftKeys:            append([]string(nil), cj.JoinLeftKeys...),
				RightKeys:           append([]string(nil), cj.JoinRightKeys...),
				KeyTypes:            wireKeyTypes(cj.JoinKeyTypes),
				BuildAlias:          cj.BuildTableAlias,
				BuildFiles:          chainBuildFiles,
				BuildBucket:         c.config.ResultBucket,
				BuildColOrigins:     cj.BuildColOrigins,
				JoinFilter:          cj.JoinFilter,
				BuildFilterExprs:    append([]string(nil), cj.BuildFilterExprs...),
				QualifyAllBuildCols: cj.QualifyAllBuildCols,
				OutputColumns:       append([]string(nil), cj.Columns...),
				HiddenColumns:       wireHiddenJoinCols(cj.HiddenJoinCols),
				// The absorbed join's own empty-input rules ride with it, so
				// the worker builds one `exec.LateralEmptyDefault` per
				// absorbed lateral join — the position the single-process
				// planner gives it, directly above the probe that made the pad
				// (#988).
				EmptyDefaults:   wireLateralDefaults(cj.LateralEmptyDefaults),
				PadMarker:       cj.LateralPadMarker,
				DropMarker:      cj.LateralDropMarker,
				LateMaterialize: c.config.LateMaterialization,
				BuildSchema:     wireColumnSpecs(cj.JoinBuildSchema),
			})
			if len(cj.FilterExprs) > 0 {
				chainedOps = append(chainedOps, distributed.OpSpec{
					Type:       distributed.OpFilter,
					Predicates: append([]string(nil), cj.FilterExprs...),
				})
			}
		}
		// Chain-terminal partial aggregate (stage-chain fusion step 2):
		// the join output collapses to partials in-process. Raw mode
		// (MergeMode=false — input is raw join rows); AVG decomposes to
		// (SUM, COUNT) exactly as the dropped stage's dispatch did, so
		// the downstream final's merge and avg-fold see identical specs.
		if len(stage.ChainedAggSpecs) > 0 || len(stage.ChainedAggGroupBy) > 0 {
			chainAggs := wireAggSpecs(stage.ChainedAggSpecs)
			chainedOps = append(chainedOps, distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    append([]string(nil), stage.ChainedAggGroupBy...),
				GroupByResolve: wireGroupKeyResolve(stage.GroupByResolve),
				GroupByTypes:   wireGroupByTypes(stage.GroupByTypes),
				GroupByDecimal: wireGroupByDecimal(stage.GroupByDecimal),
				Aggregates:     c.decomposeOhlcvFor(decomposeCovar(decomposeVar(decomposeAvg(chainAggs)))),
				// Derived group-bys / agg inputs (SUBSTR(...), price*(1-disc))
				// need the worker's input projection ahead of the aggregate —
				// without it the expression column doesn't exist and groups
				// silently collapse (caught by the three-arm differential).
				BuildProject: true,
			})
		}
		t := distributed.Task{
			ID:                   uuid.New().String()[:8],
			QueryID:              queryID,
			StageID:              stage.ID,
			Type:                 distributed.TaskTypeStage,
			StageType:            stage.Type,
			JoinType:             stage.JoinType,
			JoinLeftKeys:         stage.JoinLeftKeys,
			JoinRightKeys:        stage.JoinRightKeys,
			JoinKeyTypes:         wireKeyTypes(stage.JoinKeyTypes),
			BuildTableAlias:      stage.BuildTableAlias,
			QualifyAllBuildCols:  stage.QualifyAllBuildCols,
			BuildColOrigins:      stage.BuildColOrigins,
			JoinFilter:           stage.JoinFilter,
			NullAwareAnti:        stage.NullAwareAnti,
			BuildFilterExprs:     append([]string(nil), stage.BuildFilterExprs...),
			JoinProbeSchema:      wireColumnSpecs(stage.JoinProbeSchema),
			JoinBuildSchema:      wireColumnSpecs(stage.JoinBuildSchema),
			HiddenJoinColumns:    wireHiddenJoinCols(stage.HiddenJoinCols),
			LateralEmptyDefaults: wireLateralDefaults(stage.LateralEmptyDefaults),
			LateralPadMarker:     stage.LateralPadMarker,
			LateralDropMarker:    stage.LateralDropMarker,
			FusedJoins:           wireFused,
			GroupByCols:          stage.GroupByCols,
			Aggregates:           aggs,
			SortKeys:             sorts,
			// Task.Limit is dead (zero readers) and stage.Limit can now be NoLimit(-1);
			// deliberately not carried — see #481.
			RowLimit: stage.RowLimit,
			Inputs:   taskInputs,
			// Probe-split affinity: the rendezvous owner of this task's
			// probe files ("" on every other path — plain binpack).
			AffinityWorkerID: affinityFor(probeOwners, w),
			DataBucket:       c.config.ResultBucket,
			ResultBucket:     c.config.ResultBucket,
			ResultPrefix:     resultPrefix,
			CreatedAt:        time.Now(),
			// Output column projection for hash_join / broadcast_join stages.
			// The worker applies these as the probe operator's OutputFilter so
			// the join emits only the columns the downstream stage consumes,
			// instead of the full union of build+probe schemas. Without this
			// the wide post-join schema rides every chained shuffle (which
			// can't prune via prunedScanColumns because TableName="" upstream
			// of a join). Aggregate/sort stages ignore this field.
			Columns: append([]string(nil), stage.Columns...),
			// Filters attached to a compute stage (HAVING on
			// aggregate/final_aggregate, residual predicates on
			// hash_join) reference OUTPUT columns and must run after
			// the stage's main operator. FilterExprs on compute stages
			// would otherwise silently drop — that's the root cause of
			// Q15 ignoring `WHERE total_revenue = (SELECT MAX...)` and
			// Q18 ignoring `HAVING SUM(l_quantity) > 300`.
			PostFilterExprs: append([]string(nil), stage.FilterExprs...),
		}
		// Eager consumer dispatch: a provisional upstream (producer still
		// running) feeds this task's alias from producer-task manifests
		// (Task.EagerInputs) rather than the frozen — here empty — file
		// list in taskInputs. Aliases must match buildTaskInputsForStage's
		// convention (dep ID for single-input stages; build/probe aliases
		// for joins — eagerAliasForDep). Skew/probe-split slicing never
		// applies to eager stages (provisional accounting is nil and the
		// eager clearance already ruled out a projected split), so the
		// partition range matches what the frozen path would bind for
		// task w.
		for depID, in := range inputs {
			if in.eager == nil {
				continue
			}
			if t.EagerInputs == nil {
				t.EagerInputs = make(map[string]distributed.EagerInput, 2)
			}
			t.EagerInputs[eagerAliasForDep(stage, depID)] = in.eager.eagerInputForTask(w, numTasks)
		}
		// Join fragments execute fused downstream exchanges without an intermediate S3 hop;
		// keep legacy single-op fields populated for wire compatibility.
		// Hash/broadcast joins use OpExchangeSender for a downstream repartition,
		// OpUnpartitionedSink otherwise; GroupByCols joins are not an emitted shape.
		// Fused SortKeys become an OpSort breaker in the fragment chain.
		// Probe-split taskInputs retain their slicing; sourceForAliasWithProjection
		// uniformly detects parquet versus WSHF.
		// Probe-split requires the fragment runner's spilled-partition flush: without it,
		// a primary build partition spilled under chain pressure can yield zero rows.
		// See docs/internals/join-fragment-dispatch-contract.md for the design.
		canMigrateJoin := t.Operators == nil &&
			(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) &&
			len(stage.GroupByCols) == 0
		if canMigrateJoin {
			var sinkOp distributed.OpSpec
			if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
				sinkOp = distributed.OpSpec{
					Type:            distributed.OpExchangeSender,
					ShuffleKeys:     append([]string(nil), stage.Exchange.Keys...),
					ShuffleKeyTypes: wireKeyTypes(stage.Exchange.KeyTypes),
					NumPartitions:   stage.Exchange.Count,
				}
			} else {
				sinkOp = distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
			}
			ops, ferr := buildJoinFragment(stage, &t, taskInputs, wireFused, chainedOps, sorts, sinkOp, c.config.LateMaterialization)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build join fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		} else if len(chainedOps) > 0 {
			// A stage carrying chained joins must dispatch as a fragment —
			// the legacy single-op path would silently drop the chain.
			return StageOutput{}, fmt.Errorf("stage %s: %d chained joins on a non-fragment stage (type=%s)",
				stage.ID, len(stage.ChainedJoins), stage.Type)
		}
		// Sort-merge join stages always route through the fragment runner —
		// there is no legacy single-op execution path for them, so any
		// ineligibility is a planner bug and fails loudly.
		if stage.Type == physical.StageSortMergeJoin {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: sort_merge_join stage already claimed by another migration", stage.ID)
			}
			ops, ferr := buildSortMergeJoinFragment(stage, &t, taskInputs, wireFused, sorts)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build sort-merge join fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Aggregate fragments run ShuffleSource, HashAggregate, Filter?(HAVING), Sort?, Sink.
		// An unpartitioned sink emits one .wshf per task, one row per group per task,
		// matching legacy aggregate consumers without file-count amplification.
		// SortKeys / Limit folded from a downstream Singleton sort are handled by the
		// multi-breaker runner in the same task.
		// Only migrate tasks with no existing Operators and no ReplySubject.
		// Standalone aggregate stages consume non-scan upstream input with MergeMode=false;
		// merge_aggregate / final_aggregate merge partials with MergeMode=true.
		// buildAggregateFragment derives the mode from stage.Type.
		// See docs/internals/aggregate-fragment-dispatch-modes.md for the design.
		canMigrateAggregate := t.Operators == nil &&
			(stage.Type == "aggregate" || stage.Type == "final_aggregate" || stage.Type == "merge_aggregate") &&
			t.ReplySubject == ""
		if canMigrateAggregate {
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			// Exact input-row bound for the group-index layout decision.
			// Only the plain partition-range assignment has one: the other
			// three slicings hand a task a file group whose row counts the
			// per-partition vector cannot address.
			var rowBound int64
			if probeSets == nil && !probeSplit && rrAggGroups == nil && skewAssign == nil {
				rowBound = aggregateInputRowBound(stage, inputs, w, numTasks)
			}
			ops, ferr := buildAggregateFragment(stage, &t, taskInputs, aggs, sorts, fusedSubject, rowBound)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build aggregate fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// A RawInputAggregate final is only correct on the fragment path —
		// the legacy worker path derives merge mode from StageType and
		// would remap the raw AggSpecs into merge form. The planner never
		// emits the shape outside fragment eligibility (its dep is an
		// exchange, never a gather); fail loudly if that invariant breaks.
		if stage.RawInputAggregate && t.Operators == nil {
			return StageOutput{}, fmt.Errorf("stage %s: RawInputAggregate final_aggregate did not take the fragment path", stage.ID)
		}
		// Sort migration: dispatch standalone Sort / merge_sort stages via
		// the fragment path. The fragment runs:
		//   [OpShuffleSource, OpSort, OpUnpartitionedSink]
		// Output shape (one .wshf per task) matches the legacy
		// executeStageSort path. Eligibility excludes any stage already
		// claimed by canFuseJoin (joins have no SortKeys) or
		// canMigrateAggregate (mutually exclusive on stage.Type), so this
		// reads as "if no other migration claimed it, and it's a sort,
		// migrate it."
		canMigrateSort := t.Operators == nil &&
			(stage.Type == "sort" || stage.Type == "merge_sort") &&
			len(stage.SortKeys) > 0 &&
			t.ReplySubject == ""
		if canMigrateSort {
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildSortFragment(stage, &t, taskInputs, sorts, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build sort fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Window migration: always on the fragment path. There is no legacy
		// single-op window handler in the worker — a window stage that
		// reaches dispatch unclaimed ships with Operators == nil and dies
		// in executeStage, which is the whole of #349.
		if stage.Type == physical.StageWindow {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: window stage already claimed by another migration", stage.ID)
			}
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildWindowFragment(stage, &t, taskInputs, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build window fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Limit migration: always on the fragment path, for the window
		// stage's reason — there is no legacy single-op limit handler, so an
		// unclaimed limit stage would ship with Operators == nil and die in
		// executeStage. Failing here says which stage instead.
		if stage.Type == physical.StageLimit {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: limit stage already claimed by another migration", stage.ID)
			}
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildLimitFragment(stage, &t, taskInputs, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build limit fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Project migration: always on the fragment path, for the window
		// stage's reason — there is no legacy single-op handler, so an
		// unclaimed project stage would ship with Operators == nil and die
		// in executeStage.
		if stage.Type == physical.StageProject {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: project stage already claimed by another migration", stage.ID)
			}
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildProjectFragment(stage, &t, taskInputs, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build project fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Union migration: one arm per task, always on the fragment path.
		// There is no legacy single-op execution for a union stage, so an
		// unclaimed one would silently dispatch as a no-op pipe that
		// re-emits its arm's raw input — precisely the #346 symptom. Fail
		// instead.
		if unionStage {
			if t.Operators != nil {
				return StageOutput{}, fmt.Errorf("stage %s: union stage already claimed by another migration", stage.ID)
			}
			var fusedSubject string
			if fusion != nil {
				fusedSubject = fusion.replySubject
			}
			ops, ferr := buildUnionFragment(stage, &t, taskInputs, w, fusedSubject)
			if ferr != nil {
				return StageOutput{}, fmt.Errorf("stage %s: build union fragment: %w", stage.ID, ferr)
			}
			t.Operators = ops
		}
		// Output-side dynamic-filter emits (markSemiAntiBuildFilters): ride
		// the fragment's terminal sink OpSpec so the worker accumulates the
		// bloom over this stage's OUTPUT stream. Legacy single-op tasks
		// (t.Operators == nil) cannot emit — no partials upload, the
		// completeness check withholds the filter, and consumers run
		// unfiltered (correct, just unoptimized).
		if len(stage.EmitDynamicFilters) > 0 && len(t.Operators) > 0 {
			var emits []distributed.DynamicFilterEmit
			for _, e := range stage.EmitDynamicFilters {
				if !e.AtOutput {
					continue
				}
				emits = append(emits, distributed.DynamicFilterEmit{
					FilterID:  e.FilterID,
					KeyColumn: e.KeyColumn,
					KeyType:   e.KeyType,
					BloomBits: e.BloomBits,
					AtOutput:  true,
				})
			}
			if len(emits) > 0 {
				t.Operators[len(t.Operators)-1].DynamicFilterEmits = emits
			}
		}
		// Base-table inputs read only the columns the plan asked for. The
		// map is per stage, not per task, so it is computed once above.
		applySourceProjection(t.Operators, inputScanCols)
		// ...and as WHAT. A pass-through leaf-scan input is base-table
		// parquet, and a file written before the declared-schema footer key
		// existed cannot say what nine of its column types are (#423).
		applySourceColumnTypes(t.Operators, inputScanTypes)
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	// Dispatch via scheduler + shuffle-side-style result collection.
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {
			StageID:    stage.ID,
			Type:       distributed.TaskTypeStage,
			TotalTasks: len(tasks),
		},
	}
	c.tracker.RegisterInternal(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	subject := distributed.QueryResultSubject(stageQueryID)
	// Per-task admission estimate from upstream output sizes: partitioned
	// inputs (and the manually-sliced probe of a probe-split) divide across
	// tasks; replicated and single-part inputs are read in full by EVERY
	// task (partitionFilesForWorker hands non-partitioned deps to each task
	// whole), so they charge their full bytes.
	// skewGroups (== numTasks when no skew split) keeps the divisor at the
	// logical slot count: sub-tasks carry their own measured estimate below,
	// and unsplit tasks shouldn't see estimates diluted by the extra tasks.
	perTaskEst := estimateComputeTaskBytes(stage, inputs, skewGroups, probeSplit)
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
		if skewAssign != nil && skewAssign[i].estBytes > 0 {
			tasks[i].EstimatedBytes = skewAssign[i].estBytes
		}
	}
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Task retry: failed tasks are re-dispatched up to maxTaskAttempts
	// (deterministic durable inputs + overwrite-safe outputs make this
	// sound). DISABLED for gather-fused stages — those stream rows to the
	// client mid-task, so a retried task would duplicate streamed output.
	retrier := newTaskRetrier(tasks, fusion == nil, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		// Eager consumers: retries are otherwise verbatim, but a stale
		// Replay list would make the retried task wait on the republisher
		// for manifests published since the original build (fencing
		// recovery depends on the retry seeing the stable attempt set
		// promptly). Refresh from the feed at re-dispatch.
		refreshEagerReplay(&t, stage, inputs)
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	// A3 (eager-consumer-dispatch.md §14): a hash-partitioned compute
	// stage backs an eager feed exactly like a shuffle producer — its
	// plain sink writes one file per task and task dispatch order IS the
	// partition, which the consumer's manifest source mirrors via the
	// ProducerTaskIDs ordinal. Dispatch-time regroupings (skew split,
	// rr-agg split, probe split) remap task→partition and decline; gather
	// fusion disables the retry that fencing recovery needs; a
	// coordinator-read stage (scalar extraction) keeps the barrier.
	// Publication itself stays clearance-gated (§14/A1).
	if fusion == nil && skewAssign == nil && rrAggGroups == nil && !probeSplit &&
		len(tasks) > 0 && eagerCapableComputeProducer(stage) {
		rootID := distributed.TaskRootQueryID(&tasks[0])
		if rootID != "" && !c.stageReadByCoordinator(rootID, stage.ID) {
			if feed := c.eagerFeedHandle(queryID, stage.ID); feed != nil {
				retrier.onSuccess = c.eagerManifestPublisher(rootID, stage.ID, feed)
				if retrier.onSuccess != nil {
					ids := make([]string, len(tasks))
					for i := range tasks {
						ids[i] = tasks[i].ID
					}
					// Dispatch before publish so no completion beats the
					// feed (same ordering contract as runShuffleSide).
					feed.dispatch(rootID, stage.ID, ids, len(tasks), workerCount)
				}
			}
		}
	}
	// Eager stages with more tasks than the cluster can hold at once
	// publish in governed waves: eager tasks block on producer manifests
	// while HOLDING a worker slot, so a full join fan-out (numTasks =
	// partition count) dispatched at once could occupy every slot ahead
	// of the very producer tasks it waits on. Cap in-flight eager tasks
	// at capacity − workerCount (≥ one producer lane per worker on
	// average); top up as tasks reach terminal state. Never coexists
	// with fusion (eligibility excludes the gather-fused stage).
	publishNow := tasks
	var governor *eagerPublishGovernor
	if len(tasks) > 0 && len(tasks[0].EagerInputs) > 0 {
		capacity := c.workers.ClusterCapacity()
		eagerCap := workerCount // capacity unreported: assume ≥ 2 slots/worker
		if capacity > 0 {
			eagerCap = capacity - workerCount
		}
		publishNow, governor = newEagerPublishGovernor(tasks, eagerCap, func(t distributed.Task) {
			if ctx.Err() != nil {
				return
			}
			// Late-wave tasks get a current Replay snapshot — their
			// original one was taken at task build, before earlier waves'
			// producers reported.
			refreshEagerReplay(&t, stage, inputs)
			if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
				c.logger.Error("eager wave publish failed",
					"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
			}
		})
		if governor != nil {
			c.logger.Info("eager dispatch: governed waves",
				"stage_id", stage.ID, "tasks", len(tasks), "in_flight_cap", eagerCap)
		}
	}
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &r); err != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		if governor != nil && retrier.IsTerminal(r.TaskID) {
			// Off the subscription goroutine: the top-up publish flushes
			// NATS, same as the retry republish path.
			go governor.noteTerminal(r.TaskID)
		}
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
		return StageOutput{}, fmt.Errorf("stage %s: subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("dispatching compute stage",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"tasks", len(tasks), "subject", subject)
	// Arm gather-fusion terminal count before publish so workers can't race
	// the count past the receiver. Every task in this stage emits an
	// OpGatherSink terminal (canMigrateAggregate fires for the entire batch
	// when fusion is set — canFuseJoin is mutually exclusive on stage type,
	// canFuseGather requires no SortKeys/Limit). Tasks count == terminals
	// expected.
	if fusion != nil {
		fusion.recv.SetExpectedTerminals(len(tasks))
	}
	if err := c.scheduler.PublishTasks(ctx, publishNow); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: publish: %w", stage.ID, err)
	}

	if err := awaitStageProgress(ctx, done, progress, "compute "+stage.ID); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s with %d/%d results: %w", stage.ID, retrier.Terminal(), len(tasks), err)
	}
	c.logger.Info("compute stage complete", "stage_id", stage.ID, "tasks", len(tasks))

	if f, failed := retrier.FirstError(); failed {
		return StageOutput{}, stageTaskFailure(f, fmt.Errorf("stage %s: task %s failed after %d attempts: %s", stage.ID, f.TaskID, maxTaskAttempts, f.Message))
	}
	resultFiles := retrier.Files()
	// Output-side dynamic-filter partials (markSemiAntiBuildFilters):
	// completeness-enforced union into BuildStats, attached to every
	// StageOutput shape below so the consuming build scan can resolve them.
	var buildStats map[string]*BuildStats
	if len(stage.EmitDynamicFilters) > 0 {
		buildStats = c.mergeCompleteBuildStats(ctx, queryID, stage.ID, retrier.DynamicFilterPartials(), len(tasks), lateAttachFilterIDs(stage.EmitDynamicFilters))
	}
	// Fused join + downstream exchange-sender: each task produced N partition
	// files at "<prefix>partition=NNNN/<task>.wshf" (worker's executeFragment
	// → fragmentExchangeSink). Bucket all files across all tasks by partition
	// number — same shape as runShuffleSide / dispatchScanFilterStage's
	// fused-shuffle bucketing.
	if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 &&
		(stage.Type == physical.StageHashJoin || stage.Type == physical.StageBroadcastJoin) {
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		out := StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			BuildStats:    buildStats,
			Bytes:         retrier.TotalBytes(),
		}
		out.PartitionRows, out.PartitionBytes = retrier.PartitionAccounting(numParts)
		return out, nil
	}
	// Skew-split output re-grouping: sub-task outputs collapse back into
	// their group's slot so the stage's OutputPartitioned layout matches
	// the unsplit shape (skewGroups slots; a hot group's slot just holds k
	// files). All of a hot group's rows carry that group's key range, so
	// downstream partition-aligned consumption stays correct. The Exchange
	// branch above needs no equivalent — it buckets by partition path,
	// which is task-count agnostic.
	if skewAssign != nil {
		grouped := make([][]string, skewGroups)
		for i, a := range skewAssign {
			grouped[a.group] = append(grouped[a.group], resultFiles[i]...)
		}
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: skewGroups,
			Files:         grouped,
			BuildStats:    buildStats,
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	// resultFiles is in dispatch order (taskRetrier keys results by task ID),
	// which is deterministic — an improvement over the old arrival-order slice.
	files := resultFiles
	// Kind reflects what the planner said this stage produces. Single-
	// task Singleton stages label their output OutputSinglePart so
	// downstream consumers (partitionFilesForWorker) return the full file
	// list for every worker. Hash-partitioned stages produce Partitioned
	// output; broadcast produces Replicated; anything else falls back to
	// SinglePart (one worker consumed all input).
	kind := OutputSinglePart
	switch stage.Distribution.Kind {
	case physical.DistHashPartitioned:
		kind = OutputPartitioned
	case physical.DistBroadcast:
		kind = OutputReplicated
	}
	if probeSplit || rrAggGroups != nil || unionStage {
		// Probe-split, partial-aggregate fan-out and union keep the
		// planner's labelling (DistSingleton / DistRoundRobin) but produce
		// N physical files (one per task). Collapse all per-task files into
		// Files[0] so OutputSinglePart consumers (which read only Files[0])
		// see the full result. Downstream parallelism is preserved by
		// finalAggregateFanoutCandidate / flattenStageFiles callers that
		// iterate every Files[i].
		//
		// For a union this collapse IS the concatenation: every consumer
		// must see both arms, and reading Files[0] alone would hand it one
		// arm — the shape of the bug this stage exists to fix (#346).
		flat := make([]string, 0)
		for _, f := range files {
			flat = append(flat, f...)
		}
		return StageOutput{
			Kind:          OutputSinglePart,
			NumPartitions: 1,
			Files:         [][]string{flat},
			BuildStats:    buildStats,
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	return StageOutput{
		Kind:          kind,
		NumPartitions: len(tasks),
		Files:         files,
		BuildStats:    buildStats,
		Bytes:         retrier.TotalBytes(),
	}, nil
}
