package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// gatherReceiveTimeout bounds the coordinator's wait for worker terminal
// gather messages. Matches shuffleStageTimeout so long-running final
// aggregation stages don't falsely time out.
const gatherReceiveTimeout = 10 * time.Minute

// executeStageDAG is the native-DAG distributed executor. It walks the
// Exchange-annotated stage DAG in topological order, dispatches each
// stage via its Type-specific helper, records outputs in a per-query
// stageOutputs map, and terminates when it hits a Gather stage (which
// streams results to the coordinator directly).
//
// Gated by Coordinator.UseNativeDAG. When disabled, the legacy switch in
// ExecuteSQL handles dispatch. See spec:
// docs/superpowers/specs/2026-04-22-distribution-native-dag-execution-design.md
func (c *Coordinator) executeStageDAG(
	ctx context.Context,
	queryID, sql string,
	stages []physical.Stage,
	workerCount int,
) (*gatherResult, error) {
	if len(stages) == 0 {
		return nil, fmt.Errorf("executeStageDAG: empty stage list")
	}

	// Fail-fast on plan shapes the dispatchers can't consume — see
	// physical.ValidateNativeDAGShape. The 2026-04-23 SF10 A/B blew 10
	// minutes on a multi-merge-tree timeout before surfacing the shape
	// mismatch; this catches the same class at plan time.
	if err := physical.ValidateNativeDAGShape(stages); err != nil {
		return nil, err
	}

	// Register parent queryID in the tracker so SubjectQueryActive replies
	// "active" for this query. Without this the worker's pre-execute
	// is-query-still-active probe (worker.go:402) gets "0" back and terms
	// the task message silently. Compute stages work because
	// dispatchComputeStage registers its own ephemeral stageQueryID; the
	// Gather task rides on the parent queryID which is ONLY registered by
	// the legacy ExecuteSQL code path that native-DAG bypasses at line 479.
	trackerStages := make(map[string]*StageInfo, len(stages))
	stageOrder := make([]string, 0, len(stages))
	for _, s := range stages {
		trackerStages[s.ID] = &StageInfo{
			StageID:      s.ID,
			Type:         distributed.TaskType(s.Type),
			TotalTasks:   s.Tasks,
			Dependencies: s.Dependencies,
		}
		stageOrder = append(stageOrder, s.ID)
	}
	c.tracker.Register(queryID, sql, trackerStages, stageOrder)
	c.tracker.Start(queryID)
	// Mark Complete (not Delete) so GetQueryStatus / GetQueryResults can
	// observe the finished query. ReapCompleted (cleanup.go) prunes old
	// completed entries on a periodic sweep — same pattern legacy uses.
	defer c.tracker.Complete(queryID)

	// Separate the terminal Gather from the DAG body: Gather is always run
	// last, on the coordinator's NATS reply subscription, and returns the
	// final result batches. Everything else is dispatched in waves where
	// each wave runs all ready sibling stages concurrently (sibling = all
	// dependencies already satisfied). Without concurrent waves, plans with
	// fan-out patterns like the multi-level merge_aggregate/merge_sort tree
	// (80+ independent siblings per query at SF10) serialize to one stage
	// per coordinator-to-worker round-trip — catastrophically slow.
	var gatherStage physical.Stage
	var hasGather bool
	pending := make(map[string]physical.Stage, len(stages))
	stageByID := make(map[string]physical.Stage, len(stages))
	for _, s := range stages {
		stageByID[s.ID] = s
		if s.Type == physical.StageExchangeGather {
			if hasGather {
				return nil, fmt.Errorf("executeStageDAG: plan has multiple Gather stages")
			}
			gatherStage = s
			hasGather = true
			continue
		}
		pending[s.ID] = s
	}
	if !hasGather {
		return nil, fmt.Errorf("executeStageDAG: plan for query %s has no Gather stage", queryID)
	}

	outputs := make(map[string]StageOutput, len(stages))
	var outputsMu sync.Mutex

	// Per-stage completion signal. Each stage goroutine closes its `done`
	// channel when its output is published into `outputs`. Dependent
	// stages block on the union of their dep signals before dispatching
	// their own work. This is a straight DAG scheduler — each stage
	// starts as soon as its own dependencies are satisfied, instead of
	// waiting for an entire wave to drain. Plans with parallel branches
	// of unequal length (e.g., two sides of a shuffle join where one
	// side has an extra filter stage) now overlap properly instead of
	// stalling on the longest path in each wave.
	done := make(map[string]chan struct{}, len(pending))
	for id := range pending {
		done[id] = make(chan struct{})
	}
	// Cap how many stages can be dispatching concurrently to keep coord-side
	// result-collection state (NATS subscriptions, in-flight task batches,
	// per-stage buffers) bounded. Without this, every "ready" stage in a
	// wave dispatches simultaneously — for Q18 SF10 (17 stages, multiple
	// fan-outs ready at once) the resulting coord+worker total RSS routinely
	// overshoots the host's physical memory and the OS OOM-kills a process.
	//
	// 2 * workerCount keeps workers saturated (each worker has typically
	// max_concurrent>=2 tasks) while bounding the number of in-flight stage
	// pipelines coord must track to a small multiple of the cluster size.
	// The semaphore is acquired AFTER all upstream dependencies are
	// satisfied, so it can never deadlock waiting on a producer that itself
	// can't acquire a slot.
	// Source the slot count from the actual cluster capacity (sum of each
	// worker's auto-tuned max_concurrent reported in heartbeats) when
	// available. Workers downscale max_concurrent under memory pressure
	// (auto-detected memory budget logic in cmd/wadjet), so this gives the
	// dispatcher a memory-aware backpressure signal: if every worker has
	// shrunk to max_concurrent=2 because the box is tight, dispatch only
	// queues that many stages at a time instead of stampeding 8+ in a wave.
	// Falls back to 2 * workerCount when no worker has reported
	// MaxConcurrent yet (cluster startup, legacy workers).
	dispatchSlots := c.workers.ClusterCapacity()
	dispatchSource := "cluster_capacity"
	if dispatchSlots <= 0 {
		dispatchSlots = 2 * workerCount
		dispatchSource = "fallback_2x_workerCount"
	}
	if dispatchSlots < 2 {
		dispatchSlots = 2
		dispatchSource = "floor"
	}
	dispatchSem := make(chan struct{}, dispatchSlots)
	c.logger.Info("stage-DAG dispatch", "query", queryID,
		"stages", len(pending), "dispatch_concurrency", dispatchSlots,
		"dispatch_source", dispatchSource)

	g, gctx := errgroup.WithContext(ctx)
	for _, s := range pending {
		s := s
		g.Go(func() error {
			// Wait on each dependency's done signal. Deps not in the
			// `done` map are leaf stages / pre-computed outputs (e.g.,
			// the coordinator's initial inputs) and are treated as
			// immediately ready.
			for _, depID := range s.Dependencies {
				ch, tracked := done[depID]
				if !tracked {
					continue
				}
				select {
				case <-ch:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			// Late-bind any scalar subqueries: await each producer stage
			// separately (producer IDs are NOT in Dependencies because
			// their output feeds into FilterExprs via string substitution
			// rather than flowing in as record batches), then extract the
			// single scalar and substitute the placeholder.
			if len(s.ScalarDependencies) > 0 {
				for _, pid := range s.ScalarDependencies {
					ch, tracked := done[pid]
					if !tracked {
						continue
					}
					select {
					case <-ch:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				outputsMu.Lock()
				prod := make(map[string]StageOutput, len(s.ScalarDependencies))
				prodStages := make(map[string]physical.Stage, len(s.ScalarDependencies))
				for _, pid := range s.ScalarDependencies {
					prod[pid] = outputs[pid]
					if ps, ok := stageByID[pid]; ok {
						prodStages[pid] = ps
					}
				}
				outputsMu.Unlock()
				var subErr error
				s, subErr = c.substituteScalarDependencies(gctx, s, prod, prodStages)
				if subErr != nil {
					return fmt.Errorf("stage %s scalar substitution: %w", s.ID, subErr)
				}
			}
			outputsMu.Lock()
			inputs, err := collectInputs(s, outputs)
			outputsMu.Unlock()
			if err != nil {
				return fmt.Errorf("stage %s collect inputs: %w", s.ID, err)
			}
			// Acquire a dispatch slot now that we have inputs and are
			// about to publish tasks. Held until the stage completes so
			// concurrent stage count never exceeds dispatchSlots. Released
			// via defer so it always fires, even on dispatch error.
			select {
			case dispatchSem <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() { <-dispatchSem }()

			var out StageOutput
			switch s.Type {
			case physical.StageExchangeRepartition:
				out, err = c.dispatchShuffleStage(gctx, queryID, s, inputs, workerCount)
			case physical.StageExchangeReplicate:
				out, err = c.dispatchReplicateStage(gctx, queryID, sql, s, inputs)
			default:
				out, err = c.dispatchPipelineStage(gctx, queryID, sql, s, inputs, workerCount)
			}
			if err != nil {
				return fmt.Errorf("stage %s (%s): %w", s.ID, s.Type, err)
			}
			outputsMu.Lock()
			outputs[s.ID] = out
			outputsMu.Unlock()
			close(done[s.ID])
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Terminal Gather.
	inputs, err := collectInputs(gatherStage, outputs)
	if err != nil {
		return nil, fmt.Errorf("gather stage %s: %w", gatherStage.ID, err)
	}
	gr, gerr := c.dispatchGatherStage(ctx, queryID, sql, gatherStage, inputs, workerCount)
	if gerr != nil {
		return gr, gerr
	}
	// Apply SELECT-list aliases to the result schema. walkStages drops the
	// outer NodeProject's projections, so without this the user sees raw
	// worker column names ("n1.n_name", "substr(...)") instead of their
	// aliases ("supp_nation", "l_year"). The Gather worker is a pipe and
	// can't apply this — it has to happen here.
	if gr != nil && len(gatherStage.OutputRenames) > 0 {
		applyOutputRenames(gr, gatherStage.OutputRenames)
	}
	return gr, nil
}

// applyOutputRenames PROJECTS each gather batch (and gr.columns) to exactly
// the SELECT-list output schema described by renames: drops columns the
// worker emitted but the user didn't ask for (e.g., Q15's join carries
// supplier/lineitem internals), and renames each kept column to the user's
// alias. Match is case-insensitive to tolerate worker-side lowercasing of
// expression text.
//
// Only applies when renames is non-empty AND every entry's source column
// resolves in the batch — if any source is missing (wrapped aggregates not
// yet handled), falls back to a rename-only pass so the output is at least
// non-empty rather than truncated to nothing.
func applyOutputRenames(gr *gatherResult, renames []physical.OutputRename) {
	if gr == nil || len(renames) == 0 {
		return
	}
	// Pre-compile any expression-bearing renames (wrapped aggregates etc.)
	// once per call. Compilation can fail (e.g., unsupported AST shape); on
	// failure we degrade the whole pass to rename-only.
	compiledExprs := make(map[int]expr.Expr, len(renames))
	for i, r := range renames {
		if r.Expr == nil {
			continue
		}
		e, cerr := expr.Compile(r.Expr)
		if cerr != nil {
			// Compilation failure → degrade to rename-only.
			compiledExprs = nil
			break
		}
		compiledExprs[i] = e
	}
	// Determine if every source resolves in the schema. If not, degrade
	// to rename-only behavior to avoid hiding the entire result.
	canProject := true
	if compiledExprs == nil {
		canProject = false
	}
	if canProject && len(gr.batches) > 0 && len(gr.batches[0].Schema) > 0 {
		schema := gr.batches[0].Schema
		for _, r := range renames {
			if r.Expr != nil {
				continue // existence check uses expression evaluation
			}
			found := false
			for _, c := range schema {
				if strings.EqualFold(c.Name, r.From) {
					found = true
					break
				}
			}
			if !found {
				canProject = false
				break
			}
		}
	} else if len(gr.columns) > 0 && len(gr.batches) == 0 {
		// No batches but columns set — fall back to rename-only.
		canProject = false
	}
	if !canProject {
		// Rename-only fallback (matches old behavior).
		rename := func(name string) string {
			for _, r := range renames {
				if strings.EqualFold(name, r.From) {
					return r.To
				}
			}
			return name
		}
		for i, c := range gr.columns {
			gr.columns[i] = rename(c)
		}
		for _, b := range gr.batches {
			if b == nil {
				continue
			}
			for i := range b.Schema {
				b.Schema[i].Name = rename(b.Schema[i].Name)
			}
		}
		return
	}
	// Project: keep only the renamed/computed columns, in renames order.
	gr.columns = make([]string, len(renames))
	for i, r := range renames {
		gr.columns[i] = r.To
	}
	for bi, b := range gr.batches {
		if b == nil {
			continue
		}
		newCols := make([]*batch.Vector, len(renames))
		newSchema := make([]parquet.Column, len(renames))
		for i, r := range renames {
			if e, ok := compiledExprs[i]; ok {
				// Expression-bearing rename: evaluate per row, build a new
				// column. Used for wrapped aggregates ("SUM(x)/7.0") whose
				// post-aggregate divisor needs to be applied at gather time.
				newCols[i] = evalExprColumn(e, b)
				newSchema[i] = parquet.Column{Name: r.To, Type: parquet.TypeFloat64, Nullable: true}
				continue
			}
			srcIdx := -1
			for j, c := range b.Schema {
				if strings.EqualFold(c.Name, r.From) {
					srcIdx = j
					break
				}
			}
			if srcIdx < 0 {
				// Should not happen — canProject was true. Defensive.
				continue
			}
			newCols[i] = b.Columns[srcIdx]
			newSchema[i] = b.Schema[srcIdx]
			newSchema[i].Name = r.To
		}
		nb := &batch.RecordBatch{
			Schema:  newSchema,
			Columns: newCols,
			Len:     b.Len,
			Sel:     b.Sel,
		}
		gr.batches[bi] = nb
	}
}

// evalExprColumn builds a new Vector by evaluating e against each row of b.
// Used by applyOutputRenames to materialize wrapped-aggregate columns at
// gather time. Output type is float64 (the dominant case for wrapped
// aggregates: SUM/N, AVG-like ratios). Other types may need extension when
// new query shapes surface.
func evalExprColumn(e expr.Expr, b *batch.RecordBatch) *batch.Vector {
	v := batch.NewVector(parquet.TypeFloat64, b.Len)
	if cap(v.Float64Data) < b.Len {
		v.Float64Data = make([]float64, b.Len)
	} else {
		v.Float64Data = v.Float64Data[:b.Len]
	}
	emit := func(row, dst int) {
		val := e.Eval(b, row)
		if val == nil {
			v.Nulls.SetNull(dst)
			return
		}
		switch x := val.(type) {
		case float64:
			v.Float64Data[dst] = x
			v.Nulls.SetValid(dst)
		case float32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int64:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case bool:
			if x {
				v.Float64Data[dst] = 1
			}
			v.Nulls.SetValid(dst)
		default:
			v.Nulls.SetNull(dst)
		}
	}
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}

// dispatchShuffleStage executes a StageExchangeRepartition: reads upstream
// output, hash-partitions on stage.Exchange.Keys, writes one .wshf per
// (task, partition) to the query's shuffle prefix.
//
// Phase 3 scaffolding: wraps the existing runShuffleSide helper by adapting
// the upstream StageOutput's files into a synthetic source stage. The legacy
// helper expects a physical.Stage with ScanFiles populated — we satisfy
// that by flattening the upstream output.
//
// Known limitations (follow-up commits):
//   - Chained shuffles (Repartition consuming a previous Repartition's
//     partitioned output) use flattenStageFiles which loses partition
//     information. Correct end-to-end because the next shuffle rehashes,
//     but fails to exploit co-located partitions.
//   - Column pruning uses stage.Columns which may be over-approximated for
//     non-scan upstream stages.
func (c *Coordinator) dispatchShuffleStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if stage.Exchange == nil {
		return StageOutput{}, fmt.Errorf("repartition stage %s missing Exchange payload", stage.ID)
	}
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("repartition stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	depID := stage.Dependencies[0]
	upstream, ok := inputs[depID]
	if !ok {
		return StageOutput{}, fmt.Errorf("missing input for dep %s", depID)
	}
	sourceFiles := flattenStageFiles(upstream)
	if len(sourceFiles) == 0 {
		numParts := stage.Exchange.Count
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         make([][]string, numParts),
		}, nil
	}
	// Synthesize a source stage for runShuffleSide.
	synthetic := physical.Stage{
		ID:         stage.ID + "-src",
		Type:       physical.StageScan,
		ScanFiles:  sourceFiles,
		TableName:  stage.TableName, // may be empty for chained shuffles; worker falls back to generic scan
	}
	numParts := stage.Exchange.Count
	if numParts == 0 {
		numParts = workerCount * shufflePartitionMultiplier
	}
	shards, err := c.runShuffleSide(ctx, queryID, "stage-"+stage.ID, synthetic, stage.Exchange.Keys, numParts, workerCount)
	if err != nil {
		return StageOutput{}, err
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: numParts,
		Files:         shards,
	}, nil
}

// dispatchReplicateStage executes a StageExchangeReplicate: reads upstream
// output and materializes it into a single broadcast cache that all
// downstream workers read in full.
//
// Phase 3 scaffolding: delegates to preScanBuildTables when the upstream is
// a table scan (the common Phase 2a shape for build-side broadcasts); for
// stage-to-stage replicates the upstream files are already broadcast-shaped
// and we pass them through unchanged.
func (c *Coordinator) dispatchReplicateStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (StageOutput, error) {
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("replicate stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	// Pass-through: upstream already produced files; downstream workers
	// consume them as broadcast input. The legacy preScanBuildTables path
	// handles the source-table case via ExecuteSQL's explicit call.
	_ = ctx
	_ = queryID
	_ = sql
	return StageOutput{
		Kind:  OutputReplicated,
		Files: [][]string{flattenStageFiles(upstream)},
	}, nil
}

// dispatchPipelineStage executes a compute stage (scan/join/aggregate/etc.)
// by dispatching workerCount pipeline tasks that each read their assigned
// slice of upstream partitioned output and run the full SQL against it.
//
// Phase 3 scaffolding: wraps buildShufflePipelineTasks when the inputs look
// like a two-sided partitioned shuffle (build + probe). Leaf scan stages
// are passed through as StageOutput{Kind: OutputSinglePart, Files: scanFiles}
// without dispatching a task — they are consumed directly by the next
// Exchange stage. This matches the Phase 2a structure where leaf scans are
// not runtime-active stages.
//
// Known limitations: multi-step pipelines that need to run an intermediate
// join and emit WSHF-encoded partitioned output to a subsequent Exchange
// stage require worker-side SQL-fragment execution. That is a follow-up
// commit; the current path handles only the single-shuffle probe pipeline.
func (c *Coordinator) dispatchPipelineStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	_ = sql
	// Leaf scan stage.
	if stage.Type == physical.StageScan && len(inputs) == 0 {
		// Fused scan-aggregate: the planner marked this scan to produce
		// partial aggregates (saves the scan→aggregate round-trip in the
		// legacy single-pipeline executor). Under native-DAG we must
		// honor that signal by dispatching N partial-aggregate tasks —
		// each worker reads its slice of scan files, aggregates, and
		// emits a partial result. The downstream final_aggregate stage
		// merges across partial outputs.
		//
		// Without this path, a Singleton final_aggregate over a large
		// leaf scan ran on ONE worker reading all scan files serially —
		// at SF10 that was the dominant cost on Q01 (87s of 87s wall
		// time was the single-worker lineitem scan).
		if len(stage.FusedAggSpecs) > 0 {
			return c.dispatchScanAggregateStage(ctx, queryID, stage, workerCount)
		}
		// Plain scan with no fused aggregate. When the planner pushed
		// filters onto this scan (e.g., Q05's `r_name = 'ASIA'`), we
		// must actually apply them — otherwise the downstream join
		// sees every row, cartesian-multiplies, and returns way too
		// many results. Dispatch a filter-scan task that reads the
		// parquet files, applies FilterExprs, and writes a WSHF output
		// the rest of the native-DAG pipeline can consume.
		if len(stage.FilterExprs) > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, workerCount)
		}
		// No filter: pass the raw parquet files through to the
		// downstream stage.
		files := append([]string(nil), stage.ScanFiles...)
		return StageOutput{
			Kind:  OutputSinglePart,
			Files: [][]string{files},
		}, nil
	}
	// Compute stage: emit workerCount TaskTypeStage tasks, each reading its
	// slice of partitioned input and writing unpartitioned .wshf output.
	// The stage's output is collected into a Partitioned StageOutput where
	// partition p = the files produced by worker p — downstream Exchange
	// stages treat this as partitioned if they need to re-hash.
	return c.dispatchComputeStage(ctx, queryID, stage, inputs, workerCount)
}

// dispatchScanAggregateStage dispatches N partial-aggregate tasks, one
// per worker, each reading a disjoint slice of stage.ScanFiles. Each task
// runs a HashAggregate on its file slice and emits a partial result as
// its stage output. Downstream final_aggregate stages (which are almost
// always immediately adjacent) merge the N partial outputs into the
// logical full aggregate.
//
// Why scan-side fan-out: scan is the only leaf at plan time, so a
// Singleton-distributed final_aggregate reading "the scan's output"
// serialises to one worker reading every parquet file. Splitting the
// scan N ways + computing partial aggregates in parallel is the same
// map-reduce shape the legacy executor used (via emitMergeAggregateTree
// → scan-level fused-agg + multi-level merge tree) before native-DAG
// collapsed the tree. Now we re-introduce the fan-out, but without the
// extra merge-tree levels: one partial step, one final merge.
func (c *Coordinator) dispatchScanAggregateStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1
	}
	// AVG cannot merge across partials without decomposing into
	// sum+count pair — taking the average of per-partition averages
	// silently produces wrong values (unweighted mean). Until AVG
	// partial decomposition lands, fall back to a single task so the
	// whole aggregate runs on one worker and the merge step is a
	// pass-through. Loses parallelism for AVG queries (Q01 is the
	// notable one) but preserves correctness.
	for _, a := range stage.FusedAggSpecs {
		if strings.EqualFold(strings.TrimSpace(a.Func), "avg") {
			c.logger.Info("scan-aggregate AVG fallback: single-task",
				"stage_id", stage.ID, "func", a.Func)
			workerCount = 1
			break
		}
	}
	fileSets := splitFilesEvenly(stage.ScanFiles, workerCount)
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: 0,
			Files:         nil,
		}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchScanAggregateStage",
		"stage_id", stage.ID, "files", len(stage.ScanFiles),
		"tasks", actualTasks, "group_by", stage.FusedAggGroupBy)

	// Convert AggSpec → wire format once.
	var aggs []distributed.AggSpec
	for _, a := range stage.FusedAggSpecs {
		aggs = append(aggs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			InputExpr: a.InputExpr,
		})
	}

	tasks := make([]distributed.Task, 0, actualTasks)
	for w, files := range fileSets {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeStage,
			StageType:    "aggregate",
			TableName:    stage.TableName,
			Columns:      stage.Columns,
			GroupByCols:  stage.FusedAggGroupBy,
			Aggregates:   aggs,
			// Propagate scan-pushed WHERE fragments. Without this the worker
			// aggregates every row in the file slice and ignores the query's
			// predicate — group counts match legacy but aggregate VALUES are
			// wrong. E.g. Q01's `WHERE l_shipdate <= '1998-09-02'` was being
			// silently dropped in native-DAG before this plumb.
			FilterExprs: append([]string(nil), stage.FilterExprs...),
			Inputs: map[string][]string{
				// Use the scan's table name as alias so the worker's
				// sourceForAlias opens these files via the parquet path.
				scanAliasForStage(stage): files,
			},
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
		_ = w
	}

	// Dispatch and collect (same pattern as dispatchComputeStage).
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	for i := range tasks {
		tasks[i].QueryID = stageQueryID
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	collected := make([]taskResult, 0, len(tasks))
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= len(tasks) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s publish: %w", stage.ID, err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		return StageOutput{}, ctx.Err()
	case <-time.After(shuffleStageTimeout):
		return StageOutput{}, fmt.Errorf("scan-agg stage %s timeout", stage.ID)
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()
	files := make([][]string, len(tasks))
	for i, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("scan-agg stage %s: task failed: %s", stage.ID, r.err)
		}
		files[i] = r.files
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         files,
	}, nil
}

// dispatchScanFilterStage runs a scan-and-filter pipeline on a single
// task. Used when a leaf scan carries scan-pushed FilterExprs but no
// fused aggregate — without this path the filter would silently drop
// because downstream stages see only the raw parquet files.
func (c *Coordinator) dispatchScanFilterStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	workerCount int,
) (StageOutput, error) {
	_ = workerCount
	if len(stage.ScanFiles) == 0 {
		return StageOutput{Kind: OutputSinglePart, Files: [][]string{nil}}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeStage,
		StageType:    "scan",
		TableName:    stage.TableName,
		Columns:      stage.Columns,
		FilterExprs:  append([]string(nil), stage.FilterExprs...),
		Inputs: map[string][]string{
			scanAliasForStage(stage): append([]string(nil), stage.ScanFiles...),
		},
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		ResultPrefix: resultPrefix,
		CreatedAt:    time.Now(),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: 1},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)
	task.QueryID = stageQueryID

	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	var collected []taskResult
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= 1 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s publish: %w", stage.ID, err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		return StageOutput{}, ctx.Err()
	case <-time.After(shuffleStageTimeout):
		return StageOutput{}, fmt.Errorf("scan-filter stage %s timeout", stage.ID)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(collected) == 0 {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s: no result", stage.ID)
	}
	if collected[0].err != "" {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s: %s", stage.ID, collected[0].err)
	}
	return StageOutput{
		Kind:  OutputSinglePart,
		Files: [][]string{collected[0].files},
	}, nil
}

// scanAliasForStage returns the alias key used when handing scan-fused
// files to a worker's Inputs map. Falls back to the stage's TableName
// or scan ID so the worker can resolve the parquet source.
func scanAliasForStage(stage physical.Stage) string {
	if stage.ScanAlias != "" {
		return stage.ScanAlias
	}
	if stage.TableName != "" {
		return stage.TableName
	}
	return stage.ID
}

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
		return c.dispatchFinalAggregateFanout(ctx, queryID, stage, inputs, workerCount)
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
	// NB: stage.Tasks from the legacy planner encodes probe-split intent
	// (e.g., Tasks=workerCount for broadcast_join means "split probe files
	// across N workers"). That semantics assumes each task receives a
	// DIFFERENT slice of probe files — which only works if the coordinator
	// slices probe input per task. Native-DAG dispatch today hands every
	// Singleton-stage task the same full probe set (partitionFilesForWorker
	// on Singleton input returns the full list for every worker), so
	// honoring stage.Tasks=N duplicates the full scan/join N× and triples
	// the worker memory pressure. Until native-DAG grows probe-split
	// semantics, Distribution is the authoritative signal and stage.Tasks
	// is intentionally ignored here.
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchComputeStage",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"deps", stage.Dependencies, "num_tasks", numTasks,
		"distribution_kind", stage.Distribution.Kind,
		"distribution_count", stage.Distribution.Count,
		"inputs_aliases", len(inputs))

	// Build one task per output partition. Input slicing uses numTasks as
	// the divisor so each task reads its share of the upstream partitioned
	// input — for Singleton stages every task reads the full upstream; for
	// Hash-partitioned N-task stages each task reads its N-th partition.
	tasks := make([]distributed.Task, 0, numTasks)
	for w := 0; w < numTasks; w++ {
		taskInputs, err := buildTaskInputsForStage(stage, inputs, w, numTasks)
		if err != nil {
			return StageOutput{}, fmt.Errorf("stage %s worker %d: %w", stage.ID, w, err)
		}
		// Convert stage.AggSpecs → distributed.AggSpec.
		var aggs []distributed.AggSpec
		for _, a := range stage.AggSpecs {
			aggs = append(aggs, distributed.AggSpec{
				Func:      a.Func,
				InputCol:  a.InputCol,
				OutputCol: a.OutputCol,
				InputExpr: a.InputExpr,
			})
		}
		// Convert stage.SortKeys → distributed.SortKeySpec.
		var sorts []distributed.SortKeySpec
		for _, s := range stage.SortKeys {
			sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc})
		}
		t := distributed.Task{
			ID:              uuid.New().String()[:8],
			QueryID:         queryID,
			StageID:         stage.ID,
			Type:            distributed.TaskTypeStage,
			StageType:       stage.Type,
			JoinType:        stage.JoinType,
			JoinLeftKeys:    stage.JoinLeftKeys,
			JoinRightKeys:   stage.JoinRightKeys,
			BuildTableAlias:     stage.BuildTableAlias,
			QualifyAllBuildCols: stage.QualifyAllBuildCols,
			JoinFilter:          stage.JoinFilter,
			GroupByCols:     stage.GroupByCols,
			Aggregates:      aggs,
			SortKeys:        sorts,
			Limit:           stage.Limit,
			Inputs:          taskInputs,
			DataBucket:      c.config.ResultBucket,
			ResultBucket:    c.config.ResultBucket,
			ResultPrefix:    resultPrefix,
			CreatedAt:       time.Now(),
			// Filters attached to a compute stage (HAVING on
			// aggregate/final_aggregate, residual predicates on
			// hash_join) reference OUTPUT columns and must run after
			// the stage's main operator. FilterExprs on compute stages
			// would otherwise silently drop — that's the root cause of
			// Q15 ignoring `WHERE total_revenue = (SELECT MAX...)` and
			// Q18 ignoring `HAVING SUM(l_quantity) > 300`.
			PostFilterExprs: append([]string(nil), stage.FilterExprs...),
		}
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
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
	}
	collected := make([]taskResult, 0, len(tasks))
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &r); err != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= len(tasks) {
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
	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: publish: %w", stage.ID, err)
	}

	select {
	case <-done:
		c.logger.Info("compute stage complete", "stage_id", stage.ID, "tasks", len(tasks))
	case <-ctx.Done():
		mu.Lock()
		got := len(collected)
		mu.Unlock()
		return StageOutput{}, fmt.Errorf("stage %s: ctx done with %d/%d results: %w", stage.ID, got, len(tasks), ctx.Err())
	case <-time.After(shuffleStageTimeout):
		return StageOutput{}, fmt.Errorf("stage %s: timed out after %s", stage.ID, shuffleStageTimeout)
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()

	files := make([][]string, len(tasks))
	for i, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("stage %s: task failed: %s", stage.ID, r.err)
		}
		files[i] = r.files
	}
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
	return StageOutput{
		Kind:          kind,
		NumPartitions: len(tasks),
		Files:         files,
	}, nil
}

// finalAggregateFanoutCandidate reports whether a stage qualifies for
// dispatchFinalAggregateFanout: a Singleton final_aggregate over multiple
// upstream files with parallel workers available, none of whose AggSpecs
// is AVG (which can't decompose without separate sum/count partials —
// dispatchScanAggregateStage handles the same constraint).
//
// Returns the (single) dep ID and the flattened upstream file list when it
// qualifies; nil otherwise so callers fall through to the standard
// single-task dispatch.
func finalAggregateFanoutCandidate(
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (depID string, files []string, ok bool) {
	if stage.Type != "final_aggregate" {
		return "", nil, false
	}
	if stage.Distribution.Kind != physical.DistSingleton {
		return "", nil, false
	}
	if workerCount <= 1 {
		return "", nil, false
	}
	if len(stage.Dependencies) != 1 {
		// Multi-dep merges (the planner's two-level merge_aggregate tree
		// reaches here too) already encode parallelism via separate
		// intermediate stages — collapseMergeTreesForNativeDAG only
		// rewrites them when the upstream count is small. Don't double-
		// fan-out by re-splitting at dispatch.
		return "", nil, false
	}
	for _, a := range stage.AggSpecs {
		if strings.EqualFold(strings.TrimSpace(a.Func), "avg") {
			return "", nil, false
		}
	}
	depID = stage.Dependencies[0]
	in, present := inputs[depID]
	if !present {
		return "", nil, false
	}
	files = flattenStageFiles(in)
	// Fanout only wins when each intermediate task performs a non-trivial
	// merge (>= 2 input files). With K <= workerCount, splitFilesEvenly
	// would hand each intermediate a single file, which is just an identity
	// re-emit + an extra final merge — pure overhead. Require K > workerCount
	// so the intermediate phase does real work.
	if len(files) <= workerCount {
		return "", nil, false
	}
	return depID, files, true
}

// dispatchFinalAggregateFanout splits a Singleton final_aggregate dispatch
// into N parallel intermediate merge tasks plus a 1-task final merge.
//
// Today a Singleton final_aggregate is a single task that reads every
// upstream partial-aggregate file serially. When the upstream is a fan-out
// (e.g. dispatchScanAggregateStage emits W partial outputs) the merge
// becomes the serialization point: one worker scans all W partials while
// the others idle. This helper reshapes the dispatch into:
//
//	N=min(K, workerCount) intermediate merge tasks (each over ⌈K/N⌉ inputs)
//	  ↓
//	1 final merge task (over the N intermediate outputs)
//
// Each intermediate runs in `final_aggregate` mode (worker rewrites InputCol
// → OutputCol per executeStageAggregate) so output preserves the partial-
// aggregate column shape the final merge expects. PostFilterExprs (HAVING)
// run only on the final task — applying them to intermediates would drop
// rows before all groups had been merged across partitions.
func (c *Coordinator) dispatchFinalAggregateFanout(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	depID, files, ok := finalAggregateFanoutCandidate(stage, inputs, workerCount)
	if !ok {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout precondition failed for stage %s", stage.ID)
	}
	N := workerCount
	if N > len(files) {
		N = len(files)
	}
	groups := splitFilesEvenly(files, N)
	N = len(groups) // splitFilesEvenly may return fewer when files < N

	c.logger.Info("dispatchFinalAggregateFanout",
		"stage_id", stage.ID, "upstream_files", len(files),
		"intermediate_tasks", N)

	aggs := make([]distributed.AggSpec, 0, len(stage.AggSpecs))
	for _, a := range stage.AggSpecs {
		aggs = append(aggs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			InputExpr: a.InputExpr,
		})
	}

	// Phase 1: intermediates. Each consumes a slice of upstream files,
	// re-aggregates in merge mode, and emits its own partial output.
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	intermTasks := make([]distributed.Task, 0, N)
	for i, group := range groups {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      fmt.Sprintf("%s-merge-%d", stage.ID, i),
			Type:         distributed.TaskTypeStage,
			StageType:    "final_aggregate",
			GroupByCols:  stage.GroupByCols,
			Aggregates:   aggs,
			Inputs:       map[string][]string{depID: group},
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
			// HAVING is final-only — see comment on dispatchComputeStage.
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		intermTasks = append(intermTasks, t)
	}
	intermFiles, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-interm-%s", stage.ID, queryID),
		stage.ID+"-interm",
		intermTasks)
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: intermediate phase: %w", stage.ID, err)
	}
	// Flatten intermediate outputs into a single file list for the final.
	finalInputs := make([]string, 0)
	for _, fs := range intermFiles {
		finalInputs = append(finalInputs, fs...)
	}

	// Phase 2: single final merge task over the intermediate outputs.
	finalTask := distributed.Task{
		ID:              uuid.New().String()[:8],
		QueryID:         queryID,
		StageID:         stage.ID,
		Type:            distributed.TaskTypeStage,
		StageType:       "final_aggregate",
		GroupByCols:     stage.GroupByCols,
		Aggregates:      aggs,
		Limit:           stage.Limit,
		Inputs:          map[string][]string{depID: finalInputs},
		DataBucket:      c.config.ResultBucket,
		ResultBucket:    c.config.ResultBucket,
		ResultPrefix:    resultPrefix,
		CreatedAt:       time.Now(),
		PostFilterExprs: append([]string(nil), stage.FilterExprs...),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		finalTask.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &finalTask)

	finalFiles, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-%s", stage.ID, queryID),
		stage.ID,
		[]distributed.Task{finalTask})
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: final phase: %w", stage.ID, err)
	}
	return StageOutput{
		Kind:          OutputSinglePart,
		NumPartitions: 1,
		Files:         finalFiles,
	}, nil
}

// runStageTasks publishes the given tasks under stageQueryID, subscribes to
// the per-stage result subject, and waits for all completions. Returns one
// []string of result files per task, in completion order. Used by
// dispatchFinalAggregateFanout for both phases; mirrors the inline
// dispatch+collect in dispatchScanAggregateStage / dispatchComputeStage.
func (c *Coordinator) runStageTasks(
	ctx context.Context,
	stageQueryID, stageLabel string,
	tasks []distributed.Task,
) ([][]string, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	trackerStages := map[string]*StageInfo{
		stageLabel: {StageID: stageLabel, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stageLabel})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	for i := range tasks {
		tasks[i].QueryID = stageQueryID
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	collected := make([]taskResult, 0, len(tasks))
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= len(tasks) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("stage %s: subscribe: %w", stageLabel, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return nil, fmt.Errorf("stage %s: publish: %w", stageLabel, err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(shuffleStageTimeout):
		return nil, fmt.Errorf("stage %s: timeout after %s", stageLabel, shuffleStageTimeout)
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()
	files := make([][]string, len(tasks))
	for i, r := range results {
		if r.err != "" {
			return nil, fmt.Errorf("stage %s: task failed: %s", stageLabel, r.err)
		}
		files[i] = r.files
	}
	return files, nil
}

// buildTaskInputsForStage maps a stage's Dependencies (upstream stage IDs)
// into Task.Inputs keyed by a per-stage alias convention:
//   - hash_join/broadcast_join: use stage.BuildTableAlias for the build
//     side (dep index 1) and "probe" for the other side (dep index 0).
//   - aggregate/sort/etc: use the single dep's ID as alias.
func buildTaskInputsForStage(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, workerCount int) (map[string][]string, error) {
	inputs := make(map[string][]string)
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin:
		if len(stage.Dependencies) != 2 {
			return nil, fmt.Errorf("join stage %s expects 2 deps, got %d", stage.ID, len(stage.Dependencies))
		}
		probeDep := stage.LeftDepStage
		buildDep := stage.RightDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
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
		inputs[buildAlias] = partitionFilesForWorker(upstreams[buildDep], workerIdx, workerCount)
		inputs[probeAlias] = partitionFilesForWorker(upstreams[probeDep], workerIdx, workerCount)
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

// dispatchGatherStage is terminal: dispatches a single Gather task to one
// worker, subscribes to its reply subject, and returns the assembled
// result. GatherOrdering + GatherLimit semantics are carried on the task
// so the worker can pre-sort; the receiver concatenates in arrival order.
func (c *Coordinator) dispatchGatherStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (*gatherResult, error) {
	if len(stage.Dependencies) != 1 {
		return nil, fmt.Errorf("gather stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	// Dedicated reply subject: `wadjet.gather.<queryID>`. Avoids QueryResult
	// wildcard overlap (wadjet.results.<queryID>.>).
	replySubject := fmt.Sprintf("wadjet.gather.%s", queryID)

	// Subscribe before publishing so we don't miss early messages.
	var ordering []distributed.SortKeySpec
	var limit int
	if stage.Exchange != nil {
		for _, o := range stage.Exchange.Ordering {
			ordering = append(ordering, distributed.SortKeySpec{Column: o.Column, Desc: o.Desc})
		}
	}

	// Synthesize a task that runs the full SQL and streams its output.
	// Inputs alias: use the upstream stage ID so the worker's Inputs-based
	// source selection reads the upstream files.
	alias := stage.Dependencies[0]
	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeGather,
		SQLText:      sql,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		Inputs: map[string][]string{
			alias: flattenStageFiles(upstream),
		},
		ReplySubject:   replySubject,
		GatherOrdering: ordering,
		GatherLimit:    limit,
		CreatedAt:      time.Now(),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	// Subscribe BEFORE publishing the task so the worker's batches and
	// terminal marker are not lost to a race. NATS does not buffer raw-
	// subject messages for late subscribers, so a goroutine that starts
	// subscribing after the publish can miss every reply (observed on
	// SF10: gather hung for 6+ minutes before the stage-level timeout
	// fired).
	c.logger.Info("gather: subscribing",
		"query_id", queryID, "reply_subject", replySubject)
	recv, err := subscribeGather(c.nc, replySubject, 1)
	if err != nil {
		return nil, fmt.Errorf("subscribing gather reply: %w", err)
	}
	c.logger.Info("gather: publishing task",
		"query_id", queryID, "task_id", task.ID, "reply_subject", replySubject,
		"input_files", len(task.Inputs[alias]))
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing gather task: %w", err)
	}

	_ = workerCount // future: ordered gather dispatches one task per partition
	res, waitErr := recv.wait(ctx, gatherReceiveTimeout)
	c.logger.Info("gather: wait returned",
		"query_id", queryID, "msg_count", recv.msgCount.Load(),
		"err", waitErr)
	return res, waitErr
}
