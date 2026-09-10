// This file holds final-aggregate fan-out eligibility and dispatch.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// finalAggregateFanoutCandidate reports whether a stage qualifies for
// dispatchFinalAggregateFanout: a Singleton final_aggregate over multiple
// upstream files with parallel workers available.
//
// AVG was historically excluded here for the same reason
// dispatchScanAggregateStage forced single-task on AVG. With decomposeAvg
// (avg_decompose.go) handling the (SUM, COUNT) split + worker post-merge
// fold, AVG fan-out is now correct and the guard is gone.
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
	// A RawInputAggregate consumes RAW rows exactly once, in one task —
	// re-splitting it into per-slice partials + a COUNT→SUM merge
	// reintroduces the double-count the raw shape exists to avoid
	// (COUNT(DISTINCT) partials from overlapping slices don't sum, #291).
	if stage.RawInputAggregate {
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
	fusion *gatherFusion,
	scalars scalarResolver,
) (StageOutput, error) {
	depID, files, ok := finalAggregateFanoutCandidate(stage, inputs, workerCount)
	if !ok {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout precondition failed for stage %s", stage.ID)
	}
	// Deferred scalar substitution (q11 serial-tail fix): start the await
	// now so it overlaps the intermediate phase, and join it just before
	// the final task — the only consumer of the substituted FilterExprs
	// (scalarsDeferrableToFinalMerge guarantees the agg specs the
	// intermediates are built from carry no placeholders).
	type resolvedStage struct {
		stage physical.Stage
		err   error
	}
	var scalarCh chan resolvedStage
	if scalars != nil {
		scalarCh = make(chan resolvedStage, 1)
		go func() {
			s, err := scalars(ctx)
			scalarCh <- resolvedStage{stage: s, err: err}
		}()
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
	aggs = append(aggs, wireAggSpecs(stage.AggSpecs)...)
	// Decompose AVG specs into (SUM, COUNT) pairs. Both intermediates
	// and the final task get the decomposed list; the worker's avg-fold
	// step reconstructs AVG only on the FINAL task (mergeMode), so
	// intermediate output schemas carry the synthetic columns end-to-
	// end up to the final's fold.
	aggs = c.decomposeOhlcvFor(decomposeCovar(decomposeVar(decomposeAvg(aggs))))

	// Phase 1: intermediates. Each consumes a slice of upstream files,
	// re-aggregates in merge mode, and emits its own partial output.
	// StageType="merge_aggregate" enables InputCol→OutputCol rewrite and
	// COUNT→SUM rewrite (the merge math) but skips the AVG fold — that
	// only fires on the FINAL task. Without this distinction, intermediate
	// tasks would fold AVG synthetic columns (__avg_sum#X / __avg_count#X)
	// into a single AVG column X, and the final task's HashAggregate
	// would then attempt to merge AVG values directly (mathematically
	// wrong: avg-of-avgs is unweighted) AND fail to find the synthetic
	// columns it expects in its specs. Q17 SF0.1 Brand#23 MED BOX
	// matched parts produced 0 rows because of this — bisect at
	// 8239db4 (2026-05-01).
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	intermTasks := make([]distributed.Task, 0, N)
	// Synthetic stage for the intermediate fragments: Type="merge_aggregate"
	// keeps FoldAvg=false (AVG synthetics survive end-to-end up to the
	// final task), no SortKeys / Limit / FilterExprs — those are final-only.
	intermStage := physical.Stage{
		Type:        "merge_aggregate",
		GroupByCols: stage.GroupByCols,
	}
	// Per-task admission estimate: each intermediate reads ~1/N of the
	// upstream output (worker-reported bytes; 0 = unknown).
	intermEst := int64(0)
	if b := inputs[depID].Bytes; b > 0 && N > 0 {
		intermEst = b / int64(N)
	}
	intermScanCols := c.stageInputScanColumns(ctx, stage, inputs)
	intermScanTypes := stageInputScanSchemas(stage, inputs)
	for i, group := range groups {
		t := distributed.Task{
			ID:             uuid.New().String()[:8],
			QueryID:        queryID,
			StageID:        fmt.Sprintf("%s-merge-%d", stage.ID, i),
			Type:           distributed.TaskTypeStage,
			DataBucket:     c.config.ResultBucket,
			ResultBucket:   c.config.ResultBucket,
			ResultPrefix:   resultPrefix,
			EstimatedBytes: intermEst,
			CreatedAt:      time.Now(),
		}
		// No exact row bound: the fan-out slices the upstream by FILE, and
		// the per-partition row vector cannot address a file group.
		ops, ferr := buildAggregateFragment(intermStage, &t, map[string][]string{depID: group}, aggs, nil, "", 0)
		if ferr != nil {
			return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: build interm fragment %d: %w", stage.ID, i, ferr)
		}
		t.Operators = ops
		// This dispatcher builds its own tasks instead of going through
		// dispatchComputeStage, so it has to do that function's two
		// source-annotation steps itself. The fan-out fires over a
		// Singleton upstream, which a PASS-THROUGH leaf scan is — so
		// `group` can be base-table parquet, and without these the read
		// takes its columns and its TYPES from the files (#423/#503). The
		// final task below reads the intermediates' own WSHF and needs
		// neither.
		applySourceProjection(t.Operators, intermScanCols)
		applySourceColumnTypes(t.Operators, intermScanTypes)
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		intermTasks = append(intermTasks, t)
	}
	// Intermediates always sink to S3 (no fused subject) — retry-safe.
	intermFiles, intermBytes, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-interm-%s", stage.ID, queryID),
		stage.ID+"-interm",
		intermTasks,
		true)
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: intermediate phase: %w", stage.ID, err)
	}
	// Flatten intermediate outputs into a single file list for the final.
	finalInputs := make([]string, 0)
	for _, fs := range intermFiles {
		finalInputs = append(finalInputs, fs...)
	}

	// Join the deferred scalar substitution before anything below reads
	// stage.FilterExprs — the final task is built from the resolved copy.
	if scalarCh != nil {
		r := <-scalarCh
		if r.err != nil {
			return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: deferred scalar substitution: %w", stage.ID, r.err)
		}
		stage = r.stage
	}

	// Convert SortKeys once for both the legacy and fragment paths below.
	// fuseSortIntoPredecessor folds a downstream Singleton sort into the
	// final_aggregate stage; carrying SortKeys onto the final task lets
	// the worker apply the sort in-process instead of relying on a
	// separate sort task.
	var sorts []distributed.SortKeySpec
	for _, s := range stage.SortKeys {
		sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc,
			NullsLast: distributed.NullsLastPtr(s.NullsLast), SlotPos: s.SlotPos})
	}

	// Phase 2: single final merge task over the intermediate outputs.
	// Always emits a fragment (HashAggregate(merge, FoldAvg) → Filter?(HAVING)
	// → Sort? → OpGatherSink|OpUnpartitionedSink). Gather fusion (when
	// the planner detected the chain) replaces the terminal sink with
	// OpGatherSink and pre-arms the receiver with one terminal.
	finalTask := distributed.Task{
		ID:              uuid.New().String()[:8],
		QueryID:         queryID,
		StageID:         stage.ID,
		Type:            distributed.TaskTypeStage,
		DataBucket:      c.config.ResultBucket,
		ResultBucket:    c.config.ResultBucket,
		ResultPrefix:    resultPrefix,
		EstimatedBytes:  intermBytes, // final merge reads every intermediate output
		CreatedAt:       time.Now(),
		PostFilterExprs: append([]string(nil), stage.FilterExprs...),
	}
	finalInputsMap := map[string][]string{depID: finalInputs}
	var fusedSubject string
	if fusion != nil {
		fusedSubject = fusion.replySubject
	}
	// The final merge reads every intermediate output; runStageTasks reports
	// their bytes, not their rows, so there is no exact bound to declare.
	finalOps, ferr := buildAggregateFragment(stage, &finalTask, finalInputsMap, aggs, sorts, fusedSubject, 0)
	if ferr != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: build final fragment: %w", stage.ID, ferr)
	}
	finalTask.Operators = finalOps
	if fusion != nil {
		fusion.recv.SetExpectedTerminals(1)
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		finalTask.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &finalTask)

	// The final task streams to the client when gather-fused — retry only
	// when it sinks to S3 instead.
	finalFiles, finalBytes, err := c.runStageTasks(ctx,
		fmt.Sprintf("st-%s-%s", stage.ID, queryID),
		stage.ID,
		[]distributed.Task{finalTask},
		fusion == nil)
	if err != nil {
		return StageOutput{}, fmt.Errorf("final_aggregate fanout %s: final phase: %w", stage.ID, err)
	}
	return StageOutput{
		Kind:          OutputSinglePart,
		NumPartitions: 1,
		Files:         finalFiles,
		Bytes:         finalBytes,
	}, nil
}
