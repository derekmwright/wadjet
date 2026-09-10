// This file holds pipeline dispatch, scalar deferral, and scan fan-out sizing.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

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
	isBroadcastJoinProbe bool,
	fusion *gatherFusion,
	scalars scalarResolver,
) (StageOutput, error) {
	_ = sql
	// Leaf scan stage.
	//
	// The `inputs` map may be non-empty when a stat-dep edge is present
	// (probe-side leaf with ConsumeDynamicFilters depends on a build-side
	// leaf). Stat-dep deps don't change the leaf-ness of the scan; they
	// only provide upstream BuildStats. ScanFiles is the structural marker:
	// any stage with ScanFiles populated IS a leaf scan regardless of how
	// many soft deps it has.
	if stage.Type == physical.StageScan && len(stage.ScanFiles) > 0 {
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
		// FusedAggGroupBy alone (no aggregate functions) is a bare
		// GROUP BY — grouping IS the work, so it must route through the
		// scan-aggregate path too. Falling through to a plain scan here
		// skipped the partial dedup AND the fused hash-partition
		// exchange, so the downstream final_aggregate's N tasks each
		// read overlapping raw-row file slices and re-emitted every
		// group per task (#166: 3 groups × 3 tasks = 9 rows).
		if len(stage.FusedAggSpecs) > 0 || len(stage.FusedAggGroupBy) > 0 {
			return c.dispatchScanAggregateStage(ctx, queryID, stage, workerCount)
		}
		// Plain scan with no fused aggregate. When the planner pushed
		// filters onto this scan (e.g., Q05's `r_name = 'ASIA'`), we
		// must actually apply them — otherwise the downstream join
		// sees every row, cartesian-multiplies, and returns way too
		// many results. Dispatch a filter-scan task that reads the
		// parquet files, applies FilterExprs, and writes a WSHF output
		// the rest of the native-DAG pipeline can consume.
		//
		// ProjectExprs likewise forces the fragment path (#169): the
		// SELECT list carries expressions that must be COMPUTED before
		// the gather — raw-parquet passthrough would hand the gather
		// un-projected scan columns, and applyOutputRenames can only
		// rename/drop.
		if len(stage.FilterExprs) > 0 || len(stage.ProjectExprs) > 0 || len(stage.SecurityProjectExprs) > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
		}
		// A fused shuffle payload (fuseScanShuffle) means downstream
		// partition-binds this stage's output — pass-through raw parquet
		// (OutputSinglePart) would silently feed every consumer task the
		// full table. The planner only fuses dispatched-shape scans, so
		// this branch is defense-in-depth for planner/dispatcher drift.
		if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
		}
		// No filter: by default pass raw parquet files through (saves the
		// wshf write+read hop). But when this scan is the PROBE of a
		// downstream broadcast_join AND it's a single oversized file
		// (SF10 partsupp pre-compacted at 1.4 GB → 1 file), the
		// pass-through cascades single-tasked through the join because
		// broadcastJoinProbeSplit's >=2-file gate skips. Routing through
		// scan-filter (with empty filters) row-group-shards the single
		// file across the cluster and emits multiple output files, which
		// the downstream broadcast_join can then probe-split.
		//
		// Build-side scans of broadcast_joins are explicitly NOT sharded:
		// the broadcast cache is replicated to every join task, so each
		// task would have to load N cache files instead of 1. Q05 SF10
		// regressed 3m7s → 10m47s when the prior attempt at this fix
		// sharded all single-file plain leaf scans regardless of role.
		if isBroadcastJoinProbe && len(stage.ScanFiles) == 1 {
			capacity := c.workers.ClusterCapacity()
			if scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes) > 1 {
				return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
			}
		}
		// Build-side dynamic-filter emit must dispatch real tasks so the
		// emit op runs in the fragment pipeline and uploads its partial.
		// Consume-only (probe-side) stays as pass-through — the downstream
		// shuffle dispatcher threads the materialized DynamicFilters into
		// its shuffle tasks, which apply them at the row-group iterator
		// during their parquet scan. This avoids an extra dispatched scan-
		// filter hop that regressed Q18 SF1 +16% in the first wiring.
		if len(stage.EmitDynamicFilters) > 0 {
			return c.dispatchScanFilterStage(ctx, queryID, stage, inputs, workerCount)
		}
		files := append([]string(nil), stage.ScanFiles...)
		out := StageOutput{
			Kind:          OutputSinglePart,
			Files:         [][]string{files},
			Bytes:         stage.EstimatedBytes, // catalog-true file sizes
			ScanFileSizes: append([]int64(nil), stage.ScanFileSizes...),
			ScanTable:     stage.TableName,
			ScanColumns:   append([]string(nil), stage.Columns...),
			// The consumer of a pass-through reads base-table parquet, so
			// it inherits the scan's problem: a file written before the
			// declared-schema footer key cannot say what nine of its types
			// are (#423). Ship the catalog's answer with the files.
			ScanSchema: stage.ScanSchema,
		}
		// Materialize Consume specs into wire form so downstream shuffle/
		// stage dispatchers can ship them in their task descriptors
		// without having to re-walk the plan.
		if len(stage.ConsumeDynamicFilters) > 0 {
			out.DynamicFilters = dynamicFilterSpecsFromBuildStats(stage.ConsumeDynamicFilters, inputs, queryID, c.config.ResultBucket)
			c.logger.Info("dynamic_filter: consume specs attached to pass-through scan output",
				"stage_id", stage.ID,
				"requested", len(stage.ConsumeDynamicFilters),
				"attached", len(out.DynamicFilters))
		}
		return out, nil
	}
	// Compute stage: emit workerCount TaskTypeStage tasks, each reading its
	// slice of partitioned input and writing unpartitioned .wshf output.
	// The stage's output is collected into a Partitioned StageOutput where
	// partition p = the files produced by worker p — downstream Exchange
	// stages treat this as partitioned if they need to re-hash.
	return c.dispatchComputeStage(ctx, queryID, stage, inputs, workerCount, fusion, scalars)
}

// scalarResolver blocks until a stage's scalar-subquery producer stages have
// completed, then returns a copy of the stage with every :placeholder
// substituted (substituteScalarDependencies). Non-nil only when the stage
// goroutine chose deferral (scalarsDeferrableToFinalMerge): dispatch calls
// it at the last point before the substituted exprs are consumed, letting
// earlier phases overlap the scalar producer chain.
type scalarResolver func(context.Context) (physical.Stage, error)

// scalarsDeferrableToFinalMerge reports whether a stage's scalar
// placeholders are consumed only by its final-merge phase — i.e. they
// appear in none of the fields the fanout's intermediate tasks are built
// from (AggSpec input exprs, JoinFilter). substituteScalarDependencies
// rewrites FilterExprs, AggSpecs, FusedAggSpecs and JoinFilter; of those,
// dispatchFinalAggregateFanout's intermediates see only the agg specs
// (FilterExprs are final-only by construction — applying HAVING to a
// partial merge would drop groups before they finished merging). Only
// final_aggregate qualifies because it is the one stage type dispatched
// in two phases; every other type consumes its exprs in its only phase.
func scalarsDeferrableToFinalMerge(s physical.Stage) bool {
	if s.Type != "final_aggregate" || len(s.ScalarDependencies) == 0 {
		return false
	}
	for ph := range s.ScalarDependencies {
		tok := ":" + ph
		for _, a := range s.AggSpecs {
			if strings.Contains(a.InputExpr, tok) {
				return false
			}
		}
		for _, a := range s.FusedAggSpecs {
			if strings.Contains(a.InputExpr, tok) {
				return false
			}
		}
		if strings.Contains(s.JoinFilter, tok) {
			return false
		}
	}
	return true
}

// scanFanOutTaskCount picks the number of scan-side tasks for stage fan-out.
// Returns max(workerCount, capacity), capped at fileCount and floored at 1.
//
// Prefers cluster capacity (sum of each worker's auto-tuned MaxConcurrent
// reported in heartbeats) over the raw worker count because workers
// downscale concurrency under memory pressure. With 3 workers each running
// MaxConcurrent=2 the cluster can run 6 tasks at once; sizing scan fan-out
// to 3 leaves half the cluster idle AND doubles per-task memory pressure
// (each task reads twice as many files).
//
// Falls back to workerCount when capacity == 0 — typical for the first
// query post-cluster-startup, before any heartbeat has reported MaxConcurrent.
func scanFanOutTaskCount(workerCount, capacity, fileCount int) int {
	n := workerCount
	if capacity > n {
		n = capacity
	}
	if n > fileCount {
		n = fileCount
	}
	if n < 1 {
		n = 1
	}
	return n
}

// scanShardCountForSingleFile returns the number of row-group shard tasks
// to fan out for a single large file. Returns 1 (no sharding) when the
// file is below the threshold or when the cluster is too small to
// benefit. Caps at the cluster's effective task capacity so we don't
// over-shard a small cluster.
func scanShardCountForSingleFile(workerCount, capacity int, fileBytes int64) int {
	if fileBytes < singleFileShardThresholdBytes {
		return 1
	}
	n := workerCount
	if capacity > n {
		n = capacity
	}
	if n < 2 {
		return 1
	}
	return n
}
