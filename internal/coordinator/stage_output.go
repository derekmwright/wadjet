package coordinator

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// OutputKind describes how a stage's output is distributed across S3 keys.
type OutputKind int

const (
	// OutputPartitioned: Files[p] holds the keys for partition p, for p in
	// [0, NumPartitions). Produced by StageExchangeRepartition; consumed by
	// hash-join/aggregate downstream stages that bind one worker per
	// contiguous partition range.
	OutputPartitioned OutputKind = iota
	// OutputReplicated: Files[0] holds broadcast keys. Produced by
	// StageExchangeReplicate; every downstream worker reads the same file.
	OutputReplicated
	// OutputSinglePart: Files[0] holds the full, unpartitioned output.
	// Produced by pipeline stages feeding a Gather (single output stream).
	OutputSinglePart
)

// StageOutput is the materialized output of one stage in the native-DAG
// executor. Produced by each dispatchXxxStage helper; consumed by
// downstream stages' collectInputs.
type StageOutput struct {
	Kind          OutputKind
	NumPartitions int        // valid when Kind == OutputPartitioned
	Files         [][]string // Partitioned: Files[p]; Replicated/SinglePart: Files[0]

	// Bytes is the total on-disk size of Files as reported by the workers
	// that wrote them (ResultNotification.SizeBytes), or the catalog size
	// for pass-through leaf scans. Feeds downstream tasks' EstimatedBytes
	// for memory-aware admission. 0 = unknown (legacy worker, empty
	// output) — downstream estimates degrade to unknown, never to wrong.
	// Note these are file bytes (compressed), the same unit the scan-stage
	// estimates use; in-memory footprint is larger by the codec ratio.
	Bytes int64

	// PartitionRows/PartitionBytes: per-partition output totals reduced
	// across the producing stage's tasks (final surviving attempts only),
	// indexed by partition id. Valid when Kind == OutputPartitioned; nil
	// when the workers didn't report (legacy build) — downstream skew
	// detection degrades to off, never to wrong. Bytes are on-disk
	// uncompressed .wshf sizes, the unit per-task admission estimates use.
	PartitionRows  []int64
	PartitionBytes []int64

	// BuildStats, populated when this stage's Stage.EmitDynamicFilters was
	// non-empty. One entry per emit, keyed by FilterID. Downstream consumer
	// stages reach in via the stat-dep edge to pull the materialized stats.
	BuildStats map[string]*BuildStats

	// DynamicFilters, populated when this stage's Stage.ConsumeDynamicFilters
	// resolved against an upstream build-scan's BuildStats. Downstream
	// dispatchers (shuffle, compute) thread these into their task
	// descriptors so workers apply the bloom at scan time. Carries forward
	// across pass-through leaf scans that don't dispatch tasks themselves.
	DynamicFilters []distributed.DynamicFilterSpec

	// ScanFileSizes aligns 1:1 with Files[0] on the pass-through leaf
	// scan path (catalog SizeBytes, copied from Stage.ScanFileSizes) so
	// the downstream shuffle dispatcher's synthetic source stage can
	// byte-balance its affine fan-out. Nil elsewhere.
	ScanFileSizes []int64

	// ScanTable/ScanColumns identify a pass-through leaf scan's relation
	// and (sanitized) projection. Set ONLY on the no-task pass-through
	// path, where downstream shuffle tasks read base parquet directly —
	// dispatchShuffleStage copies them onto its synthetic source stage so
	// prunedScanColumns can narrow the shuffle payload. Without them the
	// synthetic stage has no column list and every scan-absorbed shuffle
	// leg ships full-width rows (85.69 GB vs ~12 GB needed on Q21's l2
	// leg; docs/design/exchange-reuse.md §2 A2). Empty on all other
	// outputs — WSHF inputs ignore column projection.
	ScanTable   string
	ScanColumns []string

	// eager marks a PROVISIONAL output synthesized by an eagerly-cleared
	// consumer (eagerFeed.provisionalOutput): the producer stage is still
	// running, Files is the empty layout, and dispatchComputeStage must
	// feed the corresponding input alias from Task.EagerInputs manifests
	// instead of a frozen file list. nil on every real (completed-stage)
	// output. In-memory only — never serialized.
	eager *eagerFeed
}

// BuildStats is the coordinator-merged dynamic-filter artifact for one
// FilterID across all build-scan tasks of one stage. Computed by
// mergeBuildStatsFromPartials after the build-scan stage completes.
type BuildStats struct {
	FilterID  string
	KeyType   string
	HasRange  bool
	Min, Max  int64
	RowCount  int64
	Bloom     []uint64
	BloomMask uint64
	// StagedBucket/Key set when the unioned bloom exceeded the inline
	// threshold and was uploaded; Bloom is nil in that case.
	StagedBucket string
	StagedKey    string
}

// collectInputs translates a stage's Dependencies into a map keyed by the
// producing stage's ID by looking up each dependency's output in the
// stageOutputs map. Returns an error if any dep has no recorded output —
// that indicates the topological walk missed a stage.
func collectInputs(stage physical.Stage, outputs map[string]StageOutput) (map[string]StageOutput, error) {
	inputs := make(map[string]StageOutput, len(stage.Dependencies))
	for _, depID := range stage.Dependencies {
		out, ok := outputs[depID]
		if !ok {
			return nil, fmt.Errorf("stage %s: dependency %s has no recorded output", stage.ID, depID)
		}
		inputs[depID] = out
	}
	return inputs, nil
}

// partitionFilesForWorker returns the subset of partitioned output files
// assigned to worker w of workerCount workers. Contiguous-slice assignment
// mirrors the legacy buildShufflePipelineTasks scheduling: partitions
// [0, partsPerWorker) go to worker 0, next slice to worker 1, etc., with
// the last worker absorbing any remainder.
//
// For non-partitioned kinds, returns Files[0] regardless of w (every
// worker reads the same broadcast or single-part stream).
func partitionFilesForWorker(out StageOutput, w, workerCount int) []string {
	if out.Kind != OutputPartitioned {
		if len(out.Files) == 0 {
			return nil
		}
		return out.Files[0]
	}
	start, end := partitionRangeForWorker(out.NumPartitions, w, workerCount)
	var files []string
	for p := start; p < end; p++ {
		files = append(files, out.Files[p]...)
	}
	return files
}

// partitionRangeForWorker returns the half-open partition range [start, end)
// worker w of workerCount binds under the contiguous-slice assignment.
// Returns an empty range (start == end) when w gets nothing. Extracted from
// partitionFilesForWorker so the skew-split planner (skew_split.go) groups
// partitions by EXACTLY the ranges the unsplit layout would bind — any
// divergence between the two would mis-align split decisions with task
// inputs.
func partitionRangeForWorker(numPartitions, w, workerCount int) (start, end int) {
	if numPartitions == 0 || workerCount <= 0 || w < 0 || w >= workerCount {
		return 0, 0
	}
	partsPerWorker := numPartitions / workerCount
	if partsPerWorker == 0 {
		partsPerWorker = 1
	}
	start = w * partsPerWorker
	end = start + partsPerWorker
	if w == workerCount-1 {
		end = numPartitions // last worker absorbs remainder
	}
	if start >= numPartitions {
		return 0, 0
	}
	if end > numPartitions {
		end = numPartitions
	}
	return start, end
}

// wireColumnSpecs converts a plan-declared schema to its wire form. Nil in,
// nil out — an absent declaration is the normal case for every join whose
// sides the planner could not type.
func wireColumnSpecs(cols []parquet.Column) []distributed.ColumnSpec {
	if len(cols) == 0 {
		return nil
	}
	out := make([]distributed.ColumnSpec, len(cols))
	for i, c := range cols {
		out[i] = distributed.ColumnSpec{Name: c.Name, Type: int(c.Type)}
	}
	return out
}

// flattenStageFiles returns all output files in a single flat list,
// regardless of partition layout. Used when a downstream stage treats the
// upstream as one big pile of keys (e.g. a fresh shuffle that rewrites
// partitioning, or a gather over a pipeline stage).
func flattenStageFiles(out StageOutput) []string {
	var files []string
	for _, chunk := range out.Files {
		files = append(files, chunk...)
	}
	return files
}
