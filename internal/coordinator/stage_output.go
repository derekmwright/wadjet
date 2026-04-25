package coordinator

import (
	"fmt"

	"github.com/citc-tech/wadjet/internal/planner/physical"
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
	if out.NumPartitions == 0 || workerCount <= 0 || w < 0 || w >= workerCount {
		return nil
	}
	partsPerWorker := out.NumPartitions / workerCount
	if partsPerWorker == 0 {
		partsPerWorker = 1
	}
	start := w * partsPerWorker
	end := start + partsPerWorker
	if w == workerCount-1 {
		end = out.NumPartitions // last worker absorbs remainder
	}
	if start >= out.NumPartitions {
		return nil
	}
	if end > out.NumPartitions {
		end = out.NumPartitions
	}
	var files []string
	for p := start; p < end; p++ {
		files = append(files, out.Files[p]...)
	}
	return files
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
