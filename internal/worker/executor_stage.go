package worker

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// executeStage dispatches a TaskTypeStage task (Phase 3 native-DAG
// single-operator stage fragment). The Task.StageType string matches
// physical.Stage.Type and selects which operator builder runs.
//
// Stage fragments are self-contained: one operator, inputs read from
// upstream stage output via Task.Inputs, output written to the sink
// selected by task fields (ShuffleKeys → partitionedShuffleSink,
// ReplySubject → gatherReplySink, else parquet).
//
// Scaffolding in this commit: dispatch table is wired, all per-stage
// implementations are stubs that return "not implemented". Subsequent
// commits fill in hash_join, aggregate, sort.
func (e *Executor) executeStage(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	switch task.StageType {
	case "scan":
		return e.executeStageScan(ctx, task, result)
	case "hash_join", "broadcast_join":
		return e.executeStageHashJoin(ctx, task, result)
	case "aggregate", "final_aggregate":
		return e.executeStageAggregate(ctx, task, result)
	case "sort", "merge_sort":
		return e.executeStageSort(ctx, task, result)
	default:
		return fmt.Errorf("executeStage: unsupported StageType %q on task %s",
			task.StageType, task.ID)
	}
}

// executeStageScan: read task.Files, apply projection/filter, write to
// the configured sink. Stub in this commit.
func (e *Executor) executeStageScan(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	return fmt.Errorf("executeStageScan: not yet implemented")
}

// executeStageHashJoin: build from Inputs[BuildTableAlias], probe from
// Inputs[probeAlias], HashJoin operator, write to sink. Stub in this commit.
func (e *Executor) executeStageHashJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	return fmt.Errorf("executeStageHashJoin: not yet implemented")
}

// executeStageAggregate: read Inputs, HashAggregate, write to sink.
// Stub in this commit.
func (e *Executor) executeStageAggregate(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	return fmt.Errorf("executeStageAggregate: not yet implemented")
}

// executeStageSort: read Inputs, Sort (with optional Limit), write to
// sink. Stub in this commit.
func (e *Executor) executeStageSort(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	return fmt.Errorf("executeStageSort: not yet implemented")
}
