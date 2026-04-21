package coordinator

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// dispatchHooks is a test seam for observing per-stage dispatch routing.
// Production has it nil; tests wire per-type callbacks to verify that
// dispatchStageDAG routes each stage type to the correct backend.
type dispatchHooks struct {
	onRepartition func(physical.Stage)
	onReplicate   func(physical.Stage)
	onGather      func(physical.Stage)
	onPipeline    func(physical.Stage)
	onScan        func(physical.Stage)
	onOther       func(physical.Stage)
}

// hasExchangeStages reports whether the given plan contains any
// StageExchange* stage. When true, the plan was produced by the Phase 2
// EnsureDistribution pass and should be dispatched via dispatchStageDAG
// rather than the legacy four-mode switch.
func hasExchangeStages(stages []physical.Stage) bool {
	for _, s := range stages {
		switch s.Type {
		case physical.StageExchangeRepartition,
			physical.StageExchangeReplicate,
			physical.StageExchangeGather:
			return true
		}
	}
	return false
}

// dispatchStageDAG walks plan stages in dependency order (already
// topo-sorted by the planner) and dispatches each to the matching
// backend by Type. Replaces the legacy four-mode switch for plans
// produced with Planner.UseEnsureDistribution=true.
//
// Task 14: dispatcher + test hooks in place, but not yet wired into
// the production execute path. Task 19 replaces the legacy switch
// with a call to this function.
func (c *Coordinator) dispatchStageDAG(ctx context.Context, stages []physical.Stage) error {
	for _, s := range stages {
		if hook := c.hookFor(s.Type); hook != nil {
			hook(s)
		}
		switch s.Type {
		case physical.StageExchangeRepartition:
			// Task 19 will wire the full call. Signature:
			//   c.orchestrateRepartition(ctx, queryID, shuffleCand, stages, workerCount)
			// For now, hooks-only observation — continue.
		case physical.StageExchangeReplicate:
			// Task 19 will wire:
			//   c.orchestrateReplicate(ctx, s, queryID, sql, stages, probeAlias)
		case physical.StageExchangeGather:
			// Task 19 will wire:
			//   c.orchestrateGather(s, batches, columns, mergeInfo)
		case physical.StagePipeline, physical.StageScan, physical.StageAggregate,
			physical.StageSort, physical.StageHashJoin, physical.StageBroadcastJoin,
			physical.StageWindow:
			// Pipeline-style stages: existing execute path handles these.
		default:
			return fmt.Errorf("dispatchStageDAG: unknown stage type %q (stage %s)", s.Type, s.ID)
		}
	}
	return nil
}

func (c *Coordinator) hookFor(t string) func(physical.Stage) {
	if c.dispatchHooks == nil {
		return nil
	}
	switch t {
	case physical.StageExchangeRepartition:
		return c.dispatchHooks.onRepartition
	case physical.StageExchangeReplicate:
		return c.dispatchHooks.onReplicate
	case physical.StageExchangeGather:
		return c.dispatchHooks.onGather
	case physical.StagePipeline:
		return c.dispatchHooks.onPipeline
	case physical.StageScan:
		return c.dispatchHooks.onScan
	}
	if c.dispatchHooks.onOther != nil {
		return c.dispatchHooks.onOther
	}
	return nil
}
