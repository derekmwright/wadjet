package physical

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

// ShardedSortFinals gates the shard-local sort/limit fold in
// fuseSortIntoPredecessor (sharded sort/limit finals). Kill switch
// WADJET_SHARDED_FINALS=0 restores the Singleton collapse. Exported
// atomic.Bool (the ScalarAggSemijoin pattern) so tests can pin either arm.
var ShardedSortFinals atomic.Bool

func init() {
	ShardedSortFinals.Store(os.Getenv("WADJET_SHARDED_FINALS") != "0")
}

// ValidateNativeDAGShape walks the stage list and returns an error describing
// the first stage whose shape the native-DAG executor cannot consume. Called
// by the coordinator before dispatch so plan-shape problems surface as a clear
// fail-fast at plan time, instead of as silent timeouts (Q01 SF10 2026-04-23
// case) or mid-execution input-mapping errors (Q02 SF10 case).
//
// Each branch encodes a contract the executor relies on:
//   - hash_join / broadcast_join: exactly 2 deps, no FusedJoins.
//     buildTaskInputsForStage maps probe→[0] / build→[1]; >2 deps means the
//     planner left a fused-broadcast tree the dispatcher can't unpack.
//   - exchange-repartition / replicate / gather: exactly 1 dep (Exchange
//     stages bridge a single child distribution to the parent's required
//     distribution).
//   - MergeGroupCount > 0: stage is the intermediate tier of a multi-level
//     merge_aggregate / merge_sort tree. collapseMergeTreesForNativeDAG
//     should have flattened it; if it slipped through, dispatch creates the
//     SF10 N-stage thrash.
func ValidateNativeDAGShape(stages []Stage) error {
	for _, s := range stages {
		switch s.Type {
		case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
			// A join with N FusedJoins has 2 + N dependencies: 2 for the
			// primary join (probe + build) plus 1 build-dep per fused
			// join. The worker's executeStageHashJoin consumes
			// task.FusedJoins[i].BuildFiles for each; the dispatcher
			// translates planner-side FusedJoinSpec.BuildDepStage into
			// the wire-format BuildFiles by looking up the upstream stage
			// output.
			// ChainedJoins (stage-chain fusion) add one build dep each on
			// top of the fused-join build deps.
			expectedDeps := 2 + len(s.FusedJoins) + len(s.ChainedJoins)
			if len(s.Dependencies) != expectedDeps {
				return fmt.Errorf("native-DAG: %s stage %s has %d dependencies, expected %d (2 primary + %d fused + %d chained)",
					s.Type, s.ID, len(s.Dependencies), expectedDeps, len(s.FusedJoins), len(s.ChainedJoins))
			}
		case StageExchangeRepartition, StageExchangeReplicate, StageExchangeGather:
			if len(s.Dependencies) != 1 {
				return fmt.Errorf("native-DAG: %s stage %s has %d dependencies, expected 1",
					s.Type, s.ID, len(s.Dependencies))
			}
		case StageUnion:
			// Arm i is dispatched as task i reading Dependencies[i], so the
			// two lists must stay index-aligned. A pass that rewired one
			// without the other would silently pair an arm's projection
			// with another arm's rows.
			if len(s.UnionArms) != len(s.Dependencies) {
				return fmt.Errorf("native-DAG: union stage %s has %d arms and %d dependencies",
					s.ID, len(s.UnionArms), len(s.Dependencies))
			}
			if len(s.UnionArms) < 2 {
				return fmt.Errorf("native-DAG: union stage %s has %d arms, expected at least 2",
					s.ID, len(s.UnionArms))
			}
			for i, arm := range s.UnionArms {
				if arm.DepStage != s.Dependencies[i] {
					return fmt.Errorf("native-DAG: union stage %s arm %d names producer %q but Dependencies[%d] is %q",
						s.ID, i, arm.DepStage, i, s.Dependencies[i])
				}
			}
		}
		if s.MergeGroupCount > 0 {
			return fmt.Errorf("native-DAG: stage %s has MergeGroupCount=%d (intermediate tier of a merge tree); collapseMergeTreesForNativeDAG should have flattened it",
				s.ID, s.MergeGroupCount)
		}
	}
	return nil
}

// fuseSortIntoPredecessor folds a Singleton sort stage into the compute
// stage that produces its sole input, so the predecessor applies sort
// in-process instead of writing intermediate output and letting a
// separate sort task pick it up. Same savings class as
// collapseRedundantFinalMergeSort but for the aggregate/join→sort edge:
// one fewer stage, one fewer JetStream round-trip, one fewer
// KV/S3 materialization per query.
//
// Predecessor eligibility:
//   - Singleton distribution (sort output stays Singleton regardless of
//     whether the predecessor was Hash-partitioned or Singleton; this
//     pass handles only the Singleton predecessor case to preserve
//     partition count semantics upstream).
//   - Doesn't already carry SortKeys (we'd clobber them).
//   - Is a compute stage type that the worker's Stage dispatcher knows
//     how to post-sort: hash_join, broadcast_join, aggregate,
//     final_aggregate. Other types (scan, merge_sort, window) are left
//     alone until the worker dispatcher supports post-sort there too.
//
// Sort eligibility:
//   - Type == "sort" and Singleton distribution.
//   - Exactly one dependency.
//
// Correctness: the sort stage carried SortKeys + Limit; both move onto
// the predecessor. Downstream references to the dropped sort are
// rewritten to the predecessor. No LeftDepStage/RightDepStage rewriting
// needed because the sort had only one plain dep (sort stages never
// appear as a join's left/right slot).
func fuseSortIntoPredecessor(stages []Stage, workerCount int) []Stage {
	idIndex := make(map[string]int, len(stages))
	for i := range stages {
		idIndex[stages[i].ID] = i
	}

	droppedSort := make(map[string]string) // sort ID → predecessor ID
	for i := range stages {
		s := &stages[i]
		if s.Type != "sort" {
			continue
		}
		if s.Distribution.Kind != DistSingleton {
			continue
		}
		if len(s.Dependencies) != 1 {
			continue
		}
		predIdx, ok := idIndex[s.Dependencies[0]]
		if !ok {
			continue
		}
		pred := &stages[predIdx]
		if pred.Distribution.Kind != DistSingleton {
			continue
		}
		switch pred.Type {
		case "hash_join", "broadcast_join", "sort_merge_join", "aggregate", "final_aggregate":
		default:
			continue
		}
		if len(pred.SortKeys) > 0 {
			continue // don't clobber existing post-sort
		}
		// Shard-local fold (sharded sort/limit finals): a grouped
		// final_aggregate under an ORDER BY + LIMIT collapses to one task
		// that merges every group serially — a 15 M-group merge is a 16 s
		// single-task tail on SF100 Q10/Q13. When the upstream partials fan
		// out, instead of fusing the sort away we copy SortKeys+Limit onto
		// the final as SHARD-LOCAL (EnsureDistribution then clusters its
		// input on the group keys and fans it out; each shard owns disjoint
		// groups, so exact per-shard aggregates + local top-Limit are a
		// superset of the global top-Limit) and KEEP this sort stage as the
		// N×Limit-row merge. Limit > 0 is load-bearing: it bounds the merge
		// input; ORDER BY without LIMIT keeps today's fuse.
		// Kill switch WADJET_SHARDED_FINALS=0.
		if ShardedSortFinals.Load() && workerCount > 1 &&
			pred.Type == "final_aggregate" && len(pred.GroupByCols) > 0 &&
			s.Limit > 0 && len(pred.Dependencies) == 1 {
			if depIdx, ok := idIndex[pred.Dependencies[0]]; ok &&
				stages[depIdx].Distribution.Kind != DistSingleton {
				pred.SortKeys = append([]SortKeySpec(nil), s.SortKeys...)
				pred.Limit = s.Limit
				pred.SortShardLocal = true
				continue // sort stage survives as the merge
			}
		}
		// Fold sort into predecessor.
		pred.SortKeys = append([]SortKeySpec(nil), s.SortKeys...)
		if s.Limit > 0 {
			pred.Limit = s.Limit
		}
		droppedSort[s.ID] = pred.ID
	}

	if len(droppedSort) == 0 {
		return stages
	}

	out := make([]Stage, 0, len(stages)-len(droppedSort))
	for _, s := range stages {
		if _, drop := droppedSort[s.ID]; drop {
			continue
		}
		if len(s.Dependencies) > 0 {
			newDeps := make([]string, len(s.Dependencies))
			for i, d := range s.Dependencies {
				if target, ok := droppedSort[d]; ok {
					newDeps[i] = target
				} else {
					newDeps[i] = d
				}
			}
			s.Dependencies = newDeps
		}
		if target, ok := droppedSort[s.LeftDepStage]; ok {
			s.LeftDepStage = target
		}
		if target, ok := droppedSort[s.RightDepStage]; ok {
			s.RightDepStage = target
		}
		out = append(out, s)
	}
	return out
}

// collapseRedundantFinalMergeSort removes trivial trailing merge_sort
// stages whose job is already done by their upstream Singleton sort.
// The common TPC-H shape after collapseMergeTreesForNativeDAG is:
//
//   sort-N        Singleton   SortKeys=K   deps=[...]
//   merge_sort-M  Singleton   deps=[sort-N]
//   gather        Singleton   deps=[merge_sort-M]
//
// The merge_sort-M stage is a no-op: it reads one already-sorted input
// and re-emits it. Under native-DAG this becomes a full worker round-
// trip (pull task, open input, emit output, write to KV/S3) per query
// for zero algorithmic work. We drop merge_sort-M from the plan and
// rewrite its dependents (gather, etc.) to consume sort-N directly.
//
// Conservative: only collapse when merge_sort has exactly one dep, that
// dep is a sort stage with Singleton distribution, and the merge_sort
// itself is Singleton. Partition-aware merge_sort stages that combine
// multiple sorted shards are left alone.
func collapseRedundantFinalMergeSort(stages []Stage) []Stage {
	idIndex := make(map[string]int, len(stages))
	for i := range stages {
		idIndex[stages[i].ID] = i
	}

	removed := make(map[string]string) // merge_sort ID → sort ID (rewrite target)
	for i := range stages {
		ms := &stages[i]
		if ms.Type != "merge_sort" {
			continue
		}
		if ms.Distribution.Kind != DistSingleton {
			continue
		}
		if len(ms.Dependencies) != 1 {
			continue
		}
		depIdx, ok := idIndex[ms.Dependencies[0]]
		if !ok {
			continue
		}
		dep := stages[depIdx]
		if dep.Type != "sort" {
			continue
		}
		if dep.Distribution.Kind != DistSingleton {
			continue
		}
		removed[ms.ID] = dep.ID
	}
	if len(removed) == 0 {
		return stages
	}

	out := make([]Stage, 0, len(stages)-len(removed))
	for _, s := range stages {
		if _, drop := removed[s.ID]; drop {
			continue
		}
		// Rewrite dependencies that pointed at a dropped merge_sort to
		// point at its underlying sort.
		if len(s.Dependencies) > 0 {
			newDeps := make([]string, len(s.Dependencies))
			for i, d := range s.Dependencies {
				if target, ok := removed[d]; ok {
					newDeps[i] = target
				} else {
					newDeps[i] = d
				}
			}
			s.Dependencies = newDeps
		}
		// Hash-join and similar stages carry LeftDepStage / RightDepStage
		// pointers separate from Dependencies; rewrite those too so the
		// dispatcher's alias→input mapping stays consistent.
		if target, ok := removed[s.LeftDepStage]; ok {
			s.LeftDepStage = target
		}
		if target, ok := removed[s.RightDepStage]; ok {
			s.RightDepStage = target
		}
		out = append(out, s)
	}
	return out
}

// collapseMergeTreesForNativeDAG rewrites multi-level merge_aggregate /
// merge_sort fan-out trees back into single-stage form. The trees are
// emitted by emitMergeAggregateTree / emitMergeSortTree when the upstream
// task count exceeds mergeFanout (16) — valid for the single-pipeline
// executor where intermediate merges run in parallel as inner-pipeline
// operators, but catastrophic for native-DAG execution where each
// intermediate stage becomes an independent coordinator-worker round-trip
// that re-scans the same upstream data.
//
// Shape produced by emitMergeAggregateTree (upstream > 16):
//   intermediate-0   final_aggregate  dep=[leafIDs...] MergeGroup=0
//   intermediate-1   final_aggregate  dep=[leafIDs...] MergeGroup=1
//   ...
//   intermediate-N   final_aggregate  dep=[leafIDs...] MergeGroup=N-1
//   final            final_aggregate  dep=[intermediate-0, ..., intermediate-N]
//
// Rewrite to:
//   final            final_aggregate  dep=[leafIDs...]    (no MergeGroup)
//
// The same pattern applies to merge_sort trees. Rewriting is safe because:
//   - Final stages already compute the full aggregate/sort from all upstream
//     rows; the tree only exists to parallelize intra-worker merging.
//   - Native-DAG dispatches workerCount tasks per stage, so parallelism
//     comes from task-level parallelism, not stage-level fan-out.
//   - executeStageAggregate + executeStageSort run as single-task fan-in
//     today (they consume all upstream inputs and emit merged output);
//     wiring them behind a N-partition dispatch is future work, but even
//     single-task is faster than the 83-stage tree.
//
func collapseMergeTreesForNativeDAG(stages []Stage) []Stage {
	// Pre-index stages by ID.
	idIndex := make(map[string]int, len(stages))
	for i := range stages {
		idIndex[stages[i].ID] = i
	}

	// A merge tree's "final" stage has Type == merge_aggregate-family or
	// merge_sort and all its deps look like intermediate-group entries
	// (MergeGroupCount > 0). Collect those finals.
	removed := make(map[string]bool)
	for i := range stages {
		final := &stages[i]
		if !isMergeTreeFinal(stages, final, idIndex) {
			continue
		}
		// Union of the intermediates' leaf deps becomes the new deps list.
		// In the emitted tree, every intermediate shares the same leaf
		// dep list, so we can take the first one.
		var leafDeps []string
		for _, depID := range final.Dependencies {
			idx, ok := idIndex[depID]
			if !ok {
				continue
			}
			if leafDeps == nil {
				leafDeps = append([]string(nil), stages[idx].Dependencies...)
			}
			removed[depID] = true
		}
		final.Dependencies = leafDeps
		final.MergeGroup = 0
		final.MergeGroupCount = 0
	}

	if len(removed) == 0 {
		return stages
	}
	out := make([]Stage, 0, len(stages)-len(removed))
	for _, s := range stages {
		if removed[s.ID] {
			continue
		}
		out = append(out, s)
	}
	return out
}

// isMergeTreeFinal reports true when the stage is the root of a two-level
// merge tree: it's a final_aggregate or merge_sort whose dependencies all
// reference intermediate group stages (same Type, MergeGroupCount > 0).
func isMergeTreeFinal(stages []Stage, s *Stage, idIndex map[string]int) bool {
	switch s.Type {
	case "final_aggregate", "merge_sort":
	default:
		return false
	}
	if s.MergeGroupCount != 0 {
		// This IS an intermediate, not a final.
		return false
	}
	if len(s.Dependencies) < 2 {
		return false
	}
	for _, depID := range s.Dependencies {
		idx, ok := idIndex[depID]
		if !ok {
			return false
		}
		dep := stages[idx]
		if dep.Type != s.Type {
			return false
		}
		if dep.MergeGroupCount == 0 {
			return false
		}
		// Sanity: intermediate IDs follow "<type>-<N>-<group>" pattern.
		if !strings.Contains(dep.ID, "-") {
			return false
		}
	}
	return true
}
