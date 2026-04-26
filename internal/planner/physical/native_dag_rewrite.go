package physical

import (
	"fmt"
	"strings"
)

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
		case StageHashJoin, StageBroadcastJoin:
			if len(s.Dependencies) != 2 {
				return fmt.Errorf("native-DAG: %s stage %s has %d dependencies, expected 2 (FusedJoins should be disabled by the planner via Planner.UseEnsureDistribution; see fuseJoinStages call site)",
					s.Type, s.ID, len(s.Dependencies))
			}
			if len(s.FusedJoins) > 0 {
				return fmt.Errorf("native-DAG: %s stage %s carries %d FusedJoins which executeStageHashJoin / executeStageBroadcastJoin do not consume",
					s.Type, s.ID, len(s.FusedJoins))
			}
		case StageExchangeRepartition, StageExchangeReplicate, StageExchangeGather:
			if len(s.Dependencies) != 1 {
				return fmt.Errorf("native-DAG: %s stage %s has %d dependencies, expected 1",
					s.Type, s.ID, len(s.Dependencies))
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
func fuseSortIntoPredecessor(stages []Stage) []Stage {
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
		case "hash_join", "broadcast_join", "aggregate", "final_aggregate":
		default:
			continue
		}
		if len(pred.SortKeys) > 0 {
			continue // don't clobber existing post-sort
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
// Gated by Planner.UseEnsureDistribution since that flag is synonymous
// with native-DAG in callers today.
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

// insertHashShuffleBeforeFinalAgg splices a StageExchangeRepartition (hash by
// GroupByCols) between every grouped final_aggregate and its sole upstream
// dep, so the dispatcher can fan the merge across workers. Without this, a
// Singleton final_aggregate over a fanned-out partial input runs as one task
// re-aggregating everything serially — the dominant cost for queries like
// Q18 SF10 where the partial output is large.
//
// Why a dedicated pass instead of EnsureDistribution: the property algebra
// has RequiredClusteredOn satisfied by DistSingleton (a single-worker output
// is "trivially clustered"), so labeling final_aggregate as
// RequiredClusteredOn doesn't trigger an exchange when the upstream is a
// fused-aggregate scan (whose nominal Distribution stays Singleton even
// though dispatchScanAggregateStage fans it out into N partial files at
// runtime). Working around that requires either a new DistKind or special-
// casing scan output, both of which break unrelated callers (notably the
// merge_aggregate consistency check). A targeted rewrite keeps the property
// algebra unchanged and only acts on the case that actually benefits.
//
// Skips:
//   - merge_aggregate intermediates (MergeGroupCount > 0): handled by the
//     existing merge tree shape, output is already labeled hash-partitioned.
//   - Scalar aggregates (no GROUP BY): nothing to cluster on.
//   - Multi-dep finals (the dual-level merge tree shape): the upstream is
//     already a fan-out and the dispatcher's fanout in
//     dispatchFinalAggregateFanout handles it.
//   - Stages whose sole dep is already a StageExchangeRepartition with
//     matching Keys: no-op, exchange already in place.
func insertHashShuffleBeforeFinalAgg(stages []Stage, workerCount int) []Stage {
	if workerCount <= 0 {
		workerCount = 1
	}
	idIndex := make(map[string]int, len(stages))
	for i, s := range stages {
		idIndex[s.ID] = i
	}

	out := make([]Stage, 0, len(stages))
	for _, s := range stages {
		out = append(out, s)
	}

	for i := range out {
		s := &out[i]
		if s.Type != "final_aggregate" {
			continue
		}
		if s.MergeGroupCount > 0 {
			continue
		}
		if len(s.GroupByCols) == 0 {
			continue
		}
		if len(s.Dependencies) != 1 {
			continue
		}
		// Skip when fuseSortIntoPredecessor has folded sort+limit into
		// this final_aggregate. Hash-partitioning would let each task
		// apply Limit independently (e.g., Q10 LIMIT 20 with 3 workers
		// emits 60 rows) and global sort order can't be reconstructed
		// from per-partition slices. Both require a Singleton
		// finalization step that we don't synthesize today.
		if s.Limit > 0 || len(s.SortKeys) > 0 {
			continue
		}
		depID := s.Dependencies[0]
		depIdx, ok := idIndex[depID]
		if !ok {
			continue
		}
		dep := out[depIdx]
		if dep.Type == StageExchangeRepartition && dep.Exchange != nil &&
			keysEqual(dep.Exchange.Keys, s.GroupByCols) {
			continue
		}

		exchID := fmt.Sprintf("%s-%s", StageExchangeRepartition, s.ID)
		exch := Stage{
			ID:           exchID,
			Type:         StageExchangeRepartition,
			Dependencies: []string{depID},
			Exchange: &ExchangeStage{
				Keys:  append([]string(nil), s.GroupByCols...),
				Count: workerCount,
			},
		}
		out = append(out, exch)
		idIndex[exchID] = len(out) - 1
		// Rewire the final_aggregate by index since &out[i] may now be
		// invalid after append.
		out[i].Dependencies = []string{exchID}
	}
	return out
}
