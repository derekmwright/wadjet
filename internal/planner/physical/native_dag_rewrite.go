package physical

import (
	"strings"
)

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
