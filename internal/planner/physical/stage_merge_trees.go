// This file holds stage merge trees for the physical planner, governed by ADR-0010 and ADR-0026.
package physical

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// leafStages returns the IDs of stages that are not depended upon by any other
// stage in the slice. These are the "output" stages of a subtree whose results
// the parent stage should read.
func leafStages(stages []Stage) []string {
	depended := make(map[string]bool, len(stages))
	for _, s := range stages {
		for _, d := range s.Dependencies {
			depended[d] = true
		}
		// Stages referenced via ScalarDependencies (Q15 late-bound scalar
		// producer chain) feed into FilterExprs through coordinator-side
		// substitution, not as record-batch input — but they still
		// SHOULDN'T be treated as plan-level leaves. Without this, the
		// next sort/aggregate's leafStages call picks up the producer's
		// terminal as a dependency and the producer's output gets
		// erroneously routed into the sort's input pipeline.
		for _, pid := range s.ScalarDependencies {
			depended[pid] = true
		}
	}
	var leaves []string
	for _, s := range stages {
		if !depended[s.ID] {
			leaves = append(leaves, s.ID)
		}
	}
	return leaves
}

// mergeFanout is the maximum number of upstream results a single merge task
// should handle. When upstream tasks exceed this, a multi-level merge tree
// is emitted with intermediate merge stages for parallel merging.
const mergeFanout = 16

// estimateUpstreamTasks sums the Tasks field of leaf stages in a subtree.
func estimateUpstreamTasks(childStages []Stage, leafIDs []string) int {
	leafSet := make(map[string]bool, len(leafIDs))
	for _, id := range leafIDs {
		leafSet[id] = true
	}
	total := 0
	for _, s := range childStages {
		if leafSet[s.ID] {
			if s.Tasks > 0 {
				total += s.Tasks
			} else {
				total++
			}
		}
	}
	return total
}

// emitMergeAggregateTree emits a final_aggregate stage, or a two-level merge
// tree when upstream tasks exceed mergeFanout for parallel merging.
func emitMergeAggregateTree(stages *[]Stage, leafIDs []string, groupBy []string,
	groupByTypes map[string]parquet.TypeID, groupByDecimal map[string]logical.DecimalMeta,
	aggSpecs []AggSpec, childStages []Stage) {
	upstream := estimateUpstreamTasks(childStages, leafIDs)
	if upstream <= mergeFanout {
		// Single-level: one final_aggregate merges all results
		finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:             finalStageID,
			Type:           "final_aggregate",
			Tasks:          1,
			GroupByCols:    groupBy,
			GroupByTypes:   groupByTypes,
			GroupByDecimal: groupByDecimal,
			AggSpecs:       aggSpecs,
			Dependencies:   leafIDs,
		})
		return
	}

	// Multi-level: split into groups of mergeFanout
	numGroups := (upstream + mergeFanout - 1) / mergeFanout
	intermIDs := make([]string, numGroups)
	for g := 0; g < numGroups; g++ {
		id := fmt.Sprintf("merge_aggregate-%d-%d", len(*stages), g)
		intermIDs[g] = id
		*stages = append(*stages, Stage{
			ID:              id,
			Type:            "final_aggregate",
			Tasks:           1,
			GroupByCols:     groupBy,
			GroupByTypes:    groupByTypes,
			GroupByDecimal:  groupByDecimal,
			AggSpecs:        aggSpecs,
			Dependencies:    leafIDs,
			MergeGroup:      g,
			MergeGroupCount: numGroups,
		})
	}
	// Final merge of intermediate results
	finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
	*stages = append(*stages, Stage{
		ID:             finalStageID,
		Type:           "final_aggregate",
		Tasks:          1,
		GroupByCols:    groupBy,
		GroupByTypes:   groupByTypes,
		GroupByDecimal: groupByDecimal,
		AggSpecs:       aggSpecs,
		Dependencies:   intermIDs,
	})
}

// emitMergeSortTree emits a merge_sort stage, or a two-level merge tree
// when upstream sort tasks exceed mergeFanout.
func emitMergeSortTree(stages *[]Stage, sortStageID string, sortKeys []SortKeySpec, childStages []Stage) {
	// Estimate upstream sort tasks from child scan tasks
	upstream := 0
	for _, s := range childStages {
		if s.Tasks > 0 {
			upstream += s.Tasks
		} else {
			upstream++
		}
	}
	if upstream <= mergeFanout {
		// Single-level: one merge_sort merges all partial results
		mergeStageID := fmt.Sprintf("merge_sort-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:           mergeStageID,
			Type:         "merge_sort",
			Tasks:        1,
			SortKeys:     sortKeys,
			Dependencies: []string{sortStageID},
		})
		return
	}

	// Multi-level: split into groups of mergeFanout
	numGroups := (upstream + mergeFanout - 1) / mergeFanout
	intermIDs := make([]string, numGroups)
	for g := 0; g < numGroups; g++ {
		id := fmt.Sprintf("merge_sort-%d-%d", len(*stages), g)
		intermIDs[g] = id
		*stages = append(*stages, Stage{
			ID:              id,
			Type:            "merge_sort",
			Tasks:           1,
			SortKeys:        sortKeys,
			Dependencies:    []string{sortStageID},
			MergeGroup:      g,
			MergeGroupCount: numGroups,
		})
	}
	// Final merge of intermediate sorted results
	finalMergeID := fmt.Sprintf("merge_sort-%d", len(*stages))
	*stages = append(*stages, Stage{
		ID:           finalMergeID,
		Type:         "merge_sort",
		Tasks:        1,
		SortKeys:     sortKeys,
		Dependencies: intermIDs,
	})
}
