package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Inserting a StageProject above a producer that cannot carry the SELECT list.
//
// attachScanSelectProjections fuses the outer SELECT list into the producing
// fragment wherever that fragment can evaluate it — a scan, a join, or any
// stage that FORWARDS its input's columns. Three producers can do neither:
// an aggregate family stage and a union COLLAPSE their input (their output is
// group keys, aggregates, or the set operation's result columns), and a fused
// scan-aggregate has no OpProject slot at all.
//
// For those the projection has nowhere to fuse, and declining meant nothing
// computed it: `SELECT k * 2 AS d FROM (SELECT DISTINCT id AS k, g AS v FROM
// t) s ORDER BY d` failed loud with `sort: key column "d" does not exist`,
// and without the ORDER BY it silently returned the producer's raw columns.
// A StageProject of its own is the answer the stage type exists for.
//
// It is inserted directly ABOVE the producer rather than below the gather, so
// a sort between the two keys on the projection's outputs — which is what
// `ORDER BY d` needs.

// insertProjectStageAbove puts a StageProject carrying specs between the
// stage at targetIdx and every consumer of it, and returns the new slice.
func insertProjectStageAbove(stages []Stage, targetIdx int, specs []ProjectExprSpec) []Stage {
	target := stages[targetIdx].ID
	id := fmt.Sprintf("project-%d", len(stages))
	for _, s := range stages {
		if s.ID == id {
			id = fmt.Sprintf("project-%s-%d", target, len(stages))
			break
		}
	}
	for i := range stages {
		s := &stages[i]
		if s.ID == target {
			continue
		}
		for j, dep := range s.Dependencies {
			if dep == target {
				s.Dependencies[j] = id
			}
		}
		if s.LeftDepStage == target {
			s.LeftDepStage = id
		}
		if s.RightDepStage == target {
			s.RightDepStage = id
		}
		for j := range s.FusedJoins {
			if s.FusedJoins[j].BuildDepStage == target {
				s.FusedJoins[j].BuildDepStage = id
			}
		}
		for j := range s.UnionArms {
			if s.UnionArms[j].DepStage == target {
				s.UnionArms[j].DepStage = id
			}
		}
	}
	return append(stages, Stage{
		ID:           id,
		Type:         StageProject,
		Tasks:        1,
		Dependencies: []string{target},
		ProjectExprs: specs,
		// assignStageDistributions has already run by the time the SELECT
		// list is attached, so the label is set here to what
		// OutputDistribution would answer for this type.
		Distribution: Distribution{Kind: DistSingleton},
	})
}

// stageCollapsesItsInput reports whether a stage's output is a NEW column set
// rather than its input's — so a SELECT list written over that output cannot
// be evaluated anywhere below it, and cannot be fused into the stage either.
func stageCollapsesItsInput(s *Stage) bool {
	switch s.Type {
	case StageAggregate, StageFinalAggregate, StageMergeAggregate, StageUnion:
		return true
	case StageScan:
		return len(s.FusedAggGroupBy) > 0 || len(s.FusedAggSpecs) > 0
	}
	return false
}

// projectionNeedsItsOwnStage reports whether specs must be materialized above
// the producer rather than fused into it: the producer collapses its input,
// and the projection does more than name what the producer already emits.
func projectionNeedsItsOwnStage(s *Stage, specs []ProjectExprSpec) bool {
	if !stageCollapsesItsInput(s) || len(specs) == 0 {
		return false
	}
	emitted := stageEmittedColumns(s)
	// A sort already FUSED onto this producer runs before anything inserted
	// above it, so a key the projection would have to compute can never
	// exist in time. Decline: assertGatherOutputIsReachable then refuses the
	// plan and the coordinator answers it locally, which is right, rather
	// than shipping a sort on a column nothing emits.
	for _, k := range s.SortKeys {
		if _, ok := lookupEmittedColumn(emitted, k.Column); ok {
			continue
		}
		for _, sp := range specs {
			if strings.EqualFold(sp.Name, k.Column) {
				return false
			}
		}
	}
	// Every spec has to be EVALUABLE over the producer's output, or a stage
	// above it is no better than one fused into it: `SELECT DISTINCT
	// COALESCE(x, 0) AS c1` reads a column the group-by output no longer
	// carries whichever side of the producer it runs on. Decline, and let
	// the reachability refusal route the query local.
	for _, sp := range specs {
		if len(unresolvableColumnRefs(sp.Expr, emitted)) > 0 {
			return false
		}
	}
	for _, sp := range specs {
		if _, ok := lookupEmittedColumn(emitted, sp.Expr); !ok {
			return true
		}
		if !strings.EqualFold(sp.Expr, sp.Name) {
			return true
		}
	}
	return false
}

// aliasedSpecsFor names each spec by the user's ALIAS when one exists — the
// same renaming the join and window paths apply, factored out so the
// collapsing-producer branch above can build the list before deciding.
func aliasedSpecsFor(proj []logical.Projection, specs []ProjectExprSpec) []ProjectExprSpec {
	out := make([]ProjectExprSpec, len(specs))
	for j, sp := range specs {
		out[j] = sp
		if j < len(proj) && proj[j].Alias != "" {
			out[j].Name = strings.ToLower(proj[j].Alias)
		}
	}
	return out
}

// viaSortFilters is the predicate list of the standalone sort a projection
// would sit below, or nil when there is none.
func viaSortFilters(viaSort *Stage) []string {
	if viaSort == nil {
		return nil
	}
	return viaSort.FilterExprs
}

// projectionCoversFilters reports whether every column each predicate names
// survives the projection. An OpProject narrows the batch to its outputs, so
// a predicate evaluated ABOVE it can only see what it emits.
func projectionCoversFilters(specs []ProjectExprSpec, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	emitted := make(map[string]string, len(specs))
	for _, sp := range specs {
		emitted[strings.ToLower(sp.Name)] = sp.Name
	}
	for _, f := range filters {
		if len(unresolvableColumnRefs(f, emitted)) > 0 {
			return false
		}
	}
	return true
}

// sortKeysSurviveWithout reports whether the producer at targetIdx already
// emits every one of these sort keys — so a projection placed ABOVE the sort
// takes nothing the sort needed.
func sortKeysSurviveWithout(stages []Stage, targetIdx int, keys []SortKeySpec) bool {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	// The whole STREAM, not this one stage: a window forwards its input's
	// columns and lists none of its own, so reading its column set alone
	// reported every key as lost and declined placements that were fine.
	emitted := emittedThroughPassThrough(stages, idx, &stages[targetIdx])
	for _, k := range keys {
		if _, ok := lookupEmittedColumn(emitted, k.Column); ok {
			continue
		}
		// The key may still be the DERIVED ALIAS at this point —
		// resolveDerivedAliasSortKeys runs after this pass and will point it
		// at AliasSource. If the stream carries that, the key survives.
		if k.AliasSource != "" {
			if _, ok := lookupEmittedColumn(emitted, k.AliasSource); ok {
				continue
			}
		}
		return false
	}
	return true
}
