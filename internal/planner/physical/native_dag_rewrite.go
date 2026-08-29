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
		case StageWindow:
			// buildWindowFragment reads exactly one input alias and needs a
			// column list to build an operator from; a stage that violates
			// either ships as a task that cannot run. Fail at plan time,
			// where the shape is visible, rather than three dispatch
			// attempts later (#349).
			if len(s.Dependencies) != 1 {
				return fmt.Errorf("native-DAG: window stage %s has %d dependencies, expected 1",
					s.ID, len(s.Dependencies))
			}
			if len(s.WindowCols) == 0 {
				return fmt.Errorf("native-DAG: window stage %s carries no window columns", s.ID)
			}
			// A key spelled __winkey_N is one the FRAGMENT computes, and it
			// computes it from WindowKeyExprs. A stage carrying the name
			// without the expression ships a window that will refuse its own
			// key at the worker — the #349 precedent again: fail where the
			// shape is visible, not three dispatch attempts later (#585).
			if err := validateWindowKeyExprs(s); err != nil {
				return err
			}
		case StageLimit:
			// buildLimitFragment reads exactly one input alias and needs a
			// bound to build an operator from. A stage that violates either
			// ships as a task that cannot run — fail at plan time, where the
			// shape is visible (the #349 precedent).
			if len(s.Dependencies) != 1 {
				return fmt.Errorf("native-DAG: limit stage %s has %d dependencies, expected 1",
					s.ID, len(s.Dependencies))
			}
			if !s.HasLimit && s.Offset <= 0 {
				return fmt.Errorf("native-DAG: limit stage %s carries neither a LIMIT nor an OFFSET", s.ID)
			}
			// A multi-task limit stage is not a bound: each task would keep
			// its own n rows and the union would be up to k*n. TWO fields
			// have to say one task, and neither alone is enough to assert
			// it:
			//
			//   - Distribution.Kind is what the dispatcher reads to pick
			//     numTasks, and a fusion pass CAN overwrite it
			//     (fuse_stage_chains, fuse_join_shuffle both copy a
			//     neighbour's distribution wholesale). But DistSingleton is
			//     the zero value, so this half is silent about a stage whose
			//     distribution was never assigned at all.
			//   - Tasks is what walkStages set when it built the stage, and
			//     its zero value is 0 — so it is the half that catches an
			//     unpopulated stage, and it is what a future fan-out pass
			//     would have to change.
			if s.Distribution.Kind != DistSingleton {
				return fmt.Errorf("native-DAG: limit stage %s is %v, must be Singleton for the bound to be global",
					s.ID, s.Distribution.Kind)
			}
			if s.Tasks != 1 {
				return fmt.Errorf("native-DAG: limit stage %s carries Tasks=%d, must be exactly 1 — "+
					"k tasks each keeping n rows is not the first n rows of their union", s.ID, s.Tasks)
			}
		case StageProject:
			// buildProjectFragment reads exactly one input alias, and a
			// stage carrying neither a projection nor a filter is a full
			// materialization round-trip for nothing — always a planner
			// bug, never a shape. Fail where it is visible (the #349
			// precedent).
			if len(s.Dependencies) != 1 {
				return fmt.Errorf("native-DAG: project stage %s has %d dependencies, expected 1",
					s.ID, len(s.Dependencies))
			}
			if len(s.ProjectExprs) == 0 && len(s.FilterExprs) == 0 {
				return fmt.Errorf("native-DAG: project stage %s carries neither a projection nor a filter", s.ID)
			}
			if s.Distribution.Kind != DistSingleton {
				return fmt.Errorf("native-DAG: project stage %s is %v, must be Singleton",
					s.ID, s.Distribution.Kind)
			}
			if s.Tasks != 1 {
				return fmt.Errorf("native-DAG: project stage %s carries Tasks=%d, must be exactly 1",
					s.ID, s.Tasks)
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
		// The structural half of #656: a predicate or a projection on a
		// stage whose fragment never reads the field is not a slow query or
		// a loud failure — it is the query answered WITHOUT it. Nothing
		// downstream can notice, because no operator ever sees a name it
		// cannot resolve. Refuse the plan here, where the shape is visible.
		if len(s.FilterExprs) > 0 && !stageRunsFilterExprs(s.Type) {
			return fmt.Errorf("native-DAG: stage %s (%s) carries %d filter expression(s) %v "+
				"that its fragment does not evaluate — the predicate would be silently dropped",
				s.ID, s.Type, len(s.FilterExprs), s.FilterExprs)
		}
		if len(s.ProjectExprs) > 0 && !stageAppliesProjection(&s) {
			return fmt.Errorf("native-DAG: stage %s (%s) carries %d projection(s) %v "+
				"that its fragment does not evaluate — the SELECT list would be silently dropped",
				s.ID, s.Type, len(s.ProjectExprs), stageProjectionOutputs(&s))
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
// the predecessor, and the fold is valid only while the predecessor still
// runs as ONE task. Nothing in the plan guarantees that — a Singleton
// broadcast_join is re-fanned-out at dispatch by broadcastJoinProbeSplit,
// which slices the probe files across workers — so each task would sort
// and limit its own slice and the outputs would be concatenated: the
// wrong top-N, not merely the wrong order (#390).
//
// What makes the fold safe in the shape it was written for is that the
// sort has NO DEPENDENTS at this point. EnsureDistribution has not run
// yet, so the terminal gather does not exist; a dependent-free sort is
// the one that will BECOME the gather's input, and dispatchGatherStage
// re-imposes the fused SortKeys/Limit as a merge-sort gather fragment
// (the #288 ordered-gather path). A sort that already has a dependent —
// an ORDER BY + LIMIT inside a derived table or CTE feeding a join or an
// aggregate — reaches a consumer that does no such thing: every other
// consumer of a stage's output (a downstream stage's inputs, an
// exchange-repartition or replicate source, a coordinator-read scalar)
// reads a flat concatenation of the producing tasks' files. So this pass
// refuses to fold into a predecessor whose sort someone else is reading,
// and the standalone single-task sort stage stays in the plan to do the
// global job.
//
// Downstream references to a dropped sort are rewritten to the
// predecessor.
// projectionCoversSortKeys reports whether every sort key names one of the
// projection's outputs. An OpProject narrows the batch to exactly its
// projections, so a key it does not emit cannot be sorted on downstream of it.
func projectionCoversSortKeys(specs []ProjectExprSpec, keys []SortKeySpec) bool {
	for _, k := range keys {
		covered := false
		for _, sp := range specs {
			if strings.EqualFold(sp.Name, k.Column) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func fuseSortIntoPredecessor(stages []Stage, workerCount int) []Stage {
	idIndex := make(map[string]int, len(stages))
	for i := range stages {
		idIndex[stages[i].ID] = i
	}
	hasDependent := stageDependents(stages)

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
		// A sort carrying a WHERE or a SELECT list of its own runs both
		// ABOVE its own ordering (buildSortFragment). Folding the ordering
		// into the predecessor DELETES this stage, and the predecessor
		// applies its own filter BEFORE the fused sort — so the predicate
		// would either vanish or run under the LIMIT it was written above.
		// Keep the stage (#656).
		if len(s.FilterExprs) > 0 || len(s.ProjectExprs) > 0 {
			continue
		}
		// The predecessor's projection runs BEFORE the fused sort and
		// NARROWS the batch to its outputs, so a sort key it does not emit
		// would be gone by the time the sort ran.
		if len(pred.ProjectExprs) > 0 && !projectionCoversSortKeys(pred.ProjectExprs, s.SortKeys) {
			continue
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
		// N×Limit-row merge. A set Limit is load-bearing (including a real
		// LIMIT 0 — #481): it bounds the merge input; ORDER BY without
		// LIMIT keeps today's fuse.
		// Kill switch WADJET_SHARDED_FINALS=0.
		if ShardedSortFinals.Load() && workerCount > 1 &&
			pred.Type == "final_aggregate" && len(pred.GroupByCols) > 0 &&
			s.HasLimit && len(pred.Dependencies) == 1 {
			if depIdx, ok := idIndex[pred.Dependencies[0]]; ok &&
				stages[depIdx].Distribution.Kind != DistSingleton {
				pred.SortKeys = append([]SortKeySpec(nil), s.SortKeys...)
				pred.Limit = s.Limit
				pred.HasLimit = true
				pred.SortShardLocal = true
				continue // sort stage survives as the merge
			}
		}
		if hasDependent[s.ID] {
			// Someone downstream reads this sort's output, and that
			// consumer reads the predecessor's task files concatenated.
			// See the correctness note above (#390). Checked here, AFTER
			// the shard-local branch above rather than before it: that
			// branch never drops this sort stage — it keeps it in place as
			// the global N×Limit-row merge and only annotates the
			// predecessor — so a downstream dependent of THIS stage still
			// reads a fully merged, sorted result either way, and the
			// #390 hazard does not apply to it. Only the fold-into-
			// predecessor path below, which removes this stage and
			// redirects readers to the predecessor's raw concatenated
			// output, needs the dependent check.
			continue
		}
		// Fold sort into predecessor.
		pred.SortKeys = append([]SortKeySpec(nil), s.SortKeys...)
		if s.HasLimit {
			pred.Limit = s.Limit
			pred.HasLimit = true
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

// stageDependents reports, per stage ID, whether any other stage in the
// plan reads its output. Every edge counts: the plain dependency list, a
// join's probe/build slots, a fused or chained join's build, a union arm,
// and a scalar-subquery producer. A stage with no dependent is the plan's
// root — the one EnsureDistribution will attach the terminal gather to.
func stageDependents(stages []Stage) map[string]bool {
	dep := make(map[string]bool, len(stages))
	mark := func(id string) {
		if id != "" {
			dep[id] = true
		}
	}
	for i := range stages {
		s := &stages[i]
		for _, d := range s.Dependencies {
			mark(d)
		}
		mark(s.LeftDepStage)
		mark(s.RightDepStage)
		for _, fj := range s.FusedJoins {
			mark(fj.BuildDepStage)
		}
		for _, cj := range s.ChainedJoins {
			mark(cj.BuildDepStage)
		}
		for _, arm := range s.UnionArms {
			mark(arm.DepStage)
		}
		for _, prod := range s.ScalarDependencies {
			mark(prod)
		}
		// ConsumeDynamicFilters[].SourceStageID is currently unreachable
		// through the walks above: applyDynamicFilters mirrors this same
		// ID into s.Dependencies when it wires the edge (dynamic_filter.go
		// "append the build-scan stage ID to its Dependencies"), and the
		// build-scan stage it names is independently marked anyway via the
		// hash_join stage's own LeftDepStage/RightDepStage — a join cannot
		// run without both sides regardless of any dynamic filter. But
		// applyAttachOnArrival's filterAttachedStatDeps (dynamic_filter_attach.go)
		// strips that mirrored ID back OUT of Dependencies for
		// attach-on-arrival edges (the non-blocking mode), so a future
		// shape where a scan's ONLY other reference is its dynamic-filter
		// consume would silently read as dependent-free — a false "root" —
		// without this. Marked here so stageDependents cannot regress on
		// that shape even though no known plan exercises it today.
		for _, cf := range s.ConsumeDynamicFilters {
			mark(cf.SourceStageID)
		}
	}
	return dep
}

// collapseRedundantFinalMergeSort removes trivial trailing merge_sort
// stages whose job is already done by their upstream Singleton sort.
// The common TPC-H shape after collapseMergeTreesForNativeDAG is:
//
//	sort-N        Singleton   SortKeys=K   deps=[...]
//	merge_sort-M  Singleton   deps=[sort-N]
//	gather        Singleton   deps=[merge_sort-M]
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
		// The merge_sort is where walkStages attached a WHERE that sits
		// ABOVE this ORDER BY (it is the last stage the Sort case emits), so
		// dropping the stage used to drop the predicate with it — the query
		// then answered as if the WHERE were not written (#656 shapes a–d).
		// The merge is a no-op pass-through over an already-sorted single
		// input, so the filter and any projection mean the same thing one
		// stage down: buildSortFragment runs both ABOVE OpSort, and OpSort
		// truncates to its SortLimit in Finalize, so a filter above an
		// `ORDER BY … LIMIT` still sees the limited rows. A sort that
		// already carries either is left alone rather than silently merged
		// into.
		if len(ms.FilterExprs) > 0 || len(ms.ProjectExprs) > 0 {
			if len(stages[depIdx].FilterExprs) > 0 || len(stages[depIdx].ProjectExprs) > 0 {
				continue
			}
			stages[depIdx].FilterExprs = append(stages[depIdx].FilterExprs, ms.FilterExprs...)
			// The second spelling travels with the first, index-aligned:
			// resolveFilterAliasSpelling reads it off the stage that ends up
			// carrying the predicate, not the one that first held it.
			stages[depIdx].FilterAliases = append(stages[depIdx].FilterAliases, ms.FilterAliases...)
			stages[depIdx].ProjectExprs = append(stages[depIdx].ProjectExprs, ms.ProjectExprs...)
			if len(ms.ScalarDependencies) > 0 {
				if stages[depIdx].ScalarDependencies == nil {
					stages[depIdx].ScalarDependencies = make(map[string]string, len(ms.ScalarDependencies))
				}
				for k, v := range ms.ScalarDependencies {
					stages[depIdx].ScalarDependencies[k] = v
				}
			}
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
//
//	intermediate-0   final_aggregate  dep=[leafIDs...] MergeGroup=0
//	intermediate-1   final_aggregate  dep=[leafIDs...] MergeGroup=1
//	...
//	intermediate-N   final_aggregate  dep=[leafIDs...] MergeGroup=N-1
//	final            final_aggregate  dep=[intermediate-0, ..., intermediate-N]
//
// Rewrite to:
//
//	final            final_aggregate  dep=[leafIDs...]    (no MergeGroup)
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
