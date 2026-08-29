package physical

import (
	"fmt"
	"strings"
)

// Where a Filter and a Project land on the stage DAG, and what happens when
// nothing there can run them.
//
// walkStages lowers a logical Filter by appending its predicate text to the
// stage it has just emitted, and a logical Project by emitting nothing at all
// — a Project is a pass-through on the DAG, and the gather's OutputRenames
// recover the SELECT list at the end. Both shortcuts hold only while the
// stage underneath is one that RUNS what it is handed. When it is not, the
// predicate or the projection is attached to a stage that ignores the field,
// or to a stage a later pass deletes, and the query answers WITHOUT it —
// silently, because no operator ever sees a name it cannot resolve (#656).
//
// Three things close that:
//
//   - stageEvaluatesFilter names, per stage type, whether the coordinator's
//     fragment builder emits an OpFilter for Stage.FilterExprs — and, for the
//     types whose projection runs ABOVE that filter, whether a projection is
//     already attached. It is the planner-side mirror of the fragment
//     builders, the way projectableProducer is for Stage.ProjectExprs.
//
//   - filterCarrierIndex gives a predicate a stage that will run it: the last
//     emitted stage when that stage qualifies, and otherwise a StageProject
//     of its own, inserted above it. Nothing is ever attached to a stage that
//     will not evaluate it.
//
//   - resolveFilterAliasSpelling settles WHICH of the predicate's two
//     spellings the carrying stage can evaluate, once every pass that can put
//     an alias-naming projection on a fragment has run.
//
// Two gates hold it, and between them they cover the class rather than the
// seven shapes. ValidateNativeDAGShape refuses, on every distributed query, a
// plan that populates either field on a stage whose fragment ignores it — the
// PLACEMENT half. TestStageDAGCarriesEveryFilterAndProjection asserts that
// every predicate stage emission attached is still readable off some stage
// after every rewriting pass — the CONSERVATION half, which is what a
// carrier-deleting pass breaks.

// AttachedFilterExprs lists the predicates the last PlanDistributed's stage
// emission attached to a stage. Every one of them must still be readable off
// some stage once every rewriting pass has run — the CONSERVATION half of the
// #656 gate (TestStageDAGCarriesEveryFilterAndProjection).
//
// An entry holding a NUL separates the predicate's two spellings: either may
// be the one resolveFilterAliasSpelling settles on.
func (p *Planner) AttachedFilterExprs() []string {
	return append([]string(nil), p.attachedFilterExprs...)
}

// AttachedProjectionOutputs lists the projection OUTPUT names that were on a
// stage the moment the last PlanDistributed finished emitting stages. Every
// one must still be emitted by some stage in the final plan — the projection
// half of the conservation gate, which a pass that deletes a projection's
// carrier breaks exactly as it breaks a predicate's.
func (p *Planner) AttachedProjectionOutputs() []string {
	return append([]string(nil), p.attachedProjectionOutputs...)
}

// FilterAliasSpec is the alternate, query-written spelling of one predicate.
// See Stage.FilterAliases.
type FilterAliasSpec struct {
	// Expr is the predicate as the query wrote it — naming the OUTPUT
	// columns of the Projects between the Filter and its producer. Empty
	// when the resolved spelling is the only one.
	Expr string
	// Names are those Project outputs, lowercased: the names that have to be
	// on the producing fragment for Expr to be the evaluable spelling.
	Names []string
}

// stageRunsFilterExprs reports whether the coordinator's fragment builder for
// this stage type emits an OpFilter for Stage.FilterExprs at all.
//
// It is stageEvaluatesFilter without the ordering question: this one answers
// "does the field get read", which is what a validator needs, while that one
// answers "may ANOTHER predicate be appended here", which is what stage
// emission needs. A type missing from this list ignores the field — the
// silent half of #656, and the reason ValidateNativeDAGShape refuses a plan
// that populates it.
func stageRunsFilterExprs(typ string) bool {
	switch typ {
	case StageScan, StageHashJoin, StageBroadcastJoin, StageSortMergeJoin,
		StageAggregate, StageFinalAggregate, StageMergeAggregate,
		StageSort, StageMergeSort, StageWindow, StageLimit, StageUnion,
		StageProject:
		return true
	}
	return false
}

// stageEvaluatesFilter reports whether a fragment built for s runs
// Stage.FilterExprs against the rows a Filter sitting directly above s would
// see.
//
// The per-type answers are read off the coordinator's fragment builders
// (execute_stage_dag.go); a type missing from the switch is one whose builder
// emits no OpFilter at all, and answering false for it is what routes the
// predicate to a stage of its own instead of dropping it.
//
// The ProjectExprs guard is the ordering half. Sort, window, limit and the
// aggregate family run their projection ABOVE their filter slot — that is the
// order a Project above them needs — so a predicate that must run above THAT
// projection cannot share the slot. It gets its own StageProject.
func stageEvaluatesFilter(s *Stage) bool {
	switch s.Type {
	case StageScan:
		// dispatchScanFilterStage and buildScanAggregateFragment both apply
		// FilterExprs, and both apply them BELOW ProjectExprs — which is the
		// right order for a predicate written against the scan's own
		// columns, the only kind that reaches a scan stage.
		return true
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return true
	case StageProject, StageAggregate, StageFinalAggregate, StageMergeAggregate,
		StageSort, StageMergeSort, StageWindow, StageLimit, StageUnion:
		// All of these run their projection ABOVE their filter slot — the
		// order a Project above them needs — so a predicate that must run
		// above THAT projection cannot share the slot. It gets its own
		// StageProject.
		return len(s.ProjectExprs) == 0
	}
	return false
}

// projectionRunsAfterStageOperator reports whether a stage's ProjectExprs are
// applied AFTER its own operator, so a sort key the stage itself orders by
// does not have to survive the projection.
//
// The difference decides where a SELECT list may be attached: a scan or a
// join projects BEFORE its fused ordering, so that ordering's keys must be
// among the projection's outputs; a sort, window, limit or project stage
// projects after, so they need not be.
func projectionRunsAfterStageOperator(typ string) bool {
	switch typ {
	case StageSort, StageMergeSort, StageWindow, StageLimit, StageProject:
		return true
	}
	return false
}

// filterCarrierIndex returns the index of the stage a Filter's predicates
// should be attached to, appending a StageProject above the last emitted
// stage when that stage would not run them, or when it is SHARED.
//
// cteTerminals maps a CTE body's terminal stage ID to whether that CTE is
// referenced more than once. Presence means the predicate sits ABOVE a CTE
// reference rather than inside its body, so it belongs to one consumer; a
// true value means every reference reads that stage, and the predicate must
// not land on it at all.
//
// The FIRST reference walked emits the body's real producer, so
// `filterIdx = len(*stages)-1` put its WHERE on the very stage the OTHER
// references read, and every one of them saw a filtered stream:
// `WITH c AS (…) SELECT … FROM (SELECT id FROM c WHERE v>x UNION ALL
// SELECT id FROM c)` answered 18 rows where PostgreSQL answers 109, and 27
// where three references answer 119. It is the mirror of the deduped-alias
// case ADR-0025 already names — closed there, open here — and silent the
// same way.
//
// Returns -1 when there is no stage to attach to at all, which the caller
// treats as "nothing to do" exactly as it did before.
func filterCarrierIndex(stages *[]Stage, cteTerminals map[string]bool) int {
	last := len(*stages) - 1
	if last < 0 {
		return -1
	}
	shared, isCTETerminal := cteTerminals[(*stages)[last].ID]
	if !shared && stageEvaluatesFilter(&(*stages)[last]) {
		// A single-reference CTE body's terminal may carry it, but it is
		// marked: a later pass that gives the stage a second consumer turns
		// this into the shared case, and assertNoConsumerScopedFilterOn-
		// SharedStage is what notices.
		(*stages)[last].ConsumerScoped = (*stages)[last].ConsumerScoped || isCTETerminal
		return last
	}
	depID := (*stages)[last].ID
	*stages = append(*stages, Stage{
		ID:           fmt.Sprintf("project-%d", len(*stages)),
		Type:         StageProject,
		Tasks:        1,
		Dependencies: []string{depID},
	})
	return len(*stages) - 1
}

// stageAppliesProjection reports whether a fragment built for s emits an
// OpProject for Stage.ProjectExprs, positioned ABOVE the stage's own
// operator — i.e. whether the stage can carry a Project that sits above it.
//
// It is deliberately NOT projectableProducer: that one answers a different
// question (can a computed sort key be materialized INTO this fragment, below
// its ordering), and its list is the fragments whose OpProject runs before
// the sort. Sharing one answer between the two is what left a window stage
// unable to carry either (#490's finding, one layer up).
func stageAppliesProjection(s *Stage) bool {
	switch s.Type {
	case StageScan:
		// A fused scan-aggregate routes to buildScanAggregateFragment, which
		// has no OpProject slot at all.
		return len(s.FusedAggGroupBy) == 0 && len(s.FusedAggSpecs) == 0
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin,
		StageSort, StageMergeSort, StageWindow, StageLimit,
		StageAggregate, StageFinalAggregate, StageMergeAggregate,
		StageProject:
		return true
	}
	return false
}

// resolveFilterAliasSpelling picks, for every predicate that reached a stage
// with two spellings, the one the stage's input stream can evaluate.
//
// The alias spelling wins when the producing fragment MATERIALIZES the names
// the resolved spelling substituted away — attachScanSelectProjections'
// alias-naming OpProject, which runs after walkStages and is the only thing
// that can make a derived table's alias real on the DAG. Everywhere else the
// stream carries the source columns and the resolved spelling is the one that
// resolves. The test is whether the projection COMPUTES the name, not whether
// a column of that name exists: a shadowing alias (`c_i64 AS id`) means the
// producer emits a column spelled like the alias that is the wrong one — the
// same distinction resolveDerivedAliasSortKeys draws for a sort key (#316).
//
// Runs after attachScanSelectProjections, resolveHiddenSortKeys and
// resolveDerivedAliasSortKeys, for their reason: the repair depends on what
// those passes did.
func resolveFilterAliasSpelling(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if len(s.FilterAliases) == 0 {
			continue
		}
		producer := filterInputStage(stages, idx, i)
		if producer == nil {
			continue
		}
		for k := range s.FilterAliases {
			alias := s.FilterAliases[k]
			if alias.Expr == "" || k >= len(s.FilterExprs) {
				continue
			}
			if !allMaterializedThroughPassThrough(stages, idx, producer, alias.Names) {
				continue
			}
			s.FilterExprs[k] = alias.Expr
		}
	}
}

// filterInputStage returns the stage whose output columns the filter on the
// stage at i is evaluated against.
//
// A scan or a join filters its OWN rows, so it is its own producer. Every
// other carrier applies the predicate above an operator that forwards its
// input's columns (sort, limit, project) or appends to them (window,
// aggregate), so the columns come from its single dependency.
//
// A StageProject carrying a projection of its OWN would be the exception —
// its filter runs above that projection, so the stream is the projection's
// outputs — but filterCarrierIndex only ever emits the stage empty, and
// nothing else populates it.
func filterInputStage(stages []Stage, idx map[string]int, i int) *Stage {
	s := &stages[i]
	switch s.Type {
	case StageSort, StageMergeSort, StageLimit, StageWindow, StageProject:
		if len(s.Dependencies) != 1 {
			return nil
		}
		depIdx, ok := idx[s.Dependencies[0]]
		if !ok {
			return nil
		}
		return &stages[depIdx]
	}
	return s
}

// allMaterializedThroughPassThrough reports whether every name is computed
// under that exact spelling by the producer's fragment, following a
// pass-through stage down to the fragment that forwards its columns.
func allMaterializedThroughPassThrough(stages []Stage, idx map[string]int, s *Stage, names []string) bool {
	if len(names) == 0 {
		return false
	}
	for _, n := range names {
		if !materializedThroughPassThrough(stages, idx, s, n) {
			return false
		}
	}
	return true
}

// producerMaterializesName reports whether the stage emitted last — or the
// pass-through chain below it — COMPUTES a column under this exact name.
//
// It is how a consumer emitted after a projection was absorbed learns that
// the alias is real: `ORDER BY gk` over a CTE whose `g + 1 AS gk` is now the
// aggregate stage's ProjectExprs must key on `gk`, not on the group-key
// expression text the resolver would otherwise chase it to — that column no
// longer reaches the sort.
func producerMaterializesName(stages []Stage, name string) bool {
	if len(stages) == 0 || name == "" {
		return false
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	return materializedThroughPassThrough(stages, idx, &stages[len(stages)-1], name)
}

// pruneEmptyProjectStages drops StageProject stages that ended up carrying
// neither a projection nor a filter, rewiring their dependents onto their own
// dependency. filterCarrierIndex reserves the stage before it knows the
// predicate has evaluable text; a reservation nothing landed on would
// otherwise cost a full write-and-read round-trip for an identity.
func pruneEmptyProjectStages(stages []Stage) []Stage {
	drop := map[string]string{}
	for _, s := range stages {
		if s.Type == StageProject && len(s.ProjectExprs) == 0 && len(s.FilterExprs) == 0 &&
			len(s.Dependencies) == 1 {
			drop[s.ID] = s.Dependencies[0]
		}
	}
	if len(drop) == 0 {
		return stages
	}
	resolve := func(id string) string {
		for i := 0; i <= len(drop); i++ {
			next, ok := drop[id]
			if !ok {
				return id
			}
			id = next
		}
		return id
	}
	out := make([]Stage, 0, len(stages)-len(drop))
	for _, s := range stages {
		if _, gone := drop[s.ID]; gone {
			continue
		}
		for j, dep := range s.Dependencies {
			s.Dependencies[j] = resolve(dep)
		}
		if _, ok := drop[s.LeftDepStage]; ok {
			s.LeftDepStage = resolve(s.LeftDepStage)
		}
		if _, ok := drop[s.RightDepStage]; ok {
			s.RightDepStage = resolve(s.RightDepStage)
		}
		for j, fj := range s.FusedJoins {
			if _, ok := drop[fj.BuildDepStage]; ok {
				s.FusedJoins[j].BuildDepStage = resolve(fj.BuildDepStage)
			}
		}
		for ph, prod := range s.ScalarDependencies {
			if _, ok := drop[prod]; ok {
				s.ScalarDependencies[ph] = resolve(prod)
			}
		}
		for j, arm := range s.UnionArms {
			if _, ok := drop[arm.DepStage]; ok {
				s.UnionArms[j].DepStage = resolve(arm.DepStage)
			}
		}
		out = append(out, s)
	}
	return out
}

// stageProjectionOutputs lists the names a stage's ProjectExprs emit.
func stageProjectionOutputs(s *Stage) []string {
	out := make([]string, 0, len(s.ProjectExprs))
	for _, p := range s.ProjectExprs {
		out = append(out, strings.ToLower(p.Name))
	}
	return out
}
