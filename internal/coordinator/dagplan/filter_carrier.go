// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// AttachedFilterExprs lists the predicates the last PlanDistributed's stage
// emission attached to a stage. Every one of them must still be readable off
// some stage once every rewriting pass has run — the CONSERVATION half of the
// #656 gate (TestStageDAGCarriesEveryFilterAndProjection).
//
// An entry holding a NUL separates the predicate's two spellings: either may
// be the one resolveFilterAliasSpelling settles on.
func (p *StagePlanner) AttachedFilterExprs() []string {
	return append([]string(nil), p.attachedFilterExprs...)
}

// AttachedProjectionOutputs lists the projection OUTPUT names that were on a
// stage the moment the last PlanDistributed finished emitting stages. Every
// one must still be emitted by some stage in the final plan — the projection
// half of the conservation gate, which a pass that deletes a projection's
// carrier breaks exactly as it breaks a predicate's.
func (p *StagePlanner) AttachedProjectionOutputs() []string {
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

// filterCarrierIndex attaches to the last stage or appends StageProject when
// that stage cannot evaluate the predicates or is shared. cteTerminals presence
// means this predicate is above one CTE reference, not inside the body; true
// means multiple references read the terminal, so never attach there (ADR-0025).
// This includes the first reference's real producer, not only deduped aliases.
// Return -1 when no stage exists; the caller treats that as nothing to do.
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
		//
		// #876 tried the stronger rule — never attach to ANY recorded CTE
		// terminal, give the consumer its own StageProject — because the
		// reference COUNT cannot see a CTE named only inside a scalar
		// subquery's TEXT. It was WITHDRAWN: the StageProject that then
		// carries the outer WHERE of `WITH c AS (…) SELECT COUNT(*) FROM c
		// WHERE v < (SELECT MAX(v) FROM c)` answered ZERO on both DAG arms
		// for PostgreSQL's 4838 — Q15's own shape, silently. The residual is
		// in this arc's REPORT with its mechanism; what it costs is a LOUD
		// #656 refusal for two scalar producers that each filter one shared
		// CTE reference, which is what the assert below is for.
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

// joinCarrierBuildQualifier answers, for a JOIN stage — the one carrying a
// filter, or any join below it (joinBuildQualifierFor) — the qualifier that
// names a BUILD-side column on that stage's stream:
// the scan that owns it when the build spans several (BuildColOrigins), the
// build alias otherwise — the two names the worker qualifies the build's
// columns by. Nil for every other carrier, and for a join with no build alias.
//
// logical.ResolveFilterThroughProjects uses it on the definitions it
// substitutes from the join's build arm. Those definitions' bare names were
// bound inside the arm; on the join's stream a bare name binds the PROBE
// first, so `upper(a.s) = b.k` over `(SELECT upper(s) AS k FROM t) b` arrived
// as `upper(a.s) = upper(s)` — the probe compared with itself, every pair
// with a non-NULL probe value admitted (#1678 line j). The outer-join
// residual follows the same rule (residualWithStageSpellings).
//
// A definition's reference that is already QUALIFIED keeps its qualifier only
// when the stream carries it (the build alias, or a scan alias in
// BuildColOrigins); any other qualifier is the arm's own inner name — the
// table's name, `upper(t.s)` over `FROM t` — which the stream does not carry,
// and the worker read it as the bare, probe-first name.
func joinCarrierBuildQualifier(s *Stage) func(table, col string) string {
	switch s.Type {
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
	default:
		return nil
	}
	if s.BuildTableAlias == "" {
		return nil
	}
	alias, origins := s.BuildTableAlias, s.BuildColOrigins
	return func(table, col string) string {
		if table != "" {
			if strings.EqualFold(table, alias) {
				return table
			}
			for _, o := range origins {
				if strings.EqualFold(table, o) {
					return table
				}
			}
		}
		if o := origins[strings.ToLower(col)]; o != "" {
			return o
		}
		return alias
	}
}

// joinBuildQualifierFor answers, for a logical join this pass emitted a join
// stage for, that stage's joinCarrierBuildQualifier; nil for any other node.
// logical.ResolveFilterThroughProjects asks it at EVERY join its walk
// crosses, not only the one whose stage carries the filter: a derived table
// that is a lower join's build reaches the streams above under the name the
// lower join's stream gave its columns, and its definition's bare names bind
// the probe there as surely as on the lower join itself — round 2 qualified
// only the first join and `upper(a.s) <> b.k OR lower(w.w) = lower(b.k)`
// over `a JOIN b … JOIN w` reached the upper stage as
// `upper(a.s) != (upper(s)) or …`, the probe compared with itself
// (#1678 line j, round 3).
func (p *StagePlanner) joinBuildQualifierFor(join *logical.Node) func(table, col string) string {
	return p.joinBuildQualifiers[join]
}

// ErrOuterJoinDerivedFilterDistributed hands the coordinator's single-process
// pipeline a plan in which a filter was re-spelled by
// ResolveFilterThroughProjects into the COMPUTED definition of a derived
// table that an outer join pads — the join carrying the filter, or one below
// it (#1678 line j; paddedArms, substitutesComputedDefinition). Round 3 widened
// it from the carrying join to every outer join the resolution crosses, for
// the reason paddedArms gives; the rest of this comment is the carrying join's
// half of it.
//
// The re-spelled filter reads the right columns. What it cannot survive is
// the stage's EMPTY partitions: a task whose build (or, for RIGHT and FULL,
// probe) partition holds no rows shapes that side from the plan-declared side
// schema, and for a derived side that computes a column that declaration puts
// the computed column FIRST and leaves out the source columns the stream
// carries ([k id] or [k id s] where the stream is [id s k]). Its file then
// names its columns differently from its siblings' and the gather refuses
// the statement, uncoded (`names column 2 "b.id" where an earlier file of the
// same stage input named it "k"`, ADR-0010), whenever the WHERE keeps a
// NULL-padded row; before the build arm's definitions were qualified the
// same statements answered wrong in silence instead. The declared-schema
// disagreement is wider than this filter — a plain LEFT JOIN that selects a
// derived table's computed column reaches it with no filter at all — and is
// recorded on #1678; until it is repaired, this class runs where the join is
// one operator over whole inputs. A RENAME is not in the class: the
// declaration names a renamed column by its source, as the stream does, and
// the stage DAG answers those filters itself. Nor is a derived table on the
// join's PRESERVED side (round 3, paddedArms): only a padded side is ever
// shaped from its declaration.
var ErrOuterJoinDerivedFilterDistributed = errors.New(
	"an outer join's filter over a derived table's computed column is not evaluable on the distributed path")

// substitutesComputedDefinition reports whether one of the output names a
// filter's resolution substituted (ResolveFilterThroughProjects' aliases) is a
// COMPUTED projection of a Project under n — an expression, not a bare
// column or a rename. A name two Projects publish, one computing it, answers
// true: the route is the conservative side.
func substitutesComputedDefinition(n *logical.Node, names []string) bool {
	if n == nil || len(names) == 0 {
		return false
	}
	want := make(map[string]bool, len(names))
	for _, nm := range names {
		want[strings.ToLower(nm)] = true
	}
	var walk func(*logical.Node) bool
	walk = func(cur *logical.Node) bool {
		if cur == nil {
			return false
		}
		if cur.Type == logical.NodeProject {
			for _, pr := range cur.Projections {
				if pr.Alias == "" || !want[strings.ToLower(pr.Alias)] || pr.ASTExpr == nil || pr.IsAgg {
					continue
				}
				if pr.Column == "" && !localPlanFacts.IsSimpleColRefForRename(pr.ASTExpr) {
					return true
				}
			}
		}
		for _, c := range cur.Children {
			if walk(c) {
				return true
			}
		}
		return false
	}
	return walk(n)
}

// paddedArms answers, for the subtree under a filter, the PADDED arms of
// every outer join logical.ResolveFilterThroughProjects' walk can cross: the
// side each join pads with NULLs — the build (Children[1]) of a LEFT join, the
// probe (Children[0]) of a RIGHT join, both arms of a FULL join. A computed
// definition substituted from a padded arm cannot be evaluated by the stage
// DAG wherever the filter rides:
//   - over a padded row the definition is computed from the padding's NULL
//     inputs, where PostgreSQL's derived column is NULL — `b.one IS NULL`
//     over `1 AS one` reads `(1) is null`, false on every padded row — and
//     that holds for a filter carried by an upper INNER join as much as by
//     the outer join itself (R0's join2_outer_coalesce; #1678 line j's
//     ns/lowerLeft*: wrong on the DAG at the base);
//   - a task whose padded partition is empty shapes that side from the
//     declared side schema, and the gather refuses its file (ADR-0010).
//
// A derived table on the PRESERVED side is not in the class: it is never
// padded, and a task whose preserved partition is empty emits no rows (owd/left,
// owd/leftOrNull, ow/right, ns/lowerLeftConstPreservedSelf: right on every DAG
// arm). The walk follows the resolver's: through Project, Distinct, Sort,
// LIMIT and Window, into both arms of a join, stopping anywhere else.
func (p *StagePlanner) paddedArms(child *logical.Node) []*logical.Node {
	var out []*logical.Node
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		for n != nil {
			switch n.Type {
			case logical.NodeJoin:
				if len(n.Children) != 2 {
					return
				}
				switch p.PlanContext.MapJoinType(n.JoinType) {
				case "left":
					out = append(out, n.Children[1])
				case "right":
					out = append(out, n.Children[0])
				case "full":
					out = append(out, n.Children[0], n.Children[1])
				}
				walk(n.Children[0])
				walk(n.Children[1])
				return
			case logical.NodeProject, logical.NodeDistinct, logical.NodeSort, logical.NodeLimit, logical.NodeWindow:
				if len(n.Children) != 1 {
					return
				}
				n = n.Children[0]
			default:
				return
			}
		}
	}
	walk(child)
	return out
}

// substitutesComputedDefinitionInAny is substitutesComputedDefinition over
// several subtrees.
func substitutesComputedDefinitionInAny(arms []*logical.Node, names []string) bool {
	for _, a := range arms {
		if substitutesComputedDefinition(a, names) {
			return true
		}
	}
	return false
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

// producerEmitsName reports whether the stage emitted last — or the
// pass-through chain below it — ships a column of this name.
//
// producerMaterializesName's companion, and the difference matters: that one
// asks whether a projection COMPUTES the name, this one whether the stream
// carries it at all. A sort key that already resolved to a name the producer
// emits must be left alone even when the alias is ALSO materialized, or an
// absorbed projection would silently move Q09's ORDER BY off the group key's
// own spelling.
func producerEmitsName(stages []Stage, name string) bool {
	if len(stages) == 0 || name == "" {
		return false
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	emitted := emittedThroughPassThrough(stages, idx, &stages[len(stages)-1])
	_, ok := lookupEmittedColumn(emitted, name)
	return ok
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
