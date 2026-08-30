package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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

// specsResolveAgainstStageInput reports whether every spec's expression can be
// evaluated by the stage at targetIdx — the same question
// assertCarrierSchemaResolves asks of a finished plan, asked BEFORE the attach
// so the pass can decline instead of building a plan its own validator
// rejects.
//
// `SELECT k, s FROM (SELECT id AS k, SUM(c_i64) OVER () + 1 AS s FROM t
// WHERE id < 3) x WHERE k > 0 ORDER BY k` is the shape: the outer list is two
// bare forwards, one of them a DERIVED alias whose definition lives in the
// window's own output (`__win_0`). Attaching `s AS s` to the sort above the
// window gives it a projection naming a column nothing in its input carries,
// and the column would come back NULL. Declining leaves the gather's
// OutputRename to compute it, which is what the same query without the `id AS
// k` rename already does correctly.
//
// Unmodelled inputs (joins, unions, exchanges) answer true: a check that
// cannot see the schema must not veto the attach.
func specsResolveAgainstStageInput(stages []Stage, targetIdx int, specs []ProjectExprSpec) bool {
	if targetIdx < 0 || targetIdx >= len(stages) {
		return true
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	emitted, modelled := carrierInputColumns(stages, idx, targetIdx)
	if !modelled {
		return true
	}
	return specsResolveAgainst(emitted, specs)
}

// specsResolveAgainstStageOutput is the same question for a projection that
// will sit in a stage of its OWN directly above the producer: its input is
// what the producer emits, not what the producer reads.
func specsResolveAgainstStageOutput(stages []Stage, producerIdx int, specs []ProjectExprSpec) bool {
	if producerIdx < 0 || producerIdx >= len(stages) {
		return true
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	emitted := emittedThroughPassThrough(stages, idx, &stages[producerIdx])
	if len(emitted) == 0 {
		return true
	}
	return specsResolveAgainst(emitted, specs)
}

func specsResolveAgainst(emitted map[string]string, specs []ProjectExprSpec) bool {
	for _, sp := range specs {
		if sp.Expr == "" {
			continue
		}
		if len(unresolvableColumnRefs(sp.Expr, emitted)) > 0 {
			return false
		}
	}
	return true
}

// respellSpecsOverProducerOutput rewrites every spec so it is written against
// the columns the producer at producerIdx EMITS, and reports false when one of
// them cannot be.
//
// A projection in a stage of its own reads the producer's OUTPUT, and an
// aggregate names its group key by the TEXT of the GROUP BY expression. A spec
// carrying the query's own `n_regionkey + 1` is then arithmetic over a column
// the aggregate does not emit — `SELECT n_regionkey + 1 AS gk FROM nation
// GROUP BY n_regionkey + 1 ORDER BY gk` answered NULL for every row. The same
// requote absorbAggregateOutputProjection performs on the stage itself is what
// makes the term name the column instead.
func respellSpecsOverProducerOutput(stages []Stage, producerIdx int, specs []ProjectExprSpec) ([]ProjectExprSpec, bool) {
	if producerIdx < 0 || producerIdx >= len(stages) {
		return specs, true
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	emitted := emittedThroughPassThrough(stages, idx, &stages[producerIdx])
	if len(emitted) == 0 {
		return specs, true
	}
	out := make([]ProjectExprSpec, len(specs))
	for i, sp := range specs {
		out[i] = sp
		if sp.Expr == "" {
			continue
		}
		ast, err := plansql.ParseExpression(sp.Expr)
		if err != nil {
			return nil, false
		}
		if re, ok := requoteAggOutputRefs(ast, emitted); ok {
			out[i].Expr = re.String()
			continue
		}
		// requoteAggOutputRefs declines a node kind it does not rebuild — a
		// function call, a CASE — rather than guessing. That is the right
		// policy for the absorb, and too strict here: `UPPER(CAST(k AS
		// VARCHAR)) AS d` over an aggregate needs no respelling at all, and
		// declining it costs the whole shape its distributed plan.
		//
		// Keep the spec exactly as written when every bare reference in it
		// already resolves against the producer's output, and decline only
		// when one does not — that is the case where leaving it alone would
		// answer NULL for the column.
		for _, ref := range collectColRefs(ast) {
			if !columnResolves(ref, emitted) {
				return nil, false
			}
		}
	}
	return out, true
}

// orderingSurvivesAProjectStage reports whether inserting a StageProject
// between the producer at producerIdx and its consumers keeps the producer's
// ORDERING visible to them.
//
// A stage's ordering is read off the DIRECT dependency: the coordinator asks
// what its gather's dependency is and whether that stage is ordered, and the
// worker's merge does the same one level down. A projection inserted between
// the two hides it — the new stage is a `project`, it declares no SortKeys,
// and a Tasks=1 fragment concatenating several ordered input files could not
// truthfully declare any, because concatenation is not a merge.
//
// So a producer that carries its own fused ordering keeps its consumers. The
// symptom otherwise is the sharpest kind of silent: the right rows in the
// wrong sequence. `SELECT a.s_suppkey AS lo, b.s_suppkey AS hi FROM supplier
// a JOIN supplier b ON … ORDER BY lo, hi` came back as a correct 9-row
// multiset with the ORDER BY ignored, because the projection renaming
// `a.s_suppkey` to `lo` moved in between the join's fused sort and the gather.
//
// A producer with NO ordering of its own has nothing to lose, and that is the
// case the insertion exists for: an aggregate, a union or a DISTINCT that
// collapses its input and cannot evaluate the SELECT list itself.
func orderingSurvivesAProjectStage(stages []Stage, producerIdx int, specs []ProjectExprSpec) bool {
	if producerIdx < 0 || producerIdx >= len(stages) {
		return true
	}
	s := &stages[producerIdx]
	if len(s.SortKeys) == 0 {
		return true // nothing to lose
	}
	// The ordering can ride ONTO the inserted stage, but only if both halves
	// hold. The keys must still be named the same above the projection —
	// otherwise nothing downstream can say what the stream is ordered BY —
	// and the producer must emit a single ordered stream, because the
	// inserted fragment CONCATENATES its inputs and concatenation of two
	// ordered files is not ordered. A union (Tasks = len(arms)) and a
	// probe-split join fail the second half.
	if s.Tasks > 1 {
		return false
	}
	return projectionCoversSortKeys(specs, s.SortKeys)
}

// carryOrderingOntoProjectStage copies a producer's ordering onto the stage
// inserted above it, so the consumer that reads the ordering off its direct
// dependency still finds one.
//
// Only called where orderingSurvivesAProjectStage said yes, which is what
// makes the declaration true rather than hopeful.
func carryOrderingOntoProjectStage(stages []Stage, insertedIdx int, keys []SortKeySpec) {
	if insertedIdx < 0 || insertedIdx >= len(stages) || len(keys) == 0 {
		return
	}
	stages[insertedIdx].SortKeys = append([]SortKeySpec(nil), keys...)
}
