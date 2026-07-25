package physical

import (
	"os"
	"sync/atomic"
)

// StageFusion gates fuseStageChains. Kill switch WADJET_STAGE_FUSION=0.
// Exported atomic.Bool (AggOverExchange pattern) so tests can pin either arm.
var StageFusion atomic.Bool

// StageFusionAgg gates the join→partial-aggregate absorb (step 2) on top
// of the join→join fusion. Sub-switch WADJET_STAGE_FUSION_AGG=0 isolates
// the two mechanisms for A/B; the master WADJET_STAGE_FUSION=0 kills both.
var StageFusionAgg atomic.Bool

func init() {
	StageFusion.Store(os.Getenv("WADJET_STAGE_FUSION") != "0")
	StageFusionAgg.Store(os.Getenv("WADJET_STAGE_FUSION_AGG") != "0")
}

// fuseStageChains absorbs 1:1 same-distribution downstream joins into their
// upstream hash_join so the pair runs as one worker fragment, eliding the
// per-link materialization (docs/design/stage-chain-fusion.md).
//
// The link P → C fuses when consumer task i of C reads exactly producer
// task i of P's output — i.e. both are HashPartitioned with the same count
// and C consumes P as its probe. The absorbed join becomes a ChainedJoinSpec
// executed after P's primary probe; C's build dep becomes an auxiliary dep
// of P. P keeps its ID, type, keys, and primary build, so dispatch (1:1
// input slicing, skew gate, locality identity) is unchanged from its point
// of view; P adopts C's Distribution (identical kind/count by gate, keys
// kept exact) and estimated output so downstream labels stay valid.
//
// Runs after distributions are final (post EnsureDistribution + elide +
// subsume + agg-over-exchange passes). Iterates to fixpoint so
// join→join→join chains collapse into one fragment.
//
// Conservative eligibility (v1):
//   - P: hash_join, HashPartitioned{N}, no Exchange / SortKeys / Limit /
//     GroupByCols / probe-split / build-cache / merge-tree fields.
//     Existing FusedJoins and ChainedJoins are fine.
//   - C: hash_join or broadcast_join; P's only consumer; consumes P as
//     probe (LeftDepStage) and not also as build; HashPartitioned{N} with
//     P's N; same field restrictions as P. C's FusedJoins ride along as
//     chained broadcast probes (they ran before C's primary, so they stay
//     ordered before it in the chain).
//   - No dep aliasing: C's build deps must not already be deps of P
//     (buildTaskInputsForStage's dep-count arithmetic assumes distinct
//     entries).
func fuseStageChains(stages []Stage) []Stage {
	if !StageFusion.Load() {
		return stages
	}
	for {
		fused, changed := fuseOneChainLink(stages)
		if !changed {
			return fused
		}
		stages = fused
	}
}

// chainProducerEligible reports whether s can absorb a downstream join.
// A stage that already absorbed a partial aggregate is chain-terminal.
func chainProducerEligible(s *Stage) bool {
	return s.Type == StageHashJoin &&
		len(s.ChainedAggSpecs) == 0 && len(s.ChainedAggGroupBy) == 0 &&
		s.Distribution.Kind == DistHashPartitioned && s.Distribution.Count > 0 &&
		s.Exchange == nil &&
		len(s.SortKeys) == 0 && s.Limit == 0 &&
		len(s.GroupByCols) == 0 && len(s.AggSpecs) == 0 && !s.GroupByAll &&
		s.ProbeSplitAlias == "" && len(s.ProbeSplitFiles) == 0 &&
		len(s.BuildCachePreScans) == 0 && len(s.PreComputedAggregates) == 0 &&
		s.MergeGroupCount == 0 &&
		len(s.ProjectExprs) == 0 && len(s.SecurityProjectExprs) == 0 &&
		len(s.OutputRenames) == 0 &&
		len(s.EmitDynamicFilters) == 0 && len(s.ConsumeDynamicFilters) == 0 &&
		// Scalar placeholders are substituted into stage-level FilterExprs
		// at dispatch; a chained spec's filters would be missed.
		len(s.ScalarDependencies) == 0
}

// chainConsumerEligible reports whether c can be absorbed into producer p.
func chainConsumerEligible(c, p *Stage) bool {
	if c.Type != StageHashJoin && c.Type != StageBroadcastJoin {
		return false
	}
	if c.LeftDepStage != p.ID || c.RightDepStage == p.ID || c.RightDepStage == "" {
		return false
	}
	if c.Distribution.Kind != DistHashPartitioned ||
		c.Distribution.Count != p.Distribution.Count {
		return false
	}
	// A consumer that already absorbed downstream links (ChainedJoins) is
	// handled by carrying its chain along; one that absorbed a partial
	// aggregate is chain-terminal and cannot itself be absorbed (the
	// joins-before-aggs pass order makes this unreachable; gate anyway).
	if len(c.ChainedAggSpecs) > 0 || len(c.ChainedAggGroupBy) > 0 {
		return false
	}
	// Every other producer-side restriction applies to the consumer too.
	return c.Exchange == nil &&
		len(c.SortKeys) == 0 && c.Limit == 0 &&
		len(c.GroupByCols) == 0 && len(c.AggSpecs) == 0 && !c.GroupByAll &&
		c.ProbeSplitAlias == "" && len(c.ProbeSplitFiles) == 0 &&
		len(c.BuildCachePreScans) == 0 && len(c.PreComputedAggregates) == 0 &&
		c.MergeGroupCount == 0 &&
		len(c.ProjectExprs) == 0 && len(c.SecurityProjectExprs) == 0 &&
		len(c.OutputRenames) == 0 &&
		len(c.EmitDynamicFilters) == 0 && len(c.ConsumeDynamicFilters) == 0 &&
		len(c.ScalarDependencies) == 0
}

// fuseOneChainLink finds the first fusable P→C link, rewrites it, and
// returns the updated slice. changed=false means fixpoint reached.
func fuseOneChainLink(stages []Stage) ([]Stage, bool) {
	idIndex := make(map[string]int, len(stages))
	consumers := make(map[string][]int)
	for i := range stages {
		idIndex[stages[i].ID] = i
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], i)
		}
	}
	for pi := range stages {
		p := &stages[pi]
		if !chainProducerEligible(p) {
			continue
		}
		cons := consumers[p.ID]
		if len(cons) != 1 {
			continue
		}
		c := &stages[cons[0]]
		if !chainConsumerEligible(c, p) {
			continue
		}
		// No dep aliasing between C's build-side deps and P's existing deps.
		pDeps := make(map[string]bool, len(p.Dependencies))
		for _, d := range p.Dependencies {
			pDeps[d] = true
		}
		cBuildDeps := []string{}
		for _, fj := range c.FusedJoins {
			cBuildDeps = append(cBuildDeps, fj.BuildDepStage)
		}
		cBuildDeps = append(cBuildDeps, c.RightDepStage)
		for _, cj := range c.ChainedJoins {
			cBuildDeps = append(cBuildDeps, cj.BuildDepStage)
		}
		aliased := false
		seen := make(map[string]bool, len(cBuildDeps))
		for _, d := range cBuildDeps {
			if d == "" || pDeps[d] || seen[d] {
				aliased = true
				break
			}
			seen[d] = true
		}
		if aliased {
			continue
		}

		// Absorb C into P. Chain order preserves C's runtime semantics:
		// C's FusedJoins ran before its primary, its own ChainedJoins after.
		for _, fj := range c.FusedJoins {
			p.ChainedJoins = append(p.ChainedJoins, ChainedJoinSpec{
				JoinType:        fj.JoinType,
				JoinLeftKeys:    fj.JoinLeftKeys,
				JoinRightKeys:   fj.JoinRightKeys,
				BuildDepStage:   fj.BuildDepStage,
				BuildTableAlias: fj.BuildTableAlias,
				BuildColOrigins: fj.BuildColOrigins,
				JoinFilter:      fj.JoinFilter,
				FilterExprs:     fj.FilterExprs,
			})
		}
		p.ChainedJoins = append(p.ChainedJoins, ChainedJoinSpec{
			JoinType:            c.JoinType,
			JoinLeftKeys:        c.JoinLeftKeys,
			JoinRightKeys:       c.JoinRightKeys,
			BuildDepStage:       c.RightDepStage,
			BuildTableAlias:     c.BuildTableAlias,
			BuildColOrigins:     c.BuildColOrigins,
			JoinFilter:          c.JoinFilter,
			FilterExprs:         c.FilterExprs,
			BuildFilterExprs:    c.BuildFilterExprs,
			QualifyAllBuildCols: c.QualifyAllBuildCols,
			Columns:             c.Columns,
			Partitioned:         c.Type == StageHashJoin,
		})
		p.ChainedJoins = append(p.ChainedJoins, c.ChainedJoins...)
		p.Dependencies = append(p.Dependencies, cBuildDeps...)
		p.Distribution = c.Distribution
		p.EstimatedBytes += c.EstimatedBytes
		p.EstimatedRows = c.EstimatedRows

		// Rewire every downstream reference to C onto P and drop C.
		return rewireDropped(stages, c.ID, p.ID), true
	}

	// Step 2: absorb a terminal PARTIAL aggregate. Runs only when no
	// join→join link fused this iteration, so multi-join chains collapse
	// fully before the agg attaches (a stage with a chained agg is
	// terminal). Partial aggregation is partition-agnostic — no 1:1 or
	// same-distribution requirement; the fused stage keeps its own
	// Distribution/task count and emits per-task partials that the final
	// merges exactly as it merged the dropped stage's.
	if StageFusionAgg.Load() {
		for pi := range stages {
			p := &stages[pi]
			if !chainProducerEligible(p) {
				continue
			}
			cons := consumers[p.ID]
			if len(cons) != 1 {
				continue
			}
			c := &stages[cons[0]]
			if !chainAggConsumerEligible(c, p) {
				continue
			}
			p.ChainedAggGroupBy = append([]string(nil), c.GroupByCols...)
			p.ChainedAggSpecs = append([]AggSpec(nil), c.AggSpecs...)
			// Keep p.Distribution: the dropped stage's RoundRobin label
			// only described its fan-out task count; adopting it would
			// collapse the join's 1:1 task/partition mapping at dispatch.
			p.EstimatedRows = c.EstimatedRows
			return rewireDropped(stages, c.ID, p.ID), true
		}
	}
	return stages, false
}

// chainAggConsumerEligible reports whether partial-aggregate stage c can be
// absorbed as producer p's chain-terminal aggregate.
func chainAggConsumerEligible(c, p *Stage) bool {
	if c.Type != "aggregate" || len(c.Dependencies) != 1 || c.Dependencies[0] != p.ID {
		return false
	}
	if len(c.GroupByCols) == 0 && len(c.AggSpecs) == 0 {
		return false
	}
	// GroupByAll has no distributed fragment equivalent; merge-tree
	// members, exchanges, sorts, filters, and scan-side fused-agg fields
	// never belong to the absorbable shape.
	return !c.GroupByAll &&
		c.Exchange == nil &&
		len(c.SortKeys) == 0 && c.Limit == 0 &&
		len(c.FilterExprs) == 0 &&
		len(c.FusedAggGroupBy) == 0 && len(c.FusedAggSpecs) == 0 &&
		!c.RawInputAggregate &&
		c.MergeGroupCount == 0 &&
		len(c.ProjectExprs) == 0 && len(c.SecurityProjectExprs) == 0 &&
		len(c.OutputRenames) == 0 &&
		len(c.EmitDynamicFilters) == 0 && len(c.ConsumeDynamicFilters) == 0 &&
		len(c.ScalarDependencies) == 0 &&
		len(c.ChainedJoins) == 0 &&
		len(c.ChainedAggSpecs) == 0 && len(c.ChainedAggGroupBy) == 0
}

// rewireDropped removes stage dropID from the slice, rewiring every
// reference (Dependencies, Left/RightDepStage, fused/chained build deps)
// to newID.
func rewireDropped(stages []Stage, dropID, newID string) []Stage {
	out := make([]Stage, 0, len(stages)-1)
	for i := range stages {
		if stages[i].ID == dropID {
			continue
		}
		s := stages[i]
		for k, d := range s.Dependencies {
			if d == dropID {
				s.Dependencies[k] = newID
			}
		}
		if s.LeftDepStage == dropID {
			s.LeftDepStage = newID
		}
		if s.RightDepStage == dropID {
			s.RightDepStage = newID
		}
		for k := range s.FusedJoins {
			if s.FusedJoins[k].BuildDepStage == dropID {
				s.FusedJoins[k].BuildDepStage = newID
			}
		}
		for k := range s.ChainedJoins {
			if s.ChainedJoins[k].BuildDepStage == dropID {
				s.ChainedJoins[k].BuildDepStage = newID
			}
		}
		out = append(out, s)
	}
	return out
}
