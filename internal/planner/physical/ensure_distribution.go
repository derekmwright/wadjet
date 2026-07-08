package physical

import (
	"fmt"
	"strings"
)

// requiredChildDistributionForTest lets tests override
// RequiredChildDistribution to exercise error paths in EnsureDistribution.
// Returns (req, true) to supply a value; (_, false) to fall through to
// the production helper.
var requiredChildDistributionForTest func(Stage, int) (RequiredDistribution, bool)

func requiredChildDistribution(s Stage, slot int) RequiredDistribution {
	if requiredChildDistributionForTest != nil {
		if req, ok := requiredChildDistributionForTest(s, slot); ok {
			return req
		}
	}
	return RequiredChildDistribution(s, slot)
}

// EnsureDistribution walks the stage DAG and inserts Exchange stages
// wherever a child's OutputDistribution does not satisfy its parent's
// RequiredChildDistribution. Returns a new []Stage; does not mutate
// the input slice.
func EnsureDistribution(stages []Stage, workerCount int) ([]Stage, error) {
	out := make([]Stage, 0, len(stages)+4)
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = len(out)
		out = append(out, stages[i])
	}

	// Dedup cache: (childID, required-distribution) → existing exchange stage ID.
	// Two parents with the same unmet requirement on the same child should
	// share one inserted exchange (DAG sharing, matches Trino/Spark).
	type cacheKey struct {
		childID string
		kind    RequiredKind
		keys    string
		count   int
	}
	cache := make(map[cacheKey]string)

	// Walk in dependency order. `stages` is assumed topo-sorted by the
	// planner (current invariant — dependencies appear before dependents).
	// For each stage, inspect each input slot; splice an exchange if the
	// satisfaction check fails.
	//
	// Mutations target out[i] via index (not a pointer alias) because
	// append(out, exch) may reallocate the backing array, invalidating any
	// *Stage captured before the append.
	for i := 0; i < len(out); i++ {
		parentSnapshot := out[i]
		slots := dependencySlots(&parentSnapshot)
		for _, slot := range slots {
			childID := slot.get(&parentSnapshot)
			if childID == "" {
				continue
			}
			childIdx, ok := byID[childID]
			if !ok {
				return nil, fmt.Errorf("ensure distribution: parent %q references unknown child %q", out[i].ID, childID)
			}
			req := requiredChildDistribution(parentSnapshot, slot.idx)
			actual := out[childIdx].Distribution
			if actual.Satisfies(req) {
				continue
			}
			key := cacheKey{
				childID: childID,
				kind:    req.Kind,
				keys:    strings.Join(req.Keys, ","),
				count:   req.Count,
			}
			if existingID, hit := cache[key]; hit {
				// Reuse the previously-inserted exchange.
				reparent := out[i]
				slot.set(&reparent, existingID)
				reparent.Dependencies = replaceOne(reparent.Dependencies, childID, existingID)
				out[i] = reparent
				parentSnapshot = reparent
				continue
			}
			exch, ok := exchangeVariantFor(req)
			if !ok {
				return nil, fmt.Errorf(
					"ensure distribution: no exchange variant satisfies %v from %v (parent=%s slot=%d)",
					req, actual, out[i].ID, slot.idx,
				)
			}
			exch.ID = fmt.Sprintf("%s-%s-%d", exch.Type, out[i].ID, i)
			exch.Dependencies = []string{childID}
			exch.Distribution = distributionFromRequired(req, workerCount)
			// Backfill the synthesized Exchange payload's Count to match the
			// resolved Distribution.Count. Without this, OutputDistribution
			// for StageExchangeRepartition reads Exchange.Count (still 0
			// when the consumer's RequiredClusteredOn left it unset) and
			// regresses Distribution.Count to 0 on a second
			// assignStageDistributions pass.
			if exch.Exchange != nil && exch.Exchange.Count == 0 {
				exch.Exchange.Count = exch.Distribution.Count
			}
			byID[exch.ID] = len(out)
			out = append(out, exch)
			cache[key] = exch.ID
			// Apply slot + dependency rewrites to the (possibly re-based)
			// parent by index, not through a stale pointer.
			reparent := out[i]
			slot.set(&reparent, exch.ID)
			reparent.Dependencies = replaceOne(reparent.Dependencies, childID, exch.ID)
			out[i] = reparent
			// Keep parentSnapshot in sync so subsequent slots in this loop
			// see the rewritten edges (prevents double-inserting if two
			// slots pointed at the same mismatched child).
			parentSnapshot = reparent
		}
	}

	// Always emit a terminal Gather. Native-DAG execution (Phase 3) requires
	// every distributed plan to end in Gather so executeStageDAG has a clean
	// termination. Even single-worker Singleton-rooted plans append a trivial
	// Gather so the coordinator receives its output via the same ReplySubject
	// path. Matches Trino's "output fragment" invariant.
	//
	// Picking the gather target: the LAST stage in the slice is wrong when
	// walkStages emitted a Q15-style scalar-subquery producer chain after
	// the main query's stages. The producer's terminal has no regular
	// dependents, so the naïve "last stage" pick attaches Gather to the
	// producer instead of the join — and the user sees the MAX(...) value
	// instead of the joined supplier rows. The right pick is the stage with
	// NO dependents at all (regular OR scalar). When more than one such
	// stage exists (truly multi-rooted plans, rare), the LAST one in slice
	// order is still the conservative choice.
	if len(out) > 0 {
		depended := make(map[string]bool, len(out))
		for _, s := range out {
			for _, d := range s.Dependencies {
				depended[d] = true
			}
			for _, pid := range s.ScalarDependencies {
				depended[pid] = true
			}
		}
		var root Stage
		var found bool
		for i := len(out) - 1; i >= 0; i-- {
			if !depended[out[i].ID] {
				root = out[i]
				found = true
				break
			}
		}
		if !found {
			// All stages have dependents (cycle?) — fall back to last.
			root = out[len(out)-1]
		}
		if root.Type != StageExchangeGather {
			gather := Stage{
				Type:         StageExchangeGather,
				ID:           fmt.Sprintf("%s-%s", StageExchangeGather, root.ID),
				Dependencies: []string{root.ID},
				Distribution: Distribution{Kind: DistSingleton},
			}
			out = append(out, gather)
		}
	}
	return out, nil
}

// dependencySlot describes an input edge of a stage: how to get/set the
// referenced child ID, and which integer slot index to pass to
// RequiredChildDistribution.
type dependencySlot struct {
	idx int
	get func(*Stage) string
	set func(*Stage, string)
}

func dependencySlots(s *Stage) []dependencySlot {
	switch s.Type {
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return []dependencySlot{
			{0, func(s *Stage) string { return s.LeftDepStage }, func(s *Stage, id string) { s.LeftDepStage = id }},
			{1, func(s *Stage) string { return s.RightDepStage }, func(s *Stage, id string) { s.RightDepStage = id }},
		}
	}
	if len(s.Dependencies) == 0 {
		return nil
	}
	return []dependencySlot{{
		idx: 0,
		get: func(s *Stage) string { return s.Dependencies[0] },
		set: func(s *Stage, id string) { s.Dependencies[0] = id },
	}}
}

// distributionFromRequired produces a concrete Distribution satisfying
// the given RequiredDistribution. Used when synthesising an Exchange
// stage's output properties.
func distributionFromRequired(req RequiredDistribution, workerCount int) Distribution {
	switch req.Kind {
	case RequiredBroadcast:
		return Distribution{Kind: DistBroadcast}
	case RequiredSingleton:
		return Distribution{Kind: DistSingleton}
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		n := req.Count
		if n == 0 {
			n = workerCount
		}
		return Distribution{Kind: DistHashPartitioned, Keys: append([]string(nil), req.Keys...), Count: n}
	default:
		return Distribution{Kind: DistSingleton}
	}
}

func replaceOne(xs []string, old, new string) []string {
	out := make([]string, len(xs))
	copy(out, xs)
	for i, v := range out {
		if v == old {
			out[i] = new
			return out
		}
	}
	return out
}

// exchangeVariantFor returns a skeleton Exchange Stage whose Type and
// payload satisfy the given required distribution. It does not set ID,
// Dependencies, ClusterID, Tasks, or Distribution — callers fill those
// in. Returns ok=false when the requirement is RequiredAny (no exchange
// needed).
func exchangeVariantFor(req RequiredDistribution) (Stage, bool) {
	switch req.Kind {
	case RequiredAny:
		return Stage{}, false
	case RequiredBroadcast:
		return Stage{Type: StageExchangeReplicate}, true
	case RequiredSingleton:
		return Stage{Type: StageExchangeGather}, true
	case RequiredHashPartitionedOn, RequiredClusteredOn:
		return Stage{
			Type: StageExchangeRepartition,
			Exchange: &ExchangeStage{
				Keys:  append([]string(nil), req.Keys...),
				Count: req.Count,
			},
		}, true
	default:
		return Stage{}, false
	}
}
