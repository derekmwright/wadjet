// This file holds join fusion for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

// buildSideBytes returns the EstimatedBytes of the build-side scan reachable
// from a broadcast_join's RightDepStage. Walks one level (the typical shape:
// join → exchange-replicate → scan) and accepts the direct case
// (join → scan). Returns 0 when the build subtree shape is unknown — callers
// should treat 0 as "no info; allow fusion" so fuseJoinStages doesn't
// over-restrict on edges its heuristic doesn't model.
func buildSideBytes(stages []Stage, joinStage *Stage) int64 {
	if joinStage == nil {
		return 0
	}
	buildDep := joinStage.RightDepStage
	if buildDep == "" {
		return 0
	}
	for _, s := range stages {
		if s.ID != buildDep {
			continue
		}
		if s.Type == "scan" {
			return s.EstimatedBytes
		}
		// Walk through one level of exchange-replicate to its underlying scan.
		if s.Type == StageExchangeReplicate && len(s.Dependencies) == 1 {
			for _, t := range stages {
				if t.ID == s.Dependencies[0] && t.Type == "scan" {
					return t.EstimatedBytes
				}
			}
		}
		return 0
	}
	return 0
}

// fuseJoinStages absorbs directly connected broadcast joins into a consumer
// join, chaining probes batch-by-batch without intermediate S3 materialization.
// Multi-level fusion requires cumulative build bytes below maxFusedBuildBytes:
// the consumer's primary build (if broadcast), existing fused builds, and the
// candidate's primary and fused builds all count. Each probe shard loads them all.
// Unknown EstimatedBytes for any participant prevents extension past depth 1.
// See docs/internals/broadcast-join-fusion-budget.md for the design.
func fuseJoinStages(stages []Stage) []Stage {
	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}

	// Find broadcast join stages that can be absorbed.
	// A broadcast join can be fused into its consumer if:
	// 1. It's a broadcast_join (small build side, no shuffle)
	// 2. Exactly one other stage depends on its output
	// 3. That consumer is also a join stage
	absorbed := make(map[string]bool)

	// Count how many stages depend on each stage
	depCount := make(map[string]int)
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			depCount[dep]++
		}
	}

	// cumulativeBuildBytes is the sum of EstimatedBytes for every cache the
	// given stage will load: its primary build (only for broadcast_join — a
	// hash_join shuffles its build side), plus the build-side scan behind
	// each entry in FusedJoins. Returns -1 when ANY participant has unknown
	// (zero) EstimatedBytes — forces the conservative path so we don't
	// silently approve a chain that might exceed the budget.
	cumulativeBuildBytes := func(s *Stage) int64 {
		var total int64
		if s.Type == "broadcast_join" {
			pb := buildSideBytes(stages, s)
			if pb <= 0 {
				return -1
			}
			total += pb
		}
		for _, fj := range s.FusedJoins {
			b := fusedSpecBuildBytes(stages, fj)
			if b <= 0 {
				return -1
			}
			total += b
		}
		return total
	}

	for i := range stages {
		s := &stages[i]
		if s.Type != "broadcast_join" {
			continue
		}
		// Only fuse if exactly one consumer depends on this stage
		if depCount[s.ID] != 1 {
			continue
		}
		// Find the consumer stage
		var consumer *Stage
		for j := range stages {
			if stages[j].ID == s.ID {
				continue
			}
			for _, dep := range stages[j].Dependencies {
				if dep == s.ID {
					consumer = &stages[j]
					break
				}
			}
			if consumer != nil {
				break
			}
		}
		if consumer == nil {
			continue
		}
		// Consumer must be a join stage
		if consumer.Type != "hash_join" && consumer.Type != "broadcast_join" {
			continue
		}
		// A null-aware anti join carries a semantics flag (#507) that the
		// absorbed-join spec has no field for, and absorbing it would answer
		// the two-valued question instead. FusedJoinSpec can grow the field
		// the day a shape needs it; until then, not fusing is the honest
		// alternative to carrying it silently.
		if s.NullAwareAnti || consumer.NullAwareAnti {
			continue
		}
		// A LATERAL's hidden slot and its empty-input defaults belong to the
		// JOIN that minted them, and `FusedJoinSpec` has a field for neither —
		// absorbing such a stage would publish a reserved name to the client
		// and leave an unmatched outer row's defaults unapplied (#988). Same
		// call as the two above: not fusing is the honest alternative to
		// carrying it nowhere. `ChainedJoinSpec` DOES carry both, so the
		// downstream-fusion pass absorbs these instead of declining.
		//
		// NO SQL REACHES THIS, MEASURED. With the guard disabled, a lateral's
		// join feeding another join's probe answers PostgreSQL on all four
		// arms — that shape fuses through `fuseStageChains`, which carries the
		// fields. So this is a guard on a condition no statement is known to
		// produce, and it is gated at the level it is written instead:
		// `TestFuseJoinStagesDeclinesAJoinWhoseRulesTheSpecCannotCarry`
		// drives the pass over a synthetic stage list, with a plain leaf
		// beside the marked ones so a decline for the wrong reason fails too.
		if s.LateralPadMarker != "" || len(s.HiddenJoinCols) > 0 {
			continue
		}
		// Absorbing a stage DELETES it, and `FusedJoinSpec` has no field for
		// a projection — so a candidate carrying one has its OpProject
		// dropped on the floor along with the stage. That is not a lost
		// optimization: `absorbJoinArmProjection` puts a derived arm's
		// COMPUTED SELECT list on exactly this stage (#780), so fusing it
		// away un-computes the arm's column and the enclosing reference
		// binds the arm's raw inner one again — the same silent wrong value
		// #780 is about, reintroduced one pass later, and visible only when
		// the arm lands on the side this pass fuses.
		//
		// Not fusing is the honest alternative to carrying it silently, the
		// same call the NullAwareAnti check above makes: `FusedJoinSpec` can
		// grow a projection field, and the worker an ordering for it, the
		// day a shape needs the fusion more than the correctness.
		if len(s.ProjectExprs) > 0 {
			continue
		}
		// The candidate's output must feed the consumer's PROBE side.
		// Fusion replays the absorbed join as a broadcast-probe step on the
		// consumer's probe STREAM — valid only when the absorbed output IS
		// that stream. A candidate feeding the consumer's BUILD side (bushy
		// composite builds) would replay its probe steps against the wrong
		// stream: Q07/Q08-class chains returned 0 rows (2026-07-09).
		if consumer.LeftDepStage != s.ID {
			continue
		}

		// Cumulative byte budget: cap total cache amplification across the
		// fused chain. Required because each probe-split shard loads every
		// cache the consumer references — see comment on fuseJoinStages.
		// Backwards-compat fallback: when the candidate has no existing
		// FusedJoins (the historical depth-1 case) we still allow the fuse
		// based only on the candidate's primary build size, even if the
		// consumer's existing chain has unknown bytes — preserves the
		// historical fusion shape in the absence of cardinality estimates.
		candidateBytes := buildSideBytes(stages, s) + sumFusedBytes(stages, s.FusedJoins)
		if candidateBytes <= 0 {
			continue
		}
		if candidateBytes > maxFusedBuildBytes {
			continue
		}
		if len(s.FusedJoins) > 0 || len(consumer.FusedJoins) > 0 {
			// Multi-level fusion path — require known cumulative bytes for
			// both consumer + candidate and check the sum.
			cb := cumulativeBuildBytes(consumer)
			if cb < 0 {
				continue
			}
			if cb+candidateBytes > maxFusedBuildBytes {
				continue
			}
		}

		// Absorb: PREPEND the candidate's chain (its FusedJoins followed by
		// its primary spec) to the consumer's FusedJoins. The runtime walks
		// FusedJoins in order, then the primary join — so the resulting
		// runtime chain is:
		//
		//   probe_candidate → s.FusedJoins[0..n-1] → s.primary → consumer.original_FusedJoins → consumer.primary
		//
		// which matches what the un-fused DAG would compute: the candidate's
		// stage produced (its-probe ⨝ its-fused-builds ⨝ its-primary-build)
		// and that result was the consumer's probe input, so consumer's own
		// chain runs after the candidate's. Pre-fix this code APPENDED
		// (placing s.primary AFTER consumer.original_FusedJoins) which
		// silently re-ordered joins and caused empty result sets on Q02 + Q05
		// at SF1.
		spec := FusedJoinSpec{
			JoinType:        s.JoinType,
			JoinLeftKeys:    s.JoinLeftKeys,
			JoinRightKeys:   s.JoinRightKeys,
			JoinKeyTypes:    s.JoinKeyTypes,
			BuildDepStage:   s.RightDepStage,
			BuildTableAlias: s.BuildTableAlias,
			BuildColOrigins: s.BuildColOrigins,
			JoinFilter:      s.JoinFilter,
			FilterExprs:     s.FilterExprs,
			JoinBuildSchema: s.JoinBuildSchema,
		}
		merged := make([]FusedJoinSpec, 0, len(s.FusedJoins)+1+len(consumer.FusedJoins))
		merged = append(merged, s.FusedJoins...)
		merged = append(merged, spec)
		merged = append(merged, consumer.FusedJoins...)
		consumer.FusedJoins = merged

		// Rewire: consumer's dependency on this stage → dependency on this stage's probe dep
		for k, dep := range consumer.Dependencies {
			if dep == s.ID {
				if s.LeftDepStage != "" {
					consumer.Dependencies[k] = s.LeftDepStage
				}
				// Update LeftDepStage/RightDepStage if they pointed to the absorbed stage
				if consumer.LeftDepStage == s.ID {
					consumer.LeftDepStage = s.LeftDepStage
				}
				if consumer.RightDepStage == s.ID {
					consumer.RightDepStage = s.LeftDepStage
				}
				break
			}
		}

		// Add the candidate's build-side dependency to consumer.Dependencies,
		// plus any deps that came from the candidate's existing fused chain.
		// Without these, the coordinator can't see the build-side stages as
		// upstream dependencies of the fused consumer and may schedule it
		// before its caches are ready.
		if s.RightDepStage != "" {
			consumer.Dependencies = append(consumer.Dependencies, s.RightDepStage)
		}
		for _, fj := range s.FusedJoins {
			if fj.BuildDepStage != "" {
				consumer.Dependencies = append(consumer.Dependencies, fj.BuildDepStage)
			}
		}

		absorbed[s.ID] = true
	}

	if len(absorbed) == 0 {
		return stages
	}

	// Remove absorbed stages
	result := make([]Stage, 0, len(stages)-len(absorbed))
	for _, s := range stages {
		if !absorbed[s.ID] {
			result = append(result, s)
		}
	}
	return result
}

// fusedSpecBuildBytes is buildSideBytes for a FusedJoinSpec — walks the
// stages slice to find the BuildDepStage scan and returns its EstimatedBytes.
// Returns 0 (unknown) when the build subtree shape doesn't match the
// expected join → [exchange-replicate] → scan pattern, matching
// buildSideBytes' fail-closed convention.
func fusedSpecBuildBytes(stages []Stage, fj FusedJoinSpec) int64 {
	if fj.BuildDepStage == "" {
		return 0
	}
	for _, s := range stages {
		if s.ID != fj.BuildDepStage {
			continue
		}
		if s.Type == "scan" {
			return s.EstimatedBytes
		}
		if s.Type == StageExchangeReplicate && len(s.Dependencies) == 1 {
			for _, t := range stages {
				if t.ID == s.Dependencies[0] && t.Type == "scan" {
					return t.EstimatedBytes
				}
			}
		}
		return 0
	}
	return 0
}

// sumFusedBytes returns the cumulative build-side bytes across a slice of
// fused specs, using fusedSpecBuildBytes per entry. Returns -1 when any
// participant's bytes are unknown — forces the cumulative-budget check to
// fall back to the conservative depth-1 path.
func sumFusedBytes(stages []Stage, specs []FusedJoinSpec) int64 {
	if len(specs) == 0 {
		return 0
	}
	var total int64
	for _, fj := range specs {
		b := fusedSpecBuildBytes(stages, fj)
		if b <= 0 {
			return -1
		}
		total += b
	}
	return total
}
