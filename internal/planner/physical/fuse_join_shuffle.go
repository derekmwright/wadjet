package physical

import "os"

// fuseJoinShuffleEnabled gates fuseJoinShuffle. Kill switch
// WADJET_FUSE_JOIN_SHUFFLE=0.
var fuseJoinShuffleEnabled = os.Getenv("WADJET_FUSE_JOIN_SHUFFLE") != "0"

// fuseJoinShuffle absorbs an exchange into its hash_join/broadcast_join
// dependency, which partitions probe output directly through ExchangeSender.
// Run after fuseScanShuffle; no later pass overwrites join.Exchange.
// Require one exchange dependency, the join's sole consumer to be that
// exchange, no existing join.Exchange and at least one exchange consumer.
// See docs/internals/join-shuffle-fusion.md for the design.
func fuseJoinShuffle(stages []Stage) []Stage {
	if !fuseJoinShuffleEnabled {
		return stages
	}
	consumers := make(map[string][]*Stage, len(stages))
	depCount := make(map[string]int, len(stages))
	for i := range stages {
		for _, d := range stages[i].Dependencies {
			depCount[d]++
			consumers[d] = append(consumers[d], &stages[i])
		}
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}

	absorbed := make(map[string]string) // exchange ID → upstream join ID

	for i := range stages {
		ex := &stages[i]
		if ex.Type != StageExchangeRepartition {
			continue
		}
		if len(ex.Dependencies) != 1 {
			continue
		}
		if ex.Exchange == nil {
			continue
		}
		joinIdx, ok := idx[ex.Dependencies[0]]
		if !ok {
			continue
		}
		join := &stages[joinIdx]
		// Limit fuseJoinShuffle to plain hash_joins for now. broadcast_join
		// stages can carry FusedJoins (chained absorbed broadcasts) and
		// participate in probe-split. Both shapes are technically expressible
		// in the fragment runner, but the dispatch-side wiring for fused
		// chains needs more validation than this pass alone — Q02 SF0.01
		// returned 100 rows vs expected 4-5 with broadcast_join fusion
		// enabled (2026-05-03). Hash-join fusion captures the bulk of the
		// SF10 join-shuffle wins (Q07/Q21 chained shuffle joins) without
		// the multi-broadcast risk surface; broadcast_join fusion can
		// re-enable once the fused-chain probe shape has explicit tests.
		if join.Type != StageHashJoin {
			continue
		}
		if join.Exchange != nil {
			continue
		}
		if depCount[join.ID] != 1 {
			continue
		}
		if depCount[ex.ID] == 0 {
			continue
		}
		// Consumer-shape gate (mirrors fuseScanShuffle, 765ce81): every
		// consumer of the exchange must partition-bind its input.
		// Flattening consumers (chained repartitions, replicates,
		// gathers, broadcast probe-split, singleton sorts) would ingest
		// tasks×partitions files — the 2026-05 regression class.
		binds := true
		for _, c := range consumers[ex.ID] {
			switch c.Type {
			case StageHashJoin, StageSortMergeJoin, "final_aggregate":
			default:
				binds = false
			}
		}
		if !binds {
			continue
		}
		// No computed-column machinery: the join fragment pipeline has no
		// flag-evaluation ops.
		if len(ex.Exchange.ComputedCols) > 0 || len(ex.Exchange.ExtraReadCols) > 0 {
			continue
		}
		// Width gate (#280): the dispatcher derives a compute stage's task
		// count from its Distribution, so the absorption below rewrites the
		// join's parallelism to the exchange's partition count. Absorbing a
		// narrower exchange collapses the join's width — SF100 Q18 join-8
		// absorbed its /4 final-aggregate repartition and ran 3 tasks of
		// ~78s (each pressure-collapsing to serial) where the unfused shape
		// ran 24 partition tasks of 2-4s (cold +126%, 2026-08-03 pair).
		// Fuse only width-preserving pairs; the narrower join→final_aggregate
		// legs keep their explicit repartition stage until fused joins can
		// schedule at input-binding width and bucket output per-Exchange.
		if ex.Distribution.Count != join.Distribution.Count {
			continue
		}

		join.Distribution = ex.Distribution
		join.Exchange = ex.Exchange
		absorbed[ex.ID] = join.ID
	}

	if len(absorbed) == 0 {
		return stages
	}

	for i := range stages {
		s := &stages[i]
		for k, dep := range s.Dependencies {
			if newDep, replaced := absorbed[dep]; replaced {
				s.Dependencies[k] = newDep
			}
		}
		if newDep, replaced := absorbed[s.LeftDepStage]; replaced {
			s.LeftDepStage = newDep
		}
		if newDep, replaced := absorbed[s.RightDepStage]; replaced {
			s.RightDepStage = newDep
		}
	}

	out := make([]Stage, 0, len(stages)-len(absorbed))
	for _, s := range stages {
		if _, dropped := absorbed[s.ID]; dropped {
			continue
		}
		out = append(out, s)
	}
	return out
}
