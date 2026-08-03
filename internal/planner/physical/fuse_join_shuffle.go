package physical

import "os"

// fuseJoinShuffleEnabled gates fuseJoinShuffle. Kill switch
// WADJET_FUSE_JOIN_SHUFFLE=0.
var fuseJoinShuffleEnabled = os.Getenv("WADJET_FUSE_JOIN_SHUFFLE") != "0"

// fuseJoinShuffle absorbs StageExchangeRepartition stages into their upstream
// hash_join / broadcast_join when safe, eliminating the round-trip of
// writing+re-reading an unpartitioned WSHF between the join and the shuffle.
//
// Today's flow:  join → exchange-repartition → consumer
//   join writes unpartitioned WSHF; coord dispatches a separate shuffle task
//   that reads it back and writes partitioned WSHF; the consumer reads the
//   partitions.
//
// After fusion:  join(with shuffle metadata) → consumer
//   The join task hash-partitions its probe-side output directly via the
//   worker's executeFragment path: [ShuffleSource probe, HashJoinProbe(s),
//   ExchangeSender]. Saves one S3 PUT, one S3 GET, one NATS round-trip per
//   fused pair.
//
// Run AFTER fuseScanShuffle so that any scan-fused exchanges have already
// been absorbed; the remaining repartition stages are the ones whose upstream
// is a join (or other compute stage). Setting join.Exchange directly here is
// durable — no later pass overwrites it.
//
// Safety conditions mirror fuseScanShuffle:
//  1. Exchange has exactly one dependency.
//  2. The dependency is a hash_join or broadcast_join.
//  3. The join has exactly one dependent (the exchange we're absorbing).
//  4. The join doesn't already carry an Exchange.
//  5. At least one stage depends on the exchange (otherwise no benefit; the
//     dropped exchange would orphan no consumers but the absorption is moot).
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
