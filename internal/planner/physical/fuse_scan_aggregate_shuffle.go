package physical

// fuseScanAggregateShuffle absorbs an exchange into a scan-aggregate and
// hash-partitions its partial rows directly. Run after assignStageDistributions
// sets final keys/count; later passes must not overwrite scan.Exchange.
// Require one exchange dependency: a scan with FusedAggGroupBy or FusedAggSpecs,
// no existing Exchange, and no dependent other than that exchange.
// Require at least one exchange consumer, all final_aggregate/merge_aggregate
// collapsing breakers. Other consumers reintroduce file-count amplification.
// Scan-aggregate fan-out is worker-count bounded regardless of table file count,
// keeping workerCount × numPartitions files and workerCount files per partition.
// See docs/internals/scan-aggregate-shuffle-fusion.md for the design.
func fuseScanAggregateShuffle(stages []Stage) []Stage {
	depCount := make(map[string]int, len(stages))
	consumers := make(map[string][]int, len(stages))
	for i := range stages {
		for _, d := range stages[i].Dependencies {
			depCount[d]++
			consumers[d] = append(consumers[d], i)
		}
	}
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}

	absorbed := make(map[string]string) // exchange ID → upstream scan ID

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
		scanIdx, ok := idx[ex.Dependencies[0]]
		if !ok {
			continue
		}
		scan := &stages[scanIdx]
		if scan.Type != StageScan {
			continue
		}
		if len(scan.FusedAggGroupBy) == 0 && len(scan.FusedAggSpecs) == 0 {
			continue
		}
		if scan.Exchange != nil {
			continue
		}
		if depCount[scan.ID] != 1 {
			continue
		}
		if depCount[ex.ID] == 0 {
			continue
		}
		// Every consumer of the exchange must be a collapsing
		// pipeline-breaker. emitMergeAggregateTree creates Type
		// "final_aggregate" for both intermediate (with MergeGroup) and
		// final layers; a future "merge_aggregate" type would qualify too.
		allCollapsing := true
		for _, ci := range consumers[ex.ID] {
			ct := stages[ci].Type
			if ct != "final_aggregate" && ct != "merge_aggregate" {
				allCollapsing = false
				break
			}
		}
		if !allCollapsing {
			continue
		}

		scan.Distribution = ex.Distribution
		scan.Exchange = ex.Exchange
		absorbed[ex.ID] = scan.ID
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
