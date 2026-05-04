package physical

// fuseScanShuffle absorbs StageExchangeRepartition stages into their upstream
// StageScan when safe, eliminating the round-trip of writing+re-reading an
// unpartitioned WSHF between scan and shuffle.
//
// Today's flow:  scan → exchange-repartition → consumer
//   scan emits unpartitioned WSHF; coord dispatches a separate shuffle task
//   that reads it and writes partitioned WSHF; consumer reads the partitions.
//
// After fusion: scan(with shuffle metadata) → consumer
//   scan task hash-partitions its filtered output directly
//   (writePartitionedShuffle in worker) — saves one S3 PUT, one S3 GET, one
//   NATS round-trip per fused scan-shuffle pair.
//
// Safety conditions:
//  1. Exchange has exactly one dependency (the scan).
//  2. Scan has exactly one dependent (the exchange) — no other stage reads
//     the scan's unpartitioned output.
//  3. Scan is a plain StageScan (not scan-aggregate, which has its own
//     fan-out semantics via dispatchScanAggregateStage).
//
// Worker requirements: writeStageOutput already routes to
// writePartitionedShuffle when ShuffleKeys+NumPartitions are set. Coord must
// translate stage.Exchange (when set on a scan) into task.ShuffleKeys +
// task.NumPartitions; that change lives in dispatchScanFilterStage.
//
// Run AFTER EnsureDistribution + the final assignStageDistributions, so the
// exchange stages exist to absorb. Setting scan.Distribution directly here
// is durable (no later pass overwrites it).
func fuseScanShuffle(stages []Stage) []Stage {
	// Count dependents per stage ID — a scan with multiple consumers can't be
	// fused (the others would lose the unpartitioned shape).
	depCount := make(map[string]int, len(stages))
	for i := range stages {
		for _, d := range stages[i].Dependencies {
			depCount[d]++
		}
	}

	// Map stage ID → index for efficient lookup + mutation.
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
		// Don't fuse if the scan has fused partial-aggregate semantics —
		// dispatchScanAggregateStage owns the partitioning of those.
		if len(scan.FusedAggGroupBy) > 0 {
			continue
		}
		// Don't fuse if the scan already had a non-singleton output target —
		// can't have two different shuffle shapes off one scan.
		if scan.Exchange != nil {
			continue
		}
		// Don't fuse if any OTHER stage depends on the scan — they'd see the
		// partitioned output and break.
		if depCount[scan.ID] != 1 {
			continue
		}
		// Don't fuse if any OTHER stage depends on the exchange — we're going
		// to remove it, so its other dependents would lose their input.
		if depCount[ex.ID] == 0 {
			continue
		}

		// Absorb: scan inherits the exchange's distribution + Exchange payload.
		// The dispatch layer reads scan.Exchange to decide whether to emit
		// the scan task with ShuffleKeys+NumPartitions.
		scan.Distribution = ex.Distribution
		scan.Exchange = ex.Exchange
		absorbed[ex.ID] = scan.ID
	}

	if len(absorbed) == 0 {
		return stages
	}

	// Rewire downstream: any stage that depended on an absorbed exchange now
	// depends on the upstream scan. Update Dependencies + LeftDepStage +
	// RightDepStage in place.
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

	// Drop the absorbed exchange stages.
	out := make([]Stage, 0, len(stages)-len(absorbed))
	for _, s := range stages {
		if _, dropped := absorbed[s.ID]; dropped {
			continue
		}
		out = append(out, s)
	}
	return out
}
