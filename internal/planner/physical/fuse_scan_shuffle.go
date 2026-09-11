package physical

import "os"

// fuseScanShuffleEnabled gates fuseScanShuffle. Kill switch
// WADJET_FUSE_SCAN_SHUFFLE=0.
var fuseScanShuffleEnabled = os.Getenv("WADJET_FUSE_SCAN_SHUFFLE") != "0"

// fuseScanShuffle requires a one-dependency exchange and its sole-consumer scan:
// a dispatched plain StageScan (filters/projections/security barrier/DF emits),
// not scan-aggregate, pass-through or already carrying Exchange. All consumers
// must partition-bind: hash_join, sort_merge_join or grouped final_aggregate;
// exclude flattening consumers. No exchange computed-column machinery is allowed.
// Capacity-bound scan/shuffle fan-out gives the same per-partition file count;
// do not gate on ScanFiles count. dispatchScanFilterStage must terminate with
// ExchangeSender and publish PartitionAccounting for downstream skew splitting.
// Run after pruneScanOutputColumns and all rewiring, with the consumer set final.
// See docs/internals/scan-shuffle-fusion.md for the design.
func fuseScanShuffle(stages []Stage) []Stage {
	if !fuseScanShuffleEnabled {
		return stages
	}
	// Consumers per stage ID — fusion decisions need both the count (a scan
	// with multiple consumers can't fuse) and the types (condition 4).
	consumers := make(map[string][]*Stage, len(stages))
	for i := range stages {
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], &stages[i])
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
		// Condition 5: no computed-column machinery on the exchange.
		if len(ex.Exchange.ComputedCols) > 0 || len(ex.Exchange.ExtraReadCols) > 0 {
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
		// Only fuse DISPATCHED-shape scans — ones that route through
		// dispatchScanFilterStage (pushed filters, computed projections,
		// security barriers, dynamic-filter emits) and therefore write an
		// unpartitioned intermediate today. Filter-less plain scans are
		// PASS-THROUGH: no tasks, no materialization — the downstream
		// shuffle's tasks read base parquet directly (runShuffleSide with
		// prunedScanColumns + row-group dynamic filters), which is already
		// single-write. Fusing those would also break dispatch routing:
		// the pass-through branch returns raw parquet as OutputSinglePart,
		// which a partition-binding consumer must never receive.
		if len(scan.FilterExprs) == 0 && len(scan.ProjectExprs) == 0 &&
			len(scan.SecurityProjectExprs) == 0 && len(scan.EmitDynamicFilters) == 0 {
			continue
		}
		// Don't fuse if the scan already had a non-singleton output target —
		// can't have two different shuffle shapes off one scan.
		if scan.Exchange != nil {
			continue
		}
		// Don't fuse if any OTHER stage depends on the scan — they'd see the
		// partitioned output and break.
		if len(consumers[scan.ID]) != 1 {
			continue
		}
		// Condition 4: every exchange consumer must partition-bind.
		exCons := consumers[ex.ID]
		if len(exCons) == 0 {
			continue
		}
		binds := true
		for _, c := range exCons {
			switch c.Type {
			case StageHashJoin, StageSortMergeJoin, "final_aggregate":
			default:
				binds = false
			}
		}
		if !binds {
			continue
		}

		// Absorb: scan inherits the exchange's distribution + Exchange payload.
		// The dispatch layer reads scan.Exchange to decide whether to emit
		// the scan task with the fragment fuseShuffle pipeline.
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
		// A CHAINED or FUSED join names its build side in a THIRD place, and
		// it is not a Dependencies entry: dispatch looks
		// `ChainedJoinSpec.BuildDepStage` up in the `inputs` map by ID. The
		// two fields above were rewired and these were not, so a chained
		// join whose build came through an absorbed exchange pointed at a
		// stage this pass had just deleted — `stage join-4 chained join 0:
		// build dep "exchange-repartition-7" output not found`, at DISPATCH,
		// on a query PostgreSQL and the broadcast lowering both answer
		// (#755). Only a scan with a PROJECTION or a pushed filter is
		// absorbed, which is why the shape needs a DERIVED arm: a plain
		// base-table arm's scan is pass-through and never fuses.
		for k := range s.FusedJoins {
			if newDep, replaced := absorbed[s.FusedJoins[k].BuildDepStage]; replaced {
				s.FusedJoins[k].BuildDepStage = newDep
			}
		}
		for k := range s.ChainedJoins {
			if newDep, replaced := absorbed[s.ChainedJoins[k].BuildDepStage]; replaced {
				s.ChainedJoins[k].BuildDepStage = newDep
			}
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
