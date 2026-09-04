package physical

import "os"

// fuseScanShuffleEnabled gates fuseScanShuffle. Kill switch
// WADJET_FUSE_SCAN_SHUFFLE=0.
var fuseScanShuffleEnabled = os.Getenv("WADJET_FUSE_SCAN_SHUFFLE") != "0"

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
//   (runStageScanPartitionedStreaming + partitionedShuffleSink in the
//   worker) — saves one full write+read of the scan's output (2026-08-02
//   SF100 accounting: Q03 10.0 GB, Q21 6.8 GB, Q13 2.4 GB duplicated per
//   cold run on scan legs alone), one S3 PUT+GET cycle, and one NATS
//   round-trip per fused pair.
//
// Safety conditions:
//  1. Exchange has exactly one dependency (the scan).
//  2. Scan has exactly one dependent (the exchange) — no other stage reads
//     the scan's unpartitioned output.
//  3. Scan is a plain StageScan (not scan-aggregate, which has its own
//     fan-out semantics via dispatchScanAggregateStage; no prior Exchange),
//     and it is DISPATCHED-shape (pushed filters / projections / security
//     barrier / DF emits) — pass-through scans materialize nothing today,
//     so there is no round-trip to save and fusing would misroute them.
//  4. Every consumer of the exchange PARTITION-BINDS its input
//     (buildTaskInputsForStage → partitionFilesForWorker): hash_join,
//     sort_merge_join, or a grouped final_aggregate. Flattening consumers
//     — chained repartitions (flattenStageFiles), replicate/broadcast
//     caches (materializeReplicate), gathers, broadcast_join probe-split —
//     ingest scanTasks×numPartitions files where the unfused path handed
//     them scanTasks, the amplification behind the 2026-05 Q05 SF10
//     7m4s / Q07 +85% regressions that kept this pass disabled.
//  5. The exchange carries no computed-column machinery — the fragment
//     pipeline dispatchScanFilterStage emits (scan→filter→project→
//     column-prune→exchange-sender) has no ComputedCols evaluation ops.
//
// The historical file-count gate (skip when len(ScanFiles) > workerCount)
// is gone: both scan fan-out and shuffle fan-out are capacity-bound via
// scanFanOutTaskCount now, so for partition-binding consumers the fused
// layout produces the SAME per-partition file count as the unfused
// scan→shuffle two-step (T tasks × P partitions either way). The
// consolidation the legacy exchange provided only ever mattered to the
// flattening consumers condition 4 excludes.
//
// Worker requirements: dispatchScanFilterStage translates scan.Exchange
// into the fragment fuseShuffle path (OpExchangeSender terminal); the
// fused StageOutput carries per-partition rows/bytes (PartitionAccounting)
// so downstream skew-split stays live.
//
// Run AFTER pruneScanOutputColumns (which needs the exchange present to
// compute the scan's OutputColumns) and after every stage-rewiring pass,
// so the consumer set condition 4 inspects is final.
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
