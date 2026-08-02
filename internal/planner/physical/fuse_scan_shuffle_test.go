package physical

import (
	"testing"
)

// fuseFixture builds the canonical scan → exchange → consumer DAG. The
// consumer type is the variable under test for the partition-binding gate.
func fuseFixture(consumerType string, files int) []Stage {
	scanFiles := make([]string, files)
	for i := range scanFiles {
		scanFiles[i] = "f"
	}
	return []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "t", ScanFiles: scanFiles,
			FilterExprs: []string{"x > 1"}}, // dispatched-shape: pushed filter
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 8},
		},
		{
			ID: "cons-2", Type: consumerType,
			Dependencies:  []string{"ex-1", "other"},
			LeftDepStage:  "ex-1",
			RightDepStage: "other",
		},
	}
}

// TestFuseScanShuffle_BasicAbsorbs — exchange-repartition fed by a plain
// scan with no other dependents, consumed by a partition-binding join, gets
// absorbed into the scan; the consumer's dep is rewired to the scan.
func TestFuseScanShuffle_BasicAbsorbs(t *testing.T) {
	out := fuseScanShuffle(fuseFixture(StageHashJoin, 2))

	if len(out) != 2 {
		t.Fatalf("expected 2 stages after fusion (scan + join), got %d", len(out))
	}
	var scan, join *Stage
	for i := range out {
		switch out[i].ID {
		case "scan-0":
			scan = &out[i]
		case "cons-2":
			join = &out[i]
		}
	}
	if scan == nil || join == nil {
		t.Fatalf("missing expected stages after fusion: %+v", out)
	}
	if scan.Exchange == nil || len(scan.Exchange.Keys) != 1 || scan.Exchange.Keys[0] != "k" {
		t.Errorf("scan should carry shuffle keys after absorb, got Exchange=%+v", scan.Exchange)
	}
	if scan.Distribution.Kind != DistHashPartitioned {
		t.Errorf("scan distribution should be HashPartitioned, got %v", scan.Distribution.Kind)
	}
	if join.LeftDepStage != "scan-0" {
		t.Errorf("join LeftDepStage should be rewired to scan-0, got %q", join.LeftDepStage)
	}
	if len(join.Dependencies) != 2 || join.Dependencies[0] != "scan-0" {
		t.Errorf("join Dependencies should be rewired, got %v", join.Dependencies)
	}
}

// TestFuseScanShuffle_HighFanOutScanFuses — the historical file-count gate
// is gone: scan and shuffle fan-out are both capacity-bound, so a 600-file
// fact scan fuses when its consumer partition-binds. This is the Q03/Q21/
// Q13 shape whose double materialization the pass deletes.
func TestFuseScanShuffle_HighFanOutScanFuses(t *testing.T) {
	out := fuseScanShuffle(fuseFixture(StageHashJoin, 600))
	if len(out) != 2 {
		t.Fatalf("expected fusion to fire on high-fan-out scan (2 stages), got %d", len(out))
	}
}

// TestFuseScanShuffle_SkipsFlatteningConsumers — consumers that flatten the
// upstream file list (chained repartitions, replicates, gathers, broadcast
// probe-split) would ingest tasks×partitions files; fusion must skip.
func TestFuseScanShuffle_SkipsFlatteningConsumers(t *testing.T) {
	for _, ct := range []string{
		StageExchangeRepartition,
		StageExchangeReplicate,
		StageExchangeGather,
		StageBroadcastJoin,
		StageSort,
		StagePipeline,
	} {
		out := fuseScanShuffle(fuseFixture(ct, 2))
		if len(out) != 3 {
			t.Errorf("consumer %s: expected skip (3 stages), got %d", ct, len(out))
		}
		for i := range out {
			if out[i].ID == "scan-0" && out[i].Exchange != nil {
				t.Errorf("consumer %s: scan-0 should NOT have absorbed the exchange", ct)
			}
		}
	}
}

// TestFuseScanShuffle_PartitionBindingConsumersFuse — the allowed consumer
// set: hash_join, sort_merge_join, grouped final_aggregate.
func TestFuseScanShuffle_PartitionBindingConsumersFuse(t *testing.T) {
	for _, ct := range []string{StageHashJoin, StageSortMergeJoin, "final_aggregate"} {
		out := fuseScanShuffle(fuseFixture(ct, 2))
		if len(out) != 2 {
			t.Errorf("consumer %s: expected fusion (2 stages), got %d", ct, len(out))
		}
	}
}

// TestFuseScanShuffle_SkipsComputedColExchanges — the fragment pipeline has
// no ComputedCols evaluation ops; exchanges carrying subsumption flags keep
// the two-step path.
func TestFuseScanShuffle_SkipsComputedColExchanges(t *testing.T) {
	st := fuseFixture(StageHashJoin, 2)
	st[1].Exchange.ComputedCols = []ComputedCol{{Name: "__flag", Expr: "x > 1"}}
	if out := fuseScanShuffle(st); len(out) != 3 {
		t.Errorf("ComputedCols: expected skip, got %d stages", len(out))
	}
	st = fuseFixture(StageHashJoin, 2)
	st[1].Exchange.ExtraReadCols = []string{"x"}
	if out := fuseScanShuffle(st); len(out) != 3 {
		t.Errorf("ExtraReadCols: expected skip, got %d stages", len(out))
	}
}

// TestFuseScanShuffle_SkipsScanWithMultipleConsumers — when the scan has
// more than one downstream stage, fusion would partition output the other
// consumer doesn't expect; skip.
func TestFuseScanShuffle_SkipsScanWithMultipleConsumers(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "t"},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
		},
		{ID: "join-2", Type: StageHashJoin, Dependencies: []string{"ex-1", "other"},
			LeftDepStage: "ex-1", RightDepStage: "other"},
		// Second consumer of scan-0: a broadcast-replicate stage.
		{ID: "rep-3", Type: StageExchangeReplicate, Dependencies: []string{"scan-0"}},
	}

	out := fuseScanShuffle(stages)
	if len(out) != 4 {
		t.Fatalf("expected fusion to skip (4 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan-0 should NOT have absorbed the exchange (multiple consumers)")
		}
	}
}

// TestFuseScanShuffle_SkipsPassThroughScans — a filter-less plain scan is a
// pass-through leg at dispatch (no tasks, no materialized intermediate):
// there is no write+read cycle to delete, and fusing would misroute raw
// parquet into a partition-binding consumer. Only dispatched-shape scans
// (filters / projections / security barrier / DF emits) fuse.
func TestFuseScanShuffle_SkipsPassThroughScans(t *testing.T) {
	st := fuseFixture(StageHashJoin, 2)
	st[0].FilterExprs = nil
	if out := fuseScanShuffle(st); len(out) != 3 {
		t.Fatalf("pass-through scan: expected skip (3 stages), got %d", len(out))
	}
	// DF-emit-only scans dispatch (the emit op must run) — they fuse.
	st = fuseFixture(StageHashJoin, 2)
	st[0].FilterExprs = nil
	st[0].EmitDynamicFilters = []DynamicFilterEmit{{FilterID: "df0", KeyColumn: "k"}}
	if out := fuseScanShuffle(st); len(out) != 2 {
		t.Fatalf("DF-emit scan: expected fusion (2 stages), got %d", len(out))
	}
}

// TestFuseScanShuffle_SkipsFusedAggScans — scans with FusedAggGroupBy are
// owned by dispatchScanAggregateStage's RoundRobin fan-out; don't override.
func TestFuseScanShuffle_SkipsFusedAggScans(t *testing.T) {
	st := fuseFixture("final_aggregate", 2)
	st[0].FusedAggGroupBy = []string{"g"}
	if out := fuseScanShuffle(st); len(out) != 3 {
		t.Fatalf("expected fusion to skip (3 stages), got %d", len(out))
	}
}

// TestFuseScanShuffle_KillSwitch — WADJET_FUSE_SCAN_SHUFFLE=0 semantics.
func TestFuseScanShuffle_KillSwitch(t *testing.T) {
	fuseScanShuffleEnabled = false
	defer func() { fuseScanShuffleEnabled = true }()
	if out := fuseScanShuffle(fuseFixture(StageHashJoin, 2)); len(out) != 3 {
		t.Fatalf("kill switch: expected no fusion (3 stages), got %d", len(out))
	}
}
