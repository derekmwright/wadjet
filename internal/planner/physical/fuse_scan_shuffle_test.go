package physical

import (
	"testing"
)

// TestFuseScanShuffle_BasicAbsorbs — exchange-repartition fed by a plain
// scan with no other dependents gets absorbed into the scan; the downstream
// consumer's dep is rewired to the scan.
func TestFuseScanShuffle_BasicAbsorbs(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "t"},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 8},
		},
		{
			ID: "join-2", Type: StageHashJoin,
			Dependencies:  []string{"ex-1", "other"},
			LeftDepStage:  "ex-1",
			RightDepStage: "other",
		},
	}

	out := fuseScanShuffle(stages, 0)

	if len(out) != 2 {
		t.Fatalf("expected 2 stages after fusion (scan + join), got %d", len(out))
	}
	var scan, join *Stage
	for i := range out {
		switch out[i].ID {
		case "scan-0":
			scan = &out[i]
		case "join-2":
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
		// Second consumer of scan-0: a broadcast-replicate stage.
		{ID: "rep-2", Type: StageExchangeReplicate, Dependencies: []string{"scan-0"}},
	}

	out := fuseScanShuffle(stages, 0)
	if len(out) != 3 {
		t.Fatalf("expected fusion to skip (3 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan-0 should NOT have absorbed the exchange (multiple consumers)")
		}
	}
}

// TestFuseScanShuffle_SkipsHighFanOutScan — when the scan's file count
// exceeds workerCount, the legacy exchange-repartition stage's N→workerCount
// consolidation is essential; fusing emits scanTaskCount×numPartitions
// intermediate files instead of workerCount×numPartitions, amplifying
// downstream join-task S3 reads. Skip fusion in that case.
func TestFuseScanShuffle_SkipsHighFanOutScan(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "lineitem",
			ScanFiles: []string{"f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 8},
		},
		{
			ID: "join-2", Type: StageHashJoin,
			Dependencies:  []string{"ex-1", "other"},
			LeftDepStage:  "ex-1",
			RightDepStage: "other",
		},
	}

	// 10 files, workerCount=3 → skip fusion.
	out := fuseScanShuffle(stages, 3)
	if len(out) != 3 {
		t.Fatalf("expected fusion to skip with 10 files vs workerCount=3 (3 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan-0 should NOT have absorbed the exchange (file count > workerCount)")
		}
	}

	// 10 files, workerCount=0 → uncapped (test mode), fusion fires.
	out = fuseScanShuffle(stages, 0)
	if len(out) != 2 {
		t.Fatalf("expected fusion to fire with workerCount=0 (2 stages), got %d", len(out))
	}
}

// TestFuseScanShuffle_FusesSmallScan — scans with file count ≤ workerCount
// stay below the consolidation-needed threshold; fuse them so the small
// dimension scans (customer/supplier/nation in TPC-H) capture the
// round-trip-saved win.
func TestFuseScanShuffle_FusesSmallScan(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "nation",
			ScanFiles: []string{"f1"}, // 1 file, well under workerCount=3
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 8},
		},
		{
			ID: "join-2", Type: StageHashJoin,
			Dependencies:  []string{"ex-1", "other"},
			LeftDepStage:  "ex-1",
			RightDepStage: "other",
		},
	}

	out := fuseScanShuffle(stages, 3)
	if len(out) != 2 {
		t.Fatalf("expected fusion to fire with 1 file ≤ workerCount=3 (2 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange == nil {
			t.Errorf("scan-0 should have absorbed the exchange (small fan-out)")
		}
	}
}

// TestFuseScanShuffle_SkipsFusedAggScans — scans with FusedAggGroupBy are
// owned by dispatchScanAggregateStage's RoundRobin fan-out; don't override.
func TestFuseScanShuffle_SkipsFusedAggScans(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "t", FusedAggGroupBy: []string{"g"}},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 8},
		},
		{ID: "agg-2", Type: "merge_aggregate", Dependencies: []string{"ex-1"}},
	}
	out := fuseScanShuffle(stages, 0)
	if len(out) != 3 {
		t.Fatalf("expected fusion to skip (3 stages), got %d", len(out))
	}
}
