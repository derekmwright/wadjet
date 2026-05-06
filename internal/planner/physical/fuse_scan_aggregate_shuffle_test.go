package physical

import (
	"testing"
)

// TestFuseScanAggregateShuffle_BasicAbsorbs — exchange-repartition fed by a
// scan-aggregate (StageScan with FusedAggGroupBy/Specs) and consumed by a
// final_aggregate gets absorbed into the scan; the consumer's deps are
// rewired to the scan.
func TestFuseScanAggregateShuffle_BasicAbsorbs(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 4},
		},
		{ID: "fa-2", Type: "final_aggregate", Dependencies: []string{"ex-1"}, GroupByCols: []string{"k"}},
	}

	out := fuseScanAggregateShuffle(stages)

	if len(out) != 2 {
		t.Fatalf("expected 2 stages after fusion (scan + final_aggregate), got %d", len(out))
	}
	var scan, fa *Stage
	for i := range out {
		switch out[i].ID {
		case "scan-0":
			scan = &out[i]
		case "fa-2":
			fa = &out[i]
		}
	}
	if scan == nil || fa == nil {
		t.Fatalf("missing expected stages after fusion: %+v", out)
	}
	if scan.Exchange == nil || scan.Exchange.Keys[0] != "k" || scan.Exchange.Count != 4 {
		t.Errorf("scan should carry shuffle keys after absorb, got Exchange=%+v", scan.Exchange)
	}
	if scan.Distribution.Kind != DistHashPartitioned {
		t.Errorf("scan distribution should be HashPartitioned, got %v", scan.Distribution.Kind)
	}
	if len(fa.Dependencies) != 1 || fa.Dependencies[0] != "scan-0" {
		t.Errorf("final_aggregate deps should be rewired to scan-0, got %v", fa.Dependencies)
	}
}

// TestFuseScanAggregateShuffle_SkipsPlainScan — scan without FusedAggGroupBy
// or FusedAggSpecs is a plain scan; that path stays under fuseScanShuffle's
// (currently disabled) gate, not this pass.
func TestFuseScanAggregateShuffle_SkipsPlainScan(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, TableName: "t"},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "fa-2", Type: "final_aggregate", Dependencies: []string{"ex-1"}},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 3 {
		t.Fatalf("expected fusion to skip plain scan (3 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan without FusedAgg should NOT have absorbed the exchange")
		}
	}
}

// TestFuseScanAggregateShuffle_RejectsHashJoinConsumer — when the exchange's
// consumer is hash_join, fusion would re-introduce the file-count
// amplification fuseJoinShuffle hit. Skip.
func TestFuseScanAggregateShuffle_RejectsHashJoinConsumer(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "cnt"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "build", Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 4}},
		{
			ID: "join-2", Type: StageHashJoin,
			Dependencies:  []string{"ex-1", "build"},
			LeftDepStage:  "ex-1",
			RightDepStage: "build",
		},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 4 {
		t.Fatalf("expected fusion to skip hash_join consumer (4 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan-aggregate with hash_join consumer should NOT have absorbed the exchange")
		}
	}
}

// TestFuseScanAggregateShuffle_RejectsBroadcastJoinConsumer — broadcast_join
// consumer is the same hazard as hash_join.
func TestFuseScanAggregateShuffle_RejectsBroadcastJoinConsumer(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "cnt"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "build", Distribution: Distribution{Kind: DistBroadcast}},
		{
			ID: "join-2", Type: StageBroadcastJoin,
			Dependencies:  []string{"ex-1", "build"},
			LeftDepStage:  "ex-1",
			RightDepStage: "build",
		},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 4 {
		t.Fatalf("expected fusion to skip broadcast_join consumer (4 stages), got %d", len(out))
	}
}

// TestFuseScanAggregateShuffle_AcceptsMergeAggregateConsumer —
// "merge_aggregate" is a synonymous collapsing pipeline-breaker.
// emitMergeAggregateTree currently emits Type="final_aggregate" for both
// intermediate and final layers, but the gate accepts merge_aggregate too
// for forward compatibility.
func TestFuseScanAggregateShuffle_AcceptsMergeAggregateConsumer(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "ma-2", Type: "merge_aggregate", Dependencies: []string{"ex-1"}},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 2 {
		t.Fatalf("expected fusion to fire (2 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange == nil {
			t.Errorf("scan-aggregate with merge_aggregate consumer should have absorbed the exchange")
		}
	}
}

// TestFuseScanAggregateShuffle_SkipsMultipleConsumers — when the exchange
// has multiple consumers and any one is non-collapsing, skip.
func TestFuseScanAggregateShuffle_SkipsMixedConsumers(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "cnt"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "fa-2", Type: "final_aggregate", Dependencies: []string{"ex-1"}},
		// Non-collapsing second consumer: a hash_join probe.
		{ID: "build", Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 4}},
		{
			ID: "join-3", Type: StageHashJoin,
			Dependencies:  []string{"ex-1", "build"},
			LeftDepStage:  "ex-1",
			RightDepStage: "build",
		},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 5 {
		t.Fatalf("expected fusion to skip with mixed consumers (5 stages), got %d", len(out))
	}
}

// TestFuseScanAggregateShuffle_SkipsMultipleScanDependents — scan with
// other dependents (besides the exchange) can't be fused: the others would
// see a partitioned output they don't expect.
func TestFuseScanAggregateShuffle_SkipsMultipleScanDependents(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "cnt"}},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "fa-2", Type: "final_aggregate", Dependencies: []string{"ex-1"}},
		// Second consumer of scan-0.
		{ID: "rep-3", Type: StageExchangeReplicate, Dependencies: []string{"scan-0"}},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 4 {
		t.Fatalf("expected fusion to skip (4 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange != nil {
			t.Errorf("scan-0 should NOT have absorbed the exchange (multiple dependents)")
		}
	}
}

// TestFuseScanAggregateShuffle_SkipsScanAlreadyExchangeBound — defensive
// check: if some prior pass already wired Exchange onto the scan, leave
// it alone.
func TestFuseScanAggregateShuffle_SkipsScanAlreadyExchangeBound(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-0", Type: StageScan, TableName: "t",
			FusedAggGroupBy: []string{"k"},
			FusedAggSpecs:   []AggSpec{{Func: "count", OutputCol: "cnt"}},
			Exchange:        &ExchangeStage{Keys: []string{"prior"}, Count: 4},
		},
		{
			ID: "ex-1", Type: StageExchangeRepartition,
			Dependencies: []string{"scan-0"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
		},
		{ID: "fa-2", Type: "final_aggregate", Dependencies: []string{"ex-1"}},
	}
	out := fuseScanAggregateShuffle(stages)
	if len(out) != 3 {
		t.Fatalf("expected fusion to skip (3 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "scan-0" && out[i].Exchange.Keys[0] != "prior" {
			t.Errorf("scan-0 Exchange should be untouched, got %+v", out[i].Exchange)
		}
	}
}
