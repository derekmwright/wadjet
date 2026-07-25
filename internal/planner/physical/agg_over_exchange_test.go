package physical

import "testing"

const aggOverExchangeQ18SQL = `SELECT
	c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
	SUM(l_quantity) as total_qty
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE o_orderkey IN (
	SELECT l_orderkey
	FROM lineitem
	GROUP BY l_orderkey
	HAVING SUM(l_quantity) > 300
)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100`

// TestAggOverExchange_Q18ShapeApplies pins that rewireAggOverRawExchange
// fires on the Q18 shape: the HAVING subquery's fused scan-agg leg (2nd full
// lineitem scan + partial shuffle) is dropped, the final_aggregate consumes
// the raw join-build exchange's partitions in raw mode, and the final's
// downstream identity re-shuffle is elided so the semi join builds directly
// from the final's output.
func TestAggOverExchange_Q18ShapeApplies(t *testing.T) {
	prev := AggOverExchange.Load()
	t.Cleanup(func() { AggOverExchange.Store(prev) })
	AggOverExchange.Store(true)
	// Pin stage-chain fusion off: this test asserts the UNFUSED shape (the
	// semi join as a standalone stage building from the raw final). The
	// combined behavior is pinned by TestFuseStageChains_Q18Interplay.
	prevFusion := StageFusion.Load()
	t.Cleanup(func() { StageFusion.Store(prevFusion) })
	StageFusion.Store(false)
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, aggOverExchangeQ18SQL, 3)

	byID := make(map[string]Stage, len(stages))
	lineitemScans := 0
	var rawFinal *Stage
	for i := range stages {
		s := &stages[i]
		byID[s.ID] = *s
		if s.Type == StageScan && s.TableName == "lineitem" {
			lineitemScans++
		}
		if s.RawInputAggregate {
			if rawFinal != nil {
				t.Fatalf("multiple RawInputAggregate stages: %s and %s", rawFinal.ID, s.ID)
			}
			rawFinal = s
		}
	}
	if rawFinal == nil {
		for _, s := range stages {
			t.Logf("id=%s type=%s deps=%v", s.ID, s.Type, s.Dependencies)
		}
		t.Fatal("no RawInputAggregate final_aggregate (rewrite did not fire)")
	}
	if lineitemScans != 1 {
		t.Errorf("lineitem scans = %d, want 1 (duplicate scan not dropped)", lineitemScans)
	}
	if rawFinal.Type != "final_aggregate" || len(rawFinal.Dependencies) != 1 {
		t.Fatalf("raw final shape: type=%s deps=%v", rawFinal.Type, rawFinal.Dependencies)
	}
	dep, ok := byID[rawFinal.Dependencies[0]]
	if !ok || dep.Type != StageExchangeRepartition {
		t.Fatalf("raw final's dep %q is %q, want exchange-repartition", rawFinal.Dependencies[0], dep.Type)
	}
	depScan, ok := byID[dep.Dependencies[0]]
	if !ok || depScan.Type != StageScan || depScan.TableName != "lineitem" ||
		len(depScan.FusedAggGroupBy) > 0 {
		t.Fatalf("raw final's exchange feeds %+v, want raw lineitem scan", depScan.ID)
	}
	if rawFinal.Distribution.Kind != DistHashPartitioned ||
		rawFinal.Distribution.Count != dep.Exchange.Count ||
		!keysEqual(rawFinal.Distribution.Keys, dep.Exchange.Keys) {
		t.Errorf("raw final distribution %+v does not mirror exchange {keys=%v count=%d}",
			rawFinal.Distribution, dep.Exchange.Keys, dep.Exchange.Count)
	}
	if len(rawFinal.AggSpecs) != 1 || rawFinal.AggSpecs[0].InputCol != "l_quantity" {
		t.Errorf("raw final AggSpecs = %+v, want raw sum(l_quantity)", rawFinal.AggSpecs)
	}
	if len(rawFinal.FilterExprs) == 0 {
		t.Error("HAVING filter lost from raw final")
	}
	// Bonus elide: the semi join must build directly from the raw final —
	// no surviving re-shuffle between them.
	semiBuildsFromFinal := false
	for _, s := range stages {
		if s.Type == StageHashJoin && s.JoinType == "semi" && s.RightDepStage == rawFinal.ID {
			semiBuildsFromFinal = true
		}
	}
	if !semiBuildsFromFinal {
		for _, s := range stages {
			t.Logf("id=%s type=%s join=%s right_dep=%s", s.ID, s.Type, s.JoinType, s.RightDepStage)
		}
		t.Error("semi join does not build directly from the raw final (identity exchange not elided)")
	}
}

// TestAggOverExchange_KillSwitch pins the WADJET_AGG_OVER_EXCHANGE=0 arm:
// plans keep the duplicate fused scan-agg leg untouched.
func TestAggOverExchange_KillSwitch(t *testing.T) {
	prev := AggOverExchange.Load()
	t.Cleanup(func() { AggOverExchange.Store(prev) })
	AggOverExchange.Store(false)
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, aggOverExchangeQ18SQL, 3)
	lineitemScans := 0
	for _, s := range stages {
		if s.RawInputAggregate {
			t.Errorf("stage %s has RawInputAggregate with the kill switch off", s.ID)
		}
		if s.Type == StageScan && s.TableName == "lineitem" {
			lineitemScans++
		}
	}
	if lineitemScans != 2 {
		t.Errorf("lineitem scans = %d, want 2 with the kill switch off", lineitemScans)
	}
}
