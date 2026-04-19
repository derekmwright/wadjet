package physical

import (
	"strings"
	"testing"
)

// TestPickAggregateShuffleCandidate_Q17 verifies that Q17's decorrelated plan
// is recognized as having a derived-aggregate build side. Q17's inner scan of
// lineitem (alias lineitem:1) is ~6 GB at the test catalog's SF10 metadata;
// a 4 GB threshold should fire the detection.
func TestPickAggregateShuffleCandidate_Q17(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
		  )`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	cand, ok := PickAggregateShuffleCandidate(stages, 4*1024*1024*1024)
	if !ok {
		t.Fatal("Q17: expected aggregate-shuffle candidate, got none")
	}
	if cand.InputScanAlias != "lineitem:1" && cand.InputScanAlias != "lineitem" {
		t.Errorf("Q17: unexpected InputScanAlias=%q (want lineitem or lineitem:1)", cand.InputScanAlias)
	}
	if len(cand.GroupByKeys) == 0 {
		t.Error("Q17: GroupByKeys must be non-empty")
	}
	// The aggregate's GROUP BY is on l_partkey; the outer join's build-side
	// key is l_partkey or p_partkey depending on planner choice — whichever it
	// is, it must be one of the GroupByKeys (alignment check).
	covered := false
	for _, k := range cand.JoinBuildKeys {
		for _, g := range cand.GroupByKeys {
			if k == g {
				covered = true
				break
			}
		}
	}
	if !covered {
		t.Errorf("Q17: join build keys %v not covered by GroupByKeys %v",
			cand.JoinBuildKeys, cand.GroupByKeys)
	}
	if cand.InputScanBytes <= 4*1024*1024*1024 {
		t.Errorf("Q17: expected InputScanBytes > threshold, got %d", cand.InputScanBytes)
	}
	t.Logf("Q17 candidate: scan=%s(%dB) agg=%s groupBy=%v joinBuildKeys=%v joinProbeKeys=%v",
		cand.InputScanAlias, cand.InputScanBytes, cand.AggregateStageID,
		cand.GroupByKeys, cand.JoinBuildKeys, cand.JoinProbeKeys)
}

// TestPickAggregateShuffleCandidate_Q12 verifies that Q12 (a join-only query
// with no derived aggregate on any build side) does NOT match. Q12 joins
// orders with lineitem and aggregates the join output — the aggregate is the
// top-level final stage, not a join-feeder.
func TestPickAggregateShuffleCandidate_Q12(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT l_shipmode, COUNT(*) FROM orders
		JOIN lineitem ON o_orderkey = l_orderkey
		WHERE l_shipmode IN ('MAIL', 'SHIP')
		GROUP BY l_shipmode`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	if _, ok := PickAggregateShuffleCandidate(stages, 4*1024*1024*1024); ok {
		t.Fatal("Q12: expected no aggregate-shuffle candidate, got one")
	}
}

// TestBuildAggregateShuffleSQL_Q17 verifies that the reconstructed SQL for
// Q17's inner aggregate is a self-contained GROUP BY query that a worker can
// execute standalone.
func TestBuildAggregateShuffleSQL_Q17(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
		  )`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	cand, ok := PickAggregateShuffleCandidate(stages, 4*1024*1024*1024)
	if !ok {
		t.Fatal("expected candidate")
	}
	sqlText, err := BuildAggregateShuffleSQL(cand, stages)
	if err != nil {
		t.Fatalf("BuildAggregateShuffleSQL: %v", err)
	}
	t.Logf("reconstructed pre-compute SQL: %s", sqlText)
	// Structural checks — the exact projection/agg expression shape depends on
	// planner detail but must include these pieces.
	upper := strings.ToUpper(sqlText)
	if !strings.Contains(upper, "SELECT") || !strings.Contains(upper, "FROM LINEITEM") {
		t.Errorf("SQL missing SELECT...FROM lineitem: %q", sqlText)
	}
	if !strings.Contains(sqlText, "l_partkey") {
		t.Errorf("SQL missing GROUP BY key l_partkey: %q", sqlText)
	}
	if !strings.Contains(upper, "AVG(L_QUANTITY)") {
		t.Errorf("SQL missing AVG(l_quantity): %q", sqlText)
	}
	if !strings.Contains(upper, "GROUP BY") {
		t.Errorf("SQL missing GROUP BY clause: %q", sqlText)
	}
}

// TestPickAggregateShuffleCandidate_BelowThreshold verifies that when the
// aggregate's input scan is below the threshold, no candidate is returned —
// small derived aggregates stay on the current path and don't pay the extra
// shuffle round-trip.
func TestPickAggregateShuffleCandidate_BelowThreshold(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
		  )`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	// Very large threshold — inner scan won't exceed it.
	if _, ok := PickAggregateShuffleCandidate(stages, 1<<62); ok {
		t.Fatal("expected no candidate when scan bytes below threshold")
	}
}
