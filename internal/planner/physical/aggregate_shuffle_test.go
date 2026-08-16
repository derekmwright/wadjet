package physical

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"strings"
	"testing"
)

// TestPickAggregateShuffleCandidate_Q17 verifies that Q17's decorrelated plan
// is recognized as having a derived-aggregate build side. Q17's inner scan of
// lineitem (alias lineitem:1) is ~6 GB at the test catalog's SF10 metadata;
// a 4 GB threshold should fire the detection.
func TestPickAggregateShuffleCandidate_Q17(t *testing.T) {
	// These fixtures exercise the UNreduced decorrelated shape; the
	// scalar-agg semijoin reduction (on by default) rewrites it so the
	// aggregate is no longer scan-rooted. Pin it off for this test.
	old := logical.ScalarAggSemijoin.Load()
	logical.ScalarAggSemijoin.Store(false)
	defer logical.ScalarAggSemijoin.Store(old)
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
	// These fixtures exercise the UNreduced decorrelated shape; the
	// scalar-agg semijoin reduction (on by default) rewrites it so the
	// aggregate is no longer scan-rooted. Pin it off for this test.
	old := logical.ScalarAggSemijoin.Load()
	logical.ScalarAggSemijoin.Store(false)
	defer logical.ScalarAggSemijoin.Store(old)
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

// TestPickAggregateShuffleCandidate_SF10RealisticBytes reproduces the SF10
// EC2 symptom locally: the default catalog uses 10 MB per lineitem file
// (6 GB total), which happens to cross a 4 GB threshold. Real SF10 compressed
// parquet is ~6.12 MB per file (3.67 GB total) — BELOW 4 GB, so the original
// shuffleBuildThreshold never fires. This test simulates the real byte count
// and asserts the new 1 GB aggregateShuffleThreshold does the right thing.
//
// Regression guard: if someone raises the default threshold back above
// real SF10 lineitem bytes, this test fails.
func TestPickAggregateShuffleCandidate_SF10RealisticBytes(t *testing.T) {
	// These fixtures exercise the UNreduced decorrelated shape; the
	// scalar-agg semijoin reduction (on by default) rewrites it so the
	// aggregate is no longer scan-rooted. Pin it off for this test.
	old := logical.ScalarAggSemijoin.Load()
	logical.ScalarAggSemijoin.Store(false)
	defer logical.ScalarAggSemijoin.Store(old)
	cat, ctx := setupTPCHCatalog(t)
	// Rewrite the catalog's lineitem file-size entries to match production
	// SF10 bytes measured via `aws s3 ls`: 3,673,081,624 bytes across 600
	// files = ~6.12 MB/file.
	//
	// NOTE: setupTPCHCatalog populates each lineitem file at 10 MB. We
	// can't rewrite the manifest trivially through the public API, so
	// construct the stages with bytes at the known real size and exercise
	// the Pick directly rather than through the planner.
	const sf10LineitemTotal = int64(3673081624)
	const sf10OrdersTotal = int64(981977465)
	sql := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
		  )`
	stages := sqlToStages(t, cat, ctx, sql, 3)

	// Rewrite lineitem scan EstimatedBytes to the real SF10 total.
	for i := range stages {
		if stages[i].Type != "scan" {
			continue
		}
		switch stages[i].TableName {
		case "lineitem":
			stages[i].EstimatedBytes = sf10LineitemTotal
		case "orders":
			stages[i].EstimatedBytes = sf10OrdersTotal
		}
	}

	const fourGB = int64(4 * 1024 * 1024 * 1024)
	const oneGB = int64(1 * 1024 * 1024 * 1024)

	diag4 := PickAggregateShuffleCandidateDiag(stages, fourGB)
	if diag4.Reason == AggShuffleRejectNone {
		t.Errorf("SF10 @ 4 GB threshold: expected rejection (below_threshold), got match with InputScanBytes=%d — old default threshold should not trigger at SF10 compressed size %d",
			diag4.Candidate.InputScanBytes, sf10LineitemTotal)
	} else if diag4.Reason != AggShuffleRejectBelowThreshold {
		t.Errorf("SF10 @ 4 GB threshold: expected below_threshold rejection, got %s (observed bytes=%d)",
			diag4.Reason.String(), diag4.ObservedScanBytes)
	}
	t.Logf("SF10 @ 4 GB: correctly rejected as %s (observed=%d)", diag4.Reason.String(), diag4.ObservedScanBytes)

	diag1 := PickAggregateShuffleCandidateDiag(stages, oneGB)
	if diag1.Reason != AggShuffleRejectNone {
		t.Fatalf("SF10 @ 1 GB threshold: expected match, got rejection=%s observed=%d",
			diag1.Reason.String(), diag1.ObservedScanBytes)
	}
	if diag1.Candidate.InputScanBytes != sf10LineitemTotal {
		t.Errorf("SF10 @ 1 GB: expected InputScanBytes=%d (real SF10 lineitem), got %d",
			sf10LineitemTotal, diag1.Candidate.InputScanBytes)
	}
	t.Logf("SF10 @ 1 GB: correctly matched — agg=%s scan=%s(%d bytes)",
		diag1.Candidate.AggregateStageID, diag1.Candidate.InputScanAlias, diag1.Candidate.InputScanBytes)
}

// TestPickAggregateShuffleCandidate_Q20RejectedWithFilter verifies that Q20
// (which has l_shipdate filters pushed to its inner scan) is rejected by
// the Phase 1 detector. The substitution pass rejects scans with filters
// because the pre-compute SQL currently can't carry predicate signatures
// through the worker-side match. Rejecting early avoids wasting a
// pre-compute dispatch that the worker would then decline to substitute.
func TestPickAggregateShuffleCandidate_Q20RejectedWithFilter(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	sql := `SELECT s_name, s_address FROM supplier JOIN nation ON s_nationkey = n_nationkey
	  WHERE n_name = 'CANADA' AND s_suppkey IN (
	    SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (
	      SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
	    ) AND ps_availqty > (
	      SELECT 0.5 * SUM(l_quantity) FROM lineitem
	      WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
	        AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
	    )
	  ) ORDER BY s_name`
	stages := sqlToStages(t, cat, ctx, sql, 3)
	if _, ok := PickAggregateShuffleCandidate(stages, 4*1024*1024*1024); ok {
		t.Error("Q20: expected no candidate (inner scan has filter predicates); Phase 2 will handle this shape")
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
