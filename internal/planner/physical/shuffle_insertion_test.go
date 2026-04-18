package physical

import "testing"

func TestPickShuffleCandidate_PicksLargestBuildAboveThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},      // 8 GB - above
		{ID: "scan-customer", Type: "scan", ScanAlias: "customer", EstimatedBytes: 100 << 20}, // 100 MB - below
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},  // 80 GB - probe
		// join-1: lineitem (probe, left) JOIN orders (build, right) — LeftDepStage must be the probe scan.
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", LeftDepStage: "scan-lineitem", JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}, Dependencies: []string{"scan-lineitem", "scan-orders"}},
		// join-2: left dep is join-1 (not the probe scan directly) — skipped by candidate selection.
		{ID: "join-2", Type: "hash_join", BuildTableAlias: "customer", LeftDepStage: "join-1", JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"}, Dependencies: []string{"join-1", "scan-customer"}},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30 /* threshold */)
	if !ok {
		t.Fatal("expected shuffle candidate")
	}
	if cand.BuildAlias != "orders" {
		t.Errorf("BuildAlias = %q, want orders", cand.BuildAlias)
	}
	if cand.ProbeAlias != "lineitem" {
		t.Errorf("ProbeAlias = %q, want lineitem", cand.ProbeAlias)
	}
	if len(cand.JoinKeys) != 1 || cand.JoinKeys[0] != "o_orderkey" {
		t.Errorf("JoinKeys = %v, want [o_orderkey]", cand.JoinKeys)
	}
	if cand.JoinStageID != "join-1" {
		t.Errorf("JoinStageID = %q, want join-1", cand.JoinStageID)
	}
	if cand.BuildBytes != 8<<30 {
		t.Errorf("BuildBytes = %d, want %d", cand.BuildBytes, int64(8<<30))
	}
}

func TestPickShuffleCandidate_NoCandidateBelowThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 1 << 30}, // 1 GB - below 4 GB threshold
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no shuffle candidate below threshold")
	}
}

func TestPickShuffleCandidate_BuildEqualsProbeAliasIsRejected(t *testing.T) {
	// Pathological / self-join case: if the only "build" is the same scan as the
	// probe, we shouldn't shuffle (no separate build to partition).
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 80 << 30},
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", JoinLeftKeys: []string{"a"}, JoinRightKeys: []string{"b"}},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no candidate when build alias == probe alias")
	}
}

func TestPickShuffleCandidate_NoJoins(t *testing.T) {
	stages := []Stage{
		{ID: "scan-only", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no candidate when there are no join stages")
	}
}

// TestPickShuffleCandidate_RejectsIndirectProbeJoin checks that a join where the
// build is above threshold but the probe (left dep) is not the probeAlias scan
// directly is NOT selected. This guards against "key not in schema" at shuffle
// time when the JoinLeftKeys belong to an intermediate join output rather than
// the raw probeAlias Parquet files (the Q05/Q07/Q10 regression pattern).
func TestPickShuffleCandidate_RejectsIndirectProbeJoin(t *testing.T) {
	// Simulates Q05: nation is the build, lineitem is the probe, but the only
	// join stage referencing nation has LeftDepStage=join-1 (not scan-lineitem).
	// JoinLeftKeys=[s_nationkey] would fail against raw lineitem files.
	stages := []Stage{
		{ID: "scan-nation", Type: "scan", ScanAlias: "nation", EstimatedBytes: 8 << 30},
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		{ID: "scan-supplier", Type: "scan", ScanAlias: "supplier", EstimatedBytes: 500 << 20},
		// join-1: lineitem JOIN supplier — probe (lineitem) is direct left dep.
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "supplier", LeftDepStage: "scan-lineitem",
			JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"}},
		// join-2: join-1 JOIN nation — left dep is join-1, not scan-lineitem.
		// JoinLeftKeys=[s_nationkey] belongs to supplier's output, not raw lineitem.
		{ID: "join-2", Type: "hash_join", BuildTableAlias: "nation", LeftDepStage: "join-1",
			JoinLeftKeys: []string{"s_nationkey"}, JoinRightKeys: []string{"n_nationkey"}},
	}
	// With threshold below supplier (500 MB) and nation (8 GB), both qualify.
	// join-1 (supplier build) should be selected because it directly connects probe.
	// join-2 (nation build) must be rejected because LeftDepStage != probeScanID.
	cand, ok := PickShuffleCandidate(stages, 100<<20)
	if !ok {
		t.Fatal("expected shuffle candidate for join-1 (direct probe join)")
	}
	if cand.BuildAlias != "supplier" {
		t.Errorf("BuildAlias = %q, want supplier (join-1 is the only valid direct candidate)", cand.BuildAlias)
	}
	if cand.JoinStageID != "join-1" {
		t.Errorf("JoinStageID = %q, want join-1", cand.JoinStageID)
	}
}

func TestPickShuffleCandidate_MatchesBroadcastJoin(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		// Same shape as the hash_join test, but the join is classified as broadcast_join.
		// LeftDepStage must be the probe scan for the candidate to be selected.
		{ID: "join-1", Type: "broadcast_join", BuildTableAlias: "orders",
			LeftDepStage: "scan-lineitem",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30)
	if !ok {
		t.Fatal("expected shuffle candidate for broadcast_join above threshold")
	}
	if cand.JoinKeys[0] != "o_orderkey" {
		t.Errorf("JoinKeys = %v, want [o_orderkey]", cand.JoinKeys)
	}
}
