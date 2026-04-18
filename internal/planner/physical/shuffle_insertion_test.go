package physical

import "testing"

func TestPickShuffleCandidate_PicksLargestBuildAboveThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},      // 8 GB - above
		{ID: "scan-customer", Type: "scan", ScanAlias: "customer", EstimatedBytes: 100 << 20}, // 100 MB - below
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},  // 80 GB - probe
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}, Dependencies: []string{"scan-orders", "scan-lineitem"}},
		{ID: "join-2", Type: "hash_join", BuildTableAlias: "customer", JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"}, Dependencies: []string{"join-1", "scan-customer"}},
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
