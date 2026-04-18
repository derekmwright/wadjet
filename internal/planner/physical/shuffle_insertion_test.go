package physical

import "testing"

func TestPickShuffleCandidate_PicksLargestBuildAboveThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},      // 8 GB - above
		{ID: "scan-customer", Type: "scan", ScanAlias: "customer", EstimatedBytes: 100 << 20}, // 100 MB - below
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},  // 80 GB - probe
		// join-1: lineitem (probe, left) JOIN orders (build, right).
		// LeftDepStage=scan-lineitem (probe scan directly), RightDepStage=scan-orders (candidate).
		{
			ID: "join-1", Type: "hash_join", BuildTableAlias: "orders",
			LeftDepStage: "scan-lineitem", RightDepStage: "scan-orders",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
			Dependencies: []string{"scan-lineitem", "scan-orders"},
		},
		// join-2: left dep is join-1 (not the probe scan directly) — skipped by candidate selection.
		{
			ID: "join-2", Type: "hash_join", BuildTableAlias: "customer",
			LeftDepStage: "join-1", RightDepStage: "scan-customer",
			JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"},
			Dependencies: []string{"join-1", "scan-customer"},
		},
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
		{
			ID: "join-1", Type: "hash_join", BuildTableAlias: "orders",
			LeftDepStage: "scan-lineitem", RightDepStage: "scan-orders",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
		},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no shuffle candidate below threshold")
	}
}

// TestPickShuffleCandidate_PathologicalNoOtherScans checks that a query with only
// a single (probe) scan and no other above-threshold scans returns false. The old
// "build alias == probe alias" test is subsumed by this: with only one scan, there
// is no non-probe candidate above threshold.
func TestPickShuffleCandidate_PathologicalNoOtherScans(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 80 << 30},
		{
			ID: "join-1", Type: "hash_join",
			LeftDepStage: "scan-orders", JoinLeftKeys: []string{"a"}, JoinRightKeys: []string{"b"},
		},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no candidate when there is no other above-threshold non-probe scan")
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
// candidate is the right dep but the left dep (probe) is NOT the probe scan
// directly is rejected. This guards against "key not in schema" at shuffle time
// when JoinLeftKeys belong to an intermediate join output rather than the raw
// probeAlias Parquet files (the Q05/Q07/Q10 regression pattern).
func TestPickShuffleCandidate_RejectsIndirectProbeJoin(t *testing.T) {
	// nation (8 GB) is the candidate (largest non-probe above 100 MB threshold).
	// join-2 references nation as RightDepStage, but LeftDepStage=join-1 (indirect).
	// JoinLeftKeys=[s_nationkey] would fail against raw lineitem files, so it must
	// be rejected. No other join references nation, so result is ok=false.
	stages := []Stage{
		{ID: "scan-nation", Type: "scan", ScanAlias: "nation", EstimatedBytes: 8 << 30},
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		{ID: "scan-supplier", Type: "scan", ScanAlias: "supplier", EstimatedBytes: 500 << 20},
		// join-1: lineitem JOIN supplier — probe (lineitem) is direct left dep.
		{
			ID: "join-1", Type: "hash_join", BuildTableAlias: "supplier",
			LeftDepStage: "scan-lineitem", RightDepStage: "scan-supplier",
			JoinLeftKeys: []string{"l_suppkey"}, JoinRightKeys: []string{"s_suppkey"},
		},
		// join-2: join-1 JOIN nation — left dep is join-1, not scan-lineitem.
		// nation is the candidate, but LeftDepStage is indirect → rejected.
		{
			ID: "join-2", Type: "hash_join", BuildTableAlias: "nation",
			LeftDepStage: "join-1", RightDepStage: "scan-nation",
			JoinLeftKeys: []string{"s_nationkey"}, JoinRightKeys: []string{"n_nationkey"},
		},
	}
	// nation (8 GB) is the candidate; join-2 references it but has an indirect
	// left dep → rejected. No valid join found for nation → ok=false.
	_, ok := PickShuffleCandidate(stages, 100<<20)
	if ok {
		t.Error("expected no shuffle candidate: nation's only join has an indirect probe dep")
	}
}

func TestPickShuffleCandidate_MatchesBroadcastJoin(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		// Same shape as the hash_join test, but the join is classified as broadcast_join.
		// orders (8 GB) is the candidate; it's the right dep with lineitem as direct left dep.
		{
			ID: "join-1", Type: "broadcast_join", BuildTableAlias: "orders",
			LeftDepStage: "scan-lineitem", RightDepStage: "scan-orders",
			JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"},
		},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30)
	if !ok {
		t.Fatal("expected shuffle candidate for broadcast_join above threshold")
	}
	if cand.JoinKeys[0] != "o_orderkey" {
		t.Errorf("JoinKeys = %v, want [o_orderkey]", cand.JoinKeys)
	}
}

// TestPickShuffleCandidate_Q03Shape mirrors the actual physical plan structure
// of Q03: a single fused broadcast_join with lineitem as the planner-labeled
// build (largest, will be probe-partitioned at runtime), orders as the
// LeftDepStage (the runtime broadcast build we want to shuffle), customer
// fused as a secondary build below threshold.
func TestPickShuffleCandidate_Q03Shape(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},
		{ID: "scan-customer", Type: "scan", ScanAlias: "customer", EstimatedBytes: 1500 << 20},
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 75 << 30},
		{
			ID: "join-1", Type: "broadcast_join",
			BuildTableAlias: "lineitem",                    // planner's label; NOT what we shuffle
			LeftDepStage:    "scan-orders",                 // orders is the runtime broadcast build
			RightDepStage:   "scan-lineitem",               // lineitem is the probe (probe-split)
			JoinLeftKeys:    []string{"o_orderkey"},
			JoinRightKeys:   []string{"l_orderkey"},
			FusedJoins: []FusedJoinSpec{
				{
					BuildTableAlias: "customer",
					JoinLeftKeys:    []string{"o_custkey"},
					JoinRightKeys:   []string{"c_custkey"},
				},
			},
		},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30)
	if !ok {
		t.Fatal("expected shuffle candidate (orders)")
	}
	if cand.BuildAlias != "orders" {
		t.Errorf("BuildAlias = %q, want orders", cand.BuildAlias)
	}
	if cand.ProbeAlias != "lineitem" {
		t.Errorf("ProbeAlias = %q, want lineitem", cand.ProbeAlias)
	}
	if len(cand.BuildKeys) != 1 || cand.BuildKeys[0] != "o_orderkey" {
		t.Errorf("BuildKeys = %v, want [o_orderkey]", cand.BuildKeys)
	}
	if len(cand.ProbeKeys) != 1 || cand.ProbeKeys[0] != "l_orderkey" {
		t.Errorf("ProbeKeys = %v, want [l_orderkey]", cand.ProbeKeys)
	}
	if cand.JoinStageID != "join-1" {
		t.Errorf("JoinStageID = %q, want join-1", cand.JoinStageID)
	}
}

// TestPickShuffleCandidate_FusedJoinMatch covers the case where the shuffle
// candidate is referenced only via a FusedJoin entry, not the top-level join
// deps. The candidate (large_dim at 6 GB) is the build of a fused secondary
// join; the top-level join connects lineitem (probe) to supplier directly.
func TestPickShuffleCandidate_FusedJoinMatch(t *testing.T) {
	stages := []Stage{
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 75 << 30},
		{ID: "scan-supplier", Type: "scan", ScanAlias: "supplier", EstimatedBytes: 500 << 20},
		{ID: "scan-large_dim", Type: "scan", ScanAlias: "large_dim", EstimatedBytes: 6 << 30},
		{
			// Top-level join: lineitem (probe, left) JOIN supplier (build, right).
			// large_dim is fused in as a secondary build.
			ID: "join-1", Type: "broadcast_join",
			BuildTableAlias: "supplier",
			LeftDepStage:    "scan-lineitem",
			RightDepStage:   "scan-supplier",
			JoinLeftKeys:    []string{"l_suppkey"},
			JoinRightKeys:   []string{"s_suppkey"},
			FusedJoins: []FusedJoinSpec{
				{
					BuildTableAlias: "large_dim",
					JoinLeftKeys:    []string{"l_dimkey"},
					JoinRightKeys:   []string{"d_dimkey"},
				},
			},
		},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30)
	if !ok {
		t.Fatal("expected shuffle candidate (large_dim via fused join)")
	}
	if cand.BuildAlias != "large_dim" {
		t.Errorf("BuildAlias = %q, want large_dim", cand.BuildAlias)
	}
	if cand.ProbeAlias != "lineitem" {
		t.Errorf("ProbeAlias = %q, want lineitem", cand.ProbeAlias)
	}
	// FusedJoin keys: JoinRightKeys=d_dimkey (build), JoinLeftKeys=l_dimkey (probe).
	if len(cand.BuildKeys) != 1 || cand.BuildKeys[0] != "d_dimkey" {
		t.Errorf("BuildKeys = %v, want [d_dimkey]", cand.BuildKeys)
	}
	if len(cand.ProbeKeys) != 1 || cand.ProbeKeys[0] != "l_dimkey" {
		t.Errorf("ProbeKeys = %v, want [l_dimkey]", cand.ProbeKeys)
	}
	if cand.JoinStageID != "join-1" {
		t.Errorf("JoinStageID = %q, want join-1", cand.JoinStageID)
	}
}
