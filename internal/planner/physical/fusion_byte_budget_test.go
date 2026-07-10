package physical

import (
	"testing"
)

// TestFuseJoinStages_SkipsLargeBuilds verifies the byte-budget guard:
// fuseJoinStages refuses to absorb a broadcast join whose build-side scan
// is above maxFusedBuildBytes. Above the threshold, the cluster-wide S3
// amplification of replicating the cache to every probe-split shard task
// outweighs the savings from skipping the intermediate materialization.
func TestFuseJoinStages_SkipsLargeBuilds(t *testing.T) {
	orig := maxFusedBuildBytes
	t.Cleanup(func() { maxFusedBuildBytes = orig })

	// Lower the threshold so we can build a tiny test case.
	maxFusedBuildBytes = 1024

	build := func(bytes int64) []Stage {
		return []Stage{
			// Outer join's probe upstream
			{ID: "scan-probe", Type: "scan"},
			// Outer join (the consumer that would absorb the leaf)
			{
				ID: "join-outer", Type: "broadcast_join",
				Dependencies: []string{"scan-probe", "join-leaf"},
				LeftDepStage: "join-leaf", RightDepStage: "scan-probe", // candidate feeds the PROBE side (fusion requirement since 2026-07-09)
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			// Leaf broadcast join (candidate for fusion)
			{
				ID: "join-leaf", Type: "broadcast_join",
				Dependencies: []string{"scan-leaf-probe", "scan-leaf-build"},
				LeftDepStage: "scan-leaf-probe", RightDepStage: "scan-leaf-build",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			{ID: "scan-leaf-probe", Type: "scan"},
			{
				ID: "scan-leaf-build", Type: "scan",
				EstimatedBytes: bytes,
			},
		}
	}

	t.Run("below_threshold_fuses", func(t *testing.T) {
		stages := build(maxFusedBuildBytes / 2)
		out := fuseJoinStages(stages)
		// join-leaf should be absorbed into join-outer.
		var outer *Stage
		for i := range out {
			if out[i].ID == "join-outer" {
				outer = &out[i]
			}
		}
		if outer == nil {
			t.Fatal("expected join-outer to remain")
		}
		if got := len(outer.FusedJoins); got != 1 {
			t.Errorf("small build: expected 1 fused join, got %d", got)
		}
		// join-leaf removed.
		for _, s := range out {
			if s.ID == "join-leaf" {
				t.Error("small build: join-leaf should have been absorbed")
			}
		}
	})

	t.Run("above_threshold_does_not_fuse", func(t *testing.T) {
		stages := build(maxFusedBuildBytes * 2)
		out := fuseJoinStages(stages)
		// join-leaf should NOT be absorbed.
		var outer, leaf *Stage
		for i := range out {
			switch out[i].ID {
			case "join-outer":
				outer = &out[i]
			case "join-leaf":
				leaf = &out[i]
			}
		}
		if leaf == nil {
			t.Fatal("large build: join-leaf should have remained (not fused)")
		}
		if outer == nil {
			t.Fatal("large build: join-outer should have remained")
		}
		if got := len(outer.FusedJoins); got != 0 {
			t.Errorf("large build: expected 0 fused joins on outer, got %d", got)
		}
	})
}

// TestFuseJoinStages_BuildBytesViaExchangeReplicate covers the typical native-DAG
// shape where the build side is wrapped by an exchange-replicate stage:
//
//	join-leaf → exchange-replicate-X → scan
//
// The byte-budget guard must walk through the exchange-replicate to the
// underlying scan, otherwise it would over-restrict (treat the exchange
// stage as having unknown bytes and skip the safety check).
func TestFuseJoinStages_BuildBytesViaExchangeReplicate(t *testing.T) {
	orig := maxFusedBuildBytes
	t.Cleanup(func() { maxFusedBuildBytes = orig })
	maxFusedBuildBytes = 1024

	build := func(bytes int64) []Stage {
		return []Stage{
			{ID: "scan-probe", Type: "scan"},
			{
				ID: "join-outer", Type: "broadcast_join",
				Dependencies: []string{"scan-probe", "join-leaf"},
				LeftDepStage: "join-leaf", RightDepStage: "scan-probe", // candidate feeds the PROBE side (fusion requirement since 2026-07-09)
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			{
				ID: "join-leaf", Type: "broadcast_join",
				Dependencies: []string{"scan-leaf-probe", "exchange-replicate-leaf"},
				LeftDepStage: "scan-leaf-probe", RightDepStage: "exchange-replicate-leaf",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			{ID: "scan-leaf-probe", Type: "scan"},
			{
				ID: "exchange-replicate-leaf", Type: StageExchangeReplicate,
				Dependencies: []string{"scan-leaf-build"},
			},
			{
				ID: "scan-leaf-build", Type: "scan",
				EstimatedBytes: bytes,
			},
		}
	}

	t.Run("small_build_fuses_through_replicate", func(t *testing.T) {
		stages := build(maxFusedBuildBytes / 2)
		out := fuseJoinStages(stages)
		var outer *Stage
		for i := range out {
			if out[i].ID == "join-outer" {
				outer = &out[i]
			}
		}
		if outer == nil || len(outer.FusedJoins) != 1 {
			t.Errorf("small build via exchange-replicate: expected fusion, outer=%v", outer)
		}
	})

	t.Run("large_build_skipped_through_replicate", func(t *testing.T) {
		stages := build(maxFusedBuildBytes * 2)
		out := fuseJoinStages(stages)
		var outer *Stage
		for i := range out {
			if out[i].ID == "join-outer" {
				outer = &out[i]
			}
		}
		if outer == nil {
			t.Fatal("expected join-outer to remain")
		}
		if got := len(outer.FusedJoins); got != 0 {
			t.Errorf("large build via exchange-replicate: expected NO fusion (skip), got %d fused", got)
		}
	})
}
