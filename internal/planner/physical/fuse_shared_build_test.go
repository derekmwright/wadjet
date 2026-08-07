package physical

import (
	"context"
	"testing"
)

// planQ21Shuffled plans Q21 with broadcast pinned off (hash-shuffle regime,
// the SF100 shape) and stage fusion pinned to the given arm.
func planQ21Shuffled(t *testing.T, fusion bool) []Stage {
	t.Helper()
	prev := StageFusion.Load()
	prevAgg := StageFusionAgg.Load()
	t.Cleanup(func() {
		StageFusion.Store(prev)
		StageFusionAgg.Store(prevAgg)
	})
	StageFusion.Store(fusion)
	StageFusionAgg.Store(false)
	cat, ctx := setupTPCHCatalog(t)
	return sqlToStagesShuffled(t, cat, ctx, q21SQL)
}

// Q21's semi and anti joins both build from the SAME lineitem repartition.
// The dep-aliasing rule used to force them into two stages that each
// rebuilt that exchange and materialized the 7M-row semi→anti link
// (2026-08-06 SF100 wlogs: join-12 24-46s + join-16 33-52s summed CPU per
// run). The shared-primary-build exception fuses them: one stage, the anti
// riding as a ChainedJoinSpec whose build dep duplicates the primary's.
func TestFuseStageChains_SharedBuildSemiAnti(t *testing.T) {
	off := planQ21Shuffled(t, false)
	var offSemi, offAnti *Stage
	for i := range off {
		s := &off[i]
		if s.Type == StageHashJoin && s.JoinType == "semi" {
			offSemi = s
		}
		if s.Type == StageHashJoin && s.JoinType == "anti" {
			offAnti = s
		}
	}
	if offSemi == nil || offAnti == nil {
		t.Fatalf("kill-switch arm must have separate semi+anti stages; stages:\n%s", dumpStageIDs(off))
	}
	if offSemi.RightDepStage != offAnti.RightDepStage {
		t.Fatalf("fixture drift: semi build %s != anti build %s — this test pins the shared-build shape",
			offSemi.RightDepStage, offAnti.RightDepStage)
	}

	on := planQ21Shuffled(t, true)
	var fused *Stage
	antiStages := 0
	for i := range on {
		s := &on[i]
		if s.Type == StageHashJoin && s.JoinType == "anti" {
			antiStages++
		}
		if s.Type == StageHashJoin && s.JoinType == "semi" && len(s.ChainedJoins) > 0 {
			fused = s
		}
	}
	if fused == nil {
		t.Fatalf("semi stage did not absorb the anti; stages:\n%s", dumpStageIDs(on))
	}
	if antiStages != 0 {
		t.Fatalf("anti stage survived fusion (%d anti stages remain)", antiStages)
	}
	var chainedAnti *ChainedJoinSpec
	for i := range fused.ChainedJoins {
		if fused.ChainedJoins[i].JoinType == "anti" {
			chainedAnti = &fused.ChainedJoins[i]
		}
	}
	if chainedAnti == nil {
		t.Fatalf("no chained anti spec on fused stage %s: %+v", fused.ID, fused.ChainedJoins)
	}
	if chainedAnti.BuildDepStage != fused.RightDepStage {
		t.Errorf("chained anti build dep %s != primary build %s — the shared-build exception should have fired",
			chainedAnti.BuildDepStage, fused.RightDepStage)
	}
	if !chainedAnti.Partitioned {
		t.Error("chained anti must be Partitioned (hash-partitioned 1:1 slicing)")
	}
	// The duplicate dep entry keeps the 2+fused+chained arithmetic intact.
	if err := ValidateNativeDAGShape(on); err != nil {
		t.Fatalf("fused plan fails native-DAG validation: %v", err)
	}
	occurrences := 0
	for _, d := range fused.Dependencies {
		if d == fused.RightDepStage {
			occurrences++
		}
	}
	if occurrences != 2 {
		t.Errorf("build dep %s appears %d times in deps %v, want 2 (primary + chained)",
			fused.RightDepStage, occurrences, fused.Dependencies)
	}
	// The sabf pass must still see 2 logical semi/anti builds through the
	// fused shape (primary + chained) and mark the raw lineitem scan —
	// losing the mark would silently revert the Q21 −19.7% win.
	emitters, consumers := findSemiAntiMarks(on)
	if len(emitters) == 0 || len(consumers) != 1 {
		t.Fatalf("sabf marks lost after fusion: emitters=%d consumers=%d; stages:\n%s",
			len(emitters), len(consumers), dumpStageIDs(on))
	}
}

// A consumer whose build dep aliases P's PROBE dep (not its build) must
// stay unfused — slicing a probe-shaped output as a build is not aligned.
func TestFuseStageChains_ProbeAliasStillBlocked(t *testing.T) {
	prev := StageFusion.Load()
	t.Cleanup(func() { StageFusion.Store(prev) })
	StageFusion.Store(true)
	stages := []Stage{
		{ID: "up-probe", Type: StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
			Dependencies: []string{"leaf-a"}},
		{ID: "leaf-a", Type: StageScan, TableName: "t1", ScanFiles: []string{"f"}, Columns: []string{"k"}},
		{ID: "up-build", Type: StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: 4},
			Dependencies: []string{"leaf-b"}},
		{ID: "leaf-b", Type: StageScan, TableName: "t2", ScanFiles: []string{"f"}, Columns: []string{"k"}},
		{ID: "p", Type: StageHashJoin, JoinType: "inner",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			LeftDepStage: "up-probe", RightDepStage: "up-build",
			Dependencies: []string{"up-probe", "up-build"},
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4}},
		// C builds from P's PROBE dep — must not fuse.
		{ID: "c", Type: StageHashJoin, JoinType: "semi",
			JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			LeftDepStage: "p", RightDepStage: "up-probe",
			Dependencies: []string{"p", "up-probe"},
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 4}},
	}
	out := fuseStageChains(stages)
	for i := range out {
		if len(out[i].ChainedJoins) > 0 {
			t.Fatalf("probe-dep alias fused on %s — must stay blocked", out[i].ID)
		}
	}
	_ = context.Background()
}
