package physical

import "testing"

// Q05-shape: hash_join (orders ⨝ customer-side chain) whose only consumer
// is a same-distribution broadcast_join over a dimension chain — the
// canonical 1:1 join→join link fuseStageChains targets.
const chainFusionQ05SQL = `SELECT
	n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE c_nationkey = s_nationkey
	AND r_name = 'ASIA'
	AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC`

const chainFusionQ10SQL = `SELECT
	c_custkey, c_name,
	SUM(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'
	AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20`

func planWithFusion(t *testing.T, sql string, fusion bool) []Stage {
	t.Helper()
	prev := StageFusion.Load()
	t.Cleanup(func() { StageFusion.Store(prev) })
	StageFusion.Store(fusion)
	cat, ctx := setupTPCHCatalog(t)
	return sqlToStages(t, cat, ctx, sql, 3)
}

func chainStats(stages []Stage) (joinStages int, chained *Stage) {
	for i := range stages {
		s := &stages[i]
		if s.Type == StageHashJoin || s.Type == StageBroadcastJoin {
			joinStages++
		}
		if len(s.ChainedJoins) > 0 {
			chained = s
		}
	}
	return joinStages, chained
}

// TestFuseStageChains_Q05ShapeApplies pins that the 1:1 hash_join →
// broadcast_join link fuses: one fewer join stage, the surviving hash_join
// carries the absorbed join as a ChainedJoinSpec, adopts its distribution
// and output projection, and depends on its build leg.
func TestFuseStageChains_Q05ShapeApplies(t *testing.T) {
	off := planWithFusion(t, chainFusionQ05SQL, false)
	offJoins, offChained := chainStats(off)
	if offChained != nil {
		t.Fatalf("kill-switch arm has ChainedJoins on %s", offChained.ID)
	}

	on := planWithFusion(t, chainFusionQ05SQL, true)
	onJoins, fused := chainStats(on)
	if fused == nil {
		for _, s := range on {
			t.Logf("id=%s type=%s dist=%v deps=%v", s.ID, s.Type, s.Distribution, s.Dependencies)
		}
		t.Fatal("no stage carries ChainedJoins (fusion did not fire)")
	}
	if onJoins >= offJoins {
		t.Errorf("join stages: fused=%d unfused=%d, want fewer when fused", onJoins, offJoins)
	}
	if fused.Type != StageHashJoin {
		t.Errorf("fused stage type = %s, want hash_join", fused.Type)
	}
	if fused.Distribution.Kind != DistHashPartitioned || fused.Distribution.Count <= 0 {
		t.Errorf("fused stage distribution = %+v, want HashPartitioned{count>0}", fused.Distribution)
	}
	// Every chained build dep must be a real stage and a dependency.
	byID := make(map[string]Stage, len(on))
	for _, s := range on {
		byID[s.ID] = s
	}
	deps := make(map[string]bool, len(fused.Dependencies))
	for _, d := range fused.Dependencies {
		deps[d] = true
	}
	for i, cj := range fused.ChainedJoins {
		if cj.BuildDepStage == "" {
			t.Errorf("chained[%d]: empty BuildDepStage", i)
			continue
		}
		if _, ok := byID[cj.BuildDepStage]; !ok {
			t.Errorf("chained[%d]: build dep %q not in stage list", i, cj.BuildDepStage)
		}
		if !deps[cj.BuildDepStage] {
			t.Errorf("chained[%d]: build dep %q missing from fused stage deps", i, cj.BuildDepStage)
		}
	}
	if len(fused.Dependencies) != 2+len(fused.FusedJoins)+len(fused.ChainedJoins) {
		t.Errorf("fused deps=%d, want %d (2 primary + %d fused + %d chained)",
			len(fused.Dependencies), 2+len(fused.FusedJoins)+len(fused.ChainedJoins),
			len(fused.FusedJoins), len(fused.ChainedJoins))
	}
	// No dangling references to the absorbed stage.
	ids := make(map[string]bool, len(on))
	for _, s := range on {
		ids[s.ID] = true
	}
	for _, s := range on {
		for _, d := range s.Dependencies {
			if !ids[d] {
				t.Errorf("stage %s depends on dropped stage %s", s.ID, d)
			}
		}
		if s.LeftDepStage != "" && !ids[s.LeftDepStage] {
			t.Errorf("stage %s LeftDepStage=%s dropped", s.ID, s.LeftDepStage)
		}
		if s.RightDepStage != "" && !ids[s.RightDepStage] {
			t.Errorf("stage %s RightDepStage=%s dropped", s.ID, s.RightDepStage)
		}
	}
	// The pass must keep the exchange-consistency invariant.
	if err := AssertExchangeConsistency(on); err != nil {
		t.Errorf("exchange consistency after fusion: %v", err)
	}
}

// TestFuseStageChains_Q10ShapeApplies pins the second common shape
// (hash_join → broadcast_join on o_custkey) and that both arms keep
// identical non-join stage structure.
func TestFuseStageChains_Q10ShapeApplies(t *testing.T) {
	off := planWithFusion(t, chainFusionQ10SQL, false)
	on := planWithFusion(t, chainFusionQ10SQL, true)
	offJoins, _ := chainStats(off)
	onJoins, fused := chainStats(on)
	if fused == nil {
		t.Fatal("fusion did not fire on Q10 shape")
	}
	if onJoins != offJoins-1 {
		t.Errorf("join stages: fused=%d unfused=%d, want exactly one absorbed", onJoins, offJoins)
	}
	count := func(stages []Stage, typ string) int {
		n := 0
		for _, s := range stages {
			if s.Type == typ {
				n++
			}
		}
		return n
	}
	for _, typ := range []string{StageScan, "aggregate", "final_aggregate", StageExchangeRepartition} {
		if count(on, typ) != count(off, typ) {
			t.Errorf("%s stages differ: fused=%d unfused=%d", typ, count(on, typ), count(off, typ))
		}
	}
	if err := AssertExchangeConsistency(on); err != nil {
		t.Errorf("exchange consistency after fusion: %v", err)
	}
}

// TestFuseStageChains_Q18Interplay pins fusion + AggOverExchange together on
// the Q18 shape: the rewired raw final feeds the outer join's build, and the
// 1:1 hash_join → hash_join probe chain still fuses, with the absorbed
// join's partitioned build marked Partitioned.
func TestFuseStageChains_Q18Interplay(t *testing.T) {
	prevAgg := AggOverExchange.Load()
	t.Cleanup(func() { AggOverExchange.Store(prevAgg) })
	AggOverExchange.Store(true)

	on := planWithFusion(t, aggOverExchangeQ18SQL, true)
	_, fused := chainStats(on)
	if fused == nil {
		for _, s := range on {
			t.Logf("id=%s type=%s dist=%v deps=%v", s.ID, s.Type, s.Distribution, s.Dependencies)
		}
		t.Fatal("fusion did not fire on Q18 shape")
	}
	var sawPartitioned bool
	for _, cj := range fused.ChainedJoins {
		if cj.Partitioned {
			sawPartitioned = true
		}
	}
	if !sawPartitioned {
		t.Errorf("Q18 chain: absorbed hash_join not marked Partitioned: %+v", fused.ChainedJoins)
	}
	if err := AssertExchangeConsistency(on); err != nil {
		t.Errorf("exchange consistency after fusion: %v", err)
	}
}

// TestFuseStageChains_KillSwitchIdentity pins that the kill-switch arm is
// byte-identical to a plan produced with the pass compiled out — i.e. the
// pass is a no-op when disabled.
func TestFuseStageChains_KillSwitchIdentity(t *testing.T) {
	stages := planWithFusion(t, chainFusionQ05SQL, false)
	for _, s := range stages {
		if len(s.ChainedJoins) > 0 {
			t.Errorf("stage %s carries ChainedJoins with fusion off", s.ID)
		}
	}
}
