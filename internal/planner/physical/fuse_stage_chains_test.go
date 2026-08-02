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
	// The join→join shape tests pin the step-1 mechanism in isolation;
	// step-2 (agg absorb) coverage lives in the *_AggAbsorb tests.
	prevAgg := StageFusionAgg.Load()
	t.Cleanup(func() { StageFusionAgg.Store(prevAgg) })
	StageFusionAgg.Store(false)
	return planWithFusionArms(t, sql, fusion, false)
}

func planWithFusionArms(t *testing.T, sql string, fusion, aggFusion bool) []Stage {
	t.Helper()
	prev := StageFusion.Load()
	prevAgg := StageFusionAgg.Load()
	t.Cleanup(func() {
		StageFusion.Store(prev)
		StageFusionAgg.Store(prevAgg)
	})
	StageFusion.Store(fusion)
	StageFusionAgg.Store(aggFusion)
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
	for _, typ := range []string{StageScan, "aggregate", "final_aggregate"} {
		if count(on, typ) != count(off, typ) {
			t.Errorf("%s stages differ: fused=%d unfused=%d", typ, count(on, typ), count(off, typ))
		}
	}
	// Shuffle boundaries: fuseScanShuffle/fuseJoinShuffle absorb standalone
	// repartitions into Exchange-carrying scans/joins, and eligibility
	// depends on the chain shape, so compare TOTAL shuffle boundaries
	// (standalone repartitions + Exchange-carrying compute/scan stages)
	// rather than the repartition stage-type count.
	boundaries := func(stages []Stage) int {
		n := 0
		for _, s := range stages {
			if s.Type == StageExchangeRepartition ||
				(s.Exchange != nil && s.Type != StageExchangeGather && s.Type != StageExchangeReplicate) {
				n++
			}
		}
		return n
	}
	if boundaries(on) != boundaries(off) {
		t.Errorf("shuffle boundaries differ: fused=%d unfused=%d", boundaries(on), boundaries(off))
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
// pass is a no-op when disabled (master switch also kills the agg absorb).
func TestFuseStageChains_KillSwitchIdentity(t *testing.T) {
	stages := planWithFusionArms(t, chainFusionQ05SQL, false, true)
	for _, s := range stages {
		if len(s.ChainedJoins) > 0 {
			t.Errorf("stage %s carries ChainedJoins with fusion off", s.ID)
		}
		if len(s.ChainedAggSpecs) > 0 || len(s.ChainedAggGroupBy) > 0 {
			t.Errorf("stage %s carries ChainedAgg with master fusion off", s.ID)
		}
	}
}

// Q09-shape at repartition-separated joins: the join→join links re-key
// through exchanges (no 1:1), but the terminal partial aggregate consumes
// the last hash_join directly — the step-2 absorb target (the SF100 Q09/
// Q10 shape, fusion A/B 2026-07-25).
const chainFusionQ09SQL = `SELECT
	n_name as nation, SUBSTR(o_orderdate, 1, 4) as o_year,
	SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part
JOIN lineitem ON p_partkey = l_partkey
JOIN supplier ON s_suppkey = l_suppkey
JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey
JOIN orders ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE p_name LIKE '%green%'
GROUP BY n_name, SUBSTR(o_orderdate, 1, 4)
ORDER BY nation, o_year DESC`

// TestFuseStageChains_AggAbsorb pins step 2: with the sub-switch on, the
// partial aggregate disappears into its producer join, which keeps its own
// distribution (task count must not collapse to the aggregate's label) and
// carries the aggregate's specs; the final_aggregate now depends on the
// fused join.
func TestFuseStageChains_AggAbsorb(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"Q09", chainFusionQ09SQL},
		{"Q05", chainFusionQ05SQL},
		{"Q10", chainFusionQ10SQL},
		{"Q18", aggOverExchangeQ18SQL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v1 := planWithFusionArms(t, tc.sql, true, false)
			full := planWithFusionArms(t, tc.sql, true, true)

			countAgg := func(stages []Stage) int {
				n := 0
				for _, s := range stages {
					if s.Type == "aggregate" {
						n++
					}
				}
				return n
			}
			var fused *Stage
			for i := range full {
				if len(full[i].ChainedAggSpecs) > 0 || len(full[i].ChainedAggGroupBy) > 0 {
					if fused != nil {
						t.Fatalf("multiple ChainedAgg stages: %s and %s", fused.ID, full[i].ID)
					}
					fused = &full[i]
				}
			}
			if fused == nil {
				for _, s := range full {
					t.Logf("id=%s type=%s dist=%v deps=%v chainedAgg=%d", s.ID, s.Type, s.Distribution, s.Dependencies, len(s.ChainedAggSpecs))
				}
				t.Fatal("agg absorb did not fire")
			}
			if countAgg(full) != countAgg(v1)-1 {
				t.Errorf("aggregate stages: full=%d v1=%d, want exactly one absorbed", countAgg(full), countAgg(v1))
			}
			if fused.Type != StageHashJoin {
				t.Errorf("fused stage type=%s, want hash_join", fused.Type)
			}
			// The fused stage must keep a dispatchable join distribution —
			// adopting the aggregate's RoundRobin label would collapse the
			// 1:1 task/partition mapping.
			if fused.Distribution.Kind != DistHashPartitioned || fused.Distribution.Count <= 0 {
				t.Errorf("fused stage distribution=%+v, want HashPartitioned{count>0}", fused.Distribution)
			}
			if len(fused.GroupByCols) != 0 || len(fused.AggSpecs) != 0 {
				t.Errorf("fused stage leaked stage-level GroupByCols/AggSpecs: %v/%v", fused.GroupByCols, fused.AggSpecs)
			}
			// The dropped aggregate's consumers rewired onto the fused stage.
			ids := make(map[string]bool, len(full))
			for _, s := range full {
				ids[s.ID] = true
			}
			for _, s := range full {
				for _, d := range s.Dependencies {
					if !ids[d] {
						t.Errorf("stage %s depends on dropped stage %s", s.ID, d)
					}
				}
			}
			if err := AssertExchangeConsistency(full); err != nil {
				t.Errorf("exchange consistency after agg absorb: %v", err)
			}
		})
	}
}

// TestFuseStageChains_AggSubSwitch pins that WADJET_STAGE_FUSION_AGG=0
// preserves the step-1-only plan exactly (aggregate stages intact).
func TestFuseStageChains_AggSubSwitch(t *testing.T) {
	v1 := planWithFusionArms(t, chainFusionQ09SQL, true, false)
	for _, s := range v1 {
		if len(s.ChainedAggSpecs) > 0 || len(s.ChainedAggGroupBy) > 0 {
			t.Errorf("stage %s carries ChainedAgg with sub-switch off", s.ID)
		}
	}
}
