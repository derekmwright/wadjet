package physical

import "testing"

// joinFuseFixture builds the canonical join → exchange → consumer DAG with
// the join hash-partitioned at joinCount and the exchange at exCount — the
// width relationship is the variable under test for the #280 gate.
func joinFuseFixture(joinCount, exCount int, consumerType string) []Stage {
	return []Stage{
		{ID: "up-0", Type: StageExchangeRepartition,
			Exchange:     &ExchangeStage{Keys: []string{"a"}, Count: joinCount},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: joinCount},
		},
		{
			ID: "join-1", Type: StageHashJoin,
			Dependencies: []string{"up-0"},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: joinCount},
		},
		{
			ID: "ex-2", Type: StageExchangeRepartition,
			Dependencies: []string{"join-1"},
			Exchange:     &ExchangeStage{Keys: []string{"k"}, Count: exCount},
			Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: exCount},
		},
		{
			ID: "cons-3", Type: consumerType,
			Dependencies: []string{"ex-2"},
			LeftDepStage: "ex-2",
		},
	}
}

// TestFuseJoinShuffle_WidthPreservingAbsorbs — a same-count exchange fuses
// into the join; the consumer's dep is rewired and the join carries the
// exchange's keys and distribution (Q18 join-4 ← rp-6, the 7.6 GB leg).
func TestFuseJoinShuffle_WidthPreservingAbsorbs(t *testing.T) {
	out := fuseJoinShuffle(joinFuseFixture(32, 32, StageHashJoin))
	if len(out) != 3 {
		t.Fatalf("expected 3 stages after fusion (up + join + consumer), got %d", len(out))
	}
	var join, cons *Stage
	for i := range out {
		switch out[i].ID {
		case "join-1":
			join = &out[i]
		case "cons-3":
			cons = &out[i]
		}
	}
	if join == nil || cons == nil {
		t.Fatalf("missing expected stages after fusion: %+v", out)
	}
	if join.Exchange == nil || join.Exchange.Keys[0] != "k" || join.Exchange.Count != 32 {
		t.Errorf("join should carry the absorbed exchange, got %+v", join.Exchange)
	}
	if join.Distribution.Count != 32 || join.Distribution.Keys[0] != "k" {
		t.Errorf("join distribution should be the exchange's, got %+v", join.Distribution)
	}
	if cons.LeftDepStage != "join-1" || cons.Dependencies[0] != "join-1" {
		t.Errorf("consumer deps should be rewired to the join, got %+v", cons)
	}
}

// TestFuseJoinShuffle_WidthShrinkingDeclined — the #280 regression: the
// dispatcher schedules a compute stage's tasks from its Distribution count,
// so absorbing a narrower exchange collapses the join's parallelism (SF100
// Q18 join-8 absorbed its /4 final-aggregate repartition: 24 partition
// tasks of 2-4s became 3 tasks of ~78s, cold +126%). The pass must keep
// the explicit repartition stage.
func TestFuseJoinShuffle_WidthShrinkingDeclined(t *testing.T) {
	out := fuseJoinShuffle(joinFuseFixture(32, 4, "final_aggregate"))
	if len(out) != 4 {
		t.Fatalf("expected width-shrinking fusion declined (4 stages), got %d", len(out))
	}
	for i := range out {
		if out[i].ID == "join-1" {
			if out[i].Exchange != nil {
				t.Errorf("join must not absorb a narrower exchange, got %+v", out[i].Exchange)
			}
			if out[i].Distribution.Count != 32 {
				t.Errorf("join distribution width must be preserved, got %+v", out[i].Distribution)
			}
		}
	}
}

// TestFuseJoinShuffle_FlatteningConsumerDeclined — consumer-shape gate:
// consumers that flatten the upstream file list keep the two-step path.
func TestFuseJoinShuffle_FlatteningConsumerDeclined(t *testing.T) {
	for _, ct := range []string{StageExchangeRepartition, StageExchangeGather, StageSort} {
		out := fuseJoinShuffle(joinFuseFixture(32, 32, ct))
		if len(out) != 4 {
			t.Errorf("consumer %s: expected skip (4 stages), got %d", ct, len(out))
		}
	}
}

// TestHashPartitionCount — one width rule for all hash exchanges: the
// count-unpinned reduce side (grouped finals, windows) must scale with
// cluster compute width exactly like join shuffles, not with node count
// (SF100 Q20: a 54.5M-group final_aggregate at workerCount width ran 3
// tasks × ~23s × ~7.7GB heap). Single-process planning keeps 1.
func TestHashPartitionCount(t *testing.T) {
	for _, tc := range []struct{ workers, want int }{
		{0, 1}, {1, 1}, {2, 16}, {3, 24}, {4, 32}, {8, 64},
	} {
		if got := HashPartitionCount(tc.workers); got != tc.want {
			t.Errorf("HashPartitionCount(%d) = %d, want %d", tc.workers, got, tc.want)
		}
	}
	d := distributionFromRequired(RequiredDistribution{Kind: RequiredClusteredOn, Keys: []string{"k"}}, 3)
	if d.Count != 24 {
		t.Errorf("count-unpinned clustered requirement at 3 workers = %d partitions, want 24", d.Count)
	}
}
