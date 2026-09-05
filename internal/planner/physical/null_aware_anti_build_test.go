package physical

import (
	"errors"
	"strings"
	"testing"
)

// A null-aware anti join's build is NOT partitionable, and the property system
// is where that is recorded (#539/#507).
//
// walkStages forces such a join onto the broadcast path, so in a plan this
// engine emits today slot 1 is RequiredBroadcast by stage TYPE. That is one
// place remembering one rule, and the rule's failure mode is silent: a
// partitioned build splits the "did any row have a NULL key" fact, and NOT IN
// then answers its NOT EXISTS twin. These two gates are what makes the rule a
// property of the JOIN rather than of the type it happens to carry.
func TestANullAwareAntiJoinsBuildIsRequiredBroadcast(t *testing.T) {
	base := Stage{
		Type:          StageHashJoin,
		JoinLeftKeys:  []string{"a"},
		JoinRightKeys: []string{"b"},
	}
	if got := RequiredChildDistribution(base, 1); got.Kind != RequiredClusteredOn {
		t.Fatalf("an ordinary hash join's build wants %v, want clustered_on", got.Kind)
	}
	na := base
	na.NullAwareAnti = true
	if got := RequiredChildDistribution(na, 1); got.Kind != RequiredBroadcast {
		t.Fatalf("a null-aware anti join's build wants %v, want broadcast — a partitioned "+
			"build splits NOT IN's three-valued fact (#507)", got.Kind)
	}
	// The PROBE side is unchanged: it is the build that carries the fact.
	if got := RequiredChildDistribution(na, 0); got.Kind != RequiredClusteredOn {
		t.Fatalf("a null-aware anti join's probe wants %v, want clustered_on", got.Kind)
	}
}

func TestANullAwareAntiJoinWithAPartitionedBuildIsRefused(t *testing.T) {
	partitioned := Stage{ID: "build", Type: StageScan,
		Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 3}}
	replicated := Stage{ID: "build", Type: StageExchangeReplicate,
		Distribution: Distribution{Kind: DistBroadcast}}

	join := func(typ string, tasks int) Stage {
		return Stage{ID: "join", Type: typ, NullAwareAnti: true, Tasks: tasks,
			Dependencies: []string{"probe", "build"}, LeftDepStage: "probe", RightDepStage: "build",
			Distribution: Distribution{Kind: DistHashPartitioned, Count: 3}}
	}
	probe := Stage{ID: "probe", Type: StageScan,
		Distribution: Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 3}}

	// The defect: a hash join over a hash-partitioned build.
	err := assertNullAwareAntiBuildsAreReplicated([]Stage{probe, partitioned, join(StageHashJoin, 3)})
	if !errors.Is(err, ErrNullAwareAntiBuildNotReplicated) {
		t.Fatalf("a partitioned build was accepted: %v", err)
	}
	if !strings.Contains(err.Error(), "hash-partitioned") {
		t.Errorf("the refusal does not say what the build IS: %v", err)
	}

	// The three shapes that satisfy it.
	if err := assertNullAwareAntiBuildsAreReplicated(
		[]Stage{probe, partitioned, join(StageBroadcastJoin, 3)}); err != nil {
		t.Errorf("a broadcast join was refused: %v", err)
	}
	if err := assertNullAwareAntiBuildsAreReplicated(
		[]Stage{probe, replicated, join(StageHashJoin, 3)}); err != nil {
		t.Errorf("a replicated build was refused: %v", err)
	}
	single := join(StageHashJoin, 1)
	single.Distribution = Distribution{Kind: DistSingleton}
	if err := assertNullAwareAntiBuildsAreReplicated(
		[]Stage{probe, partitioned, single}); err != nil {
		t.Errorf("a single-task join was refused: %v", err)
	}

	// And the control: the same partitioned build under a join that is NOT
	// null-aware is exactly what a shuffle is for.
	plain := join(StageHashJoin, 3)
	plain.NullAwareAnti = false
	if err := assertNullAwareAntiBuildsAreReplicated(
		[]Stage{probe, partitioned, plain}); err != nil {
		t.Errorf("an ordinary anti join over a partitioned build was refused: %v", err)
	}
}
