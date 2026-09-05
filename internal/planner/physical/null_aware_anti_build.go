package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ErrNullAwareAntiBuildNotReplicated marks a plan whose null-aware anti join
// would read a PARTITIONED build side.
//
// `x NOT IN (SELECT y FROM t)` is three-valued and its third value is a fact
// about the WHOLE build: TRUE only when x differs from every y, FALSE when it
// equals one, and UNKNOWN — so WHERE drops the row — when x is NULL and the
// build is non-empty, or when the build holds a NULL y. `exec.HashJoin`
// implements that by reading one bit off its build side and emitting nothing
// when it is set (#507).
//
// A hash-partitioned build splits that bit. The task holding the NULL
// partition emits nothing; every other task emits its probe rows; and the
// answer is the one a TWO-valued anti join gives — which is `NOT EXISTS`, a
// different question. Nothing downstream can notice, because every task
// behaved correctly for the rows it held.
//
// So the build must be REPLICATED, and this is the invariant that says it was.
// It is a refusal rather than a repair for the reason ADR-0006 gives about
// loudness: a plan this cannot show to be right is routed to the
// coordinator-local pipeline, which answers it, instead of being dispatched to
// workers that would each answer a different question.
//
// The refusal carries SQLSTATE 0A000 and the coordinator has an `errors.Is`
// arm for it (`runNullAwareAntiLocal`, counter
// `NullAwareAntiLocalRoutes`) — round 1 of this arc's review found all three
// of those claims written down and none of them implemented: a bare
// `errors.New` with no coder, and no routing arm, so a plan that tripped the
// invariant would have reached the client as `physical plan: …` with no
// class. A promise a document makes and the code does not keep is worse than
// no promise.
var ErrNullAwareAntiBuildNotReplicated = errors.New(
	"a null-aware anti join's build side is not replicated")

// assertNullAwareAntiBuildsAreReplicated checks, on the FINAL stage list, that
// every null-aware anti join reads a build side every task sees whole.
//
// Three shapes satisfy it, and they are the three ways a task can hold the
// whole build: a broadcast join (its build slot is RequiredBroadcast and
// EnsureDistribution splices a replicate exchange), a build whose own output
// distribution is already DistBroadcast, and a SINGLETON plan — one task,
// which holds everything by definition.
//
// Asked after every rewriting pass rather than at emission, because emission
// is not where the property can be lost: walkStages forces the broadcast, and
// what could take it away is a later pass that re-types the stage, fuses it,
// or splits its probe. Those are guarded one at a time today
// (fuse_stage_chains.go, planSkewSplitTasks); this is the check that does not
// have to be remembered.
func assertNullAwareAntiBuildsAreReplicated(stages []Stage) error {
	byID := make(map[string]*Stage, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
	}
	for i := range stages {
		s := &stages[i]
		if !s.NullAwareAnti {
			continue
		}
		if s.Type == StageBroadcastJoin {
			continue
		}
		// One task reads every partition of its input, so the build is whole.
		if s.Tasks <= 1 && s.Distribution.Kind != DistHashPartitioned {
			continue
		}
		build := s.RightDepStage
		if build == "" && len(s.Dependencies) == 2 {
			build = s.Dependencies[1]
		}
		if b, ok := byID[build]; ok {
			if b.Type == StageExchangeReplicate || b.Distribution.Kind == DistBroadcast ||
				b.Distribution.Kind == DistSingleton {
				continue
			}
		}
		return sqlerr.Wrap("0A000", fmt.Errorf("%w: stage %s (%s) reads build %q, which is %v"+
			" — NOT IN's three-valued rule reads one fact off the WHOLE build side (#507),"+
			" and a partitioned build splits it, so the query would answer its NOT EXISTS"+
			" twin; the coordinator runs this query single-process",
			ErrNullAwareAntiBuildNotReplicated, s.ID, s.Type, build, buildDistKind(byID, build)))
	}
	return nil
}

func buildDistKind(byID map[string]*Stage, id string) string {
	b, ok := byID[id]
	if !ok {
		return "absent"
	}
	switch b.Distribution.Kind {
	case DistBroadcast:
		return "broadcast"
	case DistSingleton:
		return "singleton"
	case DistHashPartitioned:
		return fmt.Sprintf("hash-partitioned on %v", b.Distribution.Keys)
	case DistRoundRobin:
		return "round-robin"
	}
	return "unknown"
}

// nullAwareAntiForcingDisabled disarms walkStages' forced broadcast for a
// null-aware anti join. TEST-ONLY, and it exists because the invariant below
// cannot otherwise be reached from SQL: the forcing is what keeps every plan
// this engine emits correct, so the only way to ask "does the invariant refuse,
// with its class, and does the coordinator route it" is to put the planner in
// the state a future pass that re-typed such a stage would leave it in.
var nullAwareAntiForcingDisabled bool

// DisableNullAwareAntiBroadcastForcingForTest turns the forcing off and returns
// the function that turns it back on.
func DisableNullAwareAntiBroadcastForcingForTest() func() {
	nullAwareAntiForcingDisabled = true
	return func() { nullAwareAntiForcingDisabled = false }
}

// AssertNullAwareAntiBuildsAreReplicatedForTest exports the invariant so a gate
// in another package can ask it about a hand-built plan — the class the
// refusal carries is a promise three documents make, and it needs an assertion
// that does not depend on standing a cluster up.
func AssertNullAwareAntiBuildsAreReplicatedForTest(stages []Stage) error {
	return assertNullAwareAntiBuildsAreReplicated(stages)
}

// nullAwareAntiRequiredBroadcastDisabled disarms the DISTRIBUTION PROPERTY —
// the second of the two production hunks. TEST-ONLY, and paired with the one
// above because the two are layers: with only the forcing off, the property
// still makes EnsureDistribution splice a replicate exchange and the answer
// stays right, which is the property hunk earning its keep; with both off the
// build really is hash-partitioned and the INVARIANT is what stands between a
// client and NOT IN's two-valued twin.
var nullAwareAntiRequiredBroadcastDisabled bool

// DisableNullAwareAntiRequiredBroadcastForTest turns the property off and
// returns the function that turns it back on.
func DisableNullAwareAntiRequiredBroadcastForTest() func() {
	nullAwareAntiRequiredBroadcastDisabled = true
	return func() { nullAwareAntiRequiredBroadcastDisabled = false }
}
