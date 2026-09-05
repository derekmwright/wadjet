package coordinator

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The null-aware anti-join invariant's CALL SITE, its SQLSTATE and its route.
//
// Round 1's review found that the invariant had none of the three: the gate
// its commit named covered `walkStages`'s pre-existing forced broadcast rather
// than the invariant, deleting the `assertNullAwareAntiBuildsAreReplicated`
// call from `PlanDistributed` broke nothing anywhere, and the commit body, the
// ADR and the error text all promised "refuses 0A000 and routes local" for an
// error that was a bare `errors.New` with no coder and no routing arm — a hard
// client error.
//
// The invariant cannot be tripped by SQL while `walkStages` forces the
// broadcast, which is the point of the forcing. So the gate trips it the way
// the defect would: it DISARMS the forcing through a test-only hook, which is
// exactly the state a future pass that re-types, fuses or splits such a stage
// would create. What must then happen is the promise — 0A000, the route
// counter, and PostgreSQL's rows off the coordinator-local pipeline — rather
// than a partitioned build silently answering the two-valued question.
func TestANullAwareAntiJoinWhoseBuildIsNotReplicatedRoutesRatherThanDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	// One byte: nothing is broadcast by SIZE, so the only thing that can make
	// this build replicate is the null-aware rule itself.
	coord := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })

	const sql = `SELECT COUNT(*) AS n FROM typemx a WHERE a.c_i64 NOT IN ` +
		`(SELECT b.c_i64 FROM typemx b WHERE b.id < 500 AND b.c_i64 IS NOT NULL)`

	want, err := na2Run(tmdRunSingle(ctx, single, sql))
	if err != nil {
		t.Fatalf("single arm: %v", err)
	}
	sort.Strings(want)

	// ARMED: the forcing is on, the build replicates, the invariant is silent
	// and the DAG executes the join.
	before := coord.NullAwareAntiLocalRoutes()
	got, err := na2Run(tmdRunDAG(ctx, coord, sql))
	if err != nil {
		t.Fatalf("dag arm with the forcing on: %v", err)
	}
	sort.Strings(got)
	if len(got) != len(want) || (len(got) > 0 && got[0] != want[0]) {
		t.Fatalf("dag arm: %v, single arm: %v", got, want)
	}
	if d := coord.NullAwareAntiLocalRoutes() - before; d != 0 {
		t.Fatalf("the invariant fired with the forcing ON (%d routes): it is a "+
			"defence-in-depth check and must be silent on a plan walkStages built", d)
	}

	// LAYER 1 OFF: walkStages no longer forces the broadcast. The DISTRIBUTION
	// PROPERTY still says a null-aware anti join's build slot is
	// RequiredBroadcast, so EnsureDistribution splices a replicate exchange
	// and the answer stays right with nothing refused. That is the property
	// hunk earning its keep, and it is why the coordinator gate the first
	// round shipped could not fail on this commit's revert.
	restoreForcing := physical.DisableNullAwareAntiBroadcastForcingForTest()
	t.Cleanup(restoreForcing)

	before = coord.NullAwareAntiLocalRoutes()
	got, err = na2Run(tmdRunDAG(ctx, coord, sql))
	if err != nil {
		t.Fatalf("dag arm with the forcing off: %v", err)
	}
	sort.Strings(got)
	if len(got) != len(want) || (len(got) > 0 && got[0] != want[0]) {
		t.Fatalf("dag arm with the forcing off: %v, single arm: %v\n"+
			"  the distribution property is supposed to replicate the build here", got, want)
	}
	if d := coord.NullAwareAntiLocalRoutes() - before; d != 0 {
		t.Errorf("NullAwareAntiLocalRoutes moved by %d with only the forcing off: the "+
			"property should have replicated the build, so nothing needed refusing", d)
	}

	// LAYER 2 OFF as well: now the build really is hash-partitioned, which is
	// the state that answers NOT IN's two-valued twin. The INVARIANT is what
	// stands between that and a client.
	restoreProperty := physical.DisableNullAwareAntiRequiredBroadcastForTest()
	t.Cleanup(restoreProperty)

	before = coord.NullAwareAntiLocalRoutes()
	got, err = na2Run(tmdRunDAG(ctx, coord, sql))
	if err != nil {
		t.Fatalf("dag arm with both hunks off: %v\n"+
			"  the invariant is supposed to refuse this plan and the coordinator to ROUTE "+
			"it, which answers; an error here means the routing arm is missing", err)
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("dag arm with both hunks off: %d rows, want %d\n  got %v\n  want %v",
			len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("dag arm with both hunks off, row %d:\n  got  %s\n  want %s "+
				"(the single-process arm). A partitioned build answers NOT IN's "+
				"NOT EXISTS twin, which is what the invariant exists to stop.", i, got[i], want[i])
		}
	}
	if d := coord.NullAwareAntiLocalRoutes() - before; d != 1 {
		t.Errorf("NullAwareAntiLocalRoutes moved by %d, want 1 — with both hunks off the "+
			"plan cannot show every task sees the whole build, so it must be refused and "+
			"routed rather than dispatched", d)
	}
}

// And the refusal's own CLASS, asked without a cluster: a plan whose
// null-aware anti join reads a hash-partitioned build refuses with SQLSTATE
// 0A000, because a client branches on the code and this one is "the engine
// cannot do this here", not an internal error.
func TestTheNullAwareAntiRefusalCarriesItsSQLSTATE(t *testing.T) {
	stages := []physical.Stage{
		{ID: "probe", Type: physical.StageScan,
			Distribution: physical.Distribution{Kind: physical.DistHashPartitioned,
				Keys: []string{"a"}, Count: 3}},
		{ID: "build", Type: physical.StageScan,
			Distribution: physical.Distribution{Kind: physical.DistHashPartitioned,
				Keys: []string{"b"}, Count: 3}},
		{ID: "join", Type: physical.StageHashJoin, NullAwareAnti: true, Tasks: 3,
			Dependencies: []string{"probe", "build"},
			LeftDepStage: "probe", RightDepStage: "build",
			Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 3}},
	}
	err := physical.AssertNullAwareAntiBuildsAreReplicatedForTest(stages)
	if err == nil {
		t.Fatal("a hash-partitioned build under a null-aware anti join was accepted")
	}
	if got := sqlerr.StateOf(err); got != "0A000" {
		t.Errorf("SQLSTATE %q, want 0A000 — the commit body, ADR-0021 §1f and this "+
			"message all say 0A000, and a promise the code does not keep is worse "+
			"than no promise\n  %v", got, err)
	}
}
