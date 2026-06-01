package memory

import (
	"sync"
	"testing"
	"time"
)

// fakeAccountedOp is a controllable AccountedOperator for SpillManager tests.
type fakeAccountedOp struct {
	mu         sync.Mutex
	id         uint64
	name       string
	owned      int64
	spillable  int64
	state      OpState
	deliver    int64 // bytes SpillSome actually frees per call (0 = nothing)
	spillCalls int
}

func (f *fakeAccountedOp) Inspect() OperatorFootprint {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == OpClosed {
		return OperatorFootprint{State: OpClosed, InstanceID: f.id, Name: f.name}
	}
	return OperatorFootprint{
		OwnedBytes:     f.owned,
		RetainedBytes:  f.owned,
		SpillableBytes: f.spillable,
		State:          f.state,
		InstanceID:     f.id,
		Name:           f.name,
	}
}

func (f *fakeAccountedOp) EstimateRelief(target int64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.spillable
	if target > 0 && target < s {
		return target
	}
	return s
}

func (f *fakeAccountedOp) SpillSome(_ int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spillCalls++
	n := f.deliver
	if n > f.owned {
		n = f.owned
	}
	f.owned -= n
	if f.spillable > f.owned {
		f.spillable = f.owned
	}
	return n, nil
}

func newSM(t *testing.T) *SpillManager {
	t.Helper()
	sm, err := NewSpillManager(t.TempDir(), NewTracker("t", 0))
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

func TestNextInstanceID_NeverCollidesWithReservoirOwner(t *testing.T) {
	a := NextInstanceID()
	b := NextInstanceID()
	if a < 2 || b < 2 {
		t.Fatalf("instance ids must be >= 2 (ReservoirOwner=1), got %d %d", a, b)
	}
	if a == b {
		t.Fatalf("instance ids must be unique, got %d twice", a)
	}
}

func TestInspect_ClosedReturnsZeros(t *testing.T) {
	sm := newSM(t)
	op := &fakeAccountedOp{id: NextInstanceID(), name: "op", owned: 1000, spillable: 1000, state: OpClosed}
	unreg := sm.RegisterAccounted(op)
	defer unreg()
	got := sm.Inspect()
	if len(got) != 1 {
		t.Fatalf("expected 1 footprint, got %d", len(got))
	}
	if got[0].OwnedBytes != 0 || got[0].SpillableBytes != 0 {
		t.Fatalf("closed op should report zero bytes, got %+v", got[0])
	}
}

// TestRequestRelief_AccumulatesAcrossAllWillingOps proves the no-<target/2-skip
// property: three ops none of which individually meets target/2 still combine
// to satisfy the target.
func TestRequestRelief_AccumulatesAcrossAllWillingOps(t *testing.T) {
	sm := newSM(t)
	ops := []*fakeAccountedOp{
		{id: NextInstanceID(), name: "a", owned: 100, spillable: 100, state: OpActive, deliver: 100},
		{id: NextInstanceID(), name: "b", owned: 50, spillable: 50, state: OpActive, deliver: 50},
		{id: NextInstanceID(), name: "c", owned: 30, spillable: 30, state: OpActive, deliver: 30},
	}
	for _, op := range ops {
		sm.RegisterAccounted(op)
	}
	// target 150 > any single op's spillable/2; must accumulate across all.
	freed, err := sm.RequestRelief(150)
	if err != nil {
		t.Fatal(err)
	}
	if freed < 150 {
		t.Fatalf("expected to accumulate >=150 across ops, freed %d", freed)
	}
}

// TestRequestRelief_DriftBackstop: ops claim SpillableBytes==0 but the published
// owned total exceeds tracker.Used by >20% — the backstop force-spills the
// largest-OwnedBytes op anyway.
func TestRequestRelief_DriftBackstop(t *testing.T) {
	sm := newSM(t)
	// tracker.Used small; OwnedTotal large => drift > 20%.
	sm.tracker.ForceReserve(100)
	sm.tracker.PublishOwned(1, 1000) // 1000 owned vs 100 used => drift 9x

	big := &fakeAccountedOp{id: NextInstanceID(), name: "big", owned: 1000, spillable: 0, state: OpActive, deliver: 400}
	small := &fakeAccountedOp{id: NextInstanceID(), name: "small", owned: 10, spillable: 0, state: OpActive, deliver: 5}
	sm.RegisterAccounted(big)
	sm.RegisterAccounted(small)

	freed, err := sm.RequestRelief(500)
	if err != nil {
		t.Fatal(err)
	}
	if freed == 0 {
		t.Fatal("drift-backstop should have force-spilled despite SpillableBytes==0")
	}
	if big.spillCalls == 0 {
		t.Fatal("backstop should target the largest-OwnedBytes op")
	}
}

// TestRequestRelief_NoDriftNoBackstop: when SpillableBytes==0 and there is no
// drift, RequestRelief frees nothing (no spurious force-spill).
func TestRequestRelief_NoDriftNoBackstop(t *testing.T) {
	sm := newSM(t)
	op := &fakeAccountedOp{id: NextInstanceID(), name: "op", owned: 1000, spillable: 0, state: OpActive, deliver: 400}
	sm.RegisterAccounted(op)
	freed, err := sm.RequestRelief(500)
	if err != nil {
		t.Fatal(err)
	}
	if freed != 0 || op.spillCalls != 0 {
		t.Fatalf("no drift + nothing spillable should free 0 and not spill, freed=%d calls=%d", freed, op.spillCalls)
	}
}

// TestRequestRelief_CooldownOnShortfall: an op that delivers far less than it
// claims is rested for reliefCooldown.
func TestRequestRelief_CooldownOnShortfall(t *testing.T) {
	base := time.Now()
	clk := base
	old := nowFunc
	nowFunc = func() time.Time { return clk }
	defer func() { nowFunc = old }()

	sm := newSM(t)
	// claims 1000 spillable but delivers nothing => shortfall => cooldown.
	op := &fakeAccountedOp{id: NextInstanceID(), name: "liar", owned: 1000, spillable: 1000, state: OpActive, deliver: 0}
	sm.RegisterAccounted(op)

	sm.RequestRelief(500)
	if !sm.inCooldown(op.id) {
		t.Fatal("op delivering < claimed/2 should be in cooldown")
	}
	// Advance past the cooldown window.
	clk = base.Add(reliefCooldown + time.Second)
	if sm.inCooldown(op.id) {
		t.Fatal("cooldown should expire after reliefCooldown")
	}
}

// TestShouldSpillFor_FloatingBudget: with a reservoir registry wired, the
// threshold floats against Available() rather than the static budget — but ONLY
// when floatingBudgetActive is set. This is the load-bearing decoupling test:
// wiring a reservoir registry for accounting must NOT change the spill decision.
func TestShouldSpillFor_ReservoirsWiredButDormant(t *testing.T) {
	const limit = 20 << 30 // headroom = 4 GiB (ratio)
	withMemLimit(t, limit)

	// Tracker budget 10 GiB; static SpillCheap threshold = 40% = 4 GiB.
	sm, err := NewSpillManager(t.TempDir(), NewTracker("t", 10<<30))
	if err != nil {
		t.Fatal(err)
	}
	rr := NewReservoirRegistry()
	rr.Register(NewReservoir("cache", 2<<30)) // actual 0
	sm.SetReservoirs(rr)                      // accounting only — does NOT activate floating
	sm.SetCheapFraction(0.90)

	sm.tracker.ForceReserve(5 << 30) // 5 GiB used

	// Reservoirs wired, floatingBudgetActive=false (default): the STATIC path
	// governs. 5 GiB > 4 GiB (40% of 10) => SpillCheap fires.
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Fatal("dormant floating: 5 GiB used must trip the static 40% threshold")
	}

	// Flip the floating budget active. Available = 20 - 0 - 4 = 16 GiB; floating
	// SpillCheap threshold = 0.90*16 = 14.4 GiB. 5 GiB < 14.4 => does NOT fire.
	sm.SetFloatingBudgetActive(true)
	if sm.ShouldSpillFor(SpillCheap) {
		t.Fatal("active floating: 5 GiB used < 14.4 GiB floating threshold, must NOT spill")
	}
	// Push used over the floating threshold to confirm the floating path fires.
	sm.tracker.ForceReserve(10 << 30) // now 15 GiB > 14.4
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Fatal("active floating: 15 GiB used > 14.4 GiB threshold, must spill")
	}
}

// TestShouldSpillFor_StaticPathWhenNoReservoirs: without a reservoir registry,
// the tuned static 40%/90% behavior is preserved (Phase 2 default).
func TestShouldSpillFor_StaticPathWhenNoReservoirs(t *testing.T) {
	sm, err := NewSpillManager(t.TempDir(), NewTracker("t", 1000))
	if err != nil {
		t.Fatal(err)
	}
	sm.tracker.ForceReserve(450) // 45% of 1000
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Fatal("45% used should trip the 40% SpillCheap threshold")
	}
	if sm.ShouldSpillFor(SpillExpensive) {
		t.Fatal("45% used should NOT trip the 90% SpillExpensive threshold")
	}
}
