// SPDX-License-Identifier: MIT

package memory

import (
	"runtime"
	"testing"
	"time"
)

// assertRegistryHoldsOnlyLive checks the relief registry's storage after an
// unregister: no slot of the backing array beyond len may still point at an
// operator (an append-deletion leaves the vacated tail slot aliasing the
// departed operator, which keeps it reachable for as long as the manager
// lives — on a worker, the process), and the per-instance maps keyed by
// InstanceID hold no entry for an instance that is no longer registered.
func assertRegistryHoldsOnlyLive(t *testing.T, sm *SpillManager, step string) {
	t.Helper()
	sm.accountedMu.Lock()
	full := sm.accounted[:cap(sm.accounted)]
	live := make(map[uint64]bool, len(sm.accounted))
	for _, op := range sm.accounted {
		live[op.Inspect().InstanceID] = true
	}
	for i := len(sm.accounted); i < len(full); i++ {
		if full[i] != nil {
			id := full[i].Inspect().InstanceID
			sm.accountedMu.Unlock()
			t.Fatalf("%s: slot %d beyond len %d still holds operator %d (#1706)", step, i, len(sm.accounted), id)
		}
	}
	for id := range sm.accountedPeak {
		if !live[id] {
			sm.accountedMu.Unlock()
			t.Fatalf("%s: accountedPeak keeps unregistered instance %d", step, id)
		}
	}
	sm.accountedMu.Unlock()
	sm.reliefMu.Lock()
	defer sm.reliefMu.Unlock()
	for id := range sm.lastReliefAt {
		if !live[id] {
			t.Fatalf("%s: the relief cooldown table keeps unregistered instance %d (#1706)", step, id)
		}
	}
}

// TestRegistryUnregisterLeavesNoStaleSlot exercises unregistering the first,
// a middle and the last element (and draining the rest) and checks after
// every step that the registry's storage holds only live operators and the
// departed aggregate stays one entry per distinct name.
func TestRegistryUnregisterLeavesNoStaleSlot(t *testing.T) {
	orders := map[string][]int{
		"first-middle-last": {0, 2, 4, 1, 3},
		"last-first-rest":   {4, 0, 1, 2, 3},
		"lifo":              {4, 3, 2, 1, 0},
		"fifo":              {0, 1, 2, 3, 4},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			sm := &SpillManager{}
			sentinel := &fakeAccountedOp{id: NextInstanceID(), name: "sentinel", state: OpActive}
			sm.RegisterAccounted(sentinel) // a worker's permanent registrant (the decoded cache)
			ops := make([]*fakeAccountedOp, 5)
			unregs := make([]func(), 5)
			for i := range ops {
				ops[i] = &fakeAccountedOp{id: NextInstanceID(), name: "op", owned: 100, spillable: 100, state: OpActive}
				unregs[i] = sm.RegisterAccounted(ops[i])
			}
			sm.rankCandidates() // populate accountedPeak for every live instance
			for _, op := range ops {
				sm.cooldown(op.id) // every instance enters relief cooldown once
			}
			assertRegistryHoldsOnlyLive(t, sm, "after register")
			for _, i := range order {
				ops[i].mu.Lock()
				ops[i].state = OpClosed
				ops[i].mu.Unlock()
				unregs[i]()
				assertRegistryHoldsOnlyLive(t, sm, "after unregistering op "+string(rune('0'+i)))
			}
			if len(sm.departed) != 1 || sm.departed["op"].count != 5 {
				t.Fatalf("departed aggregate = %+v, want one entry for name %q with count 5", sm.departed, "op")
			}
			if len(sm.accounted) != 1 || sm.accounted[0] != AccountedOperator(sentinel) {
				t.Fatalf("registry after draining = %v, want only the sentinel", sm.accounted)
			}
		})
	}
}

// retentionOp is an AccountedOperator carrying a heap payload, so its
// reachability is observable through runtime.AddCleanup.
type retentionOp struct {
	fakeAccountedOp
	payload []byte
}

// TestRegistryUnregisteredOperatorBecomesUnreachable registers a permanent
// sentinel plus three payload-carrying operators on a manager that stays
// alive (a worker's shared manager), unregisters each operator and drops
// every other reference to it, and requires each to be collected while the
// manager is still alive. At base, the append-deletion left the vacated tail
// slot aliasing the departed operator: unregistering the last-registered
// operator kept it reachable until a later RegisterAccounted overwrote the
// slot, which on an idle worker is never.
func TestRegistryUnregisteredOperatorBecomesUnreachable(t *testing.T) {
	for _, pos := range []int{0, 1, 2} {
		t.Run(string(rune('0'+pos)), func(t *testing.T) {
			sm := &SpillManager{}
			sm.RegisterAccounted(&fakeAccountedOp{id: NextInstanceID(), name: "sentinel", state: OpActive})
			collected := make(chan int, 3)
			unregs := make([]func(), 3)
			// Built in a helper so no stack slot of this frame keeps an op alive.
			func() {
				for i := 0; i < 3; i++ {
					op := &retentionOp{
						fakeAccountedOp: fakeAccountedOp{id: NextInstanceID(), name: "op", state: OpActive},
						payload:         make([]byte, 1<<20),
					}
					runtime.AddCleanup(op, func(i int) { collected <- i }, i)
					unregs[i] = sm.RegisterAccounted(op)
				}
			}()
			// Unregister pos first, then the others in reverse, so the last
			// unregister is the last element at least once per subtest.
			seq := []int{pos}
			for i := 2; i >= 0; i-- {
				if i != pos {
					seq = append(seq, i)
				}
			}
			for _, i := range seq {
				unregs[i]()
				unregs[i] = nil // the closure captures op; drop it too
			}
			got := map[int]bool{}
			deadline := time.Now().Add(5 * time.Second)
			for len(got) < 3 && time.Now().Before(deadline) {
				runtime.GC()
				select {
				case i := <-collected:
					got[i] = true
				case <-time.After(20 * time.Millisecond):
				}
			}
			runtime.KeepAlive(sm)
			if len(got) < 3 {
				var missing []int
				for i := 0; i < 3; i++ {
					if !got[i] {
						missing = append(missing, i)
					}
				}
				t.Fatalf("operators %v stayed reachable after unregister while the manager lived (#1706)", missing)
			}
		})
	}
}
