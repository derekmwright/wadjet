package worker

import (
	"runtime/debug"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// TestExecutor_ReservoirPropagation verifies the Phase-3 plumbing linchpin: the
// reservoir registry + floating-budget flag reach the shared SpillManager under
// BOTH setter orderings (SetReservoirs before/after SetSharedPoolBudget). If the
// self-healing wiring were missing, the floating budget would silently fall back
// to the static path (plan risk #3/#7).
func TestExecutor_ReservoirPropagation(t *testing.T) {
	const limit = 20 << 30 // headroom = 4 GiB; Available = 20-0-4 = 16 GiB
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	for _, reservoirsFirst := range []bool{true, false} {
		name := "reservoirs_after_pool"
		if reservoirsFirst {
			name = "reservoirs_before_pool"
		}
		t.Run(name, func(t *testing.T) {
			e := NewExecutor(nil, nil, nil)
			e.spillDir = t.TempDir()
			rr := memory.NewReservoirRegistry()

			if reservoirsFirst {
				e.SetReservoirs(rr)
				e.SetFloatingBudgetActive(true)
				e.SetSharedPoolBudget(10 << 30) // builds sharedSpill; self-heals the link
			} else {
				e.SetSharedPoolBudget(10 << 30) // builds sharedSpill first
				e.SetReservoirs(rr)             // links into the existing sharedSpill
				e.SetFloatingBudgetActive(true)
			}

			if e.sharedSpill == nil {
				t.Fatal("sharedSpill not constructed")
			}

			// Behavior probe: with the registry + flag propagated, ShouldSpillFor
			// uses the floating path. Push tracked usage just over the floating
			// SpillCheap threshold (0.90 × 16 GiB = 14.4 GiB) and assert it fires.
			e.sharedTracker.ForceReserve(15 << 30)
			if !e.sharedSpill.ShouldSpillFor(memory.SpillCheap) {
				t.Fatal("floating flag did not propagate: 15 GiB > 14.4 GiB floating threshold should spill")
			}
			// Below the threshold must not fire — confirms it's the floating
			// path (14.4 GiB), not the static 40% of 10 GiB (= 4 GiB).
			e.sharedTracker.Release(13 << 30) // now 2 GiB used
			if e.sharedSpill.ShouldSpillFor(memory.SpillCheap) {
				t.Fatal("2 GiB < 14.4 GiB floating threshold must not spill (static 40%=4GiB would have)")
			}
		})
	}
}

// TestExecutor_ReservoirsDormantByDefault confirms that wiring reservoirs WITHOUT
// activating the flag leaves ShouldSpillFor on the static path.
func TestExecutor_ReservoirsDormantByDefault(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(20 << 30)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	e := NewExecutor(nil, nil, nil)
	e.spillDir = t.TempDir()
	e.SetSharedPoolBudget(10 << 30)
	e.SetReservoirs(memory.NewReservoirRegistry()) // accounting only, flag stays false

	// 5 GiB used: static 40% of 10 GiB = 4 GiB → fires; floating 14.4 GiB → would not.
	e.sharedTracker.ForceReserve(5 << 30)
	if !e.sharedSpill.ShouldSpillFor(memory.SpillCheap) {
		t.Fatal("reservoirs wired but flag off: must use static 40% path (5 GiB > 4 GiB)")
	}
}
