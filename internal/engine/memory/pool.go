// SPDX-License-Identifier: MIT

package memory

import (
	"os"
)

// ExecutionPool is the query-execution memory pool of ONE engine instance: a
// root Tracker holding the instance's budget and the SpillManager every
// statement of the instance spills through, so concurrent statements share
// one budget instead of each holding all of it (ADR-0006's 2026-11-24
// amendment). It is what a worker executor has always had
// (SetSharedPoolBudget), given to the embedded engine, the HTTP API's local
// path and the coordinator's local execution.
//
// A statement does not use the pool's manager directly: the planner takes a
// StatementScope of it, which shares the tracker and the relief registry and
// owns the statement's spill files. There are no per-statement or per-task
// trackers under the pool.
//
// The budget covers TRACKED query-execution memory only. It is not a process
// ceiling: result buffering, ingest, compaction, caches, other pools in the
// same process and the embedding application are outside it (ADR-0006).
//
// Lifetime: created with the instance, and garbage with it. Nothing has to
// be closed — every statement removes its own scope's files at its end, and
// the pool's root directory (`<scratch>/wadjet-spill`) is shared with every
// other pool on the same scratch root, so no pool removes it.
type ExecutionPool struct {
	tracker *Tracker
	spill   *SpillManager
	err     error
}

// ResolveExecutionBudget is how an engine instance turns its configured
// budget into a figure: the configured value when positive, otherwise 75% of
// the container's cgroup limit, otherwise 75% of physical memory, otherwise 0
// (no budget can be determined).
func ResolveExecutionBudget(configured int64) int64 {
	if configured > 0 {
		return configured
	}
	if b := DetectBudget(); b > 0 {
		return b
	}
	if phys := DetectPhysicalMemory(); phys > 0 {
		return int64(float64(phys) * headroomFactor)
	}
	return 0
}

// NewExecutionPool builds an instance's pool with the resolved budget
// (ResolveExecutionBudget(configured)) and its spill manager under
// `<scratch>/wadjet-spill` (scratch "" = the OS temp dir).
//
// A budget that cannot be determined yields an EMPTY pool (Tracker and
// SpillManager nil): the instance's statements then run untracked, as they
// did when no budget could be found. A spill directory that cannot be created
// yields a pool with a tracker and no manager, and Err says why: its
// statements charge the shared tracker and run without spill governance. In
// neither case does a statement make a private pool of its own.
func NewExecutionPool(name string, configured int64, scratch string) *ExecutionPool {
	budget := ResolveExecutionBudget(configured)
	if budget <= 0 {
		return &ExecutionPool{}
	}
	t := NewTracker(name, budget)
	if scratch == "" {
		scratch = os.TempDir()
	}
	sm, err := NewSpillManager(scratch, t)
	if err != nil {
		return &ExecutionPool{tracker: t, err: err}
	}
	return &ExecutionPool{tracker: t, spill: sm}
}

// Tracker is the pool's root tracker; nil for an empty pool.
func (p *ExecutionPool) Tracker() *Tracker {
	if p == nil {
		return nil
	}
	return p.tracker
}

// SpillManager is the pool's manager; nil for an empty pool or one whose
// spill directory could not be created.
func (p *ExecutionPool) SpillManager() *SpillManager {
	if p == nil {
		return nil
	}
	return p.spill
}

// Budget is the pool's budget in bytes; 0 for an empty pool.
func (p *ExecutionPool) Budget() int64 {
	if p == nil || p.tracker == nil {
		return 0
	}
	return p.tracker.Budget()
}

// Err is why the pool has a tracker and no spill manager, or nil.
func (p *ExecutionPool) Err() error {
	if p == nil {
		return nil
	}
	return p.err
}
