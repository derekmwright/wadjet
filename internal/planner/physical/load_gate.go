package physical

import (
	"context"
	"sync"
)

// loadGate admits concurrent file loads by BYTES instead of file count.
//
// Its predecessor was a 4-slot counting semaphore (loadConcurrency = 4),
// sized for SF100 lineitem files: "4 × 300 MB = 1.2 GB peak". That bound
// was really a byte bound expressed in file units, and it collapsed on
// small-file layouts: SF10 ships 600 × 6.5 MB lineitem files, and 4
// concurrent GETs left a 16-core box ~98% idle on cold S3 (2026-07-05
// Q06 profile: 56.6s wall, 7.98s CPU — pure download wait; suite 7×
// slower than DuckDB httpfs on the same instance). Bounding bytes
// directly gives small-file scans the lane count they need while holding
// the same heap ceiling for large files.
//
// Semantics match the old semaphore: a slot's bytes are held from load
// admission until the file's last row group is consumed (releaseRG) or
// the scan is torn down (drainAbandoned).
//
// What those bytes MEAN differs by read path, and the difference is not
// silent:
//
//   - Whole-file read: the admitted bytes are the file, and they ARE the
//     live heap of the loaded file, not just the download window.
//   - Row-group read (scan_rowgroup_load.go): the admitted bytes are the
//     file's LARGEST ROW GROUP, and the slot may hold several row groups
//     at once — one per rg worker that has demanded one and not yet
//     released it. Measured on a 24-row-group file with 4 workers: 3-4
//     resident, 7.0x-9.1x the admitted bytes. So on this path the gate
//     bounds concurrent LOADS by a row group each, not the live heap;
//     what bounds the live heap is the per-row-group charge on the query's
//     memory tracker, which is the bound #789 was about. `budget` here is
//     therefore a download-concurrency ceiling on the row-group path and a
//     heap ceiling on the whole-file one.
//
// Progress guarantee: a load is always admitted when nothing is inflight,
// so a file larger than the whole budget still loads — alone.
type loadGate struct {
	budget   int64 // max inflight bytes (soft: single oversized load admits alone)
	maxLanes int   // hard cap on concurrent loads (connection sanity)

	mu       sync.Mutex
	inflight int64
	lanes    int
	freeCh   chan struct{} // closed+replaced on every release (broadcast)
}

// defaultLoadBudgetBytes bounds inflight loaded-file bytes per scan source
// when no per-query memory budget is configured. 1 GiB ≈ 3 concurrent
// SF100 lineitem files (the old semaphore's working point) and ~32 lanes
// of SF10-sized files (capped by loadGateMaxLanes).
const defaultLoadBudgetBytes = int64(1) << 30

// loadGateMaxLanes caps concurrent loads regardless of file size. 32
// matches the fan-out S3 handles comfortably at file granularity and
// keeps worst-case connection counts bounded on many-column scans.
const loadGateMaxLanes = 32

func newLoadGate(budget int64, maxLanes int) *loadGate {
	return &loadGate{
		budget:   budget,
		maxLanes: maxLanes,
		freeCh:   make(chan struct{}),
	}
}

// acquire blocks until n bytes fit under the budget (and a lane is free),
// or nothing is inflight, or ctx is cancelled.
func (g *loadGate) acquire(ctx context.Context, n int64) error {
	if n < 1 {
		n = 1
	}
	for {
		g.mu.Lock()
		if g.lanes == 0 || (g.inflight+n <= g.budget && g.lanes < g.maxLanes) {
			g.inflight += n
			g.lanes++
			g.mu.Unlock()
			return nil
		}
		wait := g.freeCh
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

// release returns n bytes to the budget and frees a lane.
func (g *loadGate) release(n int64) {
	if n < 1 {
		n = 1
	}
	g.mu.Lock()
	g.inflight -= n
	g.lanes--
	close(g.freeCh)
	g.freeCh = make(chan struct{})
	g.mu.Unlock()
}
