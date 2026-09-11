package physical

import (
	"context"
	"sync"
)

// loadGate admits concurrent file loads by bytes, held until the last row
// group is consumed (releaseRG) or the scan is torn down (drainAbandoned).
// Whole-file admission charges the file's live heap. Row-group admission charges
// only the file's largest row group; several RG workers may hold groups at once.
// On that path budget bounds download concurrency, not live heap: per-row-group
// charges on the query memory tracker bound the heap (#789).
// When nothing is inflight, always admit one load, even if it exceeds budget.
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
