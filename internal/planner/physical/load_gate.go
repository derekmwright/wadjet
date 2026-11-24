// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"errors"
	"sync"
)

// loadGate admits concurrent file loads by bytes, held until the last row
// group is consumed (releaseRG) or the scan is torn down (drainAbandoned).
// Whole-file admission charges the file's live heap. Row-group admission charges
// only the file's largest row group; several RG workers may hold groups at once.
// On that path budget bounds download concurrency, not live heap: per-row-group
// charges on the query memory tracker bound the heap (#789).
// When nothing is inflight, always admit one load, even if it exceeds budget.
//
// No goroutine waits here for bytes whose release needs the scan's consumer
// to make progress (#1706: TPC-H Q09, Q16 and Q18 at SF1 under 16 MiB never
// returned). A slot's bytes come back when its LAST row group has been
// decoded, and once a budgeted scan is on demand (scan_decode_admission.go)
// its remaining row groups are placeholders that only the scan's consumer
// decodes — the goroutine calling Next, or a parallel pipeline's workers,
// which share the source. So:
//
//   - the CONSUMER's load (acquireDemand) waits only while its bytes would fit
//     once the holders the workers alone will release have released — a
//     holder each of whose owed row groups is an admitted read-ahead a worker
//     is decoding. When the room it needs is held for row groups only the
//     consumer decodes, it is admitted at once, past the budget if need be;
//   - a READ-AHEAD parked here (acquireFor) holds its file's slot while it
//     waits. When the consumer comes for that slot, the read-ahead goes on
//     waiting only while the room it waits for comes back without the
//     consumer; otherwise it stops, admitting nothing, and leaves the slot
//     and its row group to the consumer.
//
// Under a parallel pipeline a sibling consumer might decode those row groups
// while one waits, and the base's waits often ended that way; but the sibling
// can itself be parked here, or blocked behind the waiter downstream (a
// partitioned aggregate's queues), and the gate cannot tell. So room owed to
// the consumer counts as room that does not come back while a consumer waits,
// whichever consumer holds it. Without a budget no scan is on demand and none
// of this is reached.
type loadGate struct {
	budget   int64 // max inflight bytes (soft: single oversized load admits alone)
	maxLanes int   // hard cap on concurrent loads (connection sanity)

	mu       sync.Mutex
	inflight int64
	lanes    int
	freeCh   chan struct{} // closed+replaced on every release (broadcast)
	// held is every slot holding an admission, with its bytes, which the
	// consumer's admission reads to know whether a wait could be on the consumer.
	held map[*fileSlot]int64
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
		held:     map[*fileSlot]int64{},
	}
}

// errLoadGateYield is what acquireFor returns when it stopped waiting because
// the consumer needs the slot and the room is owed to the consumer. Nothing
// was admitted.
var errLoadGateYield = errors.New("load gate: the consumer needs this slot; the read-ahead yields its row group to it")

// acquire blocks until n bytes fit under the budget (and a lane is free),
// or nothing is inflight, or ctx is cancelled.
func (g *loadGate) acquire(ctx context.Context, n int64) error {
	return g.acquireFor(ctx, n, nil, nil)
}

// acquireFor is a read-ahead's admission for holder's load: acquire, except
// that once wanted is closed — the consumer is waiting for holder — it returns
// errLoadGateYield, admitting nothing, as soon as n would not fit even after
// every holder the workers alone release has released (fitsWithoutConsumer):
// that wait could end only through the consumer, which waits for it. Until
// then it waits as acquire does. A nil wanted never fires.
func (g *loadGate) acquireFor(ctx context.Context, n int64, holder *fileSlot, wanted <-chan struct{}) error {
	if n < 1 {
		n = 1
	}
	isWanted := false
	for {
		g.mu.Lock()
		if g.fits(n) {
			g.take(n, holder)
			g.mu.Unlock()
			return nil
		}
		if isWanted && !g.fitsWithoutConsumer(n) {
			g.mu.Unlock()
			return errLoadGateYield
		}
		wait := g.freeCh
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wanted:
			isWanted, wanted = true, nil // re-checked above; a nil channel never fires again
		case <-wait:
		}
	}
}

// acquireDemand is the scan consumer's admission for holder's load. It waits
// for room like acquire as long as the room can come back without the
// consumer: n would fit once every holder the workers alone will release
// (fileSlot.releasesWithoutConsumer) has released. When it would not — the
// bytes it needs are held for row groups only the consumer decodes — it
// takes n at once, past the budget and the lane cap if need be. So it waits
// only where the wait ends without any consumer's progress.
func (g *loadGate) acquireDemand(ctx context.Context, n int64, holder *fileSlot) error {
	if n < 1 {
		n = 1
	}
	for {
		g.mu.Lock()
		if g.fits(n) || !g.fitsWithoutConsumer(n) {
			g.take(n, holder)
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

// fits reports whether n bytes are admitted now. Caller holds g.mu.
func (g *loadGate) fits(n int64) bool {
	return g.lanes == 0 || (g.inflight+n <= g.budget && g.lanes < g.maxLanes)
}

// fitsWithoutConsumer reports whether n would fit once every holder whose
// bytes come back with no further progress from the scan's consumer has
// released: fits, evaluated over the holders that owe the consumer a row
// group. Caller holds g.mu.
func (g *loadGate) fitsWithoutConsumer(n int64) bool {
	var bytes int64
	lanes := 0
	for s, b := range g.held {
		if !s.releasesWithoutConsumer() {
			bytes += b
			lanes++
		}
	}
	return lanes == 0 || (bytes+n <= g.budget && lanes < g.maxLanes)
}

// take records an admission. Caller holds g.mu.
func (g *loadGate) take(n int64, holder *fileSlot) {
	g.inflight += n
	g.lanes++
	if holder != nil {
		g.held[holder] = n
	}
}

// release returns n bytes to the budget and frees a lane.
func (g *loadGate) release(n int64) { g.releaseFor(n, nil) }

// releaseFor returns holder's n bytes and its lane.
func (g *loadGate) releaseFor(n int64, holder *fileSlot) {
	if n < 1 {
		n = 1
	}
	g.mu.Lock()
	g.inflight -= n
	g.lanes--
	if holder != nil {
		delete(g.held, holder)
	}
	close(g.freeCh)
	g.freeCh = make(chan struct{})
	g.mu.Unlock()
}
