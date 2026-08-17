package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Parallel emit of adopted partitions (G4 in
// docs/benchmarks/high-card-aggregation-gap-2026-08-17.md).
//
// Partitioned parallel aggregation (partitioned_agg.go) already gives each
// worker ownership of a disjoint hash partition of the group-key space, and
// MergeSink ADOPTS those partitions wholesale instead of re-inserting their
// groups. But the emission threw the parallelism away: Next() streamed the
// primary's own state and then each adopted partition ONE AT A TIME, on the
// single goroutine of a serial downstream pipeline. At ~46 ns/group that
// tail measured 16-35% of ClickBench Q33/Q34/Q35 wall clock — 100M groups
// is >=4.6s of irreducibly serial work.
//
// The partitions are disjoint by construction and each one's emission touches
// only its own state, so the drain fans out cleanly: one goroutine per unit
// (the primary's own state plus every adopted partition), each running the
// SAME per-unit emission loop as before, handing finished batches to the
// consumer over a bounded channel. The expensive half — key decoding,
// writeAccToColumn, output batch construction — runs k-wide; the consumer
// keeps doing the cheap half (downstream projection, Top-N insert), which
// profiles at <2%.
//
// What this does NOT change: the values in each row, or which groups appear.
// It DOES change the ORDER batches leave the aggregate in (partitions
// interleave instead of concatenating). That was never a contract — group
// emission order already depends on morsel arrival order under parallel
// aggregation — and any user-visible ordering comes from a downstream ORDER
// BY, which is unaffected.
//
// Spilled aggregates keep the serial path (see parallelEmitEligible): their
// emission runs through the streaming partial-state merger, which owns file
// cursors and retired off-heap registries with their own lifecycle.

// parallelEmitToggle is the kill switch (WADJET_PARALLEL_EMIT=0), registered
// so the invariance oracle sweeps it like every other optimization.
var parallelEmitToggle = optswitch.Register("parallel-emit", "WADJET_PARALLEL_EMIT",
	"concurrent drain of adopted disjoint aggregate partitions during emission")

// ParallelEmitRuns counts emissions that took the parallel drain
// (observability + test assertions).
var ParallelEmitRuns atomic.Int64

// emitDrain fans an aggregate's emission across one goroutine per drain unit.
//
// Ownership: unit i is emitted by producer i and by nobody else, so no lock
// is needed over any group state. Adopted units are CLOSED by their own
// producer (releasing that unit's off-heap registry) before its wg.Done, so
// once shutdown returns every unit is quiesced and the primary's Close can
// safely tear down shared structures.
type emitDrain struct {
	out  chan *batch.RecordBatch
	stop chan struct{}
	wg   sync.WaitGroup
	// err holds the first producer error. Always a *fmt.wrapError so the
	// atomic.Value type-consistency rule holds.
	err  atomic.Value
	once sync.Once
}

// parallelEmitEligible reports whether Next() may fan the emission out.
//
// Requirements:
//   - the kill switch is on;
//   - there is something to parallelize (>=1 adopted partition);
//   - NO unit emits through the spill machinery. A spilled unit drains via
//     finalizeViaPartialMerge's streaming k-way merger, which holds run-file
//     cursors and retired off-heap registries closed on a schedule tied to
//     the merger's own exhaustion; that path stays exactly as it was.
func (h *HashAggregate) parallelEmitEligible() bool {
	if !parallelEmitToggle.On() || len(h.adoptedPartitions) == 0 {
		return false
	}
	if h.emitsFromSpill() {
		return false
	}
	for _, ap := range h.adoptedPartitions {
		if ap == nil || ap.emitsFromSpill() {
			return false
		}
	}
	return true
}

// emitsFromSpill reports whether this aggregate's emission is served by the
// partial-state spill machinery rather than straight from in-memory group
// state. partialMerger is the live signal after Finalize; the file lists are
// belt-and-braces for callers that inspect before Finalize.
func (h *HashAggregate) emitsFromSpill() bool {
	return h.partialMerger != nil || len(h.partialSpillFiles) > 0 ||
		len(h.spillFiles) > 0 || len(h.drainedRuns) > 0
}

// startParallelEmit launches the drain. The adopted partition slice is handed
// to the drain — from here on the producers own those units' lifecycle.
func (h *HashAggregate) startParallelEmit(ctx context.Context) {
	units := h.adoptedPartitions
	h.adoptedPartitions = nil

	d := &emitDrain{
		// One slot per unit bounds in-flight batches at ~2 per unit (one
		// queued, one being built), which is the same order of transient
		// output memory the serial path holds per emitted batch.
		out:  make(chan *batch.RecordBatch, len(units)+1),
		stop: make(chan struct{}),
	}
	d.wg.Add(len(units) + 1)
	go d.produce(ctx, h, true)
	for _, u := range units {
		go d.produce(ctx, u, false)
	}
	go func() {
		d.wg.Wait()
		close(d.out)
	}()

	h.emit = d
	ParallelEmitRuns.Add(1)
}

// produce drains one unit to exhaustion. ownState is true for the primary,
// whose Close belongs to the pipeline; adopted units are closed here.
func (d *emitDrain) produce(ctx context.Context, unit *HashAggregate, ownState bool) {
	// Defers run LIFO: the unit is fully closed BEFORE wg.Done, so a
	// shutdown that observes the channel close knows every registry is
	// already unmapped.
	defer d.wg.Done()
	if !ownState {
		defer unit.Close()
	}
	for {
		b, err := unit.nextOwn(ctx)
		if err != nil {
			d.err.CompareAndSwap(nil, fmt.Errorf("draining aggregate partition: %w", err))
			return
		}
		if b == nil {
			return
		}
		select {
		case d.out <- b:
		case <-d.stop:
			b.Release()
			return
		case <-ctx.Done():
			b.Release()
			d.err.CompareAndSwap(nil, fmt.Errorf("draining aggregate partition: %w", ctx.Err()))
			return
		}
	}
}

// next returns the next emitted batch, or (nil, nil) once every unit is
// exhausted. Safe to call from multiple goroutines (it is a channel receive
// plus an atomic load), though today's downstream pipeline is serial.
func (d *emitDrain) next() (*batch.RecordBatch, error) {
	if v := d.err.Load(); v != nil {
		d.shutdown()
		return nil, v.(error)
	}
	b, ok := <-d.out
	if !ok {
		if v := d.err.Load(); v != nil {
			return nil, v.(error)
		}
		return nil, nil
	}
	return b, nil
}

// shutdown stops the producers and blocks until every one of them has exited
// and closed its unit. Idempotent. In-flight batches are released rather than
// delivered, so an early termination (downstream LIMIT satisfied, query
// cancelled) leaks nothing.
func (d *emitDrain) shutdown() {
	d.once.Do(func() { close(d.stop) })
	// Ranging to channel close is the join: the closer goroutine runs after
	// wg.Wait(), i.e. after every producer's unit.Close().
	for b := range d.out {
		b.Release()
	}
}
