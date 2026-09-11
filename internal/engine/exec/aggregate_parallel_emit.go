package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Parallel emission gives each disjoint adopted partition and the primary's own
// state one producer, running the same per-unit emission loop through a bounded channel.
// Each producer touches only its unit's state. Rows and groups stay identical;
// batches may interleave, since group emission order is not a contract.
// User-visible ordering requires downstream ORDER BY.
// Spilled units stay serial: the streaming partial-state merger owns run cursors
// and retired off-heap registries with its own lifecycle (parallelEmitEligible).
// See docs/internals/adopted-aggregate-parallel-emission.md for the design.

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
	// err holds the first producer error. FirstError boxes it so the
	// atomic.Value type-consistency rule holds no matter which shape of
	// error a producer reports (#512).
	err  FirstError
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
		// Waiting and closing is still a query's goroutine: unrecovered,
		// a panic here ends the process rather than the query (#511).
		//
		// next() blocks on a receive from d.out until this closes it, so
		// recovering without closing would hang the query instead of
		// crashing the server. The close is this goroutine's obligation and
		// the boundary discharges it (ADR-0019).
		closedOut := false
		defer CatchQueryPanic(ctx, "aggregate emit closer", func(err error) {
			d.err.Set(err)
			if !closedOut {
				closedOut = true
				close(d.out)
			}
		})
		d.wg.Wait()
		closedOut = true
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
	// A panic raised by this unit's emission happens on THIS goroutine, so
	// Pipeline.Run's recover cannot see it and the panic takes the PROCESS
	// down — in a server, every connected client's query, not just the
	// offending one (#400). Convert it to the drain's error, exactly as
	// runParallel does for its own workers, so a value the output vector
	// cannot hold (#392) becomes a query error. Since #511 that holds for
	// ANY panic, not only the FatalEvalPanic class.
	//
	// Registered last, so it runs FIRST on the way out: the unit is still
	// closed and wg.Done still fires.
	defer CatchQueryPanic(ctx, "aggregate emit", func(err error) {
		d.err.Set(fmt.Errorf("draining aggregate partition: %w", err))
	})
	for {
		b, err := unit.nextOwn(ctx)
		if err != nil {
			d.err.Set(fmt.Errorf("draining aggregate partition: %w", err))
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
			d.err.Set(fmt.Errorf("draining aggregate partition: %w", ctx.Err()))
			return
		}
	}
}

// next returns the next emitted batch, or (nil, nil) once every unit is
// exhausted. Safe to call from multiple goroutines (it is a channel receive
// plus an atomic load), though today's downstream pipeline is serial.
func (d *emitDrain) next() (*batch.RecordBatch, error) {
	if err := d.err.Err(); err != nil {
		d.shutdown()
		return nil, err
	}
	b, ok := <-d.out
	if !ok {
		if err := d.err.Err(); err != nil {
			return nil, err
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
