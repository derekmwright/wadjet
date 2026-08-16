package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// morselDispenserBudgetBytes bounds the decoded-but-unretired source bytes a
// parallel fragment may hold in flight (channel + consumer hands). The
// count-bounded channel this replaces (cap 2·k) assumed ~2048-row batches; on
// the SF100 parquet path a source batch is one decoded ROW GROUP (~280 MB for
// lineitem), so 2·k + k in-hand meant ~7 GB of tracker-invisible heap per
// task — ×max_concurrent that cleared GOMEMLIMIT and killed Q17/Q18
// (2026-07-03 A/B, docs/design/morsel-execution.md §4.1). Bytes, not counts,
// are what the never-OOM property needs.
//
// A batch larger than the whole budget is admitted alone (in-flight == 0), so
// oversized row groups degrade to serial-like alternation instead of
// deadlocking. Package-level var so tests can shrink it.
var morselDispenserBudgetBytes int64 = 512 << 20

// morsel is one unit of work handed to a fragment consumer: a batch (possibly
// a zero-copy view of a larger decoded parent) plus a retire callback that
// returns the parent's bytes to the dispenser budget once every sibling view
// has been retired. Consumers MUST call retire exactly once, after they are
// done reading the batch (post op-chain + sink consume) — for pipeline
// breakers that point is after Consume returns, when whatever the sink keeps
// is copied out (HashAggregate group state) or charged to the memory ledger
// (Sort), so the handoff from bounded-untracked to tracked is seamless.
type morsel struct {
	b      *batch.RecordBatch
	retire func()
}

// morselDispenser replaces the parallel fragment paths' raw batch channel:
// a single producer goroutine pulls from the fragment source, admits each
// decoded batch against a byte budget, optionally splits it into
// ~DefaultBatchSize zero-copy views, and feeds k consumers.
//
// Splitting is what makes row-group-sized batches safe AND parallel: k
// consumers work different slices of the same admitted parent instead of
// each holding a private 280 MB batch, per-morsel backpressure checks
// actually run every ~2048 rows instead of once per row group, and derived
// batches (join-probe output pools size off ActiveLen) stay morsel-sized.
//
// View safety rests on audited facts about the fragment op chains (see
// morsel-execution.md §4.1 v1.5): linear-path operators (exec.Filter,
// exec.Project, HashJoinProbe, DynamicFilterEmitOp) never write input column
// storage, never append to or replace in.Columns, and mutate only the batch's
// own Sel FIELD (pointer reassignment to operator-private scratch). Views
// therefore share the parent's column vectors and get a private Sel slice.
// The Sel slices are three-index subslices of one parent-sized array, so
// even an op that compacted in place into in.Sel (the KernelFilter family —
// not built for fragments today) would write only its own view's region.
//
// split must be false when the downstream sink RETAINS consumed batches and
// charges them by MemBytes (exec.Sort stores the batch and charges
// b.MemBytes(), which is Sel-blind — each retained view would charge the
// full parent). HashAggregate copies rows out during Consume, so views are
// safe there.
type morselDispenser struct {
	ch     chan morsel
	budget int64
	split  bool
	k      int // consumer count; sizes the adaptive view split (see dispatch)

	// inFlight is added to only by the producer (admit) and subtracted only
	// by retire callbacks; the producer's check-then-add is single-writer
	// safe. signal (cap 1) kicks a blocked producer after a retire.
	inFlight atomic.Int64
	signal   chan struct{}

	// Stats, logged by the fragment runner at completion.
	parents       atomic.Int64
	splits        atomic.Int64
	morsels       atomic.Int64
	peakInFlight  atomic.Int64
	producerWaits atomic.Int64 // ns spent blocked in admit

	// Width-plateau attribution (2026-08-15, q08 join-6 ~10/15 effective
	// cores): consumerDryNs is consumer time blocked on an EMPTY morsel
	// channel (slot yielded — the dispenser paces the fragment);
	// processNs is Σ consumer time inside process(m) (chain + sink), so
	// processNs / elapsed = the fragment's true effective width. Read
	// together with widthGate.claimWaitNs (token-paced): whichever of
	// dry-wait vs claim-wait dominates names the plateau's pacer.
	consumerDryNs atomic.Int64
	processNs     atomic.Int64
}

func newMorselDispenser(k int, split bool) *morselDispenser {
	return &morselDispenser{
		ch:     make(chan morsel, 2*k),
		budget: morselDispenserBudgetBytes,
		split:  split,
		k:      k,
		signal: make(chan struct{}, 1),
	}
}

// morselMaxViewRows caps the adaptive view size. Bounds the transients that
// scale with view size (join-probe output pools size off ActiveLen) and the
// backpressure/collapse checkpoint interval.
const morselMaxViewRows = 64 << 10

// viewRowsFor sizes the split for an n-active-row batch: ~4 views per
// consumer so k consumers stay load-balanced within one parent, with a
// DefaultBatchSize floor and morselMaxViewRows cap. Views sized down to 2048
// unconditionally (the v1.5 shape) made the SF10 suite 15% SLOWER: a 1M-row
// row group became ~500 channel handoffs + mutexed sink consumes, and the
// producer could not decode ahead — the fixed costs sat on the critical
// path. ~4·k views keep the same parallelism with ~16× fewer handoffs.
func (d *morselDispenser) viewRowsFor(n int) int {
	rows := batch.DefaultBatchSize
	if d.k > 0 {
		if t := (n + 4*d.k - 1) / (4 * d.k); t > rows {
			rows = t
		}
	}
	if rows > morselMaxViewRows {
		rows = morselMaxViewRows
	}
	return rows
}

// run is the producer loop: source → admit → split → channel. Closes the
// morsel channel on return. Returns nil on EOF, the source/context error
// otherwise. Runs until the source is exhausted or ctx is cancelled — after
// a pressure collapse the serial continuation keeps draining the channel, so
// the producer's lifetime is the whole fragment, not the parallel section.
func (d *morselDispenser) run(ctx context.Context, src exec.Source) error {
	defer close(d.ch)
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return fmt.Errorf("source next: %w", err)
		}
		if b == nil {
			return nil
		}
		cost := b.MemBytes()
		if err := d.admit(ctx, cost); err != nil {
			return err
		}
		if err := d.dispatch(ctx, b, cost); err != nil {
			return err
		}
	}
}

// admit blocks until cost fits under the byte budget. A batch is always
// admitted when nothing is in flight, whatever its size — one oversized row
// group must alternate with consumption, not deadlock.
func (d *morselDispenser) admit(ctx context.Context, cost int64) error {
	var blocked time.Time
	for {
		cur := d.inFlight.Load()
		if cur == 0 || cur+cost <= d.budget {
			nv := d.inFlight.Add(cost)
			if nv > d.peakInFlight.Load() {
				d.peakInFlight.Store(nv)
			}
			if !blocked.IsZero() {
				d.producerWaits.Add(time.Since(blocked).Nanoseconds())
			}
			d.parents.Add(1)
			return nil
		}
		if blocked.IsZero() {
			blocked = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.signal:
		}
	}
}

// release returns bytes to the budget and kicks the producer if it is
// blocked in admit. The non-blocking send is race-free: a missed kick means
// a signal is already pending, and the producer re-reads inFlight after
// every wake.
func (d *morselDispenser) release(n int64) {
	d.inFlight.Add(-n)
	select {
	case d.signal <- struct{}{}:
	default:
	}
}

// splitMinCost is the parent size above which dispatch splits into views:
// parents small enough that 2k of them fit under the byte budget give every
// consumer queue depth without splitting, and per-view processing is pure
// overhead for them — the SF10 v1.5/v1.6 A/Bs (2026-07-03) measured
// splitting typical SF10 units (~25 MB row groups, 64k-row chunks) at
// +10-45% wall on join/probe fragments with no memory benefit. Parents
// BIGGER than budget/(2k) are the SF100 failure shape (280 MB decoded row
// groups): without splitting, k consumers each holding one would either
// blow the heap or serialize behind the budget, so views are what make
// k-wide consumption of them memory-safe.
func (d *morselDispenser) splitMinCost() int64 {
	k := int64(d.k)
	if k < 1 {
		k = 1
	}
	return d.budget / (2 * k)
}

// dispatch splits the admitted batch into morsels and sends them. The whole
// parent's cost is released when the last sibling retires.
func (d *morselDispenser) dispatch(ctx context.Context, b *batch.RecordBatch, cost int64) error {
	n := b.ActiveLen()
	viewRows := d.viewRowsFor(n)
	if !d.split || cost <= d.splitMinCost() || n <= viewRows {
		d.morsels.Add(1)
		return d.send(ctx, morsel{b: b, retire: func() { d.release(cost) }})
	}

	numViews := (n + viewRows - 1) / viewRows
	var refs atomic.Int64
	refs.Store(int64(numViews))
	retire := func() {
		if refs.Add(-1) == 0 {
			d.release(cost)
		}
	}

	// Warm every column's lazily-cached null memo while still
	// single-threaded: Bitmap.HasNulls computes-and-caches on first call
	// (a write into the SHARED parent vector), so k consumers reading
	// sibling views would race on it. After warming, concurrent HasNulls
	// calls take the cached read-only path. Same rule as the op-chain
	// warmup for ColRef/ColumnCompare index caches.
	for _, v := range b.Columns {
		warmNullMemos(v)
	}

	// One parent-sized Sel array, subdivided into capped three-index slices
	// so no view can append or write past its own region.
	sel := make([]uint32, n)
	if b.Sel != nil {
		copy(sel, b.Sel)
	} else {
		for i := range sel {
			sel[i] = uint32(i)
		}
	}
	d.splits.Add(1)
	d.morsels.Add(int64(numViews))
	for start := 0; start < n; start += viewRows {
		end := start + viewRows
		if end > n {
			end = n
		}
		cols := make([]*batch.Vector, len(b.Columns))
		copy(cols, b.Columns)
		view := &batch.RecordBatch{
			Columns: cols,
			Schema:  b.Schema,
			Len:     b.Len,
			Sel:     sel[start:end:end],
		}
		if err := d.send(ctx, morsel{b: view, retire: retire}); err != nil {
			return err
		}
	}
	return nil
}

// warmNullMemos forces Bitmap.HasNulls' compute-and-cache for a vector and
// its nested children so later concurrent readers hit the cached path.
func warmNullMemos(v *batch.Vector) {
	if v == nil {
		return
	}
	v.Nulls.HasNulls()
	warmNullMemos(v.Child)
	for _, ch := range v.Children {
		warmNullMemos(ch)
	}
}

func (d *morselDispenser) send(ctx context.Context, m morsel) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.ch <- m:
		return nil
	}
}

// logStats emits one debug line summarizing the dispenser's run; called by
// the fragment runner after the fragment finishes.
func (d *morselDispenser) logAttrs() []any {
	return []any{
		"dispenser_parents", d.parents.Load(),
		"dispenser_splits", d.splits.Load(),
		"dispenser_morsels", d.morsels.Load(),
		"dispenser_peak_mb", d.peakInFlight.Load() / (1 << 20),
		"dispenser_producer_wait_ms", d.producerWaits.Load() / 1e6,
		"consumer_dry_wait_ms", d.consumerDryNs.Load() / 1e6,
		"process_ms", d.processNs.Load() / 1e6,
	}
}
