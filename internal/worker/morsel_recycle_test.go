package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// recyclingStubSource is a stubBatchSource that also implements batchRecycler,
// standing in for the parquet scan source whose row-group backing pool is the
// real hook (docs/design/scan-output-backing-reuse.md).
type recyclingStubSource struct {
	stubBatchSource
	count     atomic.Int64
	armed     atomic.Int64
	recycleCh chan *batch.RecordBatch
	mints     chan batch.MintStamp
}

func (s *recyclingStubSource) armBackingReuse() { s.armed.Add(1) }

// RecycleBatch records the release. The channel sends are non-blocking so a
// test that provokes MORE releases than it expects fails on the count
// assertion instead of deadlocking.
func (s *recyclingStubSource) RecycleBatch(b *batch.RecordBatch, mint batch.MintStamp) {
	s.count.Add(1)
	select {
	case s.recycleCh <- b:
	default:
	}
	select {
	case s.mints <- mint:
	default:
	}
}

func newRecyclingStubSource(batches []*batch.RecordBatch) *recyclingStubSource {
	return &recyclingStubSource{
		stubBatchSource: stubBatchSource{batches: batches},
		recycleCh:       make(chan *batch.RecordBatch, len(batches)+1),
		mints:           make(chan batch.MintStamp, len(batches)+1),
	}
}

// unwrappingStub wraps a source the way the fragment path's timedSource and
// bloomFilteredSource do.
type unwrappingStub struct{ inner exec.Source }

func (u *unwrappingStub) Init(ctx context.Context) error { return u.inner.Init(ctx) }
func (u *unwrappingStub) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return u.inner.Next(ctx)
}
func (u *unwrappingStub) Close() error              { return u.inner.Close() }
func (u *unwrappingStub) unwrapSource() exec.Source { return u.inner }

// TestDispenserRecyclesParentOnLastRetire is the RELEASE half of the scan
// backing pool's ownership rule: the parent row-group batch is offered back
// exactly once, and only after the LAST sibling view has retired — never
// while any consumer still holds one.
func TestDispenserRecyclesParentOnLastRetire(t *testing.T) {
	prev := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 64 << 10
	t.Cleanup(func() { morselDispenserBudgetBytes = prev })

	parent := makeMorselTestBatch(t, 0, 40_000)
	src := newRecyclingStubSource([]*batch.RecordBatch{parent})
	d := newMorselDispenser(4, true)
	d.budget = morselDispenserBudgetBytes

	morsels := collectMorsels(t, d, &src.stubBatchSource)
	// Resolve the hook the way run() does — collectMorsels drives the
	// embedded stub, so wire it explicitly for this assertion.
	d.recycler = src
	if len(morsels) < 2 {
		t.Fatalf("expected a split parent, got %d morsels", len(morsels))
	}
	for i, m := range morsels {
		if got := src.count.Load(); got != 0 {
			t.Fatalf("parent recycled after %d/%d retires", i, len(morsels))
		}
		m.retire()
	}
	if got := src.count.Load(); got != 1 {
		t.Fatalf("expected exactly one recycle after the last retire, got %d", got)
	}
	if got := <-src.recycleCh; got != parent {
		t.Fatal("the dispenser recycled something other than the parent")
	}
}

// TestDispenserRecyclesUnsplitParent covers the non-split path (a parent small
// enough to travel whole, or a retaining sink that forbids views).
func TestDispenserRecyclesUnsplitParent(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 64)
	src := newRecyclingStubSource([]*batch.RecordBatch{parent})
	d := newMorselDispenser(2, false)

	morsels := collectMorsels(t, d, &src.stubBatchSource)
	d.recycler = src
	if len(morsels) != 1 {
		t.Fatalf("expected one morsel, got %d", len(morsels))
	}
	morsels[0].retire()
	if got := src.count.Load(); got != 1 {
		t.Fatalf("expected one recycle, got %d", got)
	}
}

// TestBatchRecyclerOfUnwrapsFragmentSources: the fragment path wraps the scan
// source (timing, deferred blooms) before the dispenser ever sees it, so the
// optional-interface assertion must reach through.
func TestBatchRecyclerOfUnwrapsFragmentSources(t *testing.T) {
	src := newRecyclingStubSource(nil)
	if got := batchRecyclerOf(src); got == nil {
		t.Fatal("direct source not recognized")
	}
	wrapped := &unwrappingStub{inner: &unwrappingStub{inner: src}}
	if got := batchRecyclerOf(wrapped); got == nil {
		t.Fatal("wrapped source not recognized")
	}
	if src.armed.Load() != 2 {
		t.Fatalf("resolving the hook must arm the source's pool, armed=%d", src.armed.Load())
	}
	if got := batchRecyclerOf(&stubBatchSource{}); got != nil {
		t.Fatal("a source that owns nothing must not be offered a release hook")
	}
	// A wrapper chain that never terminates in a recycler is nil, not a hang.
	if got := batchRecyclerOf(&unwrappingStub{inner: &stubBatchSource{}}); got != nil {
		t.Fatal("unexpected recycler through a plain source")
	}
}

// TestDispenserRunResolvesRecycler proves run() itself wires the hook (the
// tests above set it by hand to isolate retire ordering).
func TestDispenserRunResolvesRecycler(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 32)
	src := newRecyclingStubSource([]*batch.RecordBatch{parent})
	d := newMorselDispenser(2, false)

	done := make(chan error, 1)
	go func() { done <- d.run(context.Background(), &unwrappingStub{inner: src}) }()
	var got []morsel
	for m := range d.ch {
		got = append(got, m)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if d.recycler == nil {
		t.Fatal("run did not resolve the source's recycle hook")
	}
	for _, m := range got {
		m.retire()
	}
	if n := src.count.Load(); n != 1 {
		t.Fatalf("expected one recycle, got %d", n)
	}
}

// TestDispenserUnsplitRetireIsIdempotent: the SPLIT path has always been
// retire-once (the refcount reaches zero exactly once), but the unsplit path
// ran its body on every call. A consumer that retires the same morsel twice
// therefore double-credited the byte budget and released the parent's backing
// a second time — and a second release that lands after the storage has been
// re-minted is exactly the shape that hands one buffer to two decoders.
func TestDispenserUnsplitRetireIsIdempotent(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 64)
	src := newRecyclingStubSource([]*batch.RecordBatch{parent})
	d := newMorselDispenser(2, false)

	morsels := collectMorsels(t, d, &src.stubBatchSource)
	d.recycler = src
	if len(morsels) != 1 {
		t.Fatalf("expected one unsplit morsel, got %d", len(morsels))
	}
	inFlight := d.inFlight.Load()
	if inFlight <= 0 {
		t.Fatalf("precondition: the parent's cost should be in flight, got %d", inFlight)
	}

	morsels[0].retire()
	after := d.inFlight.Load()
	morsels[0].retire()
	morsels[0].retire()

	if got := src.count.Load(); got != 1 {
		t.Fatalf("the parent was released %d times, want exactly 1", got)
	}
	if got := d.inFlight.Load(); got != after {
		t.Fatalf("repeated retires double-credited the byte budget: in flight %d then %d", after, got)
	}
}

// TestDispenserCapturesMintAtDelivery: the stamp a release names must be the
// one the batch carried when the consumer took delivery of it, not whatever it
// carries when retire runs. Reading it at release time would make a stale
// retire indistinguishable from a live one after the storage is re-minted.
func TestDispenserCapturesMintAtDelivery(t *testing.T) {
	parent := makeMorselTestBatch(t, 0, 64)
	delivered := batch.MintStamp{Owner: 7, Seq: 3}
	parent.SetMint(delivered)

	src := newRecyclingStubSource([]*batch.RecordBatch{parent})
	d := newMorselDispenser(2, false)
	morsels := collectMorsels(t, d, &src.stubBatchSource)
	d.recycler = src
	if len(morsels) != 1 {
		t.Fatalf("expected one morsel, got %d", len(morsels))
	}

	// The producer re-mints the storage before the consumer gets round to
	// retiring — the race the generation check exists for.
	parent.SetMint(batch.MintStamp{Owner: 7, Seq: 4})
	morsels[0].retire()

	if got := <-src.mints; got != delivered {
		t.Fatalf("release named stamp %+v, want the delivery stamp %+v", got, delivered)
	}
}
