package scan

import (
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// scanBackingReuse gates reuse of the decoded row-group output backing. Off,
// every row group is a fresh batch.NewRecordBatch plus a fresh PreAllocBytes
// arena per BYTES column — the pre-2026-08-22 behavior, kept as the
// invariance-oracle arm and as the kill switch for the ownership rule below.
//
// Why it exists: the 2026-08-22 SF100 window 3
// (docs/benchmarks/sf100-window3-analysis-2026-08-22.md §6) put
// scan.readRowGroupNative.func2 at 130.8 s of Go-heap-lock delay over 3
// workers × 4 runs — 10.9 s per worker per suite run, the #1 named caller once
// the join half (69aecbb, ADR-0016) was fixed — behind ~1 140 GB per suite run
// of allocation. At SF100 lineitem widths a row group is ~280 MB, so every
// allocation on this path is a large object that takes the heap lock directly.
var scanBackingReuse = optswitch.Register("scan-backing-reuse", "WADJET_SCAN_BACKING_REUSE",
	"reuse of the decoded row-group output backing across scan-source row groups")

// Defaults for BackingPool sizing. Package vars so tests can shrink them.
var (
	// defaultMaxIdleBackings is one per default decode-ahead worker
	// (DefaultDecodeAheadWorkers): every decoder that finds the ring free
	// should be able to find a backing free too.
	defaultMaxIdleBackings = DefaultDecodeAheadWorkers

	// defaultMaxIdleBackingBytes bounds the resident cost of the idle set.
	// 512 MiB is the byte budget one fragment is already permitted to hold in
	// decoded source bytes in flight (worker.morselDispenserBudgetBytes), so
	// the idle set never exceeds a number the memory policy already sanctions
	// for exactly these bytes.
	defaultMaxIdleBackingBytes int64 = 512 << 20
)

// BackingPool is one scan source's free list of decoded row-group backings.
//
// # Ownership rule
//
// A backing may be handed to a later decode only when BOTH hold:
//
//  1. RELEASED — the consumer side said it is finished. That is the morsel
//     dispenser's retire callback for the parent batch (fired once every
//     zero-copy view minted over it has retired, which is after the whole op
//     chain AND the sink consume), or the serial fragment path's return from
//     driver.push. This is what a claim check alone cannot supply: the
//     decode-ahead ring (ADR-0015) decodes group N+1 while group N is being
//     consumed, k morsel consumers read one parent concurrently, and
//     HashJoinProbe.emitViewOutput's view columns read our columns through
//     Vector.Base after Execute returned — none of those readers claim.
//
//  2. UNCLAIMED — nobody kept it: neither RecordBatch.Detach on the batch we
//     emitted nor Vector.Claim on any column, including a claim that arrived
//     transitively through a derived batch (ColumnPrune, set-op emit, selView)
//     or through Vector.Base from a view minted downstream over one of our
//     columns. This is what a release signal alone cannot supply: Sort,
//     Window, the hash-join build, SortMergeJoin and the spillable collector
//     all retain past retire, and all of them Detach (ADR-0016).
//
// The release is the liveness signal; the claim is the retention veto. The
// backing is surrendered whole or not at all — one claimed column surrenders
// the batch, permanently, because every column is reachable from the batch a
// consumer kept.
//
// The pool only ever takes back a batch it minted (see outstanding): a WSHF
// shuffle chunk, a row-based fallback batch or a batch already recycled is
// ignored, so no second owner can be created for storage someone else
// recycles.
//
// See docs/design/scan-output-backing-reuse.md for the full statement and the
// preconditions it rests on.
type BackingPool struct {
	maxIdle      int
	maxIdleBytes int64

	mu        sync.Mutex
	idle      []*batch.RecordBatch
	idleBytes int64
	// outstanding is the set of backings this pool handed out and has not
	// taken back. It is the mint stamp: Recycle ignores anything absent from
	// it. Self-pruning — an entry is deleted on the first Recycle whether or
	// not the backing is admitted to idle — so it stays the size of the live
	// set (bounded by the decode window and the dispenser budget).
	outstanding map[*batch.RecordBatch]struct{}

	hits    atomic.Int64
	misses  atomic.Int64
	claimed atomic.Int64
}

// BackingPoolOpts sizes a BackingPool. Zero fields take the defaults.
type BackingPoolOpts struct {
	// MaxIdle bounds the idle free list by count.
	MaxIdle int
	// MaxIdleBytes bounds the idle free list by MemBytes sum. One backing is
	// always keepable regardless of size, mirroring the decode ring's
	// "the delivery-cursor group is always admitted" escape: without it a
	// single row group larger than the cap disables the mechanism on exactly
	// the table that motivates it (SF100 lineitem, ~280 MB per group).
	MaxIdleBytes int64
}

// NewBackingPool returns a pool for one scan source. It is safe for concurrent
// use: decode workers call get, consumers call Recycle.
func NewBackingPool(opts BackingPoolOpts) *BackingPool {
	p := &BackingPool{maxIdle: opts.MaxIdle, maxIdleBytes: opts.MaxIdleBytes}
	if p.maxIdle <= 0 {
		p.maxIdle = defaultMaxIdleBackings
	}
	if p.maxIdleBytes <= 0 {
		p.maxIdleBytes = defaultMaxIdleBackingBytes
	}
	p.outstanding = make(map[*batch.RecordBatch]struct{}, p.maxIdle+2)
	return p
}

// BackingPoolStats is one pool's engagement counters.
type BackingPoolStats struct {
	Hits      int64 // decodes that reused a backing
	Misses    int64 // decodes that had to mint one
	Claimed   int64 // releases refused because a consumer had claimed the batch
	IdleBytes int64
}

// Stats snapshots the counters.
func (p *BackingPool) Stats() BackingPoolStats {
	if p == nil {
		return BackingPoolStats{}
	}
	p.mu.Lock()
	idle := p.idleBytes
	p.mu.Unlock()
	return BackingPoolStats{
		Hits:      p.hits.Load(),
		Misses:    p.misses.Load(),
		Claimed:   p.claimed.Load(),
		IdleBytes: idle,
	}
}

// Drop releases the idle set. Call when the owning source closes: retained
// row-group storage must not outlive the source that owns it.
func (p *BackingPool) Drop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.idle = nil
	p.idleBytes = 0
	p.outstanding = map[*batch.RecordBatch]struct{}{}
	p.mu.Unlock()
}

// get returns a backing reset for (schema, numRows), or nil when the caller
// should mint a fresh one. A returned batch is indistinguishable from
// batch.NewRecordBatch(schema, numRows): ResetForWrite re-slices and CLEARS
// every typed slot, resets the null bitmap to all-valid and empties the BYTES
// arena while retaining its capacity — which is what turns readColumnNative's
// PreAllocBytes into a no-op past the arena's high-water mark.
func (p *BackingPool) get(schema []pqt.Column, numRows int) *batch.RecordBatch {
	if p == nil || !scanBackingReuse.On() || numRows <= 0 {
		return nil
	}
	if !reusableShape(schema) {
		p.misses.Add(1)
		return nil
	}
	p.mu.Lock()
	var b *batch.RecordBatch
	for i := len(p.idle) - 1; i >= 0; i-- {
		cand := p.idle[i]
		if !batchMatchesShape(cand, schema) {
			continue
		}
		p.idle = append(p.idle[:i], p.idle[i+1:]...)
		p.idleBytes -= cand.MemBytes()
		if p.idleBytes < 0 {
			p.idleBytes = 0
		}
		b = cand
		break
	}
	if b == nil {
		p.mu.Unlock()
		p.misses.Add(1)
		return nil
	}
	p.outstanding[b] = struct{}{}
	p.mu.Unlock()

	for _, c := range b.Columns {
		c.ResetForWrite(numRows)
	}
	b.Schema = schema
	b.Len = numRows
	b.Sel = nil
	p.hits.Add(1)
	return b
}

// track records a freshly minted backing as one of ours, so a later Recycle
// will take it. Called with the batch the decode is about to write into.
func (p *BackingPool) track(b *batch.RecordBatch, schema []pqt.Column) {
	if p == nil || b == nil || !scanBackingReuse.On() || !reusableShape(schema) {
		return
	}
	p.mu.Lock()
	// Never grow the mint registry past what the caps could ever hold plus
	// the live set the budgets allow; a pathological source that never
	// releases would otherwise accumulate entries for the source's lifetime.
	if len(p.outstanding) < 4*p.maxIdle+8 {
		p.outstanding[b] = struct{}{}
	}
	p.mu.Unlock()
}

// Recycle is the release edge: the consumer side is done with b. It is a
// no-op for a batch this pool did not mint. See the ownership rule on
// BackingPool — the claim check here is the retention veto.
func (p *BackingPool) Recycle(b *batch.RecordBatch) {
	if p == nil || b == nil {
		return
	}
	p.mu.Lock()
	if _, ours := p.outstanding[b]; !ours {
		p.mu.Unlock()
		return
	}
	delete(p.outstanding, b)
	p.mu.Unlock()

	if !scanBackingReuse.On() {
		return
	}
	// Retention veto: the batch itself, or any column (including one claimed
	// transitively through a derived batch or a downstream view's Base).
	if b.Retained() {
		p.claimed.Add(1)
		return
	}
	for _, c := range b.Columns {
		if c.Claimed() {
			p.claimed.Add(1)
			return
		}
	}
	if !flatOwnedColumns(b) {
		return
	}

	sz := b.MemBytes()
	p.mu.Lock()
	// The escape hatch: an empty idle list always accepts, whatever the size.
	if len(p.idle) >= p.maxIdle || (len(p.idle) > 0 && p.idleBytes+sz > p.maxIdleBytes) {
		p.mu.Unlock()
		return
	}
	p.idle = append(p.idle, b)
	p.idleBytes += sz
	p.mu.Unlock()
}

// reusableShape reports whether a read schema's columns are all flat owned
// types that Vector.ResetForWrite can reset in place. ROW is excluded: its
// children would need a recursive reset with its own correctness argument and
// ROW schemas are not what the SF100 profile names. ARRAY/MAP never reach the
// native reader at all (HasUnsupportedColumnarTypes routes them away).
func reusableShape(schema []pqt.Column) bool {
	if len(schema) == 0 {
		return false
	}
	for _, col := range schema {
		switch col.Type {
		case batch.TypeArray, batch.TypeMap, batch.TypeRow:
			return false
		case batch.TypeVector:
			if col.Dimension <= 0 {
				return false
			}
		}
	}
	return true
}

// batchMatchesShape reports whether an idle backing can be reset to serve
// schema. Type, DECIMAL scale and VECTOR dimension must all match: a reset
// does not re-derive them, and the decoded-chunk cache's copy path refuses a
// scale mismatch outright.
func batchMatchesShape(b *batch.RecordBatch, schema []pqt.Column) bool {
	if b == nil || len(b.Columns) != len(schema) {
		return false
	}
	for i, col := range schema {
		v := b.Columns[i]
		if v == nil || v.Base != nil || v.Type != col.Type {
			return false
		}
		switch col.Type {
		case batch.TypeDecimal:
			if v.DecimalData.Scale != col.Scale {
				return false
			}
		case batch.TypeVector:
			if v.VectorDim != col.Dimension {
				return false
			}
		}
	}
	return true
}

// flatOwnedColumns re-checks at release time that nothing downstream replaced
// a column with a view or a nested vector. The audited fragment op chains
// never touch in.Columns (docs/design/morsel-execution.md §4.1.1), so this
// never fires; a mismatch costs a missed reuse, not a wrong read.
func flatOwnedColumns(b *batch.RecordBatch) bool {
	for _, v := range b.Columns {
		if v == nil || v.Base != nil || v.Indices != nil {
			return false
		}
		switch v.Type {
		case batch.TypeArray, batch.TypeMap, batch.TypeRow:
			return false
		}
	}
	return true
}
