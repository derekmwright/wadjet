package physical

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// One object GET, many buffers.
//
// The whole-file read charges a scan's entire parquet file to the query's
// memory tracker and releases it only when the file's LAST row group has been
// decoded (ADR-0006 producer 1). While that charge is resident every other
// operator's admission is measured against a floor that has nothing to do with
// what the query is holding, and how far the scan ran ahead of its consumer
// decides whether the query answers or refuses (#789).
//
// The bytes are read the same way — ONE Get per file, so the request count the
// object store sees is unchanged, which is the recorded decision
// (docs/design/scan-pread-reads.md: "one object GET beats per-chunk ranged
// GETs") — but the body is landed into one buffer per ROW GROUP instead of one
// per file, using the byte ranges the footer already carries. Each buffer is
// charged when it lands and released when its row group has been decoded, so
// the scan's resident charge is the row groups actually in flight rather than
// the file.
//
// The read is DEMAND-DRIVEN and ADMITTED: the stream advances only when a
// decode worker asks for a row group the loader has not reached, and each row
// group's bytes are reserved before they are read — memory.ReserveOrForce,
// the same bounded-wait-then-force every other non-discretionary charge uses,
// so admission can neither fail a load nor deadlock. With room (or no budget)
// every reservation is clean and the read runs at full speed; without room the
// loader waits for the row group ahead of it to decode instead of piling the
// whole file onto the ledger. One row group is always admitted without waiting
// — the floor that stops a scan holding nothing from waiting on itself.
//
// It requires the file's footer to be decoded ALREADY (the process footer
// cache, populated by buildRGUnits' pruning pass): the row-group byte ranges
// live in it, and reading it from the object separately would be the second
// request this design exists to avoid. A file whose row-group metadata came
// from the catalog's persisted blob has no footer in that cache and keeps the
// whole-file path.

// ScanRowGroupBuffers is the kill switch for row-group-at-a-time file loads.
// Off, every scan takes the whole-file read this replaced.
var ScanRowGroupBuffers = optswitch.Register("scan-rg-buffers", "WADJET_SCAN_RG_BUFFERS",
	"land a scan's parquet file into one buffer per row group, charged and released per row group, instead of one whole-file buffer")

// Engagement counters. A row set cannot tell "read row group at a time" from
// "read whole file" — both answer identically, which is the point — so the
// gates assert the counter beside the rows.
var (
	rgLoadsRowGroup  atomic.Int64
	rgLoadsWholeFile atomic.Int64
)

// RowGroupLoadStats returns how many parquet file loads this process has done
// row group at a time and how many took the whole-file read, since start.
func RowGroupLoadStats() (rowGroup, wholeFile int64) {
	return rgLoadsRowGroup.Load(), rgLoadsWholeFile.Load()
}

// rgSlabs holds one parquet file's bytes one row group at a time and feeds
// them to a parquet.FileReader in row-group mode.
//
// Locking: loadMu serializes advancing the stream and is held across the
// admission wait; mu guards the residency maps, and is the ONLY lock a
// decoding worker's release takes — so a release can always proceed while
// another worker waits for the room that release is about to create.
type rgSlabs struct {
	inner  *scanSourceInner
	slot   *fileSlot
	ctx    context.Context
	want   []int   // row groups that survived pruning, ascending
	starts []int64 // byte range of want[i]
	ends   []int64

	loadMu  sync.Mutex
	body    io.ReadCloser
	pos     int64 // file offset the body is positioned at
	next    int   // index into want of the next row group to load
	loadErr error

	mu       sync.Mutex
	bufs     map[int][]byte
	bases    map[int]int64
	charges  map[int]int64
	released map[int]bool
	closed   bool
}

// rowGroupRanges is the ascending byte range of every row group in want, or
// ok=false when this file cannot be read row group at a time: a row group with
// no bytes, ranges not in ascending file order (validateChunkLayoutSorted's
// case — no writer in the corpus produces one), or a footer whose numbers the
// reader refuses. The caller then takes the whole-file read, which is always
// correct.
func rowGroupRanges(rdr *parquet.FileReader, want []int) (starts, ends []int64, ok bool) {
	starts = make([]int64, len(want))
	ends = make([]int64, len(want))
	var prevEnd int64
	for i, rg := range want {
		start, end, has, err := rdr.RowGroupByteRange(rg)
		if err != nil || !has || start < prevEnd {
			return nil, nil, false
		}
		starts[i], ends[i] = start, end
		prevEnd = end
	}
	return starts, ends, true
}

// RowGroupBytes implements parquet.RowGroupBytes: the buffer holding rgIdx's
// bytes and the file offset it starts at, advancing the stream when the loader
// has not reached rgIdx yet. Called once per projected column of the row
// group; only the first call does any work.
func (s *rgSlabs) RowGroupBytes(rgIdx int) ([]byte, int64, error) {
	if buf, base, ok, err := s.resident(rgIdx); ok || err != nil {
		return buf, base, err
	}
	// Advance the stream. One worker reads at a time; the others wait here
	// and re-check, because the row group they want may be the one that
	// worker just landed.
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	for {
		if buf, base, ok, err := s.resident(rgIdx); ok || err != nil {
			return buf, base, err
		}
		if s.loadErr != nil {
			return nil, 0, s.loadErr
		}
		if err := s.advance(); err != nil {
			s.loadErr = err
			return nil, 0, err
		}
	}
}

// resident reports rgIdx's buffer when it is held, or the reason it will never
// arrive. ok=false with a nil error means "not yet".
func (s *rgSlabs) resident(rgIdx int) (buf []byte, base int64, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, held := s.bufs[rgIdx]; held {
		return b, s.bases[rgIdx], true, nil
	}
	if s.closed {
		return nil, 0, false, fmt.Errorf("scan of %s was torn down before row group %d was read",
			s.slot.entry.Path, rgIdx)
	}
	if s.released[rgIdx] {
		// A released row group's buffer went back to the pool and may
		// already hold another file's bytes. Never hand it out again.
		return nil, 0, false, fmt.Errorf("row group %d of %s was already released",
			rgIdx, s.slot.entry.Path)
	}
	return nil, 0, false, nil
}

// advance loads exactly one more row group from the body. Caller holds loadMu
// and no other lock.
func (s *rgSlabs) advance() error {
	s.mu.Lock()
	if s.next >= len(s.want) {
		s.mu.Unlock()
		return fmt.Errorf("scan of %s asked for a row group the stream has already passed",
			s.slot.entry.Path)
	}
	i := s.next
	rg := s.want[i]
	skip := s.released[rg]
	s.mu.Unlock()

	start, end := s.starts[i], s.ends[i]
	if s.body == nil {
		rc, _, err := s.inner.cat.Store().Get(s.ctx, s.inner.cat.Bucket(), s.slot.entry.Path)
		if err != nil {
			return fmt.Errorf("get %s: %w", s.slot.entry.Path, err)
		}
		s.body = rc
		s.pos = 0
	}
	if start > s.pos {
		if _, err := io.CopyN(io.Discard, s.body, start-s.pos); err != nil {
			return fmt.Errorf("read %s: skipping to row group %d: %w", s.slot.entry.Path, rg, err)
		}
		s.pos = start
	}
	n := end - start
	if skip {
		// The decode path finished with this row group before its bytes were
		// ever demanded (a dictionary or filter prune). Step over it rather
		// than charging a buffer nobody will read.
		if _, err := io.CopyN(io.Discard, s.body, n); err != nil {
			return fmt.Errorf("read %s: skipping row group %d: %w", s.slot.entry.Path, rg, err)
		}
		s.pos = end
		s.mu.Lock()
		s.next++
		s.mu.Unlock()
		return nil
	}

	buf := getRowGroupBuf(int(n))
	// The bytes are about to become resident, so this is the same
	// non-discretionary charge the whole-file load makes — at row-group
	// granularity, and released at row-group granularity. cap(), not n: a
	// pooled buffer larger than the row group is resident memory too, which
	// is what the whole-file path's reconcile says. A scan holding nothing
	// never waits — the floor that makes this deadlock-free.
	charge := int64(cap(buf))
	wait := fileLoadReserveWait
	if s.inner.residentSlabs.Load() == 0 {
		wait = 0
	}
	memory.ReserveOrForce(s.ctx, s.inner.memTracker, s.inner.spillMgr, charge, wait, "scan row group load")

	if _, err := io.ReadFull(s.body, buf[:n]); err != nil {
		putReadBuf(buf)
		s.releaseCharge(charge)
		return fmt.Errorf("read %s row group %d: %w", s.slot.entry.Path, rg, err)
	}
	s.pos = end
	s.mu.Lock()
	s.bufs[rg] = buf[:n]
	s.bases[rg] = start
	s.charges[rg] = charge
	s.next++
	s.mu.Unlock()
	s.inner.residentSlabs.Add(1)
	return nil
}

func (s *rgSlabs) releaseCharge(n int64) {
	if n > 0 && s.inner.memTracker != nil {
		s.inner.memTracker.Release(n)
	}
}

// release frees row group rgIdx's buffer and its tracker charge. Idempotent,
// and safe to call for a row group whose bytes were never demanded.
func (s *rgSlabs) release(rgIdx int) {
	s.mu.Lock()
	if s.released[rgIdx] {
		s.mu.Unlock()
		return
	}
	s.released[rgIdx] = true
	buf, had := s.bufs[rgIdx]
	delete(s.bufs, rgIdx)
	delete(s.bases, rgIdx)
	n := s.charges[rgIdx]
	delete(s.charges, rgIdx)
	s.mu.Unlock()

	if had {
		putReadBuf(buf)
		s.inner.residentSlabs.Add(-1)
	}
	s.releaseCharge(n)
}

// close releases every buffer and charge still held and closes the body. Safe
// to call more than once. Callers must ensure no decode is still READING a
// buffer (the rg workers have exited).
//
// It takes loadMu first and holds it throughout, which is the same order
// RowGroupBytes takes (loadMu, then mu) and is what makes it safe against a
// loader mid-advance: a slab published into the maps after they were emptied
// would be a buffer and a charge nobody ever releases.
func (s *rgSlabs) close() {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	bufs, charges := s.bufs, s.charges
	s.bufs, s.bases, s.charges = map[int][]byte{}, map[int]int64{}, map[int]int64{}
	s.mu.Unlock()

	var total int64
	for rg, buf := range bufs {
		putReadBuf(buf)
		s.inner.residentSlabs.Add(-1)
		total += charges[rg]
	}
	s.releaseCharge(total)

	if s.body != nil {
		s.body.Close()
		s.body = nil
	}
}

// getRowGroupBuf is getReadBuf with an upper bound on what it will accept from
// the pool. readBufPool is a single class with no ceiling — it holds whatever
// the largest file read put back, which at SF100 is hundreds of megabytes — so
// an unbounded Get would hand a 100 KiB row group a 283 MB buffer. That buffer
// is resident memory and cap() is what this path charges for, so the budget
// would see a row group as large as the largest file the process ever read.
func getRowGroupBuf(size int) []byte {
	if v := readBufPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size && cap(buf) <= 2*size+64*1024 {
			return buf[:size]
		}
		readBufPool.Put(buf)
	}
	return make([]byte, size)
}

// tryRowGroupLoad builds the row-group-at-a-time backing for this slot when
// everything it needs is already in hand: a decoded footer for this exact
// object and row groups whose byte ranges are usable. Returns nil when the
// caller should take the whole-file path instead. It installs nothing on the
// slot — the caller does that once it has the load gate's admission.
func (s *fileSlot) tryRowGroupLoad(inner *scanSourceInner, ctx context.Context) (*parquet.FileReader, *rgSlabs) {
	if !ScanRowGroupBuffers.On() || len(s.wantRG) == 0 || s.entry.SizeBytes <= 0 || inner.cat == nil {
		return nil, nil
	}
	// The footer must already be decoded: reading it from the object here
	// would be a second request per file, which is the decision this design
	// keeps.
	//
	// The identity carries the SIZE, and every site that populates the cache
	// keys it by the authoritative size (the one GetReaderAt or the body
	// itself reports), never by the manifest's hint. So a hit here means the
	// manifest's SizeBytes and the object agree, which is what the byte ranges
	// below are measured against; a stale manifest entry misses and takes the
	// whole-file read, which handles a wrong size explicitly.
	rdr := parquet.LookupFooter(footerCacheIdentity(inner.cat, s.entry, s.entry.SizeBytes))
	if rdr == nil {
		return nil, nil
	}
	rdr.SetRowGroupBytes(nil, s.entry.SizeBytes) // size first: the ranges are measured against it
	starts, ends, ok := rowGroupRanges(rdr, s.wantRG)
	if !ok {
		return nil, nil
	}
	slabs := &rgSlabs{
		inner: inner, slot: s, ctx: ctx,
		want: s.wantRG, starts: starts, ends: ends,
		bufs:     make(map[int][]byte, len(s.wantRG)),
		bases:    make(map[int]int64, len(s.wantRG)),
		charges:  make(map[int]int64, len(s.wantRG)),
		released: make(map[int]bool, len(s.wantRG)),
	}
	rdr.SetRowGroupBytes(slabs, s.entry.SizeBytes)
	return rdr, slabs
}

// peakRowGroupBytes is the largest row group this slot can hold at once — what
// the byte-budgeted load gate is admitting when the slot reads a row group at
// a time instead of the whole file.
func peakRowGroupBytes(starts, ends []int64) int64 {
	var peak int64
	for i := range starts {
		if n := ends[i] - starts[i]; n > peak {
			peak = n
		}
	}
	return peak
}

var _ parquet.RowGroupBytes = (*rgSlabs)(nil)
